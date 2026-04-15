"""Main Annotation Pipeline Orchestrator"""
print("[*] Starting Data Annotation Pipeline")
import json
import os
import cv2
import numpy as np
import pandas as pd
import requests
from ConfigManager import ConfigManager
from S3Manager import S3Manager
from ImageProcessor import ImageProcessor
from GeospatialAnalyzer import GeospatialAnalyzer
from DefectAnalyzer import DefectAnalyzer
from ReportGenerator import ReportGenerator
from VideoGenerator import VideoGenerator
from Gemini_Inference import GeminiInference


class NpEncoder(json.JSONEncoder):
    """Custom JSON encoder for numpy types"""
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


class AnnotationPipeline:
    """Main orchestrator for the annotation pipeline"""

    def __init__(self):
        print("[*] Initializing Annotation Pipeline")
        self.config = ConfigManager()
        self.s3_manager = S3Manager(self.config)
        self.image_processor = ImageProcessor(self.config)
        self.geo_analyzer = GeospatialAnalyzer(self.config)
        self.defect_analyzer = DefectAnalyzer(self.config)
        self.report_generator = ReportGenerator(self.config)
        self.video_generator = VideoGenerator(self.config)

        # Conditionally initialize Gemini inference
        if self.config.enable_gemini_inference:
            print("[*] Gemini inference is ENABLED")
            self.gemini_inference = GeminiInference(config=self.config)
        else:
            print("[*] Gemini inference is DISABLED")
            self.gemini_inference = None

        # Create working directories
        self.config.create_directory(self.config.working_directory)
        self.config.create_directory(self.config.og_image_directory)

        # Initialize data containers
        self.data_merge = None
        self.data = None
        self.frame_data = None
        self.codebuild_id = None
        self.inference_id = None

    def load_result_data(self):
        """Load result.json from S3 or local"""
        result_consolidated = "./result.json"

        if 'S3_KEY' in os.environ.keys():
            self.s3_manager.download_file(
                self.config.upload_bucket,
                os.environ["S3_KEY"],
                f"{self.config.working_directory}/result.json"
            )
            result_consolidated = f"{self.config.working_directory}/result.json"

        with open(result_consolidated) as merge_file:
            self.data_merge = json.loads(merge_file.read())

        self.data = self.data_merge[0]
        return result_consolidated

    def fetch_inference_data(self):
        """Fetch inference data from API"""
        # Extract CODEBUILD_BUILD_ID from images
        for image in self.data["images"]:
            if self.codebuild_id == "" or self.codebuild_id is None:
                self.codebuild_id = image["file_name"].split("/")[4]
                print(f"CODEBUILD_BUILD_ID: {self.codebuild_id}")
                break

        # Fetch inference data from API
        result = requests.post(
            self.config.base_url + "webApi/project/get_inference_from_codebuildid",
            {"codebuildid": self.codebuild_id, "secret_key_value": self.config.secret_key}
        )

        print(result)

        if json.loads(result.text)["status"] == "success":
            response_data = json.loads(result.text)["result"]
            self.frame_data = response_data["inferences"]["data"]["frame_list_data"]
            self.inference_id = response_data["inferences"]["_id"]["$oid"]
        else:
            print("Codebuild Id Not Found Error, try again")
            exit()

        return result

    def process_defects(self):
        """Process defect annotations and generate reports"""
        print("[*] Processing defects...")

        # Create DataFrame from frame data
        df = pd.DataFrame(self.frame_data)

        # Calculate distances and chainage
        road_length, distances = self.geo_analyzer.calculate_total_distance(self.frame_data)
        road_length = round(road_length, 2)
        df = self.geo_analyzer.calculate_chainage(df, distances)

        # Build annotations dictionary
        annotations = {}
        for image in self.data['images']:
            file_name = image['file_name'].split('/')[-1]
            image_id = image['id']
            image_annotations = []
            for annotation in self.data['annotations']:
                if annotation['image_id'] == image_id:
                    image_annotations.append(annotation)
            annotations[file_name] = image_annotations

        # Add annotations to DataFrame
        annotation_data = []
        for og_file in df['og_file']:
            file_name = og_file.split('/')[-1]
            annotation = annotations.get(file_name, None)
            annotation_data.append(annotation)
        df['annotation'] = annotation_data

        # Process defect types
        df = self.defect_analyzer.process_defect_types(df, annotations)

        # Calculate percentage columns for each defect type
        for defect_type in self.config.categories_mapping.keys():
            df[defect_type + '%'] = (df[defect_type] / 1000)

        # Group data by chainage
        grouped = df.groupby('chainage')

        df_grouped = grouped.agg({
            'latitude': ['first', 'last'],
            'longitude': ['first', 'last'],
        }).reset_index()

        df_grouped.columns = ['chainage', 'start_latitude', 'end_latitude', 'start_longitude', 'end_longitude']

        # Handle severity labels
        for defect_type in self.config.categories_mapping.keys():
            severity_column = defect_type + '_Severity'
            if severity_column in df.columns:
                df_grouped[severity_column] = grouped[severity_column].agg(
                    lambda x: max(x, key=lambda v: self.config.severity_order[v])
                )

        # Count defects for each chainage
        for defect_type in self.config.categories_mapping.keys():
            count_column = defect_type + '_Count'
            if count_column in df.columns:
                df_grouped[count_column] = grouped[count_column].sum()

        # Calculate mean of percentage columns
        for defect_type in self.config.categories_mapping.keys():
            percentage_column = defect_type + '%'
            if percentage_column in df.columns:
                df_grouped[percentage_column] = grouped[percentage_column].mean()

        # Rearrange columns
        column_order = ['chainage', 'start_latitude', 'start_longitude', 'end_latitude', 'end_longitude']
        for defect_type in self.config.categories_mapping.keys():
            column_order.extend([defect_type + '_Count', defect_type + '_Severity', defect_type + '%'])

        df_grouped = df_grouped[column_order]

        # Calculate IRC ratings
        irc_ratings = self.defect_analyzer.calculate_irc_ratings_for_dataframe(df)
        df['IRC_rating'] = irc_ratings

        # Calculate PCI
        df_grouped['PCI'] = df_grouped.apply(self.defect_analyzer.calculate_pci, axis=1)

        road_rating = round(df['IRC_rating'].mean(), 2)

        # Add addresses
        df_grouped = self.report_generator.generate_chainage_report(df_grouped, self.geo_analyzer)

        # Save chainage report
        df_grouped.to_csv("chainage_report.csv", index=False)

        # Prepare final report
        df_grouped = self.report_generator.prepare_final_chainage_report(df_grouped)

        # Update severity columns
        df_grouped['Patching_Severity'] = df_grouped.apply(
            lambda row: 'medium' if row['Patching_Count'] > 0 else row['Patching_Severity'], axis=1
        )
        df_grouped['Settlements_Severity'] = df_grouped.apply(
            lambda row: 'high' if row['Settlements_Count'] > 0 else row['Settlements_Severity'], axis=1
        )
        df_grouped['Shoving_Severity'] = df_grouped.apply(
            lambda row: 'high' if row['Shoving_Count'] > 0 else row['Shoving_Severity'], axis=1
        )

        # Save final report
        if 'IRC_rating' in df.columns:
            df.drop(columns=['IRC_rating'], inplace=True)

        df_grouped.to_csv('chainage_report.csv', index=False)

        # Prepare serial report
        df_serial = self.report_generator.prepare_serial_report('chainage_report.csv')

        print("IRC CSV Report Generated!")

        return df, df_grouped, df_serial, road_length, road_rating

    def process_images(self, df):
        """Download images and process annotations"""
        print("[*] Downloading and processing images...")

        # Download all images
        self.s3_manager.download_all_images(self.frame_data, self.codebuild_id)

        # Process defect annotations
        for image_name in self.data['images']:
            file_name = image_name['file_name']
            local_file_name = f"{self.config.working_directory}/{file_name.split('/')[-1]}"
            image_frame_index = file_name.split("/")[-1].split(".")[0].split("_")[-1]

            if not os.path.exists(local_file_name):
                continue

            # Find annotations for this image
            image_annotations = []
            for annotation in self.data["annotations"]:
                if annotation["image_id"] == image_name['id']:
                    image_annotations.append(annotation)

            # Run Gemini inference if enabled
            if self.gemini_inference is not None:
                try:
                    gemini_detections = self.gemini_inference.detect_defects(local_file_name)
                    # Add Gemini detections to annotations
                    for detection in gemini_detections:
                        # Convert Gemini detection format to annotation format
                        x_min = detection.get('x_min', 0)
                        y_min = detection.get('y_min', 0)
                        x_max = detection.get('x_max', 0)
                        y_max = detection.get('y_max', 0)
                        width = x_max - x_min
                        height = y_max - y_min

                        gemini_annotation = {
                            "image_id": image_name['id'],
                            "category_id": detection.get('category_id'),
                            "bbox": [x_min, y_min, width, height]
                        }
                        image_annotations.append(gemini_annotation)
                except Exception as e:
                    print(f"⚠️ Gemini inference failed for {local_file_name}: {e}")

            if image_annotations:
                final_severity = self.image_processor.process_defect_annotations(
                    local_file_name,
                    image_annotations,
                    self.config.category_information
                )
                self.frame_data[int(image_frame_index)]["defect_state"] = final_severity

                # Store inference info
                temp_inference_data = []
                for annotation in image_annotations:
                    label_full = self.config.category_information[annotation["category_id"]]
                    severity = label_full.split('_')[-1] if '_' in label_full else "high"
                    temp_inference_data.append({
                        "label": str(label_full.split('_')[0]),
                        "bbox": annotation["bbox"],
                        "severity": severity
                    })
                self.frame_data[int(image_frame_index)]["inference_info"] = temp_inference_data

        print("[✓] Defect images processed successfully")
    def generate_road_distress_report(self, result_consolidated, df):
        """Generate road distress report with homography calculations"""
        # Homography setup for area calculation
        image_points = np.array([
            [700, 100],
            [700, 850],
            [520, 300],
            [720, 400]
        ], dtype=np.float32)

        real_world_points = np.array([
            [0, 0],
            [0, 3.5],
            [1, 0],
            [1, 3.5]
        ], dtype=np.float32)

        H, _ = cv2.findHomography(image_points, real_world_points)

        def calculate_real_world_area(bbox, scaling_factor=300.764):
            """Calculate real-world area from bounding box"""
            x_min, y_min, width, height = bbox
            x_max = x_min + width
            y_max = y_min + height

            bounding_box = [
                [x_min, y_min],
                [x_max, y_min],
                [x_max, y_max],
                [x_min, y_max]
            ]

            def image_to_real_world(x, y):
                point = np.array([x, y, 1]).reshape((3, 1))
                transformed_point = np.dot(H, point)
                transformed_point = transformed_point / transformed_point[2]
                return transformed_point[0][0], transformed_point[1][0]

            real_world_bounding_box = [image_to_real_world(x, y) for x, y in bounding_box]
            width = np.linalg.norm(np.array(real_world_bounding_box[0]) - np.array(real_world_bounding_box[1]))
            height = np.linalg.norm(np.array(real_world_bounding_box[0]) - np.array(real_world_bounding_box[3]))

            return width * height * scaling_factor

        # Create safety DataFrame
        safety_df = pd.DataFrame(self.frame_data)[['og_file', 'latitude', 'longitude']]
        safety_df.to_csv("saf.csv", index=False)

        # Load result data
        with open(result_consolidated, 'r') as file:
            json_content = json.load(file)

        json_dict = json_content[0]

        # Create mappings
        image_id_to_file_name = {image['id']: image['file_name'] for image in json_dict['images']}
        category_id_to_name = {category['id']: category['name'] for category in json_dict['categories']}

        # Process annotations
        image_id_to_annotations = {}
        for annotation in json_dict['annotations']:
            image_id = annotation['image_id']
            category_id = annotation['category_id']
            bbox = annotation['bbox']
            area = calculate_real_world_area(bbox)

            if image_id not in image_id_to_annotations:
                image_id_to_annotations[image_id] = []
            image_id_to_annotations[image_id].append((category_id, area))

        # Build rows
        rows = []
        for image_id, anns in image_id_to_annotations.items():
            file_name = image_id_to_file_name[image_id]
            road_distress = []
            areas = []

            for i, (category_id, area) in enumerate(anns):
                try:
                    category_name = category_id_to_name[category_id]
                    road_distress.append(category_name)
                    areas.append(area)
                except KeyError:
                    continue

            row = {'File_name': file_name, 'Road Distress': ', '.join(road_distress)}
            for i, area in enumerate(areas):
                row[f'Area{i+1}'] = area
            rows.append(row)

        road_distress_df = pd.DataFrame(rows)

        if road_distress_df.empty:
            road_distress_df = pd.DataFrame(columns=['File_name', 'Road Distress'])

        road_distress_df.to_csv('road_distress.csv', index=False)

        # Merge with location data
        saf_df = pd.read_csv('saf.csv')
        road_distress_df['file_key'] = road_distress_df['File_name'].apply(lambda x: x.split('/')[-1])
        saf_df['file_key'] = saf_df['og_file'].apply(lambda x: x.split('/')[-1])

        merged_df = pd.merge(road_distress_df, saf_df, on='file_key', how='inner')

        # Select required columns
        columns_to_select = ['latitude', 'longitude', 'Road Distress']
        area_columns = [col for col in merged_df.columns if col.startswith('Area')]
        columns_to_select.extend(area_columns)
        columns_to_select.append('File_name')

        merged_df.rename(columns={'latitude': 'Latitude', 'longitude': 'Longitude'}, inplace=True)
        final_df_road_defect = merged_df[['Latitude', 'Longitude', 'Road Distress'] + area_columns + ['File_name']]
        final_df_road_defect['File_URL'] = ''

        final_df_road_defect.to_csv('initial_merged_road_distress_data.csv', index=False)

        # Upload to S3
        final_df_road_defect = self.s3_manager.upload_defect_images(final_df_road_defect, self.codebuild_id)

        # Remove File_name column
        final_df_road_defect.drop(columns=['File_name'], inplace=True)
        final_df_road_defect.to_csv('updated_merged_road_defect_data.csv', index=False)

        print("[✓] Road distress report generated successfully")
        return final_df_road_defect

    def run(self):
        """Main pipeline execution"""
        print("[*] Starting Data Annotation Pipeline")

        # Load result data
        result_consolidated = self.load_result_data()

        # Fetch inference data
        self.fetch_inference_data()

        # Process defects
        df, df_grouped, df_serial, road_length, road_rating = self.process_defects()

        # Process images
        self.process_images(df)

        # Calculate plot data
        plot_data = {}
        plot_data['pie_chart2'] = {}
        severity_counts, category_names = self.defect_analyzer.calculate_road_defect_percentage(self.data)
        plot_data['pie_chart2']['severity_counts'] = severity_counts
        plot_data['pie_chart2']['category_names'] = category_names

        data_submitted = self.report_generator.get_submission_date(self.data)

        # Calculate total defects
        defect_columns = [col for col in df_grouped.columns if 'Count' in col]
        total_defects = df_grouped[defect_columns].sum().sum()

        # Generate JSON output
        json_output = self.report_generator.convert_to_json(df_grouped, road_length, road_rating, total_defects)

        defect_details_df = pd.DataFrame(json_output["defectDetails"])
        defect_details_df["start"] = json_output["start"]
        defect_details_df["end"] = json_output["end"]
        defect_details_df["roadLength"] = json_output["roadLength"]
        defect_details_df["roadRating"] = json_output["roadRating"]
        defect_details_df["defect"] = json_output["defect"]

        # Process road distress report (report_3_key)
        print("[*] Generating road distress report (report_3_key)...")
        final_df_road_defect = self.generate_road_distress_report(result_consolidated, df)

        # Generate video
        print("\n[*] Starting video generation from annotated frames...")
        uid = self.video_generator.extract_uid_from_result(result_consolidated)
        if uid:
            video_path = self.video_generator.create_video_from_frames(uid, result_consolidated)
            if video_path and os.path.exists(video_path):
                self.s3_manager.upload_video(video_path, uid)

        # Prepare final update data
        updated_frame_data = {
            "frame_list_data": self.frame_data,
            "category_information": self.config.category_information,
            "CODEBUILD_BUILD_ID": self.codebuild_id,
            "NEW_CODEBUILD_BUILD_ID": os.environ.get('CODEBUILD_BUILD_ID', '').split(":")[-1],
            "report_1_key": defect_details_df.to_csv(index=False),
            "report_2_key": df_grouped.to_csv(),
            "report_3_key": final_df_road_defect.to_csv(index=False),
            "road_length": road_length,
            "dashboard_df_csv": df_serial.to_csv(),
            "data_submitted": data_submitted,
            "road_rating": road_rating
        }

        print("Updated json data")
        print(json.dumps(updated_frame_data, cls=NpEncoder))

        # Update inference data via API
        data_object = {
            "updated_data": json.dumps(updated_frame_data, cls=NpEncoder),
            "show_inference": True,
            "inference_id": self.inference_id,
            "plot_data": json.dumps({'plots': plot_data}, cls=NpEncoder),
            "secret_key_value": self.config.secret_key
        }

        update_result = requests.post(self.config.base_url + "webApi/project/update_inference_data", data_object)

        if json.loads(update_result.text)["status"] == "success":
            print("Inferences Updated Successfully")

        print("[✓] Annotation Pipeline completed successfully!")


if __name__ == "__main__":
    pipeline = AnnotationPipeline()
    pipeline.run()
