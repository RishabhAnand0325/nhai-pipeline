import os
import sys

# =========================================================================
# PATH CONFIGURATION (Must be at the very top of the file)
# =========================================================================
current_dir = os.getcwd()
sys.path.insert(0, current_dir)
# Insert bak first, then annotation-pipeline — so annotation-pipeline ends up at position 0 (highest priority)
sys.path.insert(0, os.path.join(current_dir, 'annotation-pipeline-bak'))
sys.path.insert(0, os.path.join(current_dir, 'annotation-pipeline'))

import json
import pandas as pd
import requests
import cv2
import numpy as np
import shutil
from datetime import datetime
from pymongo import MongoClient

# Infrastructure classes from annotation-pipeline-bak
try:
    from ConfigManager import ConfigManager
    from S3Manager import S3Manager
    from ImageProcessor import ImageProcessor
    from VideoGenerator import VideoGenerator
except ImportError as e:
    print(f"❌ Failed to import infrastructure modules from annotation-pipeline-bak: {e}")
    raise e

# Processing functions from annotation-pipeline
try:
    from chainage import (
        process_defect_types,
        calculate_total_distance,
        calculate_chainage,
        calculate_irc_rating_for_chunk,
        calculate_pci,
        get_address,
        _sanitize_label_to_key,
        initialize_mappings
    )
    from dashboard import convert_to_json
except ImportError as e:
    print(f"❌ Failed to import processing functions from annotation-pipeline: {e}")
    print(f"DEBUG: sys.path is {sys.path}")
    raise e

# Import Pipeline 1 from the root
from pipeline1 import RoadvisionPipeline


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer): return int(obj)
        if isinstance(obj, np.floating): return float(obj)
        if isinstance(obj, np.ndarray): return obj.tolist()
        return super(NpEncoder, self).default(obj)


class AnnotationPipeline:
    def __init__(self, frame_data, json_pavement, codebuild_id):
        print("[*] Initializing Annotation Pipeline (Local VM + GPU)")
        self.config = ConfigManager()
        self.s3_manager = S3Manager(self.config)
        self.image_processor = ImageProcessor(self.config)
        self.video_generator = VideoGenerator(self.config)

        # Build category mappings dynamically from the actual model output in json_pavement
        # This ensures alignment with pipeline1's Vertex AI model (54 categories: Crack=0, pothole=38, rutting=41, etc.)
        # rather than hardcoding the 66-NHAI schema that doesn't match the deployed model
        self.category_information = {cat['id']: cat['name'] for cat in json_pavement.get('categories', [])}
        self.categories_mapping = {cat['name']: [str(cat['id'])] for cat in json_pavement.get('categories', [])}
        self.severity_order = {'high': 3, 'medium': 2, 'low': 1, 'none': 0}

        self.config.create_directory(self.config.working_directory)
        self.config.create_directory(self.config.og_image_directory)

        self.data_merge = [json_pavement]
        self.data = json_pavement
        self.frame_data = frame_data
        self.codebuild_id = codebuild_id
        self.inference_id = None

        # Override base_url to the active GCP backend
        self.config.base_url = "https://roadvision-backend-new-505717192876.asia-south1.run.app/"

        # MongoDB connection
        self.mongo_uri = os.environ.get(
            "MONGO_URI",
            "mongodb+srv://tech_db_user:IK96qWD8AvtbpOHe@cluster0.nm6pkfg.mongodb.net/roadvision?retryWrites=true&w=majority"
        )
        self.mongo_client = MongoClient(self.mongo_uri)
        self.mongo_db = self.mongo_client["roadvision"]

    def fetch_inference_id_only(self):
        print("[*] Fetching inference_id from API...")
        try:
            result = requests.post(
                self.config.base_url + "webApi/project/get_inference_from_codebuildid",
                {"codebuildid": self.codebuild_id, "secret_key_value": self.config.secret_key}
            )
            if json.loads(result.text)["status"] == "success":
                response_data = json.loads(result.text)["result"]
                self.inference_id = response_data["inferences"]["_id"]["$oid"]
                print(f"[✓] Got inference_id: {self.inference_id}")
        except: pass
        return None

    def get_submission_date(self):
        """Extract submission date from json_pavement data"""
        try:
            date_str = self.data.get('info', {}).get('date_created', "")
            if date_str:
                date_str_normalized = date_str.replace('T', ' ')
                date_obj = datetime.strptime(date_str_normalized, "%Y-%m-%d %H:%M:%S.%f")
                return date_obj.strftime("%d-%m-%Y")
            else:
                return "Date not found."
        except Exception as e:
            print(f"Error parsing date: {e}")
            return "Date not found."

    def calculate_road_defect_percentage(self):
        """Calculate percentage of defected and non-defected roads for pie chart"""
        annotated_image_ids = {ann['image_id'] for ann in self.data['annotations']}
        total_images = len(self.data['images'])
        num_defected = len(annotated_image_ids)
        num_not_defected = total_images - num_defected

        severity_counts = {'High': 0, 'Medium': 0, 'Low': 0}
        for ann in self.data['annotations']:
            cat_id = ann['category_id']
            if cat_id in [0, 1, 2]:   # Potholes, Cracking, Rutting → High
                severity_counts['High'] += 1
            else:
                severity_counts['Low'] += 1

        total = num_not_defected + sum(severity_counts.values())
        if total == 0:
            return [100.0, 0.0, 0.0, 0.0], ['Not Defected', 'High Severity', 'Medium Severity', 'Low Severity']

        severity_counts_percent = [
            round(num_not_defected / total * 100, 2),
            round(severity_counts['High'] / total * 100, 2),
            round(severity_counts['Medium'] / total * 100, 2),
            round(severity_counts['Low'] / total * 100, 2),
        ]
        category_names = ['Not Defected', 'High Severity', 'Medium Severity', 'Low Severity']
        return severity_counts_percent, category_names

    def process_defects(self):
        """Process defects using annotation-pipeline functions (66 NHAI categories, 100m chainage)"""
        print("[*] Processing defects...")
        df = pd.DataFrame(self.frame_data)
        road_length, distances = calculate_total_distance(self.frame_data)
        road_length = round(road_length, 2)
        df = calculate_chainage(df, distances)

        # Build annotation lookup by filename
        annotations = {}
        for image in self.data['images']:
            file_name = image['file_name'].split('/')[-1]
            annotations[file_name] = [a for a in self.data['annotations'] if a['image_id'] == image['id']]

        df['annotation'] = [annotations.get(f.split('/')[-1], None) for f in df['og_file']]

        # Process defect types — creates both grouped and per-label sanitized columns
        df = process_defect_types(df, self.categories_mapping, self.category_information, self.severity_order)

        # Calculate percentage columns — build all at once to avoid fragmentation
        perc_dict = {
            defect_type + '%': (df[defect_type] / 1000)
            for defect_type in self.categories_mapping.keys()
            if defect_type in df.columns
        }
        df = pd.concat([df, pd.DataFrame(perc_dict, index=df.index)], axis=1).copy()

        # Group by chainage
        grouped = df.groupby('chainage')
        df_grouped = grouped.agg({'latitude': ['first', 'last'], 'longitude': ['first', 'last']}).reset_index()
        df_grouped.columns = ['chainage', 'start_latitude', 'end_latitude', 'start_longitude', 'end_longitude']

        # Aggregate grouped category columns (individual assignment — index-safe)
        for defect_type in self.categories_mapping.keys():
            sev_col = defect_type + '_Severity'
            if sev_col in df.columns:
                df_grouped[sev_col] = grouped[sev_col].agg(
                    lambda x: max(x, key=lambda v: self.severity_order.get(v, 0))
                ).values
            count_col = defect_type + '_Count'
            if count_col in df.columns:
                df_grouped[count_col] = grouped[count_col].sum().values
            perc_col = defect_type + '%'
            if perc_col in df.columns:
                df_grouped[perc_col] = grouped[perc_col].mean().values

        # IRC and PCI ratings
        df['IRC_rating'] = calculate_irc_rating_for_chunk(df)
        road_rating = round(df['IRC_rating'].mean(), 2)
        df_grouped['PCI'] = df_grouped.apply(calculate_pci, axis=1)

        # Reverse geocode start/end addresses in parallel
        import concurrent.futures as _cf
        coords_start = list(zip(df_grouped['start_latitude'], df_grouped['start_longitude']))
        coords_end   = list(zip(df_grouped['end_latitude'],   df_grouped['end_longitude']))
        all_coords   = coords_start + coords_end

        def _geocode(latlon):
            return get_address(self.config.gmaps_api_key, latlon[0], latlon[1])

        with _cf.ThreadPoolExecutor(max_workers=len(all_coords)) as pool:
            results = list(pool.map(_geocode, all_coords))

        n = len(coords_start)
        df_grouped['start_address'] = results[:n]
        df_grouped['end_address']   = results[n:]

        # Save intermediate chainage report
        df_grouped.to_csv("chainage_report.csv", index=False)

        # Finalize chainage report — rename column and use 100m intervals
        df_grouped = df_grouped.rename(columns={'chainage': 'Chainage'})
        df_grouped['Chainage'] = range(0, 100 * len(df_grouped), 100)
        df_grouped['PCI'] = df_grouped['PCI'].round(2)

        # Serial report: read CSV back, add serial numbers, clean up
        df_serial = pd.read_csv('chainage_report.csv')
        df_serial.insert(0, 'Serial Number', range(1, len(df_serial) + 1))
        df_serial['start_address'] = df_serial['start_address'].str.replace(',', ';').str.replace(';', '|')
        df_serial['end_address'] = df_serial['end_address'].str.replace(',', ';').str.replace(';', '|')
        severity_columns = [col for col in df_serial.columns if 'Severity' in col]
        for col in severity_columns:
            df_serial[col] = df_serial[col].replace('none', 'Nill')

        return df, df_grouped, df_serial, road_length, road_rating

    def process_images(self, df):
        """Draw bounding boxes on frames already local from Pipeline 1 (frame_data/predict/)"""
        print("[*] Processing images (using locally extracted frames from Pipeline 1)...")

        for image_name in self.data['images']:
            file_name = image_name['file_name']
            frame_filename = file_name.split('/')[-1]
            image_frame_index = frame_filename.split(".")[0].split("_")[-1]

            # Frames are already local from pipeline 1 — no GCS download needed
            local_file_name = f"frame_data/predict/{frame_filename}"

            if not os.path.exists(local_file_name):
                continue

            image_annotations = [
                a for a in self.data["annotations"] if a["image_id"] == image_name['id']
            ]

            if image_annotations:
                final_severity = self.image_processor.process_defect_annotations(
                    local_file_name,
                    image_annotations,
                    self.category_information  # 66 NHAI categories
                )
                try:
                    self.frame_data[int(image_frame_index)]["defect_state"] = final_severity
                    temp_inference_data = []
                    for annotation in image_annotations:
                        cat_id = annotation["category_id"]
                        cat_name = self.category_information.get(cat_id, f"unknown_{cat_id}")
                        severity = "high" if cat_id in [0, 1, 2] else "low"
                        temp_inference_data.append({
                            "label": cat_name,
                            "bbox": annotation["bbox"],
                            "severity": severity
                        })
                    self.frame_data[int(image_frame_index)]["inference_info"] = temp_inference_data
                except (IndexError, ValueError):
                    pass

        print("[✓] Images processed successfully")

    def generate_road_distress_report(self, result_consolidated, df):
        """Generate road distress report with real-world area calculations"""
        image_points = np.array([[700, 100], [700, 850], [520, 300], [720, 400]], dtype=np.float32)
        real_world_points = np.array([[0, 0], [0, 3.5], [1, 0], [1, 3.5]], dtype=np.float32)
        H, _ = cv2.findHomography(image_points, real_world_points)

        def calculate_real_world_area(bbox, scaling_factor=300.764):
            x_min, y_min, width, height = bbox
            x_max, y_max = x_min + width, y_min + height
            corners = [[x_min, y_min], [x_max, y_min], [x_max, y_max], [x_min, y_max]]
            def to_real(x, y):
                p = np.array([x, y, 1]).reshape((3, 1))
                t = np.dot(H, p); t /= t[2]
                return t[0][0], t[1][0]
            rw = [to_real(x, y) for x, y in corners]
            w = np.linalg.norm(np.array(rw[0]) - np.array(rw[1]))
            h = np.linalg.norm(np.array(rw[0]) - np.array(rw[3]))
            return w * h * scaling_factor

        safety_df = pd.DataFrame(self.frame_data)[['og_file', 'latitude', 'longitude']]
        safety_df.to_csv("saf.csv", index=False)

        with open(result_consolidated, 'r') as f:
            json_dict = json.load(f)[0]

        image_id_to_file_name = {img['id']: img['file_name'] for img in json_dict['images']}
        category_id_to_name = {cat['id']: cat['name'] for cat in json_dict['categories']}

        image_id_to_annotations = {}
        for ann in json_dict['annotations']:
            image_id_to_annotations.setdefault(ann['image_id'], []).append(
                (ann['category_id'], calculate_real_world_area(ann['bbox']))
            )

        rows = []
        for image_id, anns in image_id_to_annotations.items():
            file_name = image_id_to_file_name[image_id]
            road_distress, areas = [], []
            for cat_id, area in anns:
                if cat_id in category_id_to_name:
                    road_distress.append(category_id_to_name[cat_id])
                    areas.append(area)
            row = {'File_name': file_name, 'Road Distress': ', '.join(road_distress)}
            for i, area in enumerate(areas):
                row[f'Area{i+1}'] = area
            rows.append(row)

        road_distress_df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=['File_name', 'Road Distress'])
        road_distress_df.to_csv('road_distress.csv', index=False)

        saf_df = pd.read_csv('saf.csv')
        road_distress_df['file_key'] = road_distress_df['File_name'].apply(lambda x: x.split('/')[-1])
        saf_df['file_key'] = saf_df['og_file'].apply(lambda x: x.split('/')[-1])
        merged_df = pd.merge(road_distress_df, saf_df, on='file_key', how='inner')

        area_columns = [c for c in merged_df.columns if c.startswith('Area')]
        merged_df.rename(columns={'latitude': 'Latitude', 'longitude': 'Longitude'}, inplace=True)
        final_df = merged_df[['Latitude', 'Longitude', 'Road Distress'] + area_columns + ['File_name']].copy()
        final_df['File_URL'] = ''
        final_df.to_csv('initial_merged_road_distress_data.csv', index=False)

        final_df = self.s3_manager.upload_defect_images(final_df, self.codebuild_id)
        final_df.drop(columns=['File_name'], inplace=True)
        final_df.to_csv('updated_merged_road_defect_data.csv', index=False)

        print("[✓] Road distress report generated")
        return final_df

    def run(self):
        print("[*] Starting GCP Annotation Pipeline")

        # Get inference_id from API
        self.fetch_inference_id_only()

        result_consolidated = "result.json"

        # Step 1: Process defects → chainage/PCI/IRC reports
        df, df_grouped, df_serial, road_length, road_rating = self.process_defects()

        # Step 2: Draw bounding boxes on locally available frames from Pipeline 1
        self.process_images(df)

        # Step 2b: Upload annotated frames to GCS GenerateAnotatedImages/
        print("[*] Uploading annotated frames to GCS...")
        self.s3_manager.upload_frames_and_predict_folders(self.codebuild_id)

        # Step 3: Severity pie chart data
        plot_data = {'pie_chart2': {}}
        severity_counts, category_names = self.calculate_road_defect_percentage()
        plot_data['pie_chart2']['severity_counts'] = severity_counts
        plot_data['pie_chart2']['category_names'] = category_names

        data_submitted = self.get_submission_date()

        # Step 4: Total defect count — use only categories_mapping keys to avoid
        # double-counting with the sanitized per-label _Count columns added below
        defect_columns = [f"{k}_Count" for k in self.categories_mapping if f"{k}_Count" in df_grouped.columns]
        total_defects = int(df_grouped[defect_columns].sum().sum())

        # Step 5: JSON report using convert_to_json from annotation-pipeline/dashboard.py
        json_output = convert_to_json(
            df_grouped, road_length, road_rating, total_defects,
            category_labels=list(self.categories_mapping.keys())
        )
        defect_details_df = pd.DataFrame(json_output["defectDetails"]) if json_output["defectDetails"] else pd.DataFrame(columns=["name", "level", "value"])
        defect_details_df["start"] = json_output["start"]
        defect_details_df["end"] = json_output["end"]
        defect_details_df["roadLength"] = json_output["roadLength"]
        defect_details_df["roadRating"] = json_output["roadRating"]
        defect_details_df["defect"] = json_output["defect"]

        # Add unique_value = number of chainage groups where this defect appears
        # at least once (same value repeated across all 3 severity rows per defect)
        def _sanitize(label):
            key = label.strip()
            for ch in [' ', '/', '\\', '(', ')', '-', '&', ':', ',', ';']:
                key = key.replace(ch, '_')
            while '__' in key:
                key = key.replace('__', '_')
            return key.strip('_')

        unique_value_map = {}
        for defect_name in defect_details_df['name'].unique():
            count_col = f"{_sanitize(defect_name)}_Count"
            if count_col in df_grouped.columns:
                unique_value_map[defect_name] = int((df_grouped[count_col] > 0).sum())
            else:
                # Fall back to categories_mapping key
                for cat_key in self.categories_mapping:
                    if cat_key.lower() == defect_name.lower():
                        col = f"{cat_key}_Count"
                        if col in df_grouped.columns:
                            unique_value_map[defect_name] = int((df_grouped[col] > 0).sum())
                        break
                if defect_name not in unique_value_map:
                    unique_value_map[defect_name] = 0

        defect_details_df["unique_value"] = defect_details_df["name"].map(unique_value_map).fillna(0).astype(int)

        # Reorder columns to match expected format
        defect_details_df = defect_details_df[
            ["name", "level", "value", "start", "end", "roadLength", "roadRating", "defect", "unique_value"]
        ]

        # Step 6: Road distress report
        print("[*] Generating road distress report...")
        final_df_road_defect = self.generate_road_distress_report(result_consolidated, df)

        # Step 6b: Generate report_4_key (per-detection-instance CSV)
        print("[*] Generating report_4_key (per-detection report)...")
        try:
            # Lookup: image_id → full file_name path
            image_id_to_fname = {img['id']: img['file_name'] for img in self.data['images']}

            # Lookup: frame filename (basename) → File_URL
            # initial_merged_road_distress_data.csv still has File_name before drop
            try:
                _initial_df = pd.read_csv('initial_merged_road_distress_data.csv')
                # Align File_URL from final_df_road_defect (same row order, File_name already dropped there)
                _initial_df['File_URL'] = final_df_road_defect['File_URL'].values
                fname_to_url = dict(zip(
                    _initial_df['File_name'].apply(lambda x: str(x).split('/')[-1]),
                    _initial_df['File_URL']
                ))
            except Exception:
                fname_to_url = {}

            # Lookup: og_file basename → (chainage, latitude, longitude) from df
            _df_temp = df[['og_file', 'chainage', 'latitude', 'longitude']].copy()
            _df_temp['_key'] = _df_temp['og_file'].apply(lambda x: str(x).split('/')[-1])
            df_file_to_data = _df_temp.set_index('_key')[['chainage', 'latitude', 'longitude']].to_dict('index')

            report4_rows = []
            for s_no, ann in enumerate(self.data['annotations'], start=1):
                image_id = ann['image_id']
                file_name = image_id_to_fname.get(image_id, '')
                frame_filename = file_name.split('/')[-1]

                frame_info = df_file_to_data.get(frame_filename, {})
                lat = frame_info.get('latitude', '')
                lon = frame_info.get('longitude', '')
                ch_val = frame_info.get('chainage', None)

                if ch_val is not None:
                    ch_int = int(ch_val)
                    chainage_str = f"{ch_int}-{ch_int + 100}m"
                else:
                    chainage_str = ''

                cat_id = ann['category_id']
                cat_name = self.category_information.get(cat_id, f"unknown_{cat_id}")
                defect_img_url = fname_to_url.get(frame_filename, '')

                report4_rows.append({
                    'S_No': s_no,
                    'Reporting_Date': data_submitted,
                    'Asset_Type': 'Road Defect',
                    'Defect_Description': cat_name,
                    'Side': '',
                    'Chainage': chainage_str,
                    'Latitude': lat,
                    'Longitude': lon,
                    'Defect_Image': defect_img_url,
                    'NH_Number': '',
                    'Project_Name': '',
                    'UPC_Code': '',
                    'State': '',
                    'RO_Name': '',
                    'PIU_Name': '',
                    'Survey_Date': data_submitted,
                })

            report4_df = pd.DataFrame(report4_rows, columns=[
                'S_No', 'Reporting_Date', 'Asset_Type', 'Defect_Description', 'Side',
                'Chainage', 'Latitude', 'Longitude', 'Defect_Image',
                'NH_Number', 'Project_Name', 'UPC_Code', 'State', 'RO_Name', 'PIU_Name', 'Survey_Date'
            ])
            print(f"[✓] report_4_key generated: {len(report4_df)} detection rows")
        except Exception as e:
            print(f"[⚠] report_4_key generation failed: {e}")
            report4_df = pd.DataFrame(columns=[
                'S_No', 'Reporting_Date', 'Asset_Type', 'Defect_Description', 'Side',
                'Chainage', 'Latitude', 'Longitude', 'Defect_Image',
                'NH_Number', 'Project_Name', 'UPC_Code', 'State', 'RO_Name', 'PIU_Name', 'Survey_Date'
            ])

        # Step 7: Generate annotated video
        print("[*] Generating annotated video...")
        uid = self.video_generator.extract_uid_from_result(result_consolidated)
        if uid:
            video_path = self.video_generator.create_video_from_frames(uid, result_consolidated)
            if video_path and os.path.exists(video_path):
                self.s3_manager.upload_video(video_path, self.codebuild_id)

        # Step 8: Post final update to backend API
        updated_frame_data = {
            "frame_list_data": self.frame_data,
            "category_information": self.category_information,
            "CODEBUILD_BUILD_ID": self.codebuild_id,
            "NEW_CODEBUILD_BUILD_ID": self.codebuild_id,
            "report_1_key": defect_details_df.to_csv(index=False),
            "report_2_key": df_grouped.to_csv(),
            "report_3_key": final_df_road_defect.to_csv(index=False),
            "report_4_key": report4_df.to_csv(index=False),
            "road_length": road_length,
            "dashboard_df_csv": df_serial.to_csv(),
            "data_submitted": data_submitted,
            "road_rating": road_rating
        }

        data_object = {
            "updated_data": json.dumps(updated_frame_data, cls=NpEncoder),
            "show_inference": True,
            "inference_id": self.inference_id,
            "plot_data": json.dumps({'plots': plot_data}, cls=NpEncoder),
            "secret_key_value": self.config.secret_key
        }

        try:
            update_result = requests.post(
                self.config.base_url + "webApi/project/update_inference_data",
                data_object,
                timeout=60
            )
            resp = json.loads(update_result.text)
            if resp.get("status") == "success":
                print("[✓] Inferences updated successfully!")
            else:
                # Non-fatal: inference record may not exist when running standalone
                print(f"[⚠] API update skipped (inference not registered in app): {resp.get('result', update_result.text)}")
        except Exception as e:
            print(f"[⚠] API update failed (non-fatal): {e}")

        # Save all 4 reports as CSV files locally and upload to GCS
        print("[*] Saving reports to CSV and uploading to GCS...")
        road_id = os.environ.get("ROAD_ID", self.config.road_id)
        artifacts_base = f"processed-data/{road_id}/{self.codebuild_id}"

        # Save report CSVs locally
        defect_details_df.to_csv("report_1_defect_details.csv", index=False)
        df_grouped.to_csv("report_2_chainage_grouped.csv")
        final_df_road_defect.to_csv("report_3_road_distress.csv", index=False)
        report4_df.to_csv("report_4_per_detection.csv", index=False)
        print("[✓] Reports saved locally: report_1 through report_4")

        # Upload all artifacts to GCS
        artifacts = {
            "result.json": "application/json",
            "chainage_report.csv": "text/csv",
            "report_1_defect_details.csv": "text/csv",
            "report_2_chainage_grouped.csv": "text/csv",
            "report_3_road_distress.csv": "text/csv",
            "report_4_per_detection.csv": "text/csv",
        }
        for filename, content_type in artifacts.items():
            if os.path.exists(filename):
                self.s3_manager.upload_file(
                    filename,
                    self.config.artifacts_bucket,
                    f"{artifacts_base}/{filename}",
                    content_type=content_type
                )
                print(f"    ✅ {filename} → gs://{self.config.artifacts_bucket}/{artifacts_base}/{filename}")
            else:
                print(f"    ⚠ {filename} not found — skipping")
        print("[✓] All artifacts uploaded to GCS.")

        # Step 10: Save to MongoDB (annotation_segments + roads)
        self.save_to_mongodb(
            road_length=road_length,
            road_rating=road_rating,
            artifacts_base=artifacts_base,
            defect_details_df=defect_details_df,
            df_grouped=df_grouped,
            df_serial=df_serial,
            final_df_road_defect=final_df_road_defect,
            report4_df=report4_df,
            total_defects=total_defects,
            plot_data=plot_data,
            data_submitted=data_submitted,
        )

        print("✅ Annotation Pipeline completed!")

    def _load_road_metadata(self, road_id):
        """Load road_name and road_type from nhai_roads.json for the given road_id."""
        roads_json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nhai_roads.json")
        try:
            with open(roads_json_path, "r") as f:
                roads = json.load(f)
            for r in roads:
                if r.get("road_id") == road_id:
                    return r.get("road_name", ""), r.get("road_type", "highway")
        except Exception:
            pass
        return "", "highway"

    def save_to_mongodb(self, road_length, road_rating, artifacts_base,
                        defect_details_df, df_grouped, df_serial, final_df_road_defect,
                        report4_df, total_defects, plot_data, data_submitted):
        """Save pipeline results to MongoDB annotation_segments and roads collections."""
        print("[*] Saving results to MongoDB...")
        now = datetime.utcnow()
        road_id = os.environ.get("ROAD_ID", self.config.road_id)
        road_name, road_type = self._load_road_metadata(road_id)

        # road_length from calculate_total_distance() is in km
        total_length_km = round(road_length, 2)

        try:
            # ── 1. Build frame_list_data with GeoJSON location ──
            enriched_frames = []
            cumulative_km = 0.0
            for i, frame in enumerate(self.frame_data):
                lat = frame.get("latitude", 0)
                lon = frame.get("longitude", 0)

                if i > 0:
                    prev = self.frame_data[i - 1]
                    from math import radians, sin, cos, sqrt, atan2
                    R = 6371.0
                    p1, p2 = radians(prev.get("latitude", 0)), radians(lat)
                    dp = radians(lat - prev.get("latitude", 0))
                    dl = radians(lon - prev.get("longitude", 0))
                    a = sin(dp/2)**2 + cos(p1)*cos(p2)*sin(dl/2)**2
                    d = 2 * R * atan2(sqrt(a), sqrt(1 - a))
                    if d < 1.0:
                        cumulative_km += d

                enriched_frames.append({
                    "timeElapsed": frame.get("timeElapsed", 0),
                    "latitude": lat,
                    "longitude": lon,
                    "location": {"type": "Point", "coordinates": [lon, lat]},
                    "chainage_km": round(cumulative_km, 3),
                    "speed": frame.get("speed", 0),
                    "speedAccuracy": frame.get("speedAccuracy", 0),
                    "orientation": frame.get("orientation", "portraitUp"),
                    "og_file": frame.get("og_file", ""),
                    "inference_image": frame.get("og_file", "").replace("frames", "predict"),
                    "defect_state": frame.get("defect_state", "none"),
                    "inference_info": frame.get("inference_info", []),
                })

            # ── 2. Determine survey_no ──
            road_doc = self.mongo_db.roads.find_one({"road_id": road_id})
            if road_doc and road_doc.get("surveys"):
                survey_no = max(s.get("survey_no", 0) for s in road_doc["surveys"]) + 1
            else:
                survey_no = 1

            # ── 3. Build report keys as CSV strings (same format as API payload) ──
            report_1_csv = defect_details_df.to_csv(index=False)
            report_2_csv = df_grouped.to_csv()
            report_3_csv = final_df_road_defect.to_csv(index=False)
            report_4_csv = report4_df.to_csv(index=False)
            dashboard_csv = df_serial.to_csv()

            # ── 4. Upsert annotation_segments ──
            annotation_doc = {
                "uuid": self.codebuild_id,
                "road_id": road_id,
                "survey_no": survey_no,
                "total_frames": len(enriched_frames),
                "total_defects": total_defects,
                "road_length_km": total_length_km,
                "road_rating": road_rating,
                "data_submitted": data_submitted,
                "last_updated": now,
                "report_1_key": report_1_csv,
                "report_2_key": report_2_csv,
                "report_3_key": report_3_csv,
                "report_4_key": report_4_csv,
                "dashboard_df_csv": dashboard_csv,
                "severity_distribution": {
                    "counts": plot_data.get("pie_chart2", {}).get("severity_counts", []),
                    "labels": plot_data.get("pie_chart2", {}).get("category_names", []),
                },
                "category_information": {str(k): v for k, v in self.category_information.items()},
                "frame_list_data": enriched_frames,
            }

            self.mongo_db.annotation_segments.update_one(
                {"uuid": self.codebuild_id},
                {"$set": annotation_doc},
                upsert=True
            )
            print(f"    ✅ annotation_segments: uuid={self.codebuild_id}, road={road_id}, survey={survey_no}")
            print(f"       frames={len(enriched_frames)}, defects={total_defects}")

            # ── 5. Build full_route GeoJSON from unique GPS points ──
            seen = set()
            route_coords = []
            for f in enriched_frames:
                lat, lon = f["latitude"], f["longitude"]
                if abs(lat) < 0.001 and abs(lon) < 0.001:
                    continue
                key = (round(lon, 6), round(lat, 6))
                if key not in seen:
                    seen.add(key)
                    route_coords.append([lon, lat])

            full_route = {"type": "LineString", "coordinates": route_coords} if len(route_coords) >= 2 else {}

            # ── 6. Build survey entry for roads collection ──
            survey_entry = {
                "survey_no": survey_no,
                "survey_date": now,
                "surveyed_by": "pipeline",
                "total_length_km": total_length_km,
                "total_frames": len(enriched_frames),
                "road_rating": road_rating,
                "annotated_km": total_length_km,
                "annotation_pct": 100,
                "is_full_survey_confirmed": True,
                "created_at": now,
                "last_updated": now,
            }

            # ── 7. Upsert roads document ──
            if road_doc:
                update_set = {}
                if full_route:
                    update_set["full_route"] = full_route
                if road_name and not road_doc.get("road_name"):
                    update_set["road_name"] = road_name
                if not road_doc.get("road_type"):
                    update_set["road_type"] = road_type

                update_ops = {"$push": {"surveys": survey_entry}}
                if update_set:
                    update_ops["$set"] = update_set

                self.mongo_db.roads.update_one({"road_id": road_id}, update_ops)
                print(f"    ✅ roads: updated {road_id} — added survey #{survey_no} (length={total_length_km}km)")
            else:
                road_doc_new = {
                    "road_id": road_id,
                    "road_name": road_name,
                    "road_type": road_type,
                    "organization": "",
                    "created_at": now,
                    "full_route": full_route,
                    "surveys": [survey_entry],
                }
                self.mongo_db.roads.insert_one(road_doc_new)
                print(f"    ✅ roads: created {road_id} ({road_name}) — survey #{survey_no} (length={total_length_km}km, {len(route_coords)} GPS points)")

            print("[✓] MongoDB save complete.")

        except Exception as e:
            print(f"[⚠] MongoDB save failed (non-fatal): {e}")
            import traceback
            traceback.print_exc()


def main():
    print("\n[PIPELINE 1] Extraction & Local YOLO GPU Inference...")
    p1 = RoadvisionPipeline()
    p1.run()

    print("\n[PIPELINE 2] Processing Annotation & Reports...")
    p2 = AnnotationPipeline(p1.frame_data, p1.json_pavement, p1.CODEBUILD_ID)
    p2.run()
    print("\n✅ PROCESS COMPLETE!")


if __name__ == "__main__":
    main()
