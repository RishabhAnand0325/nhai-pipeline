"""Report Generator for creating CSV and JSON reports"""
import pandas as pd
import json
from datetime import datetime


class ReportGenerator:
    """Handles generation of various reports"""

    def __init__(self, config):
        self.config = config

    def convert_to_json(self, df, road_length_value, road_rating_value, defect_count_value):
        """Convert CSV data to specified JSON format"""
        json_data = {
            "start": df['start_address'][0],
            "end": df['end_address'].iloc[-1],
            "roadLength": road_length_value,
            "roadRating": road_rating_value,
            "defect": defect_count_value,
            "defectDetails": []
        }

        # Defect categories mapping
        defect_categories = {
            "Raveling": "Ravelling_Count",
            "Rut": "Rut Depth_Count",
            "Crack": "Cracking_Count",
            "Pothole": "Potholes_Count",
            "Shoving": "Shoving_Count",
            "Settlement": "Settlements_Count",
            "Patching": "Patching_Count",
        }

        severity_levels = ["Low", "Medium", "High"]

        # Calculate defect details
        for defect_name, column_name in defect_categories.items():
            for level in severity_levels:
                count = df[df[column_name.replace('Count', 'Severity')].str.lower() == level.lower()][column_name].sum()
                json_data["defectDetails"].append({
                    "name": defect_name,
                    "level": level,
                    "value": count
                })

        return json_data

    def get_submission_date(self, data):
        """Extract submission date from data"""
        try:
            date_str = data.get('info', {}).get('date_created', "")
            if date_str:
                # Handle both ISO 8601 ('2026-04-02T08:39:31.643051') and legacy ('2026-04-02 08:39:31.643051')
                date_str_normalized = date_str.replace('T', ' ')
                date_obj = datetime.strptime(date_str_normalized, "%Y-%m-%d %H:%M:%S.%f")
                return date_obj.strftime("%d-%m-%Y")
            else:
                return "Date not found."
        except Exception as e:
            print(f"Error parsing date: {e}")
            return "Date not found."

    def generate_chainage_report(self, df, geo_analyzer):
        """Generate chainage report with addresses"""
        # Add addresses
        df['start_address'] = df.apply(
            lambda row: geo_analyzer.get_address(row['start_latitude'], row['start_longitude']),
            axis=1
        )
        df['end_address'] = df.apply(
            lambda row: geo_analyzer.get_address(row['end_latitude'], row['end_longitude']),
            axis=1
        )

        return df

    def prepare_final_chainage_report(self, df):
        """Prepare final chainage report with proper formatting"""
        # Rename and format chainage column
        df = df.rename(columns={'chainage': 'Chainage'})
        df['Chainage'] = range(0, 10 * len(df), 10)

        # Update severity columns based on count columns
        df['Patching_Severity'] = df.apply(
            lambda row: 'medium' if row['Patching_Count'] > 0 else row['Patching_Severity'],
            axis=1
        )
        df['Settlements_Severity'] = df.apply(
            lambda row: 'high' if row['Settlements_Count'] > 0 else row['Settlements_Severity'],
            axis=1
        )
        df['Shoving_Severity'] = df.apply(
            lambda row: 'high' if row['Shoving_Count'] > 0 else row['Shoving_Severity'],
            axis=1
        )

        # Rearrange columns
        columns_order = [
            'Chainage', 'start_address', 'start_latitude', 'start_longitude',
            'end_address', 'end_latitude', 'end_longitude',
            'Potholes_Count', 'Potholes_Severity', 'Patching_Count', 'Patching_Severity',
            'Cracking_Count', 'Cracking_Severity', 'Ravelling_Count', 'Ravelling_Severity',
            'Rut Depth_Count', 'Rut Depth_Severity', 'Settlements_Count', 'Settlements_Severity',
            'Shoving_Count', 'Shoving_Severity', 'PCI'
        ]

        df = df[columns_order]
        df['PCI'] = df['PCI'].round(2)

        return df

    def prepare_serial_report(self, file_path):
        """Add serial numbers and format the report"""
        df_serial = pd.read_csv(file_path)

        # Add serial number column
        df_serial.insert(0, 'Serial Number', range(1, len(df_serial) + 1))

        # Rename columns
        df_serial.rename(
            columns={'Rut Depth_Count': 'Rut_Depth_Count', 'Rut Depth_Severity': 'Rut_Depth_Severity'},
            inplace=True
        )

        # Replace commas in addresses
        df_serial['start_address'] = df_serial['start_address'].str.replace(',', ';').str.replace(';', '|')
        df_serial['end_address'] = df_serial['end_address'].str.replace(',', ';').str.replace(';', '|')

        # Replace 'none' with 'Nill' in severity columns
        severity_columns = [col for col in df_serial.columns if 'Severity' in col]
        for col in severity_columns:
            df_serial[col] = df_serial[col].replace('none', 'Nill')

        return df_serial

    def create_road_distress_report(self, result_data, saf_df, image_to_real_world_fn):
        """Create road distress report with homography calculations"""
        # Create image ID to file name mapping
        image_id_to_file_name = {image['id']: image['file_name'] for image in result_data['images']}
        category_id_to_name = {category['id']: category['name'] for category in result_data['categories']}

        # Process annotations
        image_id_to_annotations = {}
        for annotation in result_data['annotations']:
            image_id = annotation['image_id']
            category_id = annotation['category_id']
            bbox = annotation['bbox']
            area = image_to_real_world_fn(bbox)

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

        df = pd.DataFrame(rows)

        # Merge with location data
        if not df.empty:
            df['file_key'] = df['File_name'].apply(lambda x: x.split('/')[-1])
            saf_df['file_key'] = saf_df['og_file'].apply(lambda x: x.split('/')[-1])
            merged_df = pd.merge(df, saf_df, on='file_key', how='inner')

            # Select required columns
            columns_to_select = ['latitude', 'longitude', 'Road Distress']
            area_columns = [col for col in merged_df.columns if col.startswith('Area')]
            columns_to_select.extend(area_columns)
            columns_to_select.append('File_name')

            merged_df.rename(columns={'latitude': 'Latitude', 'longitude': 'Longitude'}, inplace=True)
            final_df = merged_df[['Latitude', 'Longitude', 'Road Distress'] + area_columns + ['File_name']]
            final_df['File_URL'] = ''

            return final_df

        return pd.DataFrame(columns=['Latitude', 'Longitude', 'Road Distress', 'File_URL'])

    def create_road_asset_report(self, result_data, saf_df):
        """Create road asset report"""
        if len(result_data) < 2:
            return pd.DataFrame(columns=['latitude', 'longitude', 'Road Asset', 'File_URL'])

        json_dict = result_data[1]

        # Create mappings
        img_id_to_file_name = {image['id']: image['file_name'] for image in json_dict['images']}
        cat_id_to_name = {category['id']: category['name'] for category in json_dict['categories']}

        # Process annotations
        csv_rows = []
        images_with_annotations = set()

        for annot in json_dict['annotations']:
            img_id = annot['image_id']
            cat_id = annot['category_id']
            file_name = img_id_to_file_name[img_id]
            cat_name = cat_id_to_name[cat_id]

            if img_id not in images_with_annotations:
                csv_rows.append({'File_name': file_name, 'Road Asset': cat_name})
                images_with_annotations.add(img_id)
            else:
                for row in csv_rows:
                    if row['File_name'] == file_name:
                        row['Road Asset'] += f", {cat_name}"
                        break

        df = pd.DataFrame(csv_rows)

        if not df.empty:
            # Merge with location data
            df['file_key'] = df['File_name'].apply(lambda x: x.split('/')[-1])
            saf_df['file_key'] = saf_df['og_file'].apply(lambda x: x.split('/')[-1])
            merged_df = pd.merge(df, saf_df, on='file_key', how='inner')

            final_df = merged_df[['latitude', 'longitude', 'Road Asset', 'File_name']]
            final_df['File_URL'] = ''

            return final_df

        return pd.DataFrame(columns=['latitude', 'longitude', 'Road Asset', 'File_URL'])
