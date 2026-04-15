"""Defect Analyzer for processing defect data and calculating ratings"""
import pandas as pd
import geopy.distance


class DefectAnalyzer:
    """Handles defect analysis, severity calculations, and ratings"""

    def __init__(self, config):
        self.config = config

    def severity_rank(self, severity):
        """Return numeric rank for severity"""
        return self.config.severity_order.get(severity, 0)

    def process_defect_types(self, df, annotations_dict):
        """Process defect types and update DataFrame with counts and severity"""
        # Initialize columns for defect types and counts
        for defect_type in self.config.categories_mapping.keys():
            if defect_type not in df.columns:
                df[defect_type] = 0
            if defect_type + '_Count' not in df.columns:
                df[defect_type + '_Count'] = 0
            if defect_type + '_Severity' not in df.columns:
                df[defect_type + '_Severity'] = 'none'

        # Process annotations
        for index, row in df.iterrows():
            og_file = row['og_file']
            file_name = og_file.split('/')[-1]
            annotations = annotations_dict.get(file_name, None)

            if annotations is not None:
                for annotation in annotations:
                    category_id = str(annotation['category_id'])
                    area = annotation['area']
                    category_name = self.config.category_information[int(category_id)]
                    severity = category_name.split('_')[-1].lower() if '_' in category_name else 'none'

                    for category, ids in self.config.categories_mapping.items():
                        if category_id in ids:
                            # Update the area for the category
                            df.loc[index, category] += area

                            # Update the count of annotations for the category
                            df.loc[index, category + '_Count'] += 1

                            # Update the severity for the category
                            current_severity = df.loc[index, category + '_Severity']
                            if self.severity_rank(severity) > self.severity_rank(current_severity):
                                df.loc[index, category + '_Severity'] = severity

        return df

    def compute_irc_rating_for_row(self, row):
        """Calculate IRC rating for a single row"""
        categories_rules = {
            'Cracking%': (10, 10),
            'Rut Depth%': (10, 10),
            'Ravelling%': (10, 10),
            'Patching%': (10, 10),
            'Potholes%': (0.5, 1),
            'Shoving%': (0.5, 1),
            'Settlements%': (0.5, 5)
        }

        ratings = []
        for category, (threshold_1, threshold_2) in categories_rules.items():
            if category in row.index:
                average = row[category]

                if category in ['Cracking%', 'Rut Depth%', 'Ravelling%', 'Patching%']:
                    if average > threshold_1:
                        ratings.append(1)
                    elif 1 <= average <= threshold_1:
                        ratings.append(1 + 0.1 * (threshold_2 - average))
                    else:
                        ratings.append(3.0)
                elif category in ['Potholes%', 'Shoving%']:
                    if average > threshold_2:
                        ratings.append(0.5)
                    elif threshold_2 >= average >= 0.1:
                        ratings.append(1 + (threshold_2 - average))
                    else:
                        ratings.append(3.0)
                elif category == 'Settlements%':
                    if average > threshold_1:
                        ratings.append(0.5)
                    elif 1 <= average <= threshold_1:
                        ratings.append(1 + average / threshold_2)
                    else:
                        ratings.append(3.0)

        if not ratings:
            return 0

        final_rating = round(sum(ratings) / len(ratings), 2)
        return final_rating

    def calculate_pci(self, row):
        """Calculate PCI (Pavement Condition Index) based on weighted deduction values"""
        category_weights = {}
        for defect in self.config.defect_config.values():
            category = defect["category"]
            weight = defect["deduction_weight"]
            if category not in category_weights:
                category_weights[category] = weight

        pci = 100.0  # Start from perfect pavement

        # Deduct based on category percentage * weight
        for category, weight in category_weights.items():
            percent_col = category + '%'
            pci -= weight * row.get(percent_col, 0)

        pci = max(0, round(pci, 2))  # Clamp to 0 minimum
        return pci

    def calculate_irc_ratings_for_dataframe(self, df):
        """Calculate IRC ratings for all frames in DataFrame"""
        irc_ratings = []
        temp_chunk = []
        temp_dist = 0
        previous_lat, previous_lon = None, None

        for idx, row in df.iterrows():
            current_lat, current_lon = row['latitude'], row['longitude']

            if previous_lat is not None:
                dist = geopy.distance.geodesic(
                    (previous_lat, previous_lon),
                    (current_lat, current_lon)
                ).meters
                temp_dist += dist

            temp_chunk.append(row)

            if temp_dist >= 10:
                chunk_df = pd.DataFrame(temp_chunk)
                sum_percentages = chunk_df[[
                    'Potholes%', 'Patching%', 'Cracking%', 'Ravelling%',
                    'Rut Depth%', 'Settlements%', 'Shoving%'
                ]].sum()
                max_percentages = sum_percentages.apply(lambda x: min(x, 100))
                irc_rating = self.compute_irc_rating_for_row(max_percentages)
                irc_ratings.extend([irc_rating] * len(chunk_df))
                temp_chunk = []
                temp_dist = 0

            previous_lat, previous_lon = current_lat, current_lon

        # Handle any remaining chunk
        if temp_chunk:
            chunk_df = pd.DataFrame(temp_chunk)
            sum_percentages = chunk_df[[
                'Potholes%', 'Patching%', 'Cracking%', 'Ravelling%',
                'Rut Depth%', 'Settlements%', 'Shoving%'
            ]].sum()
            max_percentages = sum_percentages.apply(lambda x: min(x, 100))
            irc_rating = self.compute_irc_rating_for_row(max_percentages)
            irc_ratings.extend([irc_rating] * len(chunk_df))

        # Pad in case there's a mismatch
        if len(irc_ratings) < len(df):
            missing = len(df) - len(irc_ratings)
            irc_ratings.extend([irc_ratings[-1]] * missing)

        # Normalize ratings
        normalized_ratings = [round((abs(r) * 100 / 3), 2) for r in irc_ratings]
        return normalized_ratings

    def calculate_road_defect_percentage(self, data):
        """Calculate percentage of defected and non-defected roads"""
        annotated_image_ids = {annotation['image_id'] for annotation in data['annotations']}
        total_images = len(data['images'])
        num_defected = len(annotated_image_ids)
        num_not_defected = total_images - num_defected

        # Create severity mapping from ConfigManager's defect_config (includes all categories)
        category_severity = {}
        for category_id, defect_info in self.config.defect_config.items():
            severity_raw = defect_info.get('severity', 'high')
            # Capitalize severity
            if severity_raw == 'high':
                severity = 'High'
            elif severity_raw == 'medium':
                severity = 'Medium'
            elif severity_raw == 'low':
                severity = 'Low'
            else:
                severity = 'High'
            category_severity[category_id] = severity

        # Count severity
        severity_counts = {'High': 0, 'Medium': 0, 'Low': 0}
        for annotation in data['annotations']:
            category_id = annotation['category_id']
            # Use get() with default to handle unknown categories gracefully
            severity = category_severity.get(category_id, 'High')
            severity_counts[severity] += 1

        # Calculate percentages
        total = num_not_defected + severity_counts['High'] + severity_counts['Medium'] + severity_counts['Low']
        not_defected_percent = round((num_not_defected / total) * 100, 2)
        high_percent = round((severity_counts['High'] / total) * 100, 2)
        medium_percent = round((severity_counts['Medium'] / total) * 100, 2)
        low_percent = round((severity_counts['Low'] / total) * 100, 2)

        severity_counts_percent = [not_defected_percent, high_percent, medium_percent, low_percent]
        category_names = ['Not Defected', 'High Severity', 'Medium Severity', 'Low Severity']

        return severity_counts_percent, category_names
