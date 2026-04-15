# import pandas as pd
# import json
# import math
# import googlemaps

# def calculate_distance(lat1, lon1, lat2, lon2):
#     R = 6371
#     dLat = (lat2 - lat1) * (math.pi / 180)
#     dLon = (lon2 - lon1) * (math.pi / 180)
#     a = (
#         math.sin(dLat / 2) ** 2 +
#         math.cos(lat1 * (math.pi / 180)) * math.cos(lat2 * (math.pi / 180)) *
#         math.sin(dLon / 2) ** 2
#     )
#     c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
#     distance = R * c
#     return distance

# def calculate_total_distance(frame_list_data):
#     total_distance = 0
#     distances = []
#     for i in range(1, len(frame_list_data)):
#         prev_frame = frame_list_data[i - 1]
#         current_frame = frame_list_data[i]
#         if (
#             prev_frame['latitude'] == current_frame['latitude'] and
#             prev_frame['longitude'] == current_frame['longitude']
#         ):
#             distance = 0
#         else:
#             distance = calculate_distance(
#                 prev_frame['latitude'],
#                 prev_frame['longitude'],
#                 current_frame['latitude'],
#                 current_frame['longitude']
#             )
#         total_distance += distance
#         distances.append(distance)
#     return total_distance, distances

# def calculate_chainage(df, distances):
#     df['distance'] = [0] + distances
#     chainage = 0
#     chainages = []
#     chainage_distance = 0
#     for distance in df['distance']:
#         chainage_distance += distance * 1000
#         chainages.append(chainage)
        
#         if chainage_distance >= 10:
#             chainage += 1
#             chainage_distance -= 10

#     df['chainage'] = chainages
#     return df


# def process_defect_types(df, categories_mapping, category_information, severity_order):
#     # Define defect severity mapping
#     defect_severity_map = {
#         "Corrugation": "medium",
#         "Crown Loss": "medium",
#         "Gravel Loss": "low",
#         "Loose Material": "low",
#         "Potholes": "high",
#         "Rutting": "high",
#         "Scouring": "high",
#         "Settlement": "high",
#         "Slippery Surface": "high",
#         "Surface Heaving": "high"
#     }
    
#     # Initialize columns for defect types and counts
#     for defect_type in categories_mapping.keys():
#         if defect_type not in df.columns:
#             df[defect_type] = 0
#         if defect_type + '_Count' not in df.columns:
#             df[defect_type + '_Count'] = 0
#         if defect_type + '_Severity' not in df.columns:
#             df[defect_type + '_Severity'] = 'none'

#     # Process annotations and update DataFrame
#     for index, row in df.iterrows():
#         annotations = row['annotation']
#         if annotations is not None:
#             for annotation in annotations:
#                 category_id = str(annotation['category_id'])
#                 area = annotation['area']
#                 category_name = category_information[int(category_id)]
#                 # Use the defect severity mapping
#                 severity = defect_severity_map.get(category_name, 'none')
                
#                 for category, ids in categories_mapping.items():
#                     if category_id in ids:
#                         # Update the area for the category
#                         df.loc[index, category] += area
                        
#                         # Update the count of annotations for the category
#                         df.loc[index, category + '_Count'] += 1
                        
#                         # Update the severity for the category
#                         current_severity = df.loc[index, category + '_Severity']
#                         if severity_order[severity] > severity_order[current_severity]:
#                             df.loc[index, category + '_Severity'] = severity
#     return df

# def initialize_mappings():
#     categories_mapping = {
#         "Corrugation": ["0"],
#         "Crown Loss": ["1"],
#         "Gravel Loss": ["2"],
#         "Loose Material": ["3"],
#         "Potholes": ["4"],
#         "Rutting": ["5"],
#         "Scouring": ["6"],
#         "Settlement": ["7"],
#         "Slippery Surface": ["8"],
#         "Surface Heaving": ["9"]
#     }
#     category_information = {
#         0: 'Corrugation',
#         1: 'Crown Loss',
#         2: 'Gravel Loss',
#         3: 'Loose Material',
#         4: 'Potholes',
#         5: 'Rutting',
#         6: 'Scouring',
#         7: 'Settlement',
#         8: 'Slippery Surface',
#         9: 'Surface Heaving'
#     }
#     severity_order = {
#         'High': 3,
#         'Medium': 2,
#         'Low': 1,
#         'Nill': 0
#     }
#     return categories_mapping, category_information, severity_order



# def process_grouped_data(df, categories_mapping, severity_order,grouped):
#     # Rename columns
#     df.columns = ['chainage', 'start_latitude', 'end_latitude', 'start_longitude', 'end_longitude']

#     # Handle severity labels by choosing the maximum severity for each chainage
#     for defect_type in categories_mapping.keys():
#         severity_column = defect_type + '_Severity'
#         if severity_column in df.columns:
#             df[severity_column] = grouped[severity_column].agg(lambda x: max(x, key=lambda v: severity_order[v]))

#     # Count the number of each defect type for each chainage
#     for defect_type in categories_mapping.keys():
#         count_column = defect_type + '_Count'
#         if count_column in df.columns:
#             df[count_column] = grouped[count_column].sum()

#     # Calculate mean of percentage columns for each chainage
#     for defect_type in categories_mapping.keys():
#         percentage_column = defect_type + '%'
#         if percentage_column in df.columns:
#             df[percentage_column] = grouped[percentage_column].mean()

#     # Rearrange columns in the desired order
#     column_order = ['chainage', 'start_latitude', 'start_longitude', 'end_latitude', 'end_longitude']
#     for defect_type in categories_mapping.keys():
#         column_order.extend([defect_type + '_Count', defect_type + '_Severity', defect_type + '%'])

#     df = df[column_order]
#     return df

# def compute_condition_index(high_count, medium_count, low_count):
#     total_defects = high_count + medium_count + low_count
#     if total_defects == 0:
#         return 0.0
#     total_severity_score = (high_count * 3) + (medium_count * 2) + (low_count * 1)
#     max_possible_score = total_defects * 3
#     condition_index = (total_severity_score / max_possible_score) * 5
#     return round(condition_index, 2)


# def calculate_irc_rating_for_chunk(chunk):
#     return chunk.apply(lambda row: compute_final_ratings_row(row), axis=1)

# def calculate_pci(row):
#     deduction_weights = {
#         "Corrugation": 1,
#         "Crown Loss": 1,
#         "Gravel Loss": 1,
#         "Loose Material": 1,
#         "Potholes": 1,
#         "Rutting": 1,
#         "Scouring": 1,
#         "Settlement": 1,
#         "Slippery Surface": 1,
#         "Surface Heaving": 1
#     }
#     pci = 100.0
#     for defect, weight in deduction_weights.items():
#         percent_col = defect + '%'
#         pci -= weight * row.get(percent_col, 0)
#     pci = max(0, round(pci, 2))
#     return pci


# def get_address(api_key, latitude, longitude):
#     gmaps = googlemaps.Client(key=api_key)
    
#     # Reverse geocoding to get address
#     result = gmaps.reverse_geocode((latitude, longitude))
#     if not result:
#         return 'Locality road', 'Address not found'
    
#     address_components = result[0]['address_components']
#     formatted_address = result[0]['formatted_address']
    
#     # Check for premises or plus code and remove them from the address
#     for component in address_components:
#         if 'premise' in component['types'] or 'plus_code' in component['types']:
#             premise_or_plus_code = component['long_name']
#             formatted_address = formatted_address.replace(premise_or_plus_code, '').replace(',,', ',').strip(', ')
            
#     return formatted_address



import pandas as pd
import json
import math
import googlemaps


# ============================================================================
# DISTANCE & CHAINAGE CALCULATION
# ============================================================================
def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371
    dLat = (lat2 - lat1) * (math.pi / 180)
    dLon = (lon2 - lon1) * (math.pi / 180)
    a = (
        math.sin(dLat / 2) ** 2 +
        math.cos(lat1 * (math.pi / 180)) * math.cos(lat2 * (math.pi / 180)) *
        math.sin(dLon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance


def calculate_total_distance(frame_list_data):
    total_distance = 0
    distances = []
    for i in range(1, len(frame_list_data)):
        prev_frame = frame_list_data[i - 1]
        current_frame = frame_list_data[i]

        if not all(key in prev_frame for key in ['latitude', 'longitude']):
            print(f"Warning: Missing latitude/longitude in frame {i-1}")
            distances.append(0)
            continue

        if not all(key in current_frame for key in ['latitude', 'longitude']):
            print(f"Warning: Missing latitude/longitude in frame {i}")
            distances.append(0)
            continue

        prev_lat, prev_lon = prev_frame['latitude'], prev_frame['longitude']
        curr_lat, curr_lon = current_frame['latitude'], current_frame['longitude']

        # Skip invalid GPS coordinates (close to 0,0)
        if (abs(prev_lat) < 0.001 and abs(prev_lon) < 0.001) or \
           (abs(curr_lat) < 0.001 and abs(curr_lon) < 0.001):
            print(f"Warning: Invalid GPS coordinates (0,0) detected in frame {i-1} or {i}, skipping")
            distances.append(0)
            continue

        if prev_lat == curr_lat and prev_lon == curr_lon:
            distance = 0
        else:
            distance = calculate_distance(prev_lat, prev_lon, curr_lat, curr_lon)

        total_distance += distance
        distances.append(distance)
    return total_distance, distances


def calculate_chainage(df, distances):
    df['distance'] = [0] + distances
    chainage = 0
    chainages = []
    chainage_distance = 0
    for distance in df['distance']:
        chainage_distance += distance * 1000
        chainages.append(chainage)
        if chainage_distance >= 100:
            chainage += 1
            chainage_distance -= 100
    df['chainage'] = chainages
    return df


# ============================================================================
# LABEL SANITIZER
# ============================================================================
def _sanitize_label_to_key(label):
    key = label.strip()
    for ch in [' ', '/', '\\', '(', ')', '-', '&', ':', ',', ';']:
        key = key.replace(ch, '_')
    while '__' in key:
        key = key.replace('__', '_')
    return key.strip('_')


# ============================================================================
# PROCESS DEFECT TYPES
# ============================================================================
def process_defect_types(df, categories_mapping, category_information, severity_order):
    import pandas as _pd

    # Collect all new columns in one dict, then concat once — avoids hundreds of
    # individual df[col] = x assignments that cause pandas PerformanceWarning
    new_cols = {}

    # Grouped category columns
    for defect_type in categories_mapping.keys():
        if defect_type not in df.columns:
            new_cols[defect_type] = 0.0
        if defect_type + '_Count' not in df.columns:
            new_cols[defect_type + '_Count'] = 0.0
        if defect_type + '_Severity' not in df.columns:
            new_cols[defect_type + '_Severity'] = 'none'

    # Per-label columns for all categories
    for cat_id, cat_name in category_information.items():
        base_key = _sanitize_label_to_key(cat_name)
        count_col = f"{base_key}_Count"
        sev_col = f"{base_key}_Severity"
        if count_col not in df.columns and count_col not in new_cols:
            new_cols[count_col] = 0.0
        if sev_col not in df.columns and sev_col not in new_cols:
            new_cols[sev_col] = 'none'

    if new_cols:
        df = _pd.concat(
            [df, _pd.DataFrame(new_cols, index=df.index)],
            axis=1
        ).copy()  # .copy() defragments the frame

    for index, row in df.iterrows():
        annotations = row['annotation']
        if annotations is None:
            continue

        # First pass: count primary defects for frame severity
        crack_count = 0
        pothole_count = 0
        rutting_count = 0
        for annotation in annotations:
            category_id = str(annotation['category_id'])
            if category_id == '1':    # Cracking
                crack_count += 1
            elif category_id == '0':  # Potholes
                pothole_count += 1
            elif category_id == '2':  # Rutting
                rutting_count += 1

        # Frame-level severity
        frame_severity = 'none'
        if (crack_count >= 3 or pothole_count >= 3 or rutting_count >= 3 or
                (crack_count > 0 and pothole_count > 0 and rutting_count > 0)):
            frame_severity = 'high'
        elif crack_count > 0 or pothole_count > 0 or rutting_count > 0:
            frame_severity = 'medium'

        # Second pass: process all annotations
        for annotation in annotations:
            category_id = str(annotation['category_id'])
            area = annotation['area']
            category_name = category_information[int(category_id)]

            if category_id in ['0', '1', '2']:       # Potholes, Cracking, Rutting
                severity = frame_severity
            elif 3 <= int(category_id) <= 65:         # All other defects
                severity = 'low'
            else:
                severity = 'none'

            # Per-label update
            label_key = _sanitize_label_to_key(category_name)
            label_count_col = f"{label_key}_Count"
            label_sev_col = f"{label_key}_Severity"
            df.loc[index, label_count_col] += 1
            current_label_sev = df.loc[index, label_sev_col]
            if severity_order.get(severity, 0) > severity_order.get(current_label_sev, 0):
                df.loc[index, label_sev_col] = severity

            # Grouped category update
            for category, ids in categories_mapping.items():
                if category_id in ids:
                    df.loc[index, category] += area
                    df.loc[index, category + '_Count'] += 1
                    current_severity = df.loc[index, category + '_Severity']
                    if severity_order[severity] > severity_order[current_severity]:
                        df.loc[index, category + '_Severity'] = severity

    return df


# ============================================================================
# MAPPINGS — 66 NHAI Categories
# ============================================================================
def initialize_mappings():
    categories_mapping = {
        "Potholes": ["0"],
        "Cracking": ["1"],
        "Rutting": ["2"],
        "Stripping/Delamination": ["3"],
        "Pavement Joint": ["4"],
        "Pavement Damage (Severe)": ["5"],
        "Unsealed Road": ["6"],
        "Settlement": ["7"],
        "Shoulder - Rain Cuts": ["8"],
        "Shoulder - Edge Drop": ["9"],
        "Shoulder - Unevenness": ["10"],
        "Shoulder - Vegetation Growth": ["11"],
        "Damaged Kerb": ["12"],
        "Faded Kerb Painting": ["13"],
        "Reduced Visibility Due to Plantation Growth": ["14"],
        "Median Separator Damaged": ["15"],
        "Median Separator Paint Faded": ["16"],
        "Missing Plants / Irregular Gaps (Median)": ["17"],
        "Deteriorated or Damaged Plants (Median)": ["18"],
        "Excessive Plantation Growth": ["19"],
        "Damaged Drain Cover Slabs": ["20"],
        "Missing Drain Cover Slabs": ["21"],
        "Manhole Cover": ["22"],
        "Water Stagnation": ["23"],
        "Damaged Footpath Tiles / Paver Blocks": ["24"],
        "Damaged Crash Barriers": ["25"],
        "Damaged (MBCB) Metal Beam Crash Barrier": ["26"],
        "Damaged (PGR) Pedestrian Guard Rail": ["27"],
        "Faded Painting Concrete Crash (CC) Barrier": ["28"],
        "Barriers - Faded Painting Guard Rails": ["29"],
        "Damaged Sign Boards / Sign Structures": ["30"],
        "Signage - Poor Visibility (Day)": ["31"],
        "Signage - Poor Visibility (Night)": ["32"],
        "Damaged Blinkers": ["33"],
        "Damaged Attenuators": ["34"],
        "Damaged Delineators": ["35"],
        "Damaged Anti-Glare": ["36"],
        "Damaged Road Studs": ["37"],
        "Road Studs - Poor Visibility (Day)": ["38"],
        "Road Studs - Poor Visibility (Night)": ["39"],
        "Damaged Rumble Strips": ["40"],
        "Damaged Hazard Markers": ["41"],
        "Faded Pavement Marking": ["42"],
        "Pavement Marking - Poor Visibility (Day)": ["43"],
        "Pavement Marking - Poor Visibility (Night)": ["44"],
        "Bus Bay - Damaged Shelters": ["45"],
        "Bus Bay - Faded Markings": ["46"],
        "Bus Bay - Damaged Signages": ["47"],
        "Truck Lay By - Damaged Shelters": ["48"],
        "Truck Lay By - Faded Markings": ["49"],
        "Truck Lay By - Damaged Signages": ["50"],
        "Damaged Highway Lights": ["51"],
        "Non-Functional Highway Lights": ["52"],
        "Work Zone - Inadequate Signboard Visibility": ["53"],
        "Work Zone - Inadequate Barricading": ["54"],
        "Work Zone - Poor Diversion Arrangement / Condition": ["55"],
        "Unauthorized Median Openings": ["56"],
        "Unauthorized Signboards": ["57"],
        "Unauthorized Hoardings": ["58"],
        "Illegal Parking": ["59"],
        "General Encroachments": ["60"],
        "Cleanliness - Litter": ["61"],
        "Cleanliness - Debris": ["62"],
        "Missing Assets (Signages)": ["63"],
        "Missing Assets (Guard Rails)": ["64"],
        "Missing Assets (Street Lights)": ["65"],
    }

    category_information = {
        0: 'Potholes',
        1: 'Cracking',
        2: 'Rutting',
        3: 'Stripping/Delamination',
        4: 'Pavement Joint',
        5: 'Pavement Damage (Severe)',
        6: 'Unsealed Road',
        7: 'Settlement',
        8: 'Shoulder - Rain Cuts',
        9: 'Shoulder - Edge Drop',
        10: 'Shoulder - Unevenness',
        11: 'Shoulder - Vegetation Growth',
        12: 'Damaged Kerb',
        13: 'Faded Kerb Painting',
        14: 'Reduced Visibility Due to Plantation Growth',
        15: 'Median Separator Damaged',
        16: 'Median Separator Paint Faded',
        17: 'Missing Plants / Irregular Gaps (Median)',
        18: 'Deteriorated or Damaged Plants (Median)',
        19: 'Excessive Plantation Growth',
        20: 'Damaged Drain Cover Slabs',
        21: 'Missing Drain Cover Slabs',
        22: 'Manhole Cover',
        23: 'Water Stagnation',
        24: 'Damaged Footpath Tiles / Paver Blocks',
        25: 'Damaged Crash Barriers',
        26: 'Damaged (MBCB) Metal Beam Crash Barrier',
        27: 'Damaged (PGR) Pedestrian Guard Rail',
        28: 'Faded Painting Concrete Crash (CC) Barrier',
        29: 'Barriers - Faded Painting Guard Rails',
        30: 'Damaged Sign Boards / Sign Structures',
        31: 'Signage - Poor Visibility (Day)',
        32: 'Signage - Poor Visibility (Night)',
        33: 'Damaged Blinkers',
        34: 'Damaged Attenuators',
        35: 'Damaged Delineators',
        36: 'Damaged Anti-Glare',
        37: 'Damaged Road Studs',
        38: 'Road Studs - Poor Visibility (Day)',
        39: 'Road Studs - Poor Visibility (Night)',
        40: 'Damaged Rumble Strips',
        41: 'Damaged Hazard Markers',
        42: 'Faded Pavement Marking',
        43: 'Pavement Marking - Poor Visibility (Day)',
        44: 'Pavement Marking - Poor Visibility (Night)',
        45: 'Bus Bay - Damaged Shelters',
        46: 'Bus Bay - Faded Markings',
        47: 'Bus Bay - Damaged Signages',
        48: 'Truck Lay By - Damaged Shelters',
        49: 'Truck Lay By - Faded Markings',
        50: 'Truck Lay By - Damaged Signages',
        51: 'Damaged Highway Lights',
        52: 'Non-Functional Highway Lights',
        53: 'Work Zone - Inadequate Signboard Visibility',
        54: 'Work Zone - Inadequate Barricading',
        55: 'Work Zone - Poor Diversion Arrangement / Condition',
        56: 'Unauthorized Median Openings',
        57: 'Unauthorized Signboards',
        58: 'Unauthorized Hoardings',
        59: 'Illegal Parking',
        60: 'General Encroachments',
        61: 'Cleanliness - Litter',
        62: 'Cleanliness - Debris',
        63: 'Missing Assets (Signages)',
        64: 'Missing Assets (Guard Rails)',
        65: 'Missing Assets (Street Lights)',
    }

    severity_order = {
        'high': 3,
        'medium': 2,
        'low': 1,
        'none': 0
    }

    return categories_mapping, category_information, severity_order


# ============================================================================
# GROUPED DATA PROCESSOR
# ============================================================================
def process_grouped_data(df, categories_mapping, severity_order, grouped):
    df.columns = ['chainage', 'start_latitude', 'end_latitude', 'start_longitude', 'end_longitude']

    for defect_type in categories_mapping.keys():
        severity_column = defect_type + '_Severity'
        if severity_column in df.columns:
            df[severity_column] = grouped[severity_column].agg(
                lambda x: max(x, key=lambda v: severity_order[v])
            )
        count_column = defect_type + '_Count'
        if count_column in df.columns:
            df[count_column] = grouped[count_column].sum()
        percentage_column = defect_type + '%'
        if percentage_column in df.columns:
            df[percentage_column] = grouped[percentage_column].mean()

    column_order = ['chainage', 'start_latitude', 'start_longitude', 'end_latitude', 'end_longitude']
    for defect_type in categories_mapping.keys():
        column_order.extend([defect_type + '_Count', defect_type + '_Severity', defect_type + '%'])
    df = df[column_order]
    return df


# ============================================================================
# IRC RATING
# ============================================================================
def compute_final_ratings_row(row):
    categories_rules = {
        'Cracking%': (10, 10),
        'Rutting%': (10, 10),
        'Stripping/Delamination%': (10, 10),
        'Potholes%': (0.5, 1),
    }
    ratings = []
    for category, (threshold_1, threshold_2) in categories_rules.items():
        if category in row.index:
            average = row[category]
            if category in ['Cracking%', 'Rutting%', 'Stripping/Delamination%']:
                if average > threshold_1:
                    ratings.append(1)
                elif 1 <= average <= threshold_1:
                    ratings.append(1 + 0.1 * (threshold_2 - average))
                else:
                    ratings.append(3.0)
            elif category == 'Potholes%':
                if average > threshold_2:
                    ratings.append(0.5)
                elif threshold_2 >= average >= 0.1:
                    ratings.append(1 + (threshold_2 - average))
                else:
                    ratings.append(3.0)
    return round(sum(ratings) / len(ratings), 2) if ratings else 3.0


def calculate_irc_rating_for_chunk(chunk):
    return chunk.apply(lambda row: compute_final_ratings_row(row), axis=1)


def compute_condition_index(high_count, medium_count, low_count):
    total_defects = high_count + medium_count + low_count
    if total_defects == 0:
        return 0.0
    total_severity_score = (high_count * 3) + (medium_count * 2) + (low_count * 1)
    max_possible_score = total_defects * 3
    return round((total_severity_score / max_possible_score) * 5, 2)


def calculate_pci(row):
    pci = 100.0
    area_weights = {
        "Potholes": 2.0,
        "Cracking": 0.8,
        "Rutting": 1.2,
        "Stripping/Delamination": 0.4,
    }
    severity_multipliers = {
        'high': 1.5, 'medium': 1.0, 'low': 0.6, 'none': 0.0, 'nill': 0.0
    }
    for defect, area_weight in area_weights.items():
        area_percent = row.get(defect + '%', 0)
        severity = str(row.get(defect + '_Severity', 'none')).lower()
        count = row.get(defect + '_Count', 0)
        severity_mult = severity_multipliers.get(severity, 1.0)
        if defect == "Potholes" and count > 0:
            deduction = (area_percent * area_weight * severity_mult) + (count * 0.5 * severity_mult)
        else:
            deduction = area_percent * area_weight * severity_mult
        pci -= deduction
    return max(0, min(100, round(pci, 2)))


# ============================================================================
# ADDRESS LOOKUP
# ============================================================================
def get_address(api_key, latitude, longitude):
    gmaps = googlemaps.Client(key=api_key)
    result = gmaps.reverse_geocode((latitude, longitude))
    if not result:
        return 'Locality road'
    address_components = result[0]['address_components']
    formatted_address = result[0]['formatted_address']
    for component in address_components:
        if 'premise' in component['types'] or 'plus_code' in component['types']:
            premise_or_plus_code = component['long_name']
            formatted_address = formatted_address.replace(
                premise_or_plus_code, ''
            ).replace(',,', ',').strip(', ')
    return formatted_address
