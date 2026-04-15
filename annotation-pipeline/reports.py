from tqdm import tqdm
import pandas as pd
import numpy as np
from googlemaps import Client
import json
import pandas as pd
#Library added tqdm and gmaps



def process_and_get_counts_updated(json_data_or_path, location_list):
    # If json_data_or_path is a string, assume it's a file path and try to load the data from it
    if isinstance(json_data_or_path, str):
        with open(json_data_or_path, 'r') as json_file:
            json_data = json.load(json_file)
    else:
        json_data = json_data_or_path

    # Extracting annotations
    data = json_data["annotations"]
    count_dict = {}
    for record in data:
        image_id = str(record['image_id'])
        category_id = str(record['category_id'])

        if image_id not in count_dict:
            count_dict[image_id] = {}

        if category_id in count_dict[image_id]:
            count_dict[image_id][category_id] += 1
        else:
            count_dict[image_id][category_id] = 1

    count_df = pd.DataFrame.from_dict(count_dict, orient='index')
    count_df.fillna(0, inplace=True)

    # --- OLD 54-category labels (commented out) ---
    # all_category_names = {
    #     "0": "Crack",
    #     "1": "pothole",
    #     "2": "rutting",
    #     "3": "Patching",
    #     "4": "rain cuts",
    #     "5": "water stagnation",
    #     "6": "pavement marking(faded)",
    #     "7": "Pavement Marking - Day",
    #     "8": "Pavement Marking - Night",
    #     "9": "missing road studs",
    #     "10": "damaged road studs",
    #     "11": "studs - Day",
    #     "12": "studs - Night",
    #     "13": "missing rumble strips",
    #     "14": "damaged rumble strips",
    #     "15": "missing hazard markers",
    #     "16": "damaged hazard markers",
    #     "17": "unevenness",
    #     "18": "edge drop of shoulders",
    #     "19": "vegetation growth of shoulders",
    #     "20": "damaged drain cover slab",
    #     "21": "missing drain cover slab",
    #     "22": "damaged foot path tiles",
    #     "23": "damaged foot path pavers",
    #     "24": "cleanliness(poor)",
    #     "25": "Encroachment",
    #     "26": "illegal parking",
    #     "27": "missing plants",
    #     "28": "irregular gaps",
    #     "29": "Damaged/deteriorated plants",
    #     "30": "unauthorized median opening",
    #     "31": "reduced sight distance(due to overgrowth)",
    #     "32": "damaged kerb",
    #     "33": "faded kerb painting",
    #     "34": "damaged CC Guard Rails (Concrete Crash Barriers/jersey barriers)",
    #     "35": "damaged MBCB Guard Rails (Metal Beam Crash Barriers)",
    #     "36": "faded painting on barriers",
    #     "37": "damaged signboard/sign structure",
    #     "38": "signages - Day",
    #     "39": "signages - Night",
    #     "40": "damaged attenuators(blinkers)",
    #     "41": "damaged delineators(blinkers)",
    #     "42": "damaged anti-glare(blinkers)",
    #     "43": "highway light damaged",
    #     "44": "non functional light",
    #     "45": "unauthorized signboard/hoarding",
    #     "46": "inappropriate division",
    #     "47": "damaged bus shelters",
    #     "48": "faded bus-by markers",
    #     "49": "truck lay-by markers",
    #     "50": "holding boards",
    #     "51": "advertisement boards",
    #     "52": "work zone safety(signboard visibility)",
    #     "53": "work zone safety(barricading)"
    # }

    # --- NEW 17-category NHAI/Khammam labels (active) ---
    all_category_names = {
        "0": "Potholes",
        "1": "Cracking",
        "2": "Rutting",
        "3": "Stripping/Delamination",
        "4": "Pavement Joint",
        "5": "Pavement Damage (Severe)",
        "6": "Unsealed Road",
        "7": "Settlement",
        "8": "Shoulder - Rain Cuts",
        "9": "Shoulder - Edge Drop",
        "10": "Shoulder - Unevenness",
        "11": "Shoulder - Vegetation Growth",
        "12": "Damaged Kerb",
        "13": "Faded Kerb Painting",
        "14": "Reduced Visibility Due to Plantation Growth",
        "15": "Median Separator Damaged",
        "16": "Median Separator Paint Faded",
        "17": "Missing Plants / Irregular Gaps (Median)",
        "18": "Deteriorated or Damaged Plants (Median)",
        "19": "Excessive Plantation Growth",
        "20": "Damaged Drain Cover Slabs",
        "21": "Missing Drain Cover Slabs",
        "22": "Manhole Cover",
        "23": "Water Stagnation",
        "24": "Damaged Footpath Tiles / Paver Blocks",
        "25": "Damaged Crash Barriers",
        "26": "Damaged (MBCB) Metal Beam Crash Barrier",
        "27": "Damaged (PGR) Pedestrian Guard Rail",
        "28": "Faded Painting Concrete Crash (CC) Barrier",
        "29": "Barriers - Faded Painting Guard Rails",
        "30": "Damaged Sign Boards / Sign Structures",
        "31": "Signage - Poor Visibility (Day)",
        "32": "Signage - Poor Visibility (Night)",
        "33": "Damaged Blinkers",
        "34": "Damaged Attenuators",
        "35": "Damaged Delineators",
        "36": "Damaged Anti-Glare",
        "37": "Damaged Road Studs",
        "38": "Road Studs - Poor Visibility (Day)",
        "39": "Road Studs - Poor Visibility (Night)",
        "40": "Damaged Rumble Strips",
        "41": "Damaged Hazard Markers",
        "42": "Faded Pavement Marking",
        "43": "Pavement Marking - Poor Visibility (Day)",
        "44": "Pavement Marking - Poor Visibility (Night)",
        "45": "Bus Bay - Damaged Shelters",
        "46": "Bus Bay - Faded Markings",
        "47": "Bus Bay - Damaged Signages",
        "48": "Truck Lay By - Damaged Shelters",
        "49": "Truck Lay By - Faded Markings",
        "50": "Truck Lay By - Damaged Signages",
        "51": "Damaged Highway Lights",
        "52": "Non-Functional Highway Lights",
        "53": "Work Zone - Inadequate Signboard Visibility",
        "54": "Work Zone - Inadequate Barricading",
        "55": "Work Zone - Poor Diversion Arrangement / Condition",
        "56": "Unauthorized Median Openings",
        "57": "Unauthorized Signboards",
        "58": "Unauthorized Hoardings",
        "59": "Illegal Parking",
        "60": "General Encroachments",
        "61": "Cleanliness - Litter",
        "62": "Cleanliness - Debris",
        "63": "Missing Assets (Signages)",
        "64": "Missing Assets (Guard Rails)",
        "65": "Missing Assets (Street Lights)"
    }

    # Rename columns to human-readable category names
    for idx, name in all_category_names.items():
        if idx in count_df.columns:
            count_df.rename(columns={idx: name}, inplace=True)

    # --- OLD 54-category categories_mapping (commented out) ---
    # categories_mapping = {
    #     "Crack": ["0"],
    #     "pothole": ["1"],
    #     "rutting": ["2"],
    #     "Patching": ["3"],
    #     "rain cuts": ["4"],
    #     "water stagnation": ["5"],
    #     "pavement marking(faded)": ["6"],
    #     "Pavement Marking - Day": ["7"],
    #     "Pavement Marking - Night": ["8"],
    #     "missing road studs": ["9"],
    #     "damaged road studs": ["10"],
    #     "studs - Day": ["11"],
    #     "studs - Night": ["12"],
    #     "missing rumble strips": ["13"],
    #     "damaged rumble strips": ["14"],
    #     "missing hazard markers": ["15"],
    #     "damaged hazard markers": ["16"],
    #     "unevenness": ["17"],
    #     "edge drop of shoulders": ["18"],
    #     "vegetation growth of shoulders": ["19"],
    #     "damaged drain cover slab": ["20"],
    #     "missing drain cover slab": ["21"],
    #     "damaged foot path tiles": ["22"],
    #     "damaged foot path pavers": ["23"],
    #     "cleanliness(poor)": ["24"],
    #     "Encroachment": ["25"],
    #     "illegal parking": ["26"],
    #     "missing plants": ["27"],
    #     "irregular gaps": ["28"],
    #     "Damaged/deteriorated plants": ["29"],
    #     "unauthorized median opening": ["30"],
    #     "reduced sight distance(due to overgrowth)": ["31"],
    #     "damaged kerb": ["32"],
    #     "faded kerb painting": ["33"],
    #     "damaged CC Guard Rails (Concrete Crash Barriers/jersey barriers)": ["34"],
    #     "damaged MBCB Guard Rails (Metal Beam Crash Barriers)": ["35"],
    #     "faded painting on barriers": ["36"],
    #     "damaged signboard/sign structure": ["37"],
    #     "signages - Day": ["38"],
    #     "signages - Night": ["39"],
    #     "damaged attenuators(blinkers)": ["40"],
    #     "damaged delineators(blinkers)": ["41"],
    #     "damaged anti-glare(blinkers)": ["42"],
    #     "highway light damaged": ["43"],
    #     "non functional light": ["44"],
    #     "unauthorized signboard/hoarding": ["45"],
    #     "inappropriate division": ["46"],
    #     "damaged bus shelters": ["47"],
    #     "faded bus-by markers": ["48"],
    #     "truck lay-by markers": ["49"],
    #     "holding boards": ["50"],
    #     "advertisement boards": ["51"],
    #     "work zone safety(signboard visibility)": ["52"],
    #     "work zone safety(barricading)": ["53"]
    # }

    # --- NEW 17-category NHAI/Khammam categories_mapping (active) ---
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
        "Missing Assets (Street Lights)": ["65"]
    }

    for category, subcats in categories_mapping.items():
        count_df[category] = 0
        for subcat in subcats:
            subcat_name = all_category_names.get(subcat)
            if subcat_name and subcat_name in count_df.columns:
                count_df[category] += count_df[subcat_name]

    # Adding Latitude and Longitude to count_df using the provided list

    location_dict = {entry['inference_image'].split('/')[-1]: (entry['latitude'], entry['longitude']) for entry in location_list}
    labeled_image_files = [entry['file_name'] for entry in json_data["images"] if str(entry['id']) in count_dict.keys()]
    
    latitudes = []
    longitudes = []
    for img in labeled_image_files:
        img_name = img.split('/')[-1]
        lat, lon = location_dict.get(img_name, (None, None))
        latitudes.append(lat)
        longitudes.append(lon)

    count_df["Latitude"] = latitudes
    count_df["Longitude"] = longitudes
    
    # Reordering the columns to ensure Latitude and Longitude are at the beginning
    columns_order = ['Latitude', 'Longitude'] + [col for col in count_df if col not in ['Latitude', 'Longitude']]
    count_df = count_df[columns_order]
    count_df.to_csv("count_reports.csv")
    return count_df









#######################################################################################

def add_road_name_to_dataset(api_key, input_path, output_path):
    # Initialize the Google Maps client with your API key
    gmaps = Client(key=api_key)

    def get_road_name(lat, lon):
        result = gmaps.reverse_geocode((lat, lon), result_type="route")
        if result:
            # Extract the road name from the first result
            road_name = result[0]['formatted_address']
            return road_name
        else:
            return None

    # Load your dataset
    data = pd.read_csv(input_path)

    # Apply the function to your dataset
    data['Road name'] = data.apply(lambda row: get_road_name(row['Latitude'], row['Longitude']), axis=1)

    # Save the dataset with the new "Road name" column
    data.to_csv(output_path, index=False)

##########################################################################################


def compute_and_print_distance(filepath, API_KEY):
    data = load_and_filter_data(filepath)
    reordered_data = reorder_data_by_road_name(data)
    representative_points = compute_representative_points(reordered_data)
    distances = compute_distances_gmaps(representative_points, API_KEY)
    distance_matrix = create_distance_matrix(distances, len(representative_points))
    _, total_greedy_distance = greedy_tsp(distance_matrix)
    print(f"Total distance for the greedy traversal: {total_greedy_distance:.2f} km")
    return round(total_greedy_distance, 2)


#######################################################################################

def load_and_filter_data(filepath):
    data = pd.read_csv(filepath)
    return data[data.groupby('Road name')['Road name'].transform('size') > 2]



######################################################################################

def reorder_data_by_road_name(data):
    reordered_data = pd.DataFrame()
    unique_road_names = data['Road name'].unique()
    for road_name in tqdm(unique_road_names, desc="Reordering by road name"):
        road_data = data[data['Road name'] == road_name]
        reordered_data = pd.concat([reordered_data, road_data])
    reordered_data.reset_index(drop=True, inplace=True)
    return reordered_data




#################################################################################


def compute_distances_gmaps(representative_points, API_KEY):
    gmaps = Client(key=API_KEY)
    distances = {}
    for i in tqdm(range(len(representative_points)), desc="Computing distances"):
        for j in range(i+1, len(representative_points)):
            origin = (representative_points.iloc[i]['Latitude'], representative_points.iloc[i]['Longitude'])
            destination = (representative_points.iloc[j]['Latitude'], representative_points.iloc[j]['Longitude'])
            result = gmaps.distance_matrix(origins=[origin], destinations=[destination], mode='driving')
            distance = result['rows'][0]['elements'][0]['distance']['value'] / 1000
            distances[(i, j)] = distance
            distances[(j, i)] = distance
    return distances


##################################################################################

def greedy_tsp(distance_matrix):
    num_points = len(distance_matrix)
    visited = [False] * num_points
    path = [0]
    visited[0] = True
    total_distance = 0
    for _ in range(num_points - 1):
        last_point = path[-1]
        min_distance = float('inf')
        closest_point = None
        for j in range(num_points):
            if not visited[j] and distance_matrix[last_point][j] < min_distance:
                min_distance = distance_matrix[last_point][j]
                closest_point = j
        path.append(closest_point)
        visited[closest_point] = True
        total_distance += min_distance
    total_distance += distance_matrix[path[-1]][path[0]]
    path.append(path[0])
    return path, total_distance

def create_distance_matrix(distances, num_points):
    distance_matrix = np.zeros((num_points, num_points))
    for i in range(num_points):
        for j in range(num_points):
            if i != j:
                distance_matrix[i][j] = distances.get((i, j), float('inf'))
    return distance_matrix

def compute_representative_points(data):
    return data.groupby('Road name').agg({'Latitude': 'mean', 'Longitude': 'mean'}).reset_index()


###################################################################################################



def calculate_severity(value):
    """Determine the severity based on the given value."""
    if value == 0:
        return "Nil"
    elif value < 10:
        return "Low"
    elif 10 <= value <= 15:
        return "Medium"
    else:
        return "High"

def process_road_data(input_filepath):
    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv(input_filepath)
    
    # Reorganize the columns
    df = df[['Road name', 'Latitude', 'Longitude'] + [col for col in df if col not in ['Road name', 'Latitude', 'Longitude']]]
    
    # Update "Road name" column to retain only the string before the first comma
    df['Road name'] = df['Road name'].str.split(',').str[0]
    
    # Remove the "Unnamed: 0" column and add a serial number column at the beginning
    df = df.drop(columns=["Unnamed: 0"])
    df.insert(0, 'Serial Number', range(1, len(df) + 1))
    
    # --- OLD 54-category defect_columns (commented out) ---
    # defect_columns = [
    #     'Crack', 'pothole', 'rutting', 'Patching', 'rain cuts', 'water stagnation',
    #     'pavement marking(faded)', 'Pavement Marking - Day', 'Pavement Marking - Night',
    #     'missing road studs', 'damaged road studs', 'studs - Day', 'studs - Night',
    #     'missing rumble strips', 'damaged rumble strips', 'missing hazard markers',
    #     'damaged hazard markers', 'unevenness', 'edge drop of shoulders',
    #     'vegetation growth of shoulders', 'damaged drain cover slab', 'missing drain cover slab',
    #     'damaged foot path tiles', 'damaged foot path pavers', 'cleanliness(poor)',
    #     'Encroachment', 'illegal parking', 'missing plants', 'irregular gaps',
    #     'Damaged/deteriorated plants', 'unauthorized median opening',
    #     'reduced sight distance(due to overgrowth)', 'damaged kerb', 'faded kerb painting',
    #     'damaged CC Guard Rails (Concrete Crash Barriers/jersey barriers)',
    #     'damaged MBCB Guard Rails (Metal Beam Crash Barriers)', 'faded painting on barriers',
    #     'damaged signboard/sign structure', 'signages - Day', 'signages - Night',
    #     'damaged attenuators(blinkers)', 'damaged delineators(blinkers)',
    #     'damaged anti-glare(blinkers)', 'highway light damaged', 'non functional light',
    #     'unauthorized signboard/hoarding', 'inappropriate division', 'damaged bus shelters',
    #     'faded bus-by markers', 'truck lay-by markers', 'holding boards',
    #     'advertisement boards', 'work zone safety(signboard visibility)',
    #     'work zone safety(barricading)'
    # ]

    # --- NEW 17-category NHAI/Khammam defect_columns (active) ---
    defect_columns = [
        'Potholes', 'Cracking', 'Rutting', 'Stripping/Delamination', 'Pavement Joint',
        'Pavement Damage (Severe)', 'Unsealed Road', 'Settlement',
        'Shoulder - Rain Cuts', 'Shoulder - Edge Drop', 'Shoulder - Unevenness',
        'Shoulder - Vegetation Growth',
        'Damaged Kerb', 'Faded Kerb Painting', 'Reduced Visibility Due to Plantation Growth',
        'Median Separator Damaged', 'Median Separator Paint Faded',
        'Missing Plants / Irregular Gaps (Median)', 'Deteriorated or Damaged Plants (Median)',
        'Excessive Plantation Growth',
        'Damaged Drain Cover Slabs', 'Missing Drain Cover Slabs', 'Manhole Cover', 'Water Stagnation',
        'Damaged Footpath Tiles / Paver Blocks',
        'Damaged Crash Barriers', 'Damaged (MBCB) Metal Beam Crash Barrier',
        'Damaged (PGR) Pedestrian Guard Rail', 'Faded Painting Concrete Crash (CC) Barrier',
        'Barriers - Faded Painting Guard Rails',
        'Damaged Sign Boards / Sign Structures', 'Signage - Poor Visibility (Day)',
        'Signage - Poor Visibility (Night)',
        'Damaged Blinkers', 'Damaged Attenuators', 'Damaged Delineators', 'Damaged Anti-Glare',
        'Damaged Road Studs', 'Road Studs - Poor Visibility (Day)',
        'Road Studs - Poor Visibility (Night)', 'Damaged Rumble Strips', 'Damaged Hazard Markers',
        'Faded Pavement Marking', 'Pavement Marking - Poor Visibility (Day)',
        'Pavement Marking - Poor Visibility (Night)',
        'Bus Bay - Damaged Shelters', 'Bus Bay - Faded Markings', 'Bus Bay - Damaged Signages',
        'Truck Lay By - Damaged Shelters', 'Truck Lay By - Faded Markings',
        'Truck Lay By - Damaged Signages',
        'Damaged Highway Lights', 'Non-Functional Highway Lights',
        'Work Zone - Inadequate Signboard Visibility', 'Work Zone - Inadequate Barricading',
        'Work Zone - Poor Diversion Arrangement / Condition',
        'Unauthorized Median Openings', 'Unauthorized Signboards', 'Unauthorized Hoardings',
        'Illegal Parking', 'General Encroachments',
        'Cleanliness - Litter', 'Cleanliness - Debris',
        'Missing Assets (Signages)', 'Missing Assets (Guard Rails)', 'Missing Assets (Street Lights)'
    ]

    # Add severity columns for each defect (only if column exists in df)
    for col in defect_columns:
        if col in df.columns:
            severity_col_name = col + " Severity"
            df[severity_col_name] = df[col].apply(calculate_severity)
    
    # Rearrange the columns to place each severity column immediately after its corresponding defect column
    ordered_columns = ['Serial Number', 'Road name', 'Latitude', 'Longitude']
    for col in defect_columns:
        if col in df.columns:
            ordered_columns.append(col)
            severity_col = col + " Severity"
            if severity_col in df.columns:
                ordered_columns.append(severity_col)
    df = df[ordered_columns]
    
    return df



##############################################################################

##Pie chart 1

import pandas as pd
import json

import pandas as pd

def get_defect_percentages(csv_path: str) -> dict:
    """
    Calculate the percentage of each defect type from the input CSV file.
    
    Parameters:
        csv_path (str): Path to the CSV file containing defect data.
        
    Returns:
        dict: A dictionary containing percentages and labels of defect types.
    """
    try:
        # Load the data from the CSV file
        data = pd.read_csv(csv_path)
    except FileNotFoundError:
        return {"error": "File not found."}
    except pd.errors.EmptyDataError:
        return {"error": "The CSV file is empty."}
    except pd.errors.ParserError:
        return {"error": "Error parsing the CSV file."}
    
    # --- OLD 54-category defect_types (commented out) ---
    # defect_types = [
    #     'Crack', 'pothole', 'rutting', 'Patching', 'rain cuts', 'water stagnation',
    #     'pavement marking(faded)', 'Pavement Marking - Day', 'Pavement Marking - Night',
    #     'missing road studs', 'damaged road studs', 'studs - Day', 'studs - Night',
    #     'missing rumble strips', 'damaged rumble strips', 'missing hazard markers',
    #     'damaged hazard markers', 'unevenness', 'edge drop of shoulders',
    #     'vegetation growth of shoulders', 'damaged drain cover slab', 'missing drain cover slab',
    #     'damaged foot path tiles', 'damaged foot path pavers', 'cleanliness(poor)',
    #     'Encroachment', 'illegal parking', 'missing plants', 'irregular gaps',
    #     'Damaged/deteriorated plants', 'unauthorized median opening',
    #     'reduced sight distance(due to overgrowth)', 'damaged kerb', 'faded kerb painting',
    #     'damaged CC Guard Rails (Concrete Crash Barriers/jersey barriers)',
    #     'damaged MBCB Guard Rails (Metal Beam Crash Barriers)', 'faded painting on barriers',
    #     'damaged signboard/sign structure', 'signages - Day', 'signages - Night',
    #     'damaged attenuators(blinkers)', 'damaged delineators(blinkers)',
    #     'damaged anti-glare(blinkers)', 'highway light damaged', 'non functional light',
    #     'unauthorized signboard/hoarding', 'inappropriate division', 'damaged bus shelters',
    #     'faded bus-by markers', 'truck lay-by markers', 'holding boards',
    #     'advertisement boards', 'work zone safety(signboard visibility)',
    #     'work zone safety(barricading)'
    # ]

    # --- NEW 17-category NHAI/Khammam defect_types (active) ---
    defect_types = [
        'Potholes', 'Cracking', 'Rutting', 'Stripping/Delamination', 'Pavement Joint',
        'Pavement Damage (Severe)', 'Unsealed Road', 'Settlement',
        'Shoulder - Rain Cuts', 'Shoulder - Edge Drop', 'Shoulder - Unevenness',
        'Shoulder - Vegetation Growth',
        'Damaged Kerb', 'Faded Kerb Painting', 'Reduced Visibility Due to Plantation Growth',
        'Median Separator Damaged', 'Median Separator Paint Faded',
        'Missing Plants / Irregular Gaps (Median)', 'Deteriorated or Damaged Plants (Median)',
        'Excessive Plantation Growth',
        'Damaged Drain Cover Slabs', 'Missing Drain Cover Slabs', 'Manhole Cover', 'Water Stagnation',
        'Damaged Footpath Tiles / Paver Blocks',
        'Damaged Crash Barriers', 'Damaged (MBCB) Metal Beam Crash Barrier',
        'Damaged (PGR) Pedestrian Guard Rail', 'Faded Painting Concrete Crash (CC) Barrier',
        'Barriers - Faded Painting Guard Rails',
        'Damaged Sign Boards / Sign Structures', 'Signage - Poor Visibility (Day)',
        'Signage - Poor Visibility (Night)',
        'Damaged Blinkers', 'Damaged Attenuators', 'Damaged Delineators', 'Damaged Anti-Glare',
        'Damaged Road Studs', 'Road Studs - Poor Visibility (Day)',
        'Road Studs - Poor Visibility (Night)', 'Damaged Rumble Strips', 'Damaged Hazard Markers',
        'Faded Pavement Marking', 'Pavement Marking - Poor Visibility (Day)',
        'Pavement Marking - Poor Visibility (Night)',
        'Bus Bay - Damaged Shelters', 'Bus Bay - Faded Markings', 'Bus Bay - Damaged Signages',
        'Truck Lay By - Damaged Shelters', 'Truck Lay By - Faded Markings',
        'Truck Lay By - Damaged Signages',
        'Damaged Highway Lights', 'Non-Functional Highway Lights',
        'Work Zone - Inadequate Signboard Visibility', 'Work Zone - Inadequate Barricading',
        'Work Zone - Poor Diversion Arrangement / Condition',
        'Unauthorized Median Openings', 'Unauthorized Signboards', 'Unauthorized Hoardings',
        'Illegal Parking', 'General Encroachments',
        'Cleanliness - Litter', 'Cleanliness - Debris',
        'Missing Assets (Signages)', 'Missing Assets (Guard Rails)', 'Missing Assets (Street Lights)'
    ]
    
    # Summing the count of each defect type
    defect_counts = data[defect_types].sum()
    
    # Calculating the percentage for each defect type
    defect_percentages = (defect_counts / defect_counts.sum()) * 100
    
    # Creating a return dictionary
    
    
    return defect_percentages.tolist(),defect_types


##################################################################
####################################################################
##Pie chart2 
import json

def calculate_road_defect_percentage(data):
    """
    Calculate the percentage of defected and non-defected roads based on annotations.

    Parameters:
    - data: dict or str, The JSON data containing 'images' and 'annotations' keys, or the path to a JSON file

    Returns:
    - tuple: A tuple containing two lists - the first with the percentage of defected and non-defected roads,
             and the second with the corresponding labels ["Defected", "Non-Defected"]
    """
    
    # Load JSON data from file if data is a string
    if isinstance(data, str):
        with open(data, 'r') as file:
            data = json.load(file)
    
    # Extract image ids from annotations
    annotated_image_ids = {annotation['image_id'] for annotation in data['annotations']}
    
    # Count the total number of images
    total_images = len(data['images'])
    
    # Count the number of defected and non-defected roads
    defected_roads = len(annotated_image_ids)
    non_defected_roads = total_images - defected_roads
    
    # Calculate percentages and round to 2 decimal places
    defected_percentage = round((defected_roads / total_images) * 100, 2)
    non_defected_percentage = round((non_defected_roads / total_images) * 100, 2)
    
    return (
        [defected_percentage, non_defected_percentage],
        ["Defected", "Non-Defected"]
    )


# Example usage:
# road_defect_percentages = calculate_road_defect_percentage(data)
# print(road_defect_percentages)


import json
from datetime import datetime

def get_submission_date(data: str) -> str:
    """
    Extracts the submission date from the JSON file and converts it to a string in the "date-month-year" format.
    
    Parameters:
        json_path (str): Path to the JSON file.
        
    Returns:
        str: The submission date in "date-month-year" format, or an error message.
    """
    try:
        # with open(json_path, 'r') as file:
        # data = json.load(file)
        
        date_str = data.get('info', {}).get('date_created', "")
        if date_str:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")
            return date_obj.strftime("%d-%m-%Y")
        else:
            return "Date not found."
    except FileNotFoundError:
        return "File not found."
    except json.JSONDecodeError:
        return "Error decoding the JSON file."
    except ValueError:
        return "Error parsing the date."

# Example usage:
# final_submission_date = get_submission_date('/path/to/your/file.json')

def calculate_road_defect_percentage_2(json_data):
    # Load JSON data from file
    # with open(json_file, 'r') as file:
    #     json_data = json.load(file)

    # Count the number of images with and without annotations
    image_ids_with_annotations = {annotation['image_id'] for annotation in json_data['annotations']}
    num_images = len(json_data['images'])
    num_defected = len(image_ids_with_annotations)
    num_not_defected = num_images - num_defected

    # Create a mapping of category id to severity based on new criteria
    category_severity = {}
    for category in json_data['categories']:
        category_id = category['id']
        # HIGH: category IDs 0, 1, 2 (Potholes, Cracking, Rutting)
        # LOW: category IDs 3-65 (all other defects)
        if category_id in [0, 1, 2]:
            # For potholes/cracking/rutting, we need to count per image to determine severity
            # For now, mark them as potential high/medium (will be recalculated below)
            category_severity[category_id] = 'High'  # Default, will be adjusted per image
        elif category_id in range(3, 66):
            category_severity[category_id] = 'Low'
        else:
            category_severity[category_id] = 'Low'

    # Count severity per image to properly handle potholes/cracking/rutting rules
    image_severity_counts = {}
    for annotation in json_data['annotations']:
        image_id = annotation['image_id']
        if image_id not in image_severity_counts:
            image_severity_counts[image_id] = {'crack': 0, 'pothole': 0, 'rutting': 0, 'low': 0}

        category_id = annotation['category_id']
        if category_id == 0:
            image_severity_counts[image_id]['pothole'] += 1
        elif category_id == 1:
            image_severity_counts[image_id]['crack'] += 1
        elif category_id == 2:
            image_severity_counts[image_id]['rutting'] += 1
        elif category_id in range(3, 66):
            image_severity_counts[image_id]['low'] += 1

    # Calculate final severity counts based on per-image rules
    severity_counts = {'High': 0, 'Medium': 0, 'Low': 0}
    for image_id, counts in image_severity_counts.items():
        crack_c = counts['crack']
        pothole_c = counts['pothole']
        rutting_c = counts['rutting']
        low_c = counts['low']

        # Determine image severity
        if (crack_c >= 3 or pothole_c >= 3 or rutting_c >= 3 or
            (crack_c > 0 and pothole_c > 0 and rutting_c > 0)):
            # HIGH severity for this image
            severity_counts['High'] += (crack_c + pothole_c + rutting_c)
        elif crack_c > 0 or pothole_c > 0 or rutting_c > 0:
            # MEDIUM severity for this image
            severity_counts['Medium'] += (crack_c + pothole_c + rutting_c)

        # Add low severity defects
        severity_counts['Low'] += low_c

    # Calculate percentages for the consolidated pie chart
    total = num_not_defected + severity_counts['High'] + severity_counts['Medium'] + severity_counts['Low']
    not_defected_percent = round((num_not_defected / total) * 100,2)
    high_percent = round((severity_counts['High'] / total) * 100,2)
    medium_percent = round((severity_counts['Medium'] / total) * 100,2)
    low_percent = round((severity_counts['Low'] / total) * 100,2)

    severity_counts_percent = [not_defected_percent, high_percent, medium_percent, low_percent]
    category_names = ['Not Defected', 'High Severity', 'Medium Severity', 'Low Severity']

    return severity_counts_percent, category_names
