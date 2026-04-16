# import os
# import json
# import pandas as pd
# import matplotlib.pyplot as plt
# import json
# from googlemaps import Client

# def get_gmaps_key():
    
#     return 'YOUR_API_KEY'


# def create_dir(directory):
#     if not os.path.exists(directory):
#         print("Making the Working Directory...")
#         os.makedirs(directory)

# def process_and_export_final_updated(json_path, location_list, area, output_csv_path='1_inspection.csv'):
#     with open(json_path, 'r') as json_file:
#         data_ = json.load(json_file)

#     # Extracting annotations
#     data = data_["annotations"]
#     result_dict = {}
#     for record in data:
#         image_id = str(record['image_id'])
#         category_id = str(record['category_id'])
#         area_val = record['area']

#         if image_id not in result_dict:
#             result_dict[image_id] = {}

#         if category_id in result_dict[image_id]:
#             result_dict[image_id][category_id] += area_val
#         else:
#             result_dict[image_id][category_id] = area_val

#     result_df = pd.DataFrame.from_dict(result_dict, orient='index')
#     result_df.fillna(0, inplace=True)

#     # Complete category mapping (all categories)
#     all_category_names = {
#         0: 'Crack',
#         1: 'pothole',
#         2: 'rutting',
#         3: 'Patching',
#         4: 'rain cuts',
#         5: 'water stagnation',
#         6: 'pavement marking(faded)',
#         7: 'Pavement Marking - Day',
#         8: 'Pavement Marking - Night',
#         9: 'missing road studs',
#         10: 'damaged road studs',
#         11: 'studs - Day',
#         12: 'studs - Night',
#         13: 'missing rumble strips',
#         14: 'damaged rumble strips',
#         15: 'missing hazard markers',
#         16: 'damaged hazard markers',
#         17: 'unevenness',
#         18: 'edge drop of shoulders',
#         19: 'vegetation growth of shoulders',
#         20: 'damaged drain cover slab',
#         21: 'missing drain cover slab',
#         22: 'damaged foot path tiles',
#         23: 'damaged foot path pavers',
#         24: 'cleanliness(poor)',
#         25: 'Encroachment',
#         26: 'illegal parking',
#         27: 'missing plants',
#         28: 'irregular gaps',
#         29: 'Damaged/deteriorated plants',
#         30: 'unauthorized median opening',
#         31: 'reduced sight distance(due to overgrowth)',
#         32: 'damaged kerb',
#         33: 'faded kerb painting',
#         34: 'damaged CC Guard Rails (Concrete Crash Barriers/jersey barriers)',
#         35: 'damaged MBCB Guard Rails (Metal Beam Crash Barriers)',
#         36: 'faded painting on barriers',
#         37: 'damaged signboard/sign structure',
#         38: 'signages - Day',
#         39: 'signages - Night',
#         40: 'damaged attenuators(blinkers)',
#         41: 'damaged delineators(blinkers)',
#         42: 'damaged anti-glare(blinkers)',
#         43: 'highway light damaged',
#         44: 'non functional light',
#         45: 'unauthorized signboard/hoarding',
#         46: 'inappropriate division',
#         47: 'damaged bus shelters',
#         48: 'faded bus-by markers',
#         49: 'truck lay-by markers',
#         50: 'holding boards',
#         51: 'advertisement boards',
#         52: 'work zone safety(signboard visibility)',
#         53: 'work zone safety(barricading)'
#     }

#     # Rename columns to human-readable category names
#     for idx, name in all_category_names.items():
#         if idx in result_df.columns:
#             result_df[idx] = (result_df[idx] / area).round(2)
#             result_df.rename(columns={idx: name}, inplace=True)

#     # Map all individual categories (each to its own ID)
#     categories_mapping = {
#         "Crack": ["0"],
#         "pothole": ["1"],
#         "rutting": ["2"],
#         "Patching": ["3"],
#         "rain cuts": ["4"],
#         "water stagnation": ["5"],
#         "pavement marking(faded)": ["6"],
#         "Pavement Marking - Day": ["7"],
#         "Pavement Marking - Night": ["8"],
#         "missing road studs": ["9"],
#         "damaged road studs": ["10"],
#         "studs - Day": ["11"],
#         "studs - Night": ["12"],
#         "missing rumble strips": ["13"],
#         "damaged rumble strips": ["14"],
#         "missing hazard markers": ["15"],
#         "damaged hazard markers": ["16"],
#         "unevenness": ["17"],
#         "edge drop of shoulders": ["18"],
#         "vegetation growth of shoulders": ["19"],
#         "damaged drain cover slab": ["20"],
#         "missing drain cover slab": ["21"],
#         "damaged foot path tiles": ["22"],
#         "damaged foot path pavers": ["23"],
#         "cleanliness(poor)": ["24"],
#         "Encroachment": ["25"],
#         "illegal parking": ["26"],
#         "missing plants": ["27"],
#         "irregular gaps": ["28"],
#         "Damaged/deteriorated plants": ["29"],
#         "unauthorized median opening": ["30"],
#         "reduced sight distance(due to overgrowth)": ["31"],
#         "damaged kerb": ["32"],
#         "faded kerb painting": ["33"],
#         "damaged CC Guard Rails (Concrete Crash Barriers/jersey barriers)": ["34"],
#         "damaged MBCB Guard Rails (Metal Beam Crash Barriers)": ["35"],
#         "faded painting on barriers": ["36"],
#         "damaged signboard/sign structure": ["37"],
#         "signages - Day": ["38"],
#         "signages - Night": ["39"],
#         "damaged attenuators(blinkers)": ["40"],
#         "damaged delineators(blinkers)": ["41"],
#         "damaged anti-glare(blinkers)": ["42"],
#         "highway light damaged": ["43"],
#         "non functional light": ["44"],
#         "unauthorized signboard/hoarding": ["45"],
#         "inappropriate division": ["46"],
#         "damaged bus shelters": ["47"],
#         "faded bus-by markers": ["48"],
#         "truck lay-by markers": ["49"],
#         "holding boards": ["50"],
#         "advertisement boards": ["51"],
#         "work zone safety(signboard visibility)": ["52"],
#         "work zone safety(barricading)": ["53"]
#     }

#     for category, subcats in categories_mapping.items():
#         result_df[category] = 0
#         for subcat in subcats:
#             subcat_name = all_category_names.get(int(subcat))
#             if subcat_name and subcat_name in result_df.columns:
#                 result_df[category] += result_df[subcat_name]
#         result_df[category] = result_df[category].round(2)

#     # Adding Latitude and Longitude to result_df using the provided list
#     location_dict = {entry['inference_image'].split('/')[-1]: (entry['latitude'], entry['longitude']) for entry in location_list}
#     labeled_image_files = [entry['file_name'] for entry in data_["images"] if str(entry['id']) in result_dict.keys()]
#     latitudes = [location_dict[img.split('/')[-1]][0] for img in labeled_image_files if img.split('/')[-1] in location_dict]
#     longitudes = [location_dict[img.split('/')[-1]][1] for img in labeled_image_files if img.split('/')[-1] in location_dict]

#     result_df["Latitude"] = latitudes
#     result_df["Longitude"] = longitudes
    
#     # Reordering the columns to ensure Latitude and Longitude are at the beginning
#     columns_order = ['Latitude', 'Longitude'] + [col for col in result_df if col not in ['Latitude', 'Longitude']]
#     result_df = result_df[columns_order]

#     # Exporting the dataframe to CSV
#     result_df.to_csv(output_csv_path)

#     return result_df



# import pandas as pd
# from googlemaps import Client

# def add_road_name_to_dataset_1(api_key, input_path, output_path):
#     # Initialize the Google Maps client with your API key
#     gmaps = Client(key=api_key)

#     def get_road_name(lat, lon):
#         try:
#             result = gmaps.reverse_geocode((lat, lon), result_type="route")
#             if result:
#                 road_name = result[0]['formatted_address']
#                 return road_name
#             else:
#                 print(f"No result for {lat}, {lon}")
#                 return None
#         except Exception as e:
#             print(f"Error for {lat}, {lon}: {str(e)}")
#             return None

#     # Load your dataset
#     data = pd.read_csv(input_path)

#     # Remove the Unnamed: 0 column if it exists
#     data = data.loc[:, ~data.columns.str.contains('^Unnamed')]

#     # Apply the function to your dataset
#     data['Road name'] = data.apply(lambda row: get_road_name(row['Latitude'], row['Longitude']), axis=1)

#     # Reorder columns to have 'Road name' first, followed by 'Latitude' and 'Longitude'
#     data = data[['Road name'] + [col for col in data if col != 'Road name']]
    
#     # Save the dataset with the new "Road name" column and a custom header
#     with open(output_path, 'w', newline='') as f:
#         f.write("Roadvision AI Visual Inspection Report\n")
#         data.to_csv(f, index=False)
    
#     # Return the data after file operations are complete
#     return data

    
# # A mock call for reference:
# # report_df_final = process_and_export_final('path_to_result.json', location_data, 3456)



# # A mock call for reference:
# # report_df_corrected = process_and_export_corrected('path_to_result.json', location_data, 3456)



# # A mock call (won't execute here, but just for reference):
# # report_df = process_and_export_updated('path_to_result.json', frame_data, 3456)





# def process_and_export(json_path, area):
#     with open(json_path, 'r') as json_file:
#         data_ = json.load(json_file)

#     data = data_["annotations"]
#     result_dict = {}
#     for record in data:
#         image_id = str(record['image_id'])
#         category_id = str(record['category_id'])
#         area_val = record['area']

#         if image_id not in result_dict:
#             result_dict[image_id] = {}

#         if category_id in result_dict[image_id]:
#             result_dict[image_id][category_id] += area_val
#         else:
#             result_dict[image_id][category_id] = area_val

#     result_df_1 = pd.DataFrame.from_dict(result_dict, orient='index')
#     result_df_1.fillna(0, inplace=True)

#     # Complete category mapping (all categories)
#     all_category_names = {
#         "0": "Crack",
#         "1": "pothole",
#         "2": "rutting",
#         "3": "Patching",
#         "4": "rain cuts",
#         "5": "water stagnation",
#         "6": "pavement marking(faded)",
#         "7": "Pavement Marking - Day",
#         "8": "Pavement Marking - Night",
#         "9": "missing road studs",
#         "10": "damaged road studs",
#         "11": "studs - Day",
#         "12": "studs - Night",
#         "13": "missing rumble strips",
#         "14": "damaged rumble strips",
#         "15": "missing hazard markers",
#         "16": "damaged hazard markers",
#         "17": "unevenness",
#         "18": "edge drop of shoulders",
#         "19": "vegetation growth of shoulders",
#         "20": "damaged drain cover slab",
#         "21": "missing drain cover slab",
#         "22": "damaged foot path tiles",
#         "23": "damaged foot path pavers",
#         "24": "cleanliness(poor)",
#         "25": "Encroachment",
#         "26": "illegal parking",
#         "27": "missing plants",
#         "28": "irregular gaps",
#         "29": "Damaged/deteriorated plants",
#         "30": "unauthorized median opening",
#         "31": "reduced sight distance(due to overgrowth)",
#         "32": "damaged kerb",
#         "33": "faded kerb painting",
#         "34": "damaged CC Guard Rails (Concrete Crash Barriers/jersey barriers)",
#         "35": "damaged MBCB Guard Rails (Metal Beam Crash Barriers)",
#         "36": "faded painting on barriers",
#         "37": "damaged signboard/sign structure",
#         "38": "signages - Day",
#         "39": "signages - Night",
#         "40": "damaged attenuators(blinkers)",
#         "41": "damaged delineators(blinkers)",
#         "42": "damaged anti-glare(blinkers)",
#         "43": "highway light damaged",
#         "44": "non functional light",
#         "45": "unauthorized signboard/hoarding",
#         "46": "inappropriate division",
#         "47": "damaged bus shelters",
#         "48": "faded bus-by markers",
#         "49": "truck lay-by markers",
#         "50": "holding boards",
#         "51": "advertisement boards",
#         "52": "work zone safety(signboard visibility)",
#         "53": "work zone safety(barricading)"
#     }

#     # Rename columns to human-readable category names
#     for idx, name in all_category_names.items():
#         if idx in result_df_1.columns:
#             result_df_1[idx] = (result_df_1[idx] / area).round(2)
#             result_df_1.rename(columns={idx: name}, inplace=True)

#     # Map all individual categories (each to its own ID)
#     categories_mapping = {
#         "Crack": ["0"],
#         "pothole": ["1"],
#         "rutting": ["2"],
#         "Patching": ["3"],
#         "rain cuts": ["4"],
#         "water stagnation": ["5"],
#         "pavement marking(faded)": ["6"],
#         "Pavement Marking - Day": ["7"],
#         "Pavement Marking - Night": ["8"],
#         "missing road studs": ["9"],
#         "damaged road studs": ["10"],
#         "studs - Day": ["11"],
#         "studs - Night": ["12"],
#         "missing rumble strips": ["13"],
#         "damaged rumble strips": ["14"],
#         "missing hazard markers": ["15"],
#         "damaged hazard markers": ["16"],
#         "unevenness": ["17"],
#         "edge drop of shoulders": ["18"],
#         "vegetation growth of shoulders": ["19"],
#         "damaged drain cover slab": ["20"],
#         "missing drain cover slab": ["21"],
#         "damaged foot path tiles": ["22"],
#         "damaged foot path pavers": ["23"],
#         "cleanliness(poor)": ["24"],
#         "Encroachment": ["25"],
#         "illegal parking": ["26"],
#         "missing plants": ["27"],
#         "irregular gaps": ["28"],
#         "Damaged/deteriorated plants": ["29"],
#         "unauthorized median opening": ["30"],
#         "reduced sight distance(due to overgrowth)": ["31"],
#         "damaged kerb": ["32"],
#         "faded kerb painting": ["33"],
#         "damaged CC Guard Rails (Concrete Crash Barriers/jersey barriers)": ["34"],
#         "damaged MBCB Guard Rails (Metal Beam Crash Barriers)": ["35"],
#         "faded painting on barriers": ["36"],
#         "damaged signboard/sign structure": ["37"],
#         "signages - Day": ["38"],
#         "signages - Night": ["39"],
#         "damaged attenuators(blinkers)": ["40"],
#         "damaged delineators(blinkers)": ["41"],
#         "damaged anti-glare(blinkers)": ["42"],
#         "highway light damaged": ["43"],
#         "non functional light": ["44"],
#         "unauthorized signboard/hoarding": ["45"],
#         "inappropriate division": ["46"],
#         "damaged bus shelters": ["47"],
#         "faded bus-by markers": ["48"],
#         "truck lay-by markers": ["49"],
#         "holding boards": ["50"],
#         "advertisement boards": ["51"],
#         "work zone safety(signboard visibility)": ["52"],
#         "work zone safety(barricading)": ["53"]
#     }

#     for category, subcats in categories_mapping.items():
#         result_df_1[category] = 0
#         for subcat in subcats:
#             subcat_name = all_category_names.get(subcat)
#             if subcat_name and subcat_name in result_df_1.columns:
#                 result_df_1[category] += result_df_1[subcat_name]
#         result_df_1[category] = result_df_1[category].round(2)
        
#     return result_df_1

# def compute_final_ratings(series_input):
#     # Ensure the input is a Series (e.g., max_percentages)
#     series_input.index = series_input.index.str.replace('%', '').str.strip()
#     df1 = series_input.to_frame(name="average")
#     df1["rating"] = 0.0
#     df1 = df1[df1["average"] != 0.0]

#     categories_rules = {
#         'Cracking': (10, 10),
#         'Rut Depth': (10, 10),
#         'Ravelling': (10, 10),
#         'Patching': (10, 10),
#         'Potholes': (1, 1),
#         'Shoving': (1, 1),
#         'Settlements': (5, 5)
#     }

#     for index, row in df1.iterrows():
#         average = row['average']
#         threshold_1, threshold_2 = categories_rules.get(index, (None, None))

#         if index in ['Cracking', 'Rut Depth', 'Ravelling', 'Patching']:
#             if average > threshold_1:
#                 df1.at[index, 'rating'] = 1
#             elif 1 <= average <= threshold_1:
#                 df1.at[index, 'rating'] = 1 + 0.1 * (threshold_2 - average)
#             else:
#                 df1.at[index, 'rating'] = 2.5
#         elif index in ['Potholes', 'Shoving']:
#             if average > threshold_2:
#                 df1.at[index, 'rating'] = 1
#             elif threshold_2 >= average >= 0.1:
#                 df1.at[index, 'rating'] = 1 + (threshold_2 - average)
#             else:
#                 df1.at[index, 'rating'] = 2.5
#         elif index == 'Settlements':
#             if average > threshold_1:
#                 df1.at[index, 'rating'] = 1
#             elif 1 <= average <= threshold_1:
#                 df1.at[index, 'rating'] = 1 + average / threshold_2
#             else:
#                 df1.at[index, 'rating'] = 2.5

#     final_rating = round(df1["rating"].mean(), 2)
#     print("Final Rating: " + str(final_rating))
#     return final_rating


# def draw_defect_distribution_chart(result_df):
#     # Calculate the total percentages for each defect type
#     percentages = [
#         result_df["Potholes"].sum(),
#         result_df["Cracking"].sum(),
#         result_df["Rut Depth"].sum(),
#         result_df["Patching"].sum(),
#         result_df["Ravelling"].sum(),
#         result_df["Settlements"].sum(),
#         result_df["Shoving"].sum()
#     ]
#     labels = [
#         "Potholes",
#         "Cracking",
#         "Rut Depth",
#         "Patching",
#         "Ravelling",
#         "Settlements",
#         "Shoving"
#     ]
#     return percentages,labels
  


# ### Below for bar chart
# def plot_top_categories(json_path, top_n=4):
#     """
#     Plots the top N categories from the given JSON file.

#     Parameters:
#     - json_path (str): Path to the JSON file containing the data.
#     - top_n (int): Number of top categories to display. Default is 4.

#     Returns:
#     None. The function will display a bar chart.
#     """

#     # Load the JSON content from the specified path
#     with open(json_path, 'r') as file:
#         json_content = json.load(file)

#     # Convert annotations into a pandas DataFrame
#     df_annotations = pd.DataFrame(json_content["annotations"])

#     # Create a mapping of category ids to category names
#     category_names = {
#         0: 'Crack',
#         1: 'pothole',
#         2: 'rutting',
#         3: 'Patching',
#         4: 'rain cuts',
#         5: 'water stagnation',
#         6: 'pavement marking(faded)',
#         7: 'Pavement Marking - Day',
#         8: 'Pavement Marking - Night',
#         9: 'missing road studs',
#         10: 'damaged road studs',
#         11: 'studs - Day',
#         12: 'studs - Night',
#         13: 'missing rumble strips',
#         14: 'damaged rumble strips',
#         15: 'missing hazard markers',
#         16: 'damaged hazard markers',
#         17: 'unevenness',
#         18: 'edge drop of shoulders',
#         19: 'vegetation growth of shoulders',
#         20: 'damaged drain cover slab',
#         21: 'missing drain cover slab',
#         22: 'damaged foot path tiles',
#         23: 'damaged foot path pavers',
#         24: 'cleanliness(poor)',
#         25: 'Encroachment',
#         26: 'illegal parking',
#         27: 'missing plants',
#         28: 'irregular gaps',
#         29: 'Damaged/deteriorated plants',
#         30: 'unauthorized median opening',
#         31: 'reduced sight distance(due to overgrowth)',
#         32: 'damaged kerb',
#         33: 'faded kerb painting',
#         34: 'damaged CC Guard Rails (Concrete Crash Barriers/jersey barriers)',
#         35: 'damaged MBCB Guard Rails (Metal Beam Crash Barriers)',
#         36: 'faded painting on barriers',
#         37: 'damaged signboard/sign structure',
#         38: 'signages - Day',
#         39: 'signages - Night',
#         40: 'damaged attenuators(blinkers)',
#         41: 'damaged delineators(blinkers)',
#         42: 'damaged anti-glare(blinkers)',
#         43: 'highway light damaged',
#         44: 'non functional light',
#         45: 'unauthorized signboard/hoarding',
#         46: 'inappropriate division',
#         47: 'damaged bus shelters',
#         48: 'faded bus-by markers',
#         49: 'truck lay-by markers',
#         50: 'holding boards',
#         51: 'advertisement boards',
#         52: 'work zone safety(signboard visibility)',
#         53: 'work zone safety(barricading)'
#     }

#     # Distribution of Annotations by Category
#     category_counts = df_annotations['category_id'].value_counts().reset_index()
#     category_counts.columns = ['category_id', 'count']
#     category_counts['category_name'] = category_counts['category_id'].map(category_names)

#     # Sort and select top N categories
#     category_counts = category_counts.sort_values(by='count', ascending=False)

#     # Colors for bars
#     colors = ['red', 'yellow', 'blue', 'green'][:top_n]
#     return category_counts
    


# def plot_severity_distribution(json_path):
#     """
#     Plots the distribution of annotations by severity from the given JSON file.

#     Parameters:
#     - json_path (str): Path to the JSON file containing the data.

#     Returns:
#     None. The function will display a pie chart.
#     """

#     # Load the JSON content from the specified path
#     with open(json_path, 'r') as file:
#         json_content = json.load(file)

#     # Convert annotations into a pandas DataFrame
#     df_annotations = pd.DataFrame(json_content["annotations"])

#     # Create a mapping of category ids to category names
#     category_names = {
#         0: 'Crack',
#         1: 'pothole',
#         2: 'rutting',
#         3: 'Patching',
#         4: 'rain cuts',
#         5: 'water stagnation',
#         6: 'pavement marking(faded)',
#         7: 'Pavement Marking - Day',
#         8: 'Pavement Marking - Night',
#         9: 'missing road studs',
#         10: 'damaged road studs',
#         11: 'studs - Day',
#         12: 'studs - Night',
#         13: 'missing rumble strips',
#         14: 'damaged rumble strips',
#         15: 'missing hazard markers',
#         16: 'damaged hazard markers',
#         17: 'unevenness',
#         18: 'edge drop of shoulders',
#         19: 'vegetation growth of shoulders',
#         20: 'damaged drain cover slab',
#         21: 'missing drain cover slab',
#         22: 'damaged foot path tiles',
#         23: 'damaged foot path pavers',
#         24: 'cleanliness(poor)',
#         25: 'Encroachment',
#         26: 'illegal parking',
#         27: 'missing plants',
#         28: 'irregular gaps',
#         29: 'Damaged/deteriorated plants',
#         30: 'unauthorized median opening',
#         31: 'reduced sight distance(due to overgrowth)',
#         32: 'damaged kerb',
#         33: 'faded kerb painting',
#         34: 'damaged CC Guard Rails (Concrete Crash Barriers/jersey barriers)',
#         35: 'damaged MBCB Guard Rails (Metal Beam Crash Barriers)',
#         36: 'faded painting on barriers',
#         37: 'damaged signboard/sign structure',
#         38: 'signages - Day',
#         39: 'signages - Night',
#         40: 'damaged attenuators(blinkers)',
#         41: 'damaged delineators(blinkers)',
#         42: 'damaged anti-glare(blinkers)',
#         43: 'highway light damaged',
#         44: 'non functional light',
#         45: 'unauthorized signboard/hoarding',
#         46: 'inappropriate division',
#         47: 'damaged bus shelters',
#         48: 'faded bus-by markers',
#         49: 'truck lay-by markers',
#         50: 'holding boards',
#         51: 'advertisement boards',
#         52: 'work zone safety(signboard visibility)',
#         53: 'work zone safety(barricading)'
#     }

#     # Count severity per image to properly handle crack/pothole/rutting rules
#     image_severity_counts = {}
#     for _, annotation in df_annotations.iterrows():
#         image_id = annotation['image_id']
#         category_id = annotation['category_id']

#         if image_id not in image_severity_counts:
#             image_severity_counts[image_id] = {'crack': 0, 'pothole': 0, 'rutting': 0, 'low': 0}

#         if category_id == 0:
#             image_severity_counts[image_id]['crack'] += 1
#         elif category_id == 1:
#             image_severity_counts[image_id]['pothole'] += 1
#         elif category_id == 2:
#             image_severity_counts[image_id]['rutting'] += 1
#         elif category_id in range(3, 54):
#             image_severity_counts[image_id]['low'] += 1

#     # Calculate final severity counts based on per-image rules
#     severity_counts_dict = {'high': 0, 'medium': 0, 'low': 0}
#     for image_id, counts in image_severity_counts.items():
#         crack_c = counts['crack']
#         pothole_c = counts['pothole']
#         rutting_c = counts['rutting']
#         low_c = counts['low']

#         # Determine image severity
#         if (crack_c >= 3 or pothole_c >= 3 or rutting_c >= 3 or
#             (crack_c > 0 and pothole_c > 0 and rutting_c > 0)):
#             # HIGH severity for this image
#             severity_counts_dict['high'] += (crack_c + pothole_c + rutting_c)
#         elif crack_c > 0 or pothole_c > 0 or rutting_c > 0:
#             # MEDIUM severity for this image
#             severity_counts_dict['medium'] += (crack_c + pothole_c + rutting_c)

#         # Add low severity defects
#         severity_counts_dict['low'] += low_c

#     # Convert to pandas Series for compatibility
#     import pandas as pd
#     severity_counts = pd.Series(severity_counts_dict)
#     return severity_counts.to_json(), category_names


import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import json
from googlemaps import Client

def get_gmaps_key():
    
    return 'GMAPS_KEY'


def create_dir(directory):
    if not os.path.exists(directory):
        print("Making the Working Directory...")
        os.makedirs(directory)

def process_and_export_final_updated(json_path, location_list, area, output_csv_path='1_inspection.csv'):
    with open(json_path, 'r') as json_file:
        data_ = json.load(json_file)

    # Extracting annotations
    data = data_["annotations"]
    result_dict = {}
    for record in data:
        image_id = str(record['image_id'])
        category_id = str(record['category_id'])
        area_val = record['area']

        if image_id not in result_dict:
            result_dict[image_id] = {}

        if category_id in result_dict[image_id]:
            result_dict[image_id][category_id] += area_val
        else:
            result_dict[image_id][category_id] = area_val

    result_df = pd.DataFrame.from_dict(result_dict, orient='index')
    result_df.fillna(0, inplace=True)

    # --- OLD 54-category labels - OLD BHIL (commented out) ---
    # all_category_names = {
    #     0: 'Crack',
    #     1: 'pothole',
    #     2: 'rutting',
    #     3: 'Patching',
    #     4: 'rain cuts',
    #     5: 'water stagnation',
    #     6: 'pavement marking(faded)',
    #     7: 'Pavement Marking - Day',
    #     8: 'Pavement Marking - Night',
    #     9: 'missing road studs',
    #     10: 'damaged road studs',
    #     11: 'studs - Day',
    #     12: 'studs - Night',
    #     13: 'missing rumble strips',
    #     14: 'damaged rumble strips',
    #     15: 'missing hazard markers',
    #     16: 'damaged hazard markers',
    #     17: 'unevenness',
    #     18: 'edge drop of shoulders',
    #     19: 'vegetation growth of shoulders',
    #     20: 'damaged drain cover slab',
    #     21: 'missing drain cover slab',
    #     22: 'damaged foot path tiles',
    #     23: 'damaged foot path pavers',
    #     24: 'cleanliness(poor)',
    #     25: 'Encroachment',
    #     26: 'illegal parking',
    #     27: 'missing plants',
    #     28: 'irregular gaps',
    #     29: 'Damaged/deteriorated plants',
    #     30: 'unauthorized median opening',
    #     31: 'reduced sight distance(due to overgrowth)',
    #     32: 'damaged kerb',
    #     33: 'faded kerb painting',
    #     34: 'damaged CC Guard Rails (Concrete Crash Barriers/jersey barriers)',
    #     35: 'damaged MBCB Guard Rails (Metal Beam Crash Barriers)',
    #     36: 'faded painting on barriers',
    #     37: 'damaged signboard/sign structure',
    #     38: 'signages - Day',
    #     39: 'signages - Night',
    #     40: 'damaged attenuators(blinkers)',
    #     41: 'damaged delineators(blinkers)',
    #     42: 'damaged anti-glare(blinkers)',
    #     43: 'highway light damaged',
    #     44: 'non functional light',
    #     45: 'unauthorized signboard/hoarding',
    #     46: 'inappropriate division',
    #     47: 'damaged bus shelters',
    #     48: 'faded bus-by markers',
    #     49: 'truck lay-by markers',
    #     50: 'holding boards',
    #     51: 'advertisement boards',
    #     52: 'work zone safety(signboard visibility)',
    #     53: 'work zone safety(barricading)'
    # }

    # --- NEW 17-category NHAI/Khammam labels (active) ---
    all_category_names = {
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
        65: 'Missing Assets (Street Lights)'
    }

    # Rename columns to human-readable category names
    for idx, name in all_category_names.items():
        if idx in result_df.columns:
            result_df[idx] = (result_df[idx] / area).round(2)
            result_df.rename(columns={idx: name}, inplace=True)

    # --- OLD 54-category categories_mapping - OLD BHIL (commented out) ---
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
        result_df[category] = 0
        for subcat in subcats:
            subcat_name = all_category_names.get(int(subcat))
            if subcat_name and subcat_name in result_df.columns:
                result_df[category] += result_df[subcat_name]
        result_df[category] = result_df[category].round(2)

    # Adding Latitude and Longitude to result_df using the provided list
    location_dict = {entry['inference_image'].split('/')[-1]: (entry['latitude'], entry['longitude']) for entry in location_list}
    labeled_image_files = [entry['file_name'] for entry in data_["images"] if str(entry['id']) in result_dict.keys()]
    latitudes = [location_dict[img.split('/')[-1]][0] for img in labeled_image_files if img.split('/')[-1] in location_dict]
    longitudes = [location_dict[img.split('/')[-1]][1] for img in labeled_image_files if img.split('/')[-1] in location_dict]

    result_df["Latitude"] = latitudes
    result_df["Longitude"] = longitudes
    
    # Reordering the columns to ensure Latitude and Longitude are at the beginning
    columns_order = ['Latitude', 'Longitude'] + [col for col in result_df if col not in ['Latitude', 'Longitude']]
    result_df = result_df[columns_order]

    # Exporting the dataframe to CSV
    result_df.to_csv(output_csv_path)

    return result_df



import pandas as pd
from googlemaps import Client

def add_road_name_to_dataset_1(api_key, input_path, output_path):
    # Initialize the Google Maps client with your API key
    gmaps = Client(key=api_key)

    def get_road_name(lat, lon):
        try:
            result = gmaps.reverse_geocode((lat, lon), result_type="route")
            if result:
                road_name = result[0]['formatted_address']
                return road_name
            else:
                print(f"No result for {lat}, {lon}")
                return None
        except Exception as e:
            print(f"Error for {lat}, {lon}: {str(e)}")
            return None

    # Load your dataset
    data = pd.read_csv(input_path)

    # Remove the Unnamed: 0 column if it exists
    data = data.loc[:, ~data.columns.str.contains('^Unnamed')]

    # Apply the function to your dataset
    data['Road name'] = data.apply(lambda row: get_road_name(row['Latitude'], row['Longitude']), axis=1)

    # Reorder columns to have 'Road name' first, followed by 'Latitude' and 'Longitude'
    data = data[['Road name'] + [col for col in data if col != 'Road name']]
    
    # Save the dataset with the new "Road name" column and a custom header
    with open(output_path, 'w', newline='') as f:
        f.write("Roadvision AI Visual Inspection Report\n")
        data.to_csv(f, index=False)
    
    # Return the data after file operations are complete
    return data

    
# A mock call for reference:
# report_df_final = process_and_export_final('path_to_result.json', location_data, 3456)



# A mock call for reference:
# report_df_corrected = process_and_export_corrected('path_to_result.json', location_data, 3456)



# A mock call (won't execute here, but just for reference):
# report_df = process_and_export_updated('path_to_result.json', frame_data, 3456)





def process_and_export(json_path, area):
    with open(json_path, 'r') as json_file:
        data_ = json.load(json_file)

    data = data_["annotations"]
    result_dict = {}
    for record in data:
        image_id = str(record['image_id'])
        category_id = str(record['category_id'])
        area_val = record['area']

        if image_id not in result_dict:
            result_dict[image_id] = {}

        if category_id in result_dict[image_id]:
            result_dict[image_id][category_id] += area_val
        else:
            result_dict[image_id][category_id] = area_val

    result_df_1 = pd.DataFrame.from_dict(result_dict, orient='index')
    result_df_1.fillna(0, inplace=True)

    # --- OLD 54-category labels - OLD BHIL (commented out) ---
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
        if idx in result_df_1.columns:
            result_df_1[idx] = (result_df_1[idx] / area).round(2)
            result_df_1.rename(columns={idx: name}, inplace=True)

    # --- OLD 54-category categories_mapping - OLD BHIL (commented out) ---
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
        result_df_1[category] = 0
        for subcat in subcats:
            subcat_name = all_category_names.get(subcat)
            if subcat_name and subcat_name in result_df_1.columns:
                result_df_1[category] += result_df_1[subcat_name]
        result_df_1[category] = result_df_1[category].round(2)
        
    return result_df_1

def compute_final_ratings(series_input):
    # Ensure the input is a Series (e.g., max_percentages)
    series_input.index = series_input.index.str.replace('%', '').str.strip()
    df1 = series_input.to_frame(name="average")
    df1["rating"] = 0.0
    df1 = df1[df1["average"] != 0.0]

    # --- NEW 17-category NHAI/Khammam IRC rules (active) ---
    categories_rules = {
        'Cracking': (10, 10),
        'Rutting': (10, 10),
        'Stripping/Delamination': (10, 10),
        'Potholes': (0.5, 1)
    }

    for index, row in df1.iterrows():
        average = row['average']
        threshold_1, threshold_2 = categories_rules.get(index, (None, None))

        if threshold_1 is None:
            continue

        if index in ['Cracking', 'Rutting', 'Stripping/Delamination']:
            if average > threshold_1:
                df1.at[index, 'rating'] = 1
            elif 1 <= average <= threshold_1:
                df1.at[index, 'rating'] = 1 + 0.1 * (threshold_2 - average)
            else:
                df1.at[index, 'rating'] = 3.0
        elif index in ['Potholes']:
            if average > threshold_2:
                df1.at[index, 'rating'] = 0.5
            elif threshold_2 >= average >= 0.1:
                df1.at[index, 'rating'] = 1 + (threshold_2 - average)
            else:
                df1.at[index, 'rating'] = 3.0

    final_rating = round(df1["rating"].mean(), 2) if not df1.empty else 3.0
    print("Final Rating: " + str(final_rating))
    return final_rating


def draw_defect_distribution_chart(result_df):
    # Calculate the total percentages for each defect type
    # --- NEW 17-category NHAI/Khammam labels (active) ---
    percentages = [
        result_df["Potholes"].sum(),
        result_df["Cracking"].sum(),
        result_df["Rutting"].sum(),
        result_df["Stripping/Delamination"].sum()
    ]
    labels = [
        "Potholes",
        "Cracking",
        "Rutting",
        "Stripping/Delamination"
    ]
    return percentages, labels
  


### Below for bar chart
def plot_top_categories(json_path, top_n=4):
    """
    Plots the top N categories from the given JSON file.

    Parameters:
    - json_path (str): Path to the JSON file containing the data.
    - top_n (int): Number of top categories to display. Default is 4.

    Returns:
    None. The function will display a bar chart.
    """

    # Load the JSON content from the specified path
    with open(json_path, 'r') as file:
        json_content = json.load(file)

    # Convert annotations into a pandas DataFrame
    df_annotations = pd.DataFrame(json_content["annotations"])

    # --- OLD 54-category labels - OLD BHIL (commented out) ---
    # category_names = {
    #     0: 'Crack',
    #     1: 'pothole',
    #     2: 'rutting',
    #     3: 'Patching',
    #     4: 'rain cuts',
    #     5: 'water stagnation',
    #     6: 'pavement marking(faded)',
    #     7: 'Pavement Marking - Day',
    #     8: 'Pavement Marking - Night',
    #     9: 'missing road studs',
    #     10: 'damaged road studs',
    #     11: 'studs - Day',
    #     12: 'studs - Night',
    #     13: 'missing rumble strips',
    #     14: 'damaged rumble strips',
    #     15: 'missing hazard markers',
    #     16: 'damaged hazard markers',
    #     17: 'unevenness',
    #     18: 'edge drop of shoulders',
    #     19: 'vegetation growth of shoulders',
    #     20: 'damaged drain cover slab',
    #     21: 'missing drain cover slab',
    #     22: 'damaged foot path tiles',
    #     23: 'damaged foot path pavers',
    #     24: 'cleanliness(poor)',
    #     25: 'Encroachment',
    #     26: 'illegal parking',
    #     27: 'missing plants',
    #     28: 'irregular gaps',
    #     29: 'Damaged/deteriorated plants',
    #     30: 'unauthorized median opening',
    #     31: 'reduced sight distance(due to overgrowth)',
    #     32: 'damaged kerb',
    #     33: 'faded kerb painting',
    #     34: 'damaged CC Guard Rails (Concrete Crash Barriers/jersey barriers)',
    #     35: 'damaged MBCB Guard Rails (Metal Beam Crash Barriers)',
    #     36: 'faded painting on barriers',
    #     37: 'damaged signboard/sign structure',
    #     38: 'signages - Day',
    #     39: 'signages - Night',
    #     40: 'damaged attenuators(blinkers)',
    #     41: 'damaged delineators(blinkers)',
    #     42: 'damaged anti-glare(blinkers)',
    #     43: 'highway light damaged',
    #     44: 'non functional light',
    #     45: 'unauthorized signboard/hoarding',
    #     46: 'inappropriate division',
    #     47: 'damaged bus shelters',
    #     48: 'faded bus-by markers',
    #     49: 'truck lay-by markers',
    #     50: 'holding boards',
    #     51: 'advertisement boards',
    #     52: 'work zone safety(signboard visibility)',
    #     53: 'work zone safety(barricading)'
    # }

    # --- NEW 17-category NHAI/Khammam labels (active) ---
    category_names = {
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
        65: 'Missing Assets (Street Lights)'
    }

    # # UPDATED: Create a mapping of category ids to category names - 54 NHAI categories
    # category_names = {
    #     0: "Crack",
    #     1: "Damaged/deteriorated plants",
    #     2: "Encroachment",
    #     3: "Patching",
    #     4: "Pavement Marking - Day",
    #     5: "Pavement Marking - Night",
    #     6: "advertisement boards",
    #     7: "cleanliness(poor)",
    #     8: "damaged CC Guard Rails (Concrete Crash Barriers/jersey barriers)",
    #     9: "damaged MBCB Guard Rails (Metal Beam Crash Barriers)",
    #     10: "damaged anti-glare(blinkers)",
    #     11: "damaged attenuators(blinkers)",
    #     12: "damaged bus shelters",
    #     13: "damaged delineators(blinkers)",
    #     14: "damaged drain cover slab",
    #     15: "damaged foot path pavers",
    #     16: "damaged foot path tiles",
    #     17: "damaged hazard markers",
    #     18: "damaged kerb",
    #     19: "damaged road studs",
    #     20: "damaged rumble strips",
    #     21: "damaged signboard/sign structure",
    #     22: "edge drop of shoulders",
    #     23: "faded bus-by markers",
    #     24: "faded kerb painting",
    #     25: "faded painting on barriers",
    #     26: "highway light damaged",
    #     27: "holding boards",
    #     28: "illegal parking",
    #     29: "inappropriate division",
    #     30: "irregular gaps",
    #     31: "missing drain cover slab",
    #     32: "missing hazard markers",
    #     33: "missing plants",
    #     34: "missing road studs",
    #     35: "missing rumble strips",
    #     36: "non functional light",
    #     37: "pavement marking(faded)",
    #     38: "pothole",
    #     39: "rain cuts",
    #     40: "reduced sight distance(due to overgrowth)",
    #     41: "rutting",
    #     42: "signages - Day",
    #     43: "signages - Night",
    #     44: "studs - Day",
    #     45: "studs - Night",
    #     46: "truck lay-by markers",
    #     47: "unauthorized median opening",
    #     48: "unauthorized signboard/hoarding",
    #     49: "unevenness",
    #     50: "vegetation growth of shoulders",
    #     51: "water stagnation",
    #     52: "work zone safety(barricading)",
    #     53: "work zone safety(signboard visibility)"
    # }

    # Distribution of Annotations by Category
    category_counts = df_annotations['category_id'].value_counts().reset_index()
    category_counts.columns = ['category_id', 'count']
    category_counts['category_name'] = category_counts['category_id'].map(category_names)

    # Sort and select top N categories
    category_counts = category_counts.sort_values(by='count', ascending=False)

    # Colors for bars
    colors = ['red', 'yellow', 'blue', 'green'][:top_n]
    return category_counts
    


def plot_severity_distribution(json_path):
    """
    Plots the distribution of annotations by severity from the given JSON file.

    Parameters:
    - json_path (str): Path to the JSON file containing the data.

    Returns:
    None. The function will display a pie chart.
    """

    # Load the JSON content from the specified path
    with open(json_path, 'r') as file:
        json_content = json.load(file)

    # Convert annotations into a pandas DataFrame
    df_annotations = pd.DataFrame(json_content["annotations"])

    # --- OLD 54-category labels - OLD BHIL (commented out) ---
    # category_names = {
    #     0: 'Crack',
    #     1: 'pothole',
    #     2: 'rutting',
    #     3: 'Patching',
    #     4: 'rain cuts',
    #     5: 'water stagnation',
    #     6: 'pavement marking(faded)',
    #     7: 'Pavement Marking - Day',
    #     8: 'Pavement Marking - Night',
    #     9: 'missing road studs',
    #     10: 'damaged road studs',
    #     11: 'studs - Day',
    #     12: 'studs - Night',
    #     13: 'missing rumble strips',
    #     14: 'damaged rumble strips',
    #     15: 'missing hazard markers',
    #     16: 'damaged hazard markers',
    #     17: 'unevenness',
    #     18: 'edge drop of shoulders',
    #     19: 'vegetation growth of shoulders',
    #     20: 'damaged drain cover slab',
    #     21: 'missing drain cover slab',
    #     22: 'damaged foot path tiles',
    #     23: 'damaged foot path pavers',
    #     24: 'cleanliness(poor)',
    #     25: 'Encroachment',
    #     26: 'illegal parking',
    #     27: 'missing plants',
    #     28: 'irregular gaps',
    #     29: 'Damaged/deteriorated plants',
    #     30: 'unauthorized median opening',
    #     31: 'reduced sight distance(due to overgrowth)',
    #     32: 'damaged kerb',
    #     33: 'faded kerb painting',
    #     34: 'damaged CC Guard Rails (Concrete Crash Barriers/jersey barriers)',
    #     35: 'damaged MBCB Guard Rails (Metal Beam Crash Barriers)',
    #     36: 'faded painting on barriers',
    #     37: 'damaged signboard/sign structure',
    #     38: 'signages - Day',
    #     39: 'signages - Night',
    #     40: 'damaged attenuators(blinkers)',
    #     41: 'damaged delineators(blinkers)',
    #     42: 'damaged anti-glare(blinkers)',
    #     43: 'highway light damaged',
    #     44: 'non functional light',
    #     45: 'unauthorized signboard/hoarding',
    #     46: 'inappropriate division',
    #     47: 'damaged bus shelters',
    #     48: 'faded bus-by markers',
    #     49: 'truck lay-by markers',
    #     50: 'holding boards',
    #     51: 'advertisement boards',
    #     52: 'work zone safety(signboard visibility)',
    #     53: 'work zone safety(barricading)'
    # }

    # --- NEW 17-category NHAI/Khammam labels (active) ---
    category_names = {
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
        65: 'Missing Assets (Street Lights)'
    }

    # # UPDATED: Create a mapping of category ids to category names - 54 NHAI categories
    # category_names = {
    #     0: "Crack",
    #     1: "Damaged/deteriorated plants",
    #     2: "Encroachment",
    #     3: "Patching",
    #     4: "Pavement Marking - Day",
    #     5: "Pavement Marking - Night",
    #     6: "advertisement boards",
    #     7: "cleanliness(poor)",
    #     8: "damaged CC Guard Rails (Concrete Crash Barriers/jersey barriers)",
    #     9: "damaged MBCB Guard Rails (Metal Beam Crash Barriers)",
    #     10: "damaged anti-glare(blinkers)",
    #     11: "damaged attenuators(blinkers)",
    #     12: "damaged bus shelters",
    #     13: "damaged delineators(blinkers)",
    #     14: "damaged drain cover slab",
    #     15: "damaged foot path pavers",
    #     16: "damaged foot path tiles",
    #     17: "damaged hazard markers",
    #     18: "damaged kerb",
    #     19: "damaged road studs",
    #     20: "damaged rumble strips",
    #     21: "damaged signboard/sign structure",
    #     22: "edge drop of shoulders",
    #     23: "faded bus-by markers",
    #     24: "faded kerb painting",
    #     25: "faded painting on barriers",
    #     26: "highway light damaged",
    #     27: "holding boards",
    #     28: "illegal parking",
    #     29: "inappropriate division",
    #     30: "irregular gaps",
    #     31: "missing drain cover slab",
    #     32: "missing hazard markers",
    #     33: "missing plants",
    #     34: "missing road studs",
    #     35: "missing rumble strips",
    #     36: "non functional light",
    #     37: "pavement marking(faded)",
    #     38: "pothole",
    #     39: "rain cuts",
    #     40: "reduced sight distance(due to overgrowth)",
    #     41: "rutting",
    #     42: "signages - Day",
    #     43: "signages - Night",
    #     44: "studs - Day",
    #     45: "studs - Night",
    #     46: "truck lay-by markers",
    #     47: "unauthorized median opening",
    #     48: "unauthorized signboard/hoarding",
    #     49: "unevenness",
    #     50: "vegetation growth of shoulders",
    #     51: "water stagnation",
    #     52: "work zone safety(barricading)",
    #     53: "work zone safety(signboard visibility)"
    # }

    # Count severity per image to properly handle crack/pothole/rutting rules
    image_severity_counts = {}
    for _, annotation in df_annotations.iterrows():
        image_id = annotation['image_id']
        category_id = annotation['category_id']

        if image_id not in image_severity_counts:
            image_severity_counts[image_id] = {'crack': 0, 'pothole': 0, 'rutting': 0, 'low': 0}

        # OLD: Wrong severity logic with OLD BHIL IDs
        # In new mapping: 0=Potholes, 1=Cracking, 2=Rutting
        if category_id == 0:
            image_severity_counts[image_id]['pothole'] += 1
        elif category_id == 1:
            image_severity_counts[image_id]['crack'] += 1
        elif category_id == 2:
            image_severity_counts[image_id]['rutting'] += 1
        elif category_id in range(3, 66):
            image_severity_counts[image_id]['low'] += 1

        # # UPDATED: Correct severity logic with 54 NHAI IDs
        # if category_id == 0:
        #     image_severity_counts[image_id]['crack'] += 1
        # elif category_id == 38:
        #     image_severity_counts[image_id]['pothole'] += 1
        # elif category_id == 41:
        #     image_severity_counts[image_id]['rutting'] += 1
        # elif 1 <= category_id <= 53:
        #     image_severity_counts[image_id]['low'] += 1

    # Calculate final severity counts based on per-image rules
    severity_counts_dict = {'high': 0, 'medium': 0, 'low': 0}
    for image_id, counts in image_severity_counts.items():
        crack_c = counts['crack']
        pothole_c = counts['pothole']
        rutting_c = counts['rutting']
        low_c = counts['low']

        # Determine image severity
        if (crack_c >= 3 or pothole_c >= 3 or rutting_c >= 3 or
            (crack_c > 0 and pothole_c > 0 and rutting_c > 0)):
            # HIGH severity for this image
            severity_counts_dict['high'] += (crack_c + pothole_c + rutting_c)
        elif crack_c > 0 or pothole_c > 0 or rutting_c > 0:
            # MEDIUM severity for this image
            severity_counts_dict['medium'] += (crack_c + pothole_c + rutting_c)

        # Add low severity defects
        severity_counts_dict['low'] += low_c

    # Convert to pandas Series for compatibility
    import pandas as pd
    severity_counts = pd.Series(severity_counts_dict)
    return severity_counts.to_json(), category_names
