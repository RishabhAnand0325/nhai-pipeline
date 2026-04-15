# print("[*] Starting Unified GCP Data Annotation Pipeline (Full Business Logic)")
# import json
# import os
# import cv2
# import threading
# import time
# import requests
# import re
# import matplotlib.pyplot as plt
# import numpy as np
# import pandas as pd
# import geopy.distance
# from google.cloud import storage
# from datetime import datetime

# # Import custom modules (Business Logic Preserved)
# from helpers import *
# from reports import *
# from severity import *
# from chainage import *
# from dashboard import *

# # ---------------------------------------------------
# # 1. CONFIGURATION & GCP CLIENT INITIALIZATION
# # ---------------------------------------------------
# storage_client = storage.Client()

# # GCP Backend and Bucket Configs
# BASE_URL = "https://roadvision-backend-505717192876.asia-south1.run.app/"
# BUCKET_NAME = "codepipeline-ap-south-1-1510246084881" # Artifacts
# UPLOAD_BUCKET = "roadvisionvideoframes1"              # Input JSON
# NEW_BUCKET_GCS = 'roadvisionai1'                      # Public Assets

# working_directory = "./predict"
# og_image_file_directory = "./frames"
# create_dir(working_directory)
# create_dir(og_image_file_directory)

# # ---------------------------------------------------
# # 2. BUSINESS LOGIC MAPS & CONSTANTS
# # ---------------------------------------------------
# category_information = {
#     0: 'Corrugation', 1: 'Crown Loss', 2: 'Gravel Loss', 3: 'Loose Material',
#     4: 'Potholes', 5: 'Rutting', 6: 'Scouring', 7: 'Settlement',
#     8: 'Slippery Surface', 9: 'Surface Heaving'
# }

# colors = {
#     "Corrugation": (0, 255, 22), "Crown Loss": (123, 225, 255),
#     "Gravel Loss": (237, 254, 151), "Loose Material": (172, 255, 254),
#     "Potholes": (244, 255, 151), "Rutting": (255, 143, 6),
#     "Scouring": (155, 155, 255), "Settlement": (255, 0, 0),
#     "Slippery Surface": (0, 255, 255), "Surface Heaving": (255, 255, 0)
# }

# defect_severity_map = {
#     "Corrugation": "Medium", "Crown Loss": "Medium", "Gravel Loss": "Low",
#     "Loose Material": "Low", "Potholes": "High", "Rutting": "High",
#     "Scouring": "High", "Settlement": "High", "Slippery Surface": "High",
#     "Surface Heaving": "High"
# }

# severity_order = {"none": 0, "low": 1, "medium": 2, "high": 3}

# # Font settings for drawing
# font = cv2.FONT_HERSHEY_TRIPLEX
# font_scale = 0.5
# light_thickness = 1
# text_color_black = (0, 0, 0)
# thick_thickness = 3

# class NpEncoder(json.JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, np.integer): return int(obj)
#         if isinstance(obj, np.floating): return float(obj)
#         if isinstance(obj, np.ndarray): return obj.tolist()
#         return super(NpEncoder, self).default(obj)

# # ---------------------------------------------------
# # 3. HELPER FUNCTIONS
# # ---------------------------------------------------
# def download_gcs_file(bucket_name, blob_name, target_path):
#     try:
#         bucket = storage_client.bucket(bucket_name)
#         blob = bucket.blob(blob_name)
#         blob.download_to_filename(target_path)
#         print(f"Downloaded: {blob_name}")
#     except Exception as e:
#         print(f"Failed to download {blob_name}: {str(e)}")

# def download_all_images(images_data, build_id):
#     # Logic adapted to download both Inference and Original frames using Threading
#     # Uses 'build_id' to construct the path (matching your skeleton)
#     for image in images_data['inferences']['data']['frame_list_data']:
#         while(threading.active_count() > 100): 
#             print("Thread count > 100, waiting...")
#             time.sleep(1)
            
#         inf_fn = image['inference_image'].split('/')[-1]
#         og_fn = image['og_file'].split('/')[-1]
        
#         # Mapping S3 structure to GCS structure
#         inf_gcs = f'videoModelOut/{build_id}/artifacts/frame_data/predict/{inf_fn}'
#         og_image_gcs = f'videoModelOut/{build_id}/artifacts/frame_data/frames/{og_fn}'
        
#         threading.Thread(target=download_gcs_file, args=[BUCKET_NAME, inf_gcs, 'predict/'+inf_fn]).start()
#         threading.Thread(target=download_gcs_file, args=[BUCKET_NAME, og_image_gcs, 'frames/'+og_fn]).start()

# def draw_annotation_and_store_data_cv2(image, bbox, category_name):
#     # Original CV2 drawing logic
#     x, y, w, h = bbox
#     x, y, w, h = map(int, [x, y, w, h])
#     x2, y2 = x + w, y + h

#     color = colors.get(category_name, (255, 255, 255))
#     cv2.rectangle(image, (x, y), (x2, y2), color, 2)
    
#     font = cv2.FONT_HERSHEY_SIMPLEX
#     text_size = cv2.getTextSize(category_name, font, 0.5, 1)
#     text_width, text_height = text_size[0]
#     label_bg_y1 = y - text_height - 5

#     cv2.rectangle(image, (x, label_bg_y1), (x + text_width, y), color, -1)
#     cv2.putText(image, category_name, (x, y - 7), font, 0.5, (0, 0, 0), 1)

# # ---------------------------------------------------
# # 4. INITIALIZATION & DATA LOADING
# # ---------------------------------------------------
# result_consolidated = "./result.json"

# # Strict GCP Check
# if 'GCS_KEY' in os.environ:
#     gcs_key = os.environ["GCS_KEY"]
#     print(f"[*] GCS_KEY detected: {gcs_key}. Attempting download...")
#     download_gcs_file(UPLOAD_BUCKET, gcs_key, result_consolidated)
# else:
#     print("❌ CRITICAL: GCS_KEY environment variable not found. Pipeline cannot continue.")
#     exit(1)

# if not os.path.exists(result_consolidated):
#     print(f"❌ CRITICAL ERROR: {result_consolidated} not found.")
#     exit(1)

# with open(result_consolidated, 'r') as f:
#     data_merge = json.load(f)
# data = data_merge[0]

# # NTC FIX: Filter invalid annotations
# valid_image_ids = {img['id'] for img in data.get('images', [])}
# data['annotations'] = [ann for ann in data.get('annotations', []) if ann.get('image_id') in valid_image_ids]

# # ---------------------------------------------------
# # BUILD_ID EXTRACTION (CLOUD BUILD COMPATIBLE)
# # ---------------------------------------------------
# BUILD_ID = ""
# filename_to_id_hash = {}

# # 1. Try to extract from Image Filenames using Regex (Robust method)
# for image in data["images"]:
#     if BUILD_ID == "":
#         match = re.search(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', image["file_name"])
#         if match:
#             BUILD_ID = match.group(0)
#             print(f"BUILD_ID Extracted from filename: {BUILD_ID}")
    
#     # Map for later processing
#     if image["file_name"] not in filename_to_id_hash:
#         filename_to_id_hash[image["file_name"]] = [image["id"]]
#     else:
#         filename_to_id_hash[image["file_name"]].append(image["id"])

# # 2. Fallback to GCS_KEY if regex failed
# if not BUILD_ID and 'GCS_KEY' in os.environ:
#     BUILD_ID = os.environ["GCS_KEY"].split('/')[0]
#     print(f"BUILD_ID Extracted from GCS_KEY: {BUILD_ID}")

# # Fetch Metadata from Backend
# print(f"[*] Fetching Inference Data for Build ID: {BUILD_ID}")
# res = requests.post(BASE_URL + "webApi/project/get_inference_from_codebuildid",
#                     data={"codebuildid": BUILD_ID, "secret_key_value": "sleeksitescodepipeline"})

# if res.status_code == 200 and res.json().get("status") == "success":
#     frame_data = res.json()["result"]["inferences"]["data"]["frame_list_data"]
#     inference_id = res.json()["result"]["inferences"]["_id"]["$oid"]
# else:
#     print(f"Build ID Not Found Error. Status: {res.status_code}, Response: {res.text}")
#     exit(1)

# # ---------------------------------------------------
# # 5. CORE LOGIC: CHAINAGE & DEFECTS
# # ---------------------------------------------------
# api_key = get_gmaps_key()
# df = pd.DataFrame(frame_data)
# road_length, distances = calculate_total_distance(frame_data)
# road_length = round(road_length, 2)
# df = calculate_chainage(df, distances)

# # Map annotations to frames
# annotations_map = {}
# for img in data['images']:
#     fn = img['file_name'].split('/')[-1]
#     annotations_map[fn] = [a for a in data['annotations'] if a['image_id'] == img['id']]

# df['annotation'] = df['og_file'].apply(lambda x: annotations_map.get(x.split('/')[-1], []))

# # Process defects (Category mapping)
# categories_mapping = {k: [str(i) for i, v in category_information.items() if v == k] for k in defect_severity_map.keys()}
# df = process_defect_types(df, categories_mapping, category_information, severity_order)

# # Ensure columns exist
# for defect_type in defect_severity_map.keys():
#     if defect_type not in df.columns: df[defect_type] = 0
#     # Percentage calculation logic
#     count_column = defect_type + '_Count'
#     if count_column in df.columns:
#         df[defect_type + '%'] = (df[count_column] * 10)
#     else:
#         df[defect_type + '%'] = 0

# # ---------------------------------------------------
# # 6. IRC RATING LOGIC (Original Loop)
# # ---------------------------------------------------
# irc_ratings = []
# temp_dist, temp_chunk = 0, []
# prev_lat, prev_lon = None, None

# for idx, row in df.iterrows():
#     if prev_lat is not None:
#         temp_dist += geopy.distance.geodesic((prev_lat, prev_lon), (row['latitude'], row['longitude'])).meters
#     temp_chunk.append(row)
    
#     if temp_dist >= 10:
#         h, m, l = 0, 0, 0
#         for r in temp_chunk:
#             for d, s in defect_severity_map.items():
#                 if r.get(f"{d}_Count", 0) > 0:
#                     if s == "High": h += 1
#                     elif s == "Medium": m += 1
#                     else: l += 1
#         rating = compute_condition_index(h, m, l)
#         irc_ratings.extend([rating] * len(temp_chunk))
#         temp_dist, temp_chunk = 0, []
#     prev_lat, prev_lon = row['latitude'], row['longitude']

# if temp_chunk: irc_ratings.extend([compute_condition_index(0,0,0)] * len(temp_chunk))

# # Pad mismatch
# if len(irc_ratings) < len(df):
#     irc_ratings.extend([irc_ratings[-1]] * (len(df) - len(irc_ratings)))

# df['IRC_rating'] = [min(max(100 - int(round((abs(r) * 100 / 5))), 0), 100) for r in irc_ratings[:len(df)]]
# road_rating = int(round(df['IRC_rating'].mean()))

# # ---------------------------------------------------
# # 7. HOMOGRAPHY & CSV GENERATION
# # ---------------------------------------------------
# # Standard Homography Points
# image_points = np.array([[700, 100], [700, 850], [520, 300], [720, 400]], dtype=np.float32)
# real_world_points = np.array([[0, 0], [0, 3.5], [1, 0], [1, 3.5]], dtype=np.float32)
# H, _ = cv2.findHomography(image_points, real_world_points)

# # Grouped Chainage Report
# df_grouped = df.groupby('chainage').agg({'latitude': ['first', 'last'], 'longitude': ['first', 'last']}).reset_index()
# df_grouped.columns = ['Chainage', 'start_latitude', 'end_latitude', 'start_longitude', 'end_longitude']

# # Re-aggregate counts for the grouped DF
# grouped = df.groupby('chainage')
# for defect_type in defect_severity_map.keys():
#     count_column = defect_type + '_Count'
#     if count_column in df.columns:
#         df_grouped[count_column] = grouped[count_column].sum()
#         # Max severity
#         sev_col = defect_type + '_Severity'
#         if sev_col in df.columns:
#             df_grouped[sev_col] = grouped[sev_col].agg(lambda x: max(x, key=lambda v: severity_order.get(v, 0)))

# df_grouped['PCI'] = df_grouped.apply(calculate_pci, axis=1)
# df_grouped['start_address'] = df_grouped.apply(lambda r: get_address(api_key, r['start_latitude'], r['start_longitude']), axis=1)
# df_grouped['end_address'] = df_grouped.apply(lambda r: get_address(api_key, r['end_latitude'], r['end_longitude']), axis=1)

# # Format grouped report columns
# df_grouped = df_grouped.rename(columns={'chainage': 'Chainage'})
# df_grouped['Chainage'] = range(0, 10 * len(df_grouped), 10)

# # Calculate Condition Index for rows
# def calculate_condition_index_for_row(row):
#     high_count = 0
#     medium_count = 0
#     low_count = 0
#     for defect, severity in defect_severity_map.items():
#         count_column = defect + '_Count'
#         if count_column in row and row[count_column] > 0:
#             if severity == "High": high_count += row[count_column]
#             elif severity == "Medium": medium_count += row[count_column]
#             elif severity == "Low": low_count += row[count_column]
#     total_defects = high_count + medium_count + low_count
#     if total_defects == 0: return 0.0
#     total_severity_score = (high_count * 3) + (medium_count * 2) + (low_count * 1)
#     max_possible_score = total_defects * 3
#     condition_index = (total_severity_score / max_possible_score) * 5
#     return round(condition_index, 2)

# df_grouped['Condition_Index'] = df_grouped.apply(calculate_condition_index_for_row, axis=1)

# # Save Chainage Report locally
# df_grouped.to_csv("chainage_report.csv", index=False)

# # ---------------------------------------------------
# # 8. IMAGE PROCESSING & DRAWING
# # ---------------------------------------------------
# download_all_images(res.json()["result"], BUILD_ID)
# time.sleep(5) # Wait for downloads

# # Load Category ID Map
# categories_list = data.get('categories', [])
# category_id_to_name = {cat['id']: cat['name'] for cat in categories_list}

# # Process frames: Draw boxes + Update metadata
# for unique_image in filename_to_id_hash.keys():
#     local_file_name = os.path.join(working_directory, unique_image.split('/')[-1])
    
#     # Identify frame index
#     try:
#         frame_idx = int(unique_image.split("/")[-1].split(".")[0].split("_")[-1])
#     except:
#         continue

#     temp_inference_data = []
#     final_severity = None

#     if os.path.exists(local_file_name):
#         img = cv2.imread(local_file_name)
#         if img is not None:
#             # Find annotations for this image
#             for image_id in filename_to_id_hash[unique_image]:
#                 for ann in data['annotations']:
#                     if ann['image_id'] == image_id:
#                         # Draw
#                         cat_id = ann.get('category_id')
#                         label = category_id_to_name.get(cat_id, ann.get('category_name', 'Defect'))
#                         bbox = ann['bbox']
                        
#                         draw_annotation_and_store_data_cv2(img, bbox, label)
                        
#                         # Logic: Track highest severity
#                         sev = defect_severity_map.get(label, "Nill")
#                         sev_rank = {"High": 3, "Medium": 2, "Low": 1, "Nill": 0}
#                         if final_severity is None or sev_rank.get(sev, 0) > sev_rank.get(final_severity, 0):
#                             final_severity = sev
                        
#                         temp_inference_data.append({"label": label, "bbox": bbox, "severity": sev})
            
#             # Save modified image
#             cv2.imwrite(local_file_name, img)
            
#             # Update Frame Data structure
#             if frame_idx < len(frame_data):
#                 frame_data[frame_idx]["defect_state"] = final_severity
#                 frame_data[frame_idx]["inference_info"] = temp_inference_data

# # ---------------------------------------------------
# # 9. FINAL REPORTS & UPLOAD (GCP)
# # ---------------------------------------------------
# # Safety Report
# pd.DataFrame(frame_data)[['og_file', 'latitude', 'longitude']].to_csv("saf.csv")

# # Homography Calculation Helper
# def calculate_real_world_area(bbox, H, scaling_factor=300.764):
#     def img_to_real(x, y, H):
#         pt = np.dot(H, np.array([x, y, 1]).reshape((3, 1)))
#         return pt[0][0]/pt[2][0], pt[1][0]/pt[2][0]
    
#     x, y, w, h = bbox
#     p1 = img_to_real(x, y, H)
#     p2 = img_to_real(x+w, y, H)
#     p3 = img_to_real(x, y+h, H)
    
#     width = np.linalg.norm(np.array(p1) - np.array(p2))
#     height = np.linalg.norm(np.array(p1) - np.array(p3))
#     return width * height * scaling_factor

# # Build Distress Data with Areas
# distress_rows = []
# for ann in data['annotations']:
#     img_meta = next((i for i in data['images'] if i['id'] == ann['image_id']), None)
#     if img_meta:
#         area = calculate_real_world_area(ann['bbox'], H)
#         cat_name = category_id_to_name.get(ann['category_id'], 'Defect')
#         distress_rows.append({
#             'File_name': img_meta['file_name'],
#             'Road Distress': cat_name,
#             'Area': area,
#             'Severity': defect_severity_map.get(cat_name, 'Nill')
#         })

# # Aggregate per file for Final CSV
# if distress_rows:
#     distress_df = pd.DataFrame(distress_rows)
#     # Pivot logic simplified for robust merging
#     final_rows = []
#     for fn, group in distress_df.groupby('File_name'):
#         row = {
#             'File_name': fn,
#             'Road Distress': ", ".join(group['Road Distress'].tolist())
#         }
#         # Add areas
#         for i, a in enumerate(group['Area'].tolist()):
#             row[f'Area{i+1}'] = a
#         final_rows.append(row)
    
#     final_df_road_defect = pd.DataFrame(final_rows)
# else:
#     final_df_road_defect = pd.DataFrame(columns=['File_name', 'Road Distress'])

# # Merge with Lat/Long
# saf_df = pd.read_csv("saf.csv")
# saf_df['key'] = saf_df['og_file'].apply(lambda x: x.split('/')[-1])
# final_df_road_defect['key'] = final_df_road_defect['File_name'].apply(lambda x: x.split('/')[-1])

# final_df_road_defect = pd.merge(final_df_road_defect, saf_df[['key', 'latitude', 'longitude']], on='key', how='left')
# final_df_road_defect['File_URL'] = ""
# final_df_road_defect.rename(columns={'latitude': 'Latitude', 'longitude': 'Longitude'}, inplace=True)

# # GCP Upload
# print(f"[*] Uploading defects to {NEW_BUCKET_GCS}...")
# for index, row in final_df_road_defect.iterrows():
#     local_path = os.path.join('predict', os.path.basename(row['File_name']))
    
#     if os.path.exists(local_path):
#         blob_path = f"Defects/{BUILD_ID}/{os.path.basename(local_path)}"
#         try:
#             blob = storage_client.bucket(NEW_BUCKET_GCS).blob(blob_path)
#             blob.upload_from_filename(local_path)
            
#             public_url = f"https://storage.googleapis.com/{NEW_BUCKET_GCS}/{blob_path}"
#             final_df_road_defect.at[index, 'File_URL'] = public_url
#             print(f"Uploaded: {blob_path}")
#         except Exception as e:
#             print(f"Upload failed: {e}")

# # Cleanup for Report
# final_df_road_defect.drop(columns=['key', 'File_name'], inplace=True, errors='ignore')
# area_cols = [c for c in final_df_road_defect.columns if c.startswith('Area')]
# final_df_road_defect.drop(columns=area_cols, inplace=True)

# # Add Immediate Action
# immediate_action_map = {
#     'Corrugation': 'Light grading to smoothen the surface.', 'Crown Loss': 'Re-grading to restore the crown profile.',
#     'Gravel Loss': 'Spot re-graveling', 'Loose Material': 'Redistribute & compact',
#     'Potholes': 'Fill & compact', 'Rutting': 'Grading to remove shallow ruts.',
#     'Scouring': 'Refill scoured sections with gravel.', 'Settlement': 'Fill & compact settled areas.',
#     'Slippery Surface': 'Scarify & mix with coarse gravel', 'Surface Heaving': 'Remove & recompact'
# }
# def get_action(row):
#     acts = []
#     if isinstance(row.get('Road Distress'), str):
#         for k, v in immediate_action_map.items():
#             if k in row['Road Distress']: acts.append(v)
#     return "; ".join(acts)

# final_df_road_defect['Immediate_Action'] = final_df_road_defect.apply(get_action, axis=1)

# # CRITICAL: Save inspection.csv for Cloud Build Artifacts
# print("[*] Saving inspection.csv...")
# final_df_road_defect.to_csv("inspection.csv", index=False)

# # ---------------------------------------------------
# # 10. BACKEND UPDATE
# # ---------------------------------------------------
# plot_data = {'pie_chart2': calculate_road_defect_percentage_2(data)}

# # Dashboard Serial CSV
# df_grouped_serial = pd.read_csv("chainage_report.csv")
# df_grouped_serial.insert(0, 'Serial Number', range(1, len(df_grouped_serial) + 1))

# updated_frame_data = {
#     "frame_list_data": frame_data,
#     "category_information": category_information,
#     "BUILD_ID": BUILD_ID,
#     "NEW_BUILD_ID": BUILD_ID,
#     "report_1_key": pd.DataFrame(convert_to_json(df_grouped, road_length, road_rating, 0)["defectDetails"]).to_csv(index=False),
#     "report_2_key": df_grouped.to_csv(index=False),
#     "report_3_key": final_df_road_defect.to_csv(index=False),
#     "road_length": road_length,
#     "road_rating": road_rating,
#     "dashboard_df_csv": df_grouped_serial.to_csv(index=False),
#     "data_submitted": get_submission_date(data)
# }

# final_payload = {
#     "updated_data": json.dumps(updated_frame_data),
#     "show_inference": True,
#     "inference_id": inference_id,
#     "plot_data": json.dumps({'plots': plot_data}, cls=NpEncoder),
#     "secret_key_value": "sleeksitescodepipeline"
# }

# print("[*] Sending Final Update...")
# res = requests.post(BASE_URL + "webApi/project/update_inference_data", data=final_payload)
# print(f"✅ Pipeline Completed. Backend Status: {res.status_code}")



if __name__ == "__main__":
    print("[*] Starting Data Annotation Pipeline (GCP)")
    import json
    import os
    import cv2
    import threading
    import time
    import requests
    import re
    import math
    import numpy as np
    import pandas as pd
    import geopy.distance
    from datetime import datetime
    from google.cloud import storage as gcs_storage

    from helpers import *
    from reports import *
    from severity import *
    from chainage import *
    from dashboard import *

    from deduplication import (
        process_annotations_with_deduplication,
        get_deduplicated_chainage_counts,
        apply_deduplicated_counts_to_grouped,
        print_deduplication_summary,
        DEDUP_DISTANCE_THRESHOLD
    )

    try:
        from standalone_report_generator import generate_standalone_reports_from_pipeline
        STANDALONE_GENERATOR_AVAILABLE = True
    except:
        STANDALONE_GENERATOR_AVAILABLE = False

    # ============================================================================
    # CONFIGURATION
    # ============================================================================
    storage_client = gcs_storage.Client()

    BASE_URL        = "https://roadvision-backend-505717192876.asia-south1.run.app/"
    BUCKET_NAME     = "codepipeline-ap-south-1-1510246084881"   # Frames + artifacts
    UPLOAD_BUCKET   = "roadvisionvideoframes1"                  # Input result.json
    NEW_BUCKET_GCS  = "roadvisionai1"                          # Public defect images
    VIDEO_BUCKET    = "nhvideobucket"                          # Annotated video

    working_directory       = "./predict"
    og_image_file_directory = "./frames"
    create_dir(working_directory)
    create_dir(og_image_file_directory)

    # ============================================================================
    # COLORS — 66 NHAI categories
    # ============================================================================
    colors = {
        "Potholes": (255, 182, 193), "Cracking": (255, 192, 203),
        "Rutting": (255, 160, 180), "Stripping/Delamination": (255, 228, 225),
        "Pavement Joint": (255, 218, 185), "Pavement Damage (Severe)": (255, 200, 200),
        "Unsealed Road": (255, 235, 205), "Settlement": (255, 240, 245),
        "Shoulder - Rain Cuts": (244, 164, 96), "Shoulder - Edge Drop": (222, 184, 135),
        "Shoulder - Unevenness": (210, 180, 140), "Shoulder - Vegetation Growth": (245, 222, 179),
        "Damaged Kerb": (221, 160, 221), "Faded Kerb Painting": (218, 112, 214),
        "Reduced Visibility Due to Plantation Growth": (230, 230, 250),
        "Median Separator Damaged": (216, 191, 216), "Median Separator Paint Faded": (238, 224, 229),
        "Missing Plants / Irregular Gaps (Median)": (144, 238, 144),
        "Deteriorated or Damaged Plants (Median)": (152, 251, 152),
        "Excessive Plantation Growth": (204, 255, 204),
        "Damaged Drain Cover Slabs": (173, 216, 230), "Missing Drain Cover Slabs": (176, 224, 230),
        "Manhole Cover": (135, 206, 250), "Water Stagnation": (175, 238, 238),
        "Damaged Footpath Tiles / Paver Blocks": (211, 211, 211),
        "Damaged Crash Barriers": (192, 192, 192),
        "Damaged (MBCB) Metal Beam Crash Barrier": (220, 220, 220),
        "Damaged (PGR) Pedestrian Guard Rail": (245, 245, 245),
        "Faded Painting Concrete Crash (CC) Barrier": (176, 196, 222),
        "Barriers - Faded Painting Guard Rails": (230, 230, 230),
        "Damaged Sign Boards / Sign Structures": (255, 218, 185),
        "Signage - Poor Visibility (Day)": (255, 228, 181),
        "Signage - Poor Visibility (Night)": (255, 239, 213),
        "Damaged Blinkers": (255, 255, 224), "Damaged Attenuators": (255, 250, 205),
        "Damaged Delineators": (255, 255, 153), "Damaged Anti-Glare": (255, 255, 204),
        "Damaged Road Studs": (175, 238, 238),
        "Road Studs - Poor Visibility (Day)": (176, 224, 230),
        "Road Studs - Poor Visibility (Night)": (224, 255, 255),
        "Damaged Rumble Strips": (188, 238, 238), "Damaged Hazard Markers": (127, 255, 212),
        "Faded Pavement Marking": (255, 248, 220),
        "Pavement Marking - Poor Visibility (Day)": (250, 250, 210),
        "Pavement Marking - Poor Visibility (Night)": (238, 232, 170),
        "Bus Bay - Damaged Shelters": (255, 182, 193), "Bus Bay - Faded Markings": (255, 192, 203),
        "Bus Bay - Damaged Signages": (255, 218, 224),
        "Truck Lay By - Damaged Shelters": (255, 228, 225),
        "Truck Lay By - Faded Markings": (255, 240, 245),
        "Truck Lay By - Damaged Signages": (255, 235, 238),
        "Damaged Highway Lights": (175, 238, 238),
        "Non-Functional Highway Lights": (173, 216, 230),
        "Work Zone - Inadequate Signboard Visibility": (255, 160, 122),
        "Work Zone - Inadequate Barricading": (250, 128, 114),
        "Work Zone - Poor Diversion Arrangement / Condition": (255, 200, 180),
        "Unauthorized Median Openings": (245, 245, 220),
        "Unauthorized Signboards": (240, 230, 140), "Unauthorized Hoardings": (238, 232, 170),
        "Illegal Parking": (250, 240, 230), "General Encroachments": (255, 250, 240),
        "Cleanliness - Litter": (240, 230, 140), "Cleanliness - Debris": (189, 183, 107),
        "Missing Assets (Signages)": (245, 222, 179),
        "Missing Assets (Guard Rails)": (222, 184, 135),
        "Missing Assets (Street Lights)": (255, 248, 220),
    }

    # ============================================================================
    # JSON ENCODER
    # ============================================================================
    class NpEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, np.integer): return int(obj)
            if isinstance(obj, np.floating): return float(obj)
            if isinstance(obj, np.ndarray): return obj.tolist()
            return super(NpEncoder, self).default(obj)

    font = cv2.FONT_HERSHEY_TRIPLEX
    font_scale = 0.5
    light_thickness = 1
    text_color_black = (0, 0, 0)
    thick_thickness = 3

    # ============================================================================
    # GCS HELPERS
    # ============================================================================
    def download_gcs_file(bucket_name, blob_name, target_path):
        try:
            bucket = storage_client.bucket(bucket_name)
            bucket.blob(blob_name).download_to_filename(target_path)
            print(f"Downloaded: {blob_name}")
        except Exception as e:
            print(f"Failed to download {blob_name}: {e}")


    def upload_gcs_file(bucket_name, blob_name, local_path, content_type="image/jpeg"):
        try:
            bucket = storage_client.bucket(bucket_name)
            bucket.blob(blob_name).upload_from_filename(local_path, content_type=content_type)
            return f"https://storage.googleapis.com/{bucket_name}/{blob_name}"
        except Exception as e:
            print(f"Upload failed {local_path} → {blob_name}: {e}")
            return ""


    def download_all_images(images_data, build_id):
        for image in images_data['inferences']['data']['frame_list_data']:
            while threading.active_count() > 100:
                print("Thread count > 100, waiting...")
                time.sleep(1)
            inf_fn = image['inference_image'].split('/')[-1]
            og_fn  = image['og_file'].split('/')[-1]
            inf_gcs = f"videoModelOut/{build_id}/artifacts/frame_data/predict/{inf_fn}"
            og_gcs  = f"videoModelOut/{build_id}/artifacts/frame_data/frames/{og_fn}"
            threading.Thread(target=download_gcs_file,
                             args=[BUCKET_NAME, inf_gcs, f"predict/{inf_fn}"]).start()
            threading.Thread(target=download_gcs_file,
                             args=[BUCKET_NAME, og_gcs, f"frames/{og_fn}"]).start()

    # ============================================================================
    # DRAWING
    # ============================================================================
    def draw_finalized_bounding_boxes(img, bounding_boxes, labels):
        for bbox, lbl in zip(bounding_boxes, labels):
            x1, y1, w, h = map(int, bbox)
            x2, y2 = x1 + w, y1 + h
            color = colors.get(lbl, (255, 255, 255))
            cv2.rectangle(img, (x1, y1), (x2, y2), color, thick_thickness)
            (text_width, text_height), _ = cv2.getTextSize(lbl, font, font_scale, light_thickness)
            label_bg_y1 = y1 - text_height - 5
            cv2.rectangle(img, (x1, label_bg_y1), (x1 + text_width, y1), color, -1)
            cv2.putText(img, lbl, (x1, y1 - 7), font, font_scale, text_color_black, light_thickness)
        return img

    # ============================================================================
    # FILENAME NORMALIZER
    # ============================================================================
    def normalize_filename(file_path):
        try:
            if file_path is None or file_path == '' or pd.isna(file_path):
                return ''
            filename = str(file_path).split('/')[-1]
            filename_without_ext = os.path.splitext(filename)[0]
            return filename_without_ext.lower().strip()
        except:
            return ''

    # ============================================================================
    # 1. LOAD result.json FROM GCS
    # ============================================================================
    result_consolidated = "./result.json"

    if 'GCS_KEY' in os.environ:
        gcs_key = os.environ["GCS_KEY"]
        print(f"[*] GCS_KEY detected: {gcs_key}. Attempting download...")
        download_gcs_file(UPLOAD_BUCKET, gcs_key, result_consolidated)
    else:
        print("❌ CRITICAL: GCS_KEY environment variable not found.")
        exit(1)

    if not os.path.exists(result_consolidated):
        print(f"❌ CRITICAL: {result_consolidated} not found after download.")
        exit(1)

    merge_file = open(result_consolidated)
    data_merge = json.loads(merge_file.read())

    if isinstance(data_merge, list):
        data = data_merge[0]
    elif isinstance(data_merge, dict):
        data = data_merge
    else:
        raise ValueError(f"Unexpected data_merge type: {type(data_merge)}")

    # Filter invalid annotations
    valid_image_ids = {img['id'] for img in data.get('images', [])}
    data['annotations'] = [a for a in data.get('annotations', []) if a.get('image_id') in valid_image_ids]

    # ============================================================================
    # 2. PIPELINE CONFIG
    # ============================================================================
    pipeline_config = {"pipeline": "default"}
    full_config_data = {}
    try:
        if os.path.exists("video/data.json"):
            with open("video/data.json", "r") as config_file:
                full_config_data = json.load(config_file)
                pipeline_value = full_config_data.get("pipeline")
                pipeline_config = {
                    "pipeline": "NHAI" if pipeline_value and pipeline_value.upper() == "NHAI" else "default"
                }
        print(f"🔧 Pipeline Configuration: {pipeline_config['pipeline']}")
    except Exception as e:
        print(f"⚠️ Warning: Could not read config: {e}")

    # ============================================================================
    # 3. BUILD_ID EXTRACTION
    # ============================================================================
    CODEBUILD_BUILD_ID = ""
    filename_to_id_hash = {}

    for image in data["images"]:
        if CODEBUILD_BUILD_ID == "":
            match = re.search(
                r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',
                image["file_name"]
            )
            if match:
                CODEBUILD_BUILD_ID = match.group(0)
                print(f"CODEBUILD_BUILD_ID Extracted from filename: {CODEBUILD_BUILD_ID}")

        if image["file_name"] not in filename_to_id_hash:
            filename_to_id_hash[image["file_name"]] = [image["id"]]
        else:
            filename_to_id_hash[image["file_name"]].append(image["id"])

    if not CODEBUILD_BUILD_ID and 'GCS_KEY' in os.environ:
        CODEBUILD_BUILD_ID = os.environ["GCS_KEY"].split('/')[0]
        print(f"CODEBUILD_BUILD_ID Extracted from GCS_KEY: {CODEBUILD_BUILD_ID}")

    # ============================================================================
    # 4. FETCH FRAME DATA FROM BACKEND
    # ============================================================================
    print(f"[*] Fetching Inference Data for Build ID: {CODEBUILD_BUILD_ID}")
    result = requests.post(
        BASE_URL + "webApi/project/get_inference_from_codebuildid",
        data={"codebuildid": CODEBUILD_BUILD_ID, "secret_key_value": "sleeksitescodepipeline"}
    )

    if json.loads(result.text).get("status") == "success":
        frame_data   = json.loads(result.text)["result"]["inferences"]["data"]["frame_list_data"]
        inference_id = json.loads(result.text)["result"]["inferences"]["_id"]["$oid"]
    else:
        print(f"Codebuild Id Not Found Error. Status: {result.status_code}, Response: {result.text}")
        exit(1)

    print(result.text)

    # ============================================================================
    # 5. CHAINAGE & DEFECT PROCESSING
    # ============================================================================
    api_key = get_gmaps_key()

    df = pd.DataFrame(frame_data)
    road_length, distances = calculate_total_distance(frame_data)
    road_length = round(road_length, 2)
    df = calculate_chainage(df, distances)

    # Build annotation maps (exact match + normalized)
    results_data = data
    annotations             = {}
    annotations_by_normalized_name = {}

    for image in results_data['images']:
        file_name = image['file_name'].split('/')[-1]
        normalized_name = normalize_filename(image['file_name'])
        image_id = image['id']
        image_annotations = [a for a in results_data['annotations'] if a['image_id'] == image_id]
        annotations[file_name] = image_annotations
        annotations_by_normalized_name[normalized_name] = image_annotations

    annotation_data = []
    for og_file in df['og_file']:
        file_name = og_file.split('/')[-1]
        normalized_name = normalize_filename(og_file)
        ann = annotations.get(file_name, None)
        if ann is None or len(ann) == 0:
            ann = annotations_by_normalized_name.get(normalized_name, None)
        annotation_data.append(ann)

    df['annotation'] = annotation_data

    matched_count = sum(1 for a in annotation_data if a is not None and len(a) > 0)
    print(f"DEBUG: Matched {matched_count} out of {len(annotation_data)} images with annotations")

    categories_mapping, category_information, severity_order = initialize_mappings()
    df = process_defect_types(df, categories_mapping, category_information, severity_order)

    # ============================================================================
    # 6. DEDUPLICATION
    # ============================================================================
    print(f"\n[*] Running GPS-based defect deduplication (threshold: {DEDUP_DISTANCE_THRESHOLD}m)...")

    df, dedup_tracker, dedup_stats = process_annotations_with_deduplication(
        df, categories_mapping, category_information, severity_order,
        distance_threshold=DEDUP_DISTANCE_THRESHOLD
    )
    print_deduplication_summary(dedup_stats, category_information)

    dedup_chainage_counts = get_deduplicated_chainage_counts(
        dedup_tracker, categories_mapping, category_information
    )

    # Percentage columns
    for defect_type in categories_mapping.keys():
        df[defect_type + '%'] = df[defect_type] / 1000

    # ============================================================================
    # 7. GROUP BY CHAINAGE
    # ============================================================================
    grouped = df.groupby('chainage')

    df_grouped = grouped.agg({
        'latitude': ['first', 'last'],
        'longitude': ['first', 'last'],
    }).reset_index()
    df_grouped.columns = ['chainage', 'start_latitude', 'end_latitude', 'start_longitude', 'end_longitude']

    for defect_type in categories_mapping.keys():
        severity_column = defect_type + '_Severity'
        if severity_column in df.columns:
            df_grouped[severity_column] = grouped[severity_column].agg(
                lambda x: max(x, key=lambda v: severity_order[v])
            )
        count_column = defect_type + '_Count'
        if count_column in df.columns:
            df_grouped[count_column] = grouped[count_column].sum()
        percentage_column = defect_type + '%'
        if percentage_column in df.columns:
            df_grouped[percentage_column] = grouped[percentage_column].mean()

    # Aggregate ALL per-label columns
    for col in df.columns:
        if col.endswith('_Count') and col not in df_grouped.columns:
            df_grouped[col] = grouped[col].sum()
        elif col.endswith('_Severity') and col not in df_grouped.columns:
            df_grouped[col] = grouped[col].agg(lambda x: max(x, key=lambda v: severity_order.get(v, 0)))

    # Column ordering
    column_order = ['chainage', 'start_latitude', 'start_longitude', 'end_latitude', 'end_longitude']
    for defect_type in categories_mapping.keys():
        column_order.extend([defect_type + '_Count', defect_type + '_Severity', defect_type + '%'])
    for col in df_grouped.columns:
        if col.endswith('_Count') and col not in column_order:
            base = col[:-6]
            sev_col = f"{base}_Severity"
            if sev_col in df_grouped.columns:
                column_order.extend([col, sev_col])
    column_order = [col for col in column_order if col in df_grouped.columns]
    df_grouped = df_grouped[column_order]

    # Apply deduplicated counts
    print("[*] Replacing raw counts with deduplicated unique counts...")
    df_grouped = apply_deduplicated_counts_to_grouped(df_grouped, dedup_chainage_counts)

    # ============================================================================
    # 8. IRC RATING
    # ============================================================================
    irc_ratings = []
    temp_chunk  = []
    temp_dist   = 0
    previous_lat, previous_lon = None, None

    for idx, row in df.iterrows():
        current_lat, current_lon = row['latitude'], row['longitude']
        if previous_lat is not None:
            temp_dist += geopy.distance.geodesic(
                (previous_lat, previous_lon), (current_lat, current_lon)
            ).meters
        temp_chunk.append(row)
        if temp_dist >= 100:
            chunk_df = pd.DataFrame(temp_chunk)
            sum_percentages = chunk_df[['Potholes%', 'Stripping/Delamination%', 'Cracking%', 'Rutting%']].sum()
            max_percentages = sum_percentages.apply(lambda x: min(x, 100))
            irc_rating = compute_final_ratings_row(max_percentages)
            irc_ratings.extend([irc_rating] * len(chunk_df))
            temp_chunk = []
            temp_dist  = 0
        previous_lat, previous_lon = current_lat, current_lon

    if temp_chunk:
        chunk_df = pd.DataFrame(temp_chunk)
        sum_percentages = chunk_df[['Potholes%', 'Stripping/Delamination%', 'Cracking%', 'Rutting%']].sum()
        max_percentages = sum_percentages.apply(lambda x: min(x, 100))
        irc_ratings.extend([compute_final_ratings_row(max_percentages)] * len(chunk_df))

    if len(irc_ratings) < len(df):
        irc_ratings.extend([irc_ratings[-1]] * (len(df) - len(irc_ratings)))

    normalized_ratings = [round((abs(r) * 100 / 3), 2) for r in irc_ratings]
    df['IRC_rating'] = normalized_ratings

    df_grouped['PCI'] = df_grouped.apply(calculate_pci, axis=1)
    road_rating = round(df['IRC_rating'].mean(), 2)

    # Fill missing chainages
    import math as _math
    expected_chainages  = _math.ceil(road_length * 1000 / 100)
    current_max_chainage = df_grouped['chainage'].max() if len(df_grouped) > 0 else -1

    if current_max_chainage < expected_chainages - 1:
        print(f"WARNING: Adding missing chainages {current_max_chainage+1} to {expected_chainages-1}")
        last_lat = df_grouped.iloc[-1]['end_latitude'] if len(df_grouped) > 0 else 0
        last_lon = df_grouped.iloc[-1]['end_longitude'] if len(df_grouped) > 0 else 0
        missing_rows = []
        for mc in range(current_max_chainage + 1, expected_chainages):
            new_row = {
                'chainage': mc,
                'start_latitude': last_lat, 'end_latitude': last_lat,
                'start_longitude': last_lon, 'end_longitude': last_lon,
            }
            for col in df_grouped.columns:
                if col.endswith('_Count'):      new_row[col] = 0
                elif col.endswith('_Severity'): new_row[col] = 'none'
                elif col.endswith('%'):         new_row[col] = 0.0
                elif col == 'PCI':             new_row[col] = 100.0
            missing_rows.append(new_row)
        if missing_rows:
            df_grouped = pd.concat([df_grouped, pd.DataFrame(missing_rows)], ignore_index=True)
            df_grouped = df_grouped.sort_values('chainage').reset_index(drop=True)
            print(f"Added {len(missing_rows)} missing chainage rows")

    # ============================================================================
    # 9. ADDRESSES & CHAINAGE REPORT
    # ============================================================================
    df_grouped['start_address'] = df_grouped.apply(
        lambda r: get_address(api_key, r['start_latitude'], r['start_longitude']), axis=1)
    df_grouped['end_address'] = df_grouped.apply(
        lambda r: get_address(api_key, r['end_latitude'], r['end_longitude']), axis=1)

    # Recalculate counts from frame_data if all zero (fallback)
    defect_count_columns = [col for col in df_grouped.columns if col.endswith('_Count')]
    total_counts = sum(df_grouped[col].sum() for col in defect_count_columns if col in df_grouped.columns)

    if total_counts == 0 and frame_data:
        print("WARNING: All defect counts are zero. Recalculating from frame_data inference_info...")
        frame_data_map = {}
        for idx, frame in enumerate(frame_data):
            if 'og_file' in frame:
                frame_data_map[normalize_filename(frame['og_file'])] = idx

        for idx, row in df.iterrows():
            og_file = row.get('og_file', '')
            if not og_file:
                continue
            frame_idx = frame_data_map.get(normalize_filename(og_file))
            if frame_idx is not None and 'inference_info' in frame_data[frame_idx]:
                for inf in (frame_data[frame_idx]['inference_info'] or []):
                    label = inf.get('label', '')
                    severity = inf.get('severity', 'none')
                    if not label:
                        continue
                    label_key = _sanitize_label_to_key(label)
                    lcc = f"{label_key}_Count"
                    lsc = f"{label_key}_Severity"
                    if lcc in df.columns:
                        df.loc[idx, lcc] += 1
                    if lsc in df.columns:
                        curr = df.loc[idx, lsc]
                        if severity_order.get(severity, 0) > severity_order.get(curr, 0):
                            df.loc[idx, lsc] = severity
                    label_lower = label.lower()
                    for grp, kw in [('Potholes', 'pothole'), ('Cracking', 'crack'),
                                      ('Rutting', 'rut'), ('Stripping/Delamination', 'stripping')]:
                        if kw in label_lower:
                            if f'{grp}_Count' in df.columns:
                                df.loc[idx, f'{grp}_Count'] += 1
                            if f'{grp}_Severity' in df.columns:
                                curr = df.loc[idx, f'{grp}_Severity']
                                if severity_order.get(severity, 0) > severity_order.get(curr, 0):
                                    df.loc[idx, f'{grp}_Severity'] = severity

        grouped = df.groupby('chainage')
        for col in df.columns:
            if col.endswith('_Count') and col in df_grouped.columns:
                df_grouped[col] = grouped[col].sum()
            elif col.endswith('_Severity') and col in df_grouped.columns:
                df_grouped[col] = grouped[col].agg(
                    lambda x: max(x, key=lambda v: severity_order.get(v, 0))
                )
        total_after = sum(df_grouped[col].sum() for col in defect_count_columns if col in df_grouped.columns)
        print(f"Recalculated defect counts from frame_data: {total_after} total defects")

    df_grouped.to_csv("chainage_report.csv", index=False)

    # Format chainage ranges
    df_grouped = df_grouped.rename(columns={'chainage': 'Chainage'})
    total_road_length_m = int(round(road_length * 1000))
    chainage_values = []
    for idx in range(len(df_grouped)):
        start = idx * 100
        end = total_road_length_m if idx == len(df_grouped) - 1 else (idx + 1) * 100
        chainage_values.append(f"{start}-{end}m")
    df_grouped['Chainage'] = chainage_values

    if 'Stripping/Delamination_Count' in df_grouped.columns and 'Stripping/Delamination_Severity' in df_grouped.columns:
        df_grouped['Stripping/Delamination_Severity'] = df_grouped.apply(
            lambda row: 'medium' if row['Stripping/Delamination_Count'] > 0
            else row['Stripping/Delamination_Severity'], axis=1)

    # Column ordering
    base_cols = ['Chainage', 'start_address', 'start_latitude', 'start_longitude',
                 'end_address', 'end_latitude', 'end_longitude']
    columns_order = list(base_cols)
    for col in df_grouped.columns:
        if col.endswith('_Count'):
            base = col[:-6]
            sev_col = f"{base}_Severity"
            if col not in columns_order:
                columns_order.append(col)
            if sev_col in df_grouped.columns and sev_col not in columns_order:
                columns_order.append(sev_col)
    if 'PCI' in df_grouped.columns:
        columns_order.append('PCI')
    columns_order = [c for c in columns_order if c in df_grouped.columns]
    df_grouped = df_grouped[columns_order]
    df_grouped['PCI'] = df_grouped['PCI'].round(2)
    if 'IRC_rating' in df.columns:
        df.drop(columns=['IRC_rating'], inplace=True)

    file_path = 'chainage_report.csv'
    df_grouped.to_csv(file_path, index=False)
    print(road_rating)

    df_serail = pd.read_csv(file_path)
    df_serail.insert(0, 'Serial Number', range(1, len(df_serail) + 1))
    rename_dict = {col: col.replace(' ', '_') for col in df_serail.columns if ' ' in col}
    if rename_dict:
        df_serail.rename(columns=rename_dict, inplace=True)
    df_serail['start_address'] = df_serail['start_address'].str.replace(',', ';').str.replace(';', '|')
    df_serail['end_address']   = df_serail['end_address'].str.replace(',', ';').str.replace(';', '|')
    for col in [c for c in df_serail.columns if 'Severity' in c]:
        df_serail[col] = df_serail[col].replace('none', 'Nill')
    print("IRC CSV Report Generated!")

    # ============================================================================
    # 10. STANDALONE VIDEO (pre-processing, runs before main drawing loop)
    # ============================================================================
    try:
        if STANDALONE_GENERATOR_AVAILABLE:
            print("\n[STANDALONE] Waiting for frame downloads...")
            _wait_start = time.time()
            while threading.active_count() > 5 and (time.time() - _wait_start) < 60:
                time.sleep(1)
            try:
                _frames_dir = './frames'
                if not os.path.exists(_frames_dir) or len(os.listdir(_frames_dir)) == 0:
                    _frames_dir = './predict'
                from standalone_report_generator import generate_video_only
                _video_result = generate_video_only(
                    result_json_path=result_consolidated,
                    frames_dir=_frames_dir,
                    upload_to_s3=False   # GCP: set to False, handled by upload_video_to_gcs below
                )
                if _video_result:
                    print(f"[STANDALONE] Video generated: {_video_result}")
            except Exception as _e:
                print(f"[STANDALONE] Video generation failed (ignored): {_e}")
    except:
        pass

    # ============================================================================
    # 11. PLOT DATA
    # ============================================================================
    plot_data = {'pie_chart2': calculate_road_defect_percentage_2(data)}
    data_submitted = get_submission_date(data)

    # ============================================================================
    # 12. DOWNLOAD IMAGES
    # ============================================================================
    download_all_images(json.loads(result.text)["result"], CODEBUILD_BUILD_ID)
    time.sleep(5)

    # ============================================================================
    # 13. DRAWING BOUNDING BOXES
    # ============================================================================
    category_id_to_name = {cat['id']: cat['name'] for cat in data.get('categories', [])}

    for unique_image in filename_to_id_hash.keys():
        local_file_name = os.path.join(working_directory, unique_image.split('/')[-1])
        try:
            image_frame_index = int(unique_image.split("/")[-1].split(".")[0].split("_")[-1])
        except:
            continue

        temp_inference_data = []
        crack_count = pothole_count = rutting_count = 0

        for image_id in filename_to_id_hash[unique_image]:
            for annotation in data["annotations"]:
                if annotation["image_id"] == image_id:
                    cid = annotation["category_id"]
                    if cid == 1:   crack_count   += 1
                    elif cid == 0: pothole_count += 1
                    elif cid == 2: rutting_count += 1

        frame_severity = 'none'
        if (crack_count >= 3 or pothole_count >= 3 or rutting_count >= 3 or
                (crack_count > 0 and pothole_count > 0 and rutting_count > 0)):
            frame_severity = 'high'
        elif crack_count > 0 or pothole_count > 0 or rutting_count > 0:
            frame_severity = 'medium'

        for image_id in filename_to_id_hash[unique_image]:
            for annotation in data["annotations"]:
                if annotation["image_id"] != image_id:
                    continue

                if not os.path.exists(local_file_name):
                    base = os.path.splitext(local_file_name)[0]
                    alt  = base + ('.jpg' if local_file_name.lower().endswith('.png') else '.png')
                    frames_candidate = og_image_file_directory + "/" + unique_image.split('/')[-1]
                    if os.path.exists(alt):
                        local_file_name = alt
                    elif os.path.exists(frames_candidate):
                        local_file_name = frames_candidate
                    else:
                        print(f"Error: {local_file_name} does not exist.")
                        continue

                downloaded_image = cv2.imread(local_file_name)
                if downloaded_image is None:
                    print(f"Error: Failed to read {local_file_name}.")
                    continue

                cid        = annotation["category_id"]
                label_full = category_id_to_name.get(cid, category_information.get(cid, f"Category_{cid}"))
                label      = label_full

                if cid in [0, 1, 2]:
                    severity = frame_severity
                elif 3 <= cid <= 65:
                    severity = 'low'
                else:
                    severity = 'none'

                bbbox = annotation["bbox"]
                temp_inference_data.append({"label": str(label), "bbox": bbbox, "severity": severity})

                downloaded_image = draw_finalized_bounding_boxes(downloaded_image, [bbbox], [label_full])
                cv2.imwrite(local_file_name, downloaded_image)

        if image_frame_index < len(frame_data):
            frame_data[image_frame_index]["defect_state"]   = frame_severity
            frame_data[image_frame_index]["inference_info"] = temp_inference_data

    # ============================================================================
    # 14. REPORT 1 KEY
    # ============================================================================
    data_csv = pd.read_csv("chainage_report.csv")
    defect_columns = [col for col in data_csv.columns if col.endswith('_Count')]
    total_defects  = int(data_csv[defect_columns].sum().sum())

    print(f"\n[DEDUPLICATION RESULTS]")
    print(f"  Raw: {dedup_stats['raw_total']}, Unique: {dedup_stats['unique_total']}")
    reduction_pct = (
        (dedup_stats['raw_total'] - dedup_stats['unique_total']) / dedup_stats['raw_total'] * 100
        if dedup_stats['raw_total'] > 0 else 0
    )
    print(f"  Reduction: {reduction_pct:.1f}%")
    print(f"  Total defects in report: {total_defects}")

    json_output = convert_to_json(data_csv, road_length, road_rating, total_defects)
    print(json_output)

    defect_details_df = pd.DataFrame(json_output["defectDetails"])
    defect_details_df["start"]      = json_output["start"]
    defect_details_df["end"]        = json_output["end"]
    defect_details_df["roadLength"] = json_output["roadLength"]
    defect_details_df["roadRating"] = json_output["roadRating"]
    defect_details_df["defect"]     = json_output["defect"]

    def _sanitize_for_match(name):
        return name.lower().strip().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')

    name_to_cat_id = {}
    for cat_id, cat_name in category_information.items():
        name_to_cat_id[_sanitize_for_match(cat_name)] = str(cat_id)
        name_to_cat_id[cat_name.lower()]              = str(cat_id)

    unique_values = []
    for idx, row in defect_details_df.iterrows():
        defect_name = row.get('name', '')
        cat_id = name_to_cat_id.get(_sanitize_for_match(defect_name)) or name_to_cat_id.get(defect_name.lower())
        if cat_id and cat_id in dedup_stats.get('per_category', {}):
            unique_count = dedup_stats['per_category'][cat_id].get('unique', 0)
        else:
            unique_count = row.get('value', 0)
        unique_values.append(unique_count)
    defect_details_df['unique_value'] = unique_values
    print("[*] Added unique_value column to defect_details_df")

    # ============================================================================
    # 15. ROAD DISTRESS CSV (report_3_key)
    # ============================================================================
    image_points      = np.array([[700, 100], [700, 850], [520, 300], [720, 400]], dtype=np.float32)
    real_world_points = np.array([[0, 0], [0, 3.5], [1, 0], [1, 3.5]], dtype=np.float32)
    H, _ = cv2.findHomography(image_points, real_world_points)

    def image_to_real_world(x, y, H):
        pt = np.dot(H, np.array([x, y, 1]).reshape((3, 1)))
        return pt[0][0]/pt[2][0], pt[1][0]/pt[2][0]

    def calculate_real_world_area(bbox, H, scaling_factor=300.764):
        x, y, w, h = bbox
        p1 = image_to_real_world(x, y, H)
        p2 = image_to_real_world(x+w, y, H)
        p3 = image_to_real_world(x, y+h, H)
        width  = np.linalg.norm(np.array(p1) - np.array(p2))
        height = np.linalg.norm(np.array(p1) - np.array(p3))
        return width * height * scaling_factor

    with open(result_consolidated, 'r') as f:
        json_content = json.load(f)

    json_dict = json_content[0] if isinstance(json_content, list) else json_content

    # Fallback to second block if annotations empty
    if isinstance(json_content, list) and len(json_dict.get('annotations', [])) == 0 and len(json_content) > 1:
        alt = json_content[1]
        if isinstance(alt, dict) and len(alt.get('annotations', [])) > 0:
            json_dict = alt

    image_id_to_file_name = {img['id']: img['file_name'] for img in json_dict['images']}
    cat_id_to_name_map    = {cat['id']: cat['name'] for cat in json_dict['categories']}
    all_annotations       = json_dict['annotations']

    image_id_to_annotations = {}
    for ann in all_annotations:
        iid  = ann['image_id']
        cid  = ann['category_id']
        bbox = ann['bbox']
        area = calculate_real_world_area(bbox, H)
        if iid not in image_id_to_annotations:
            image_id_to_annotations[iid] = []
        image_id_to_annotations[iid].append((cid, area))

    rows = []
    for iid, anns in image_id_to_annotations.items():
        if iid not in image_id_to_file_name:
            print(f"Warning: Skipping annotations for missing image_id {iid}")
            continue
        file_name     = image_id_to_file_name[iid]
        road_distress = []
        areas         = []
        for cid, area in anns:
            road_distress.append(cat_id_to_name_map.get(cid, f"Category_{cid}"))
            areas.append(area)
        row = {'File_name': file_name, 'Road Distress': ', '.join(road_distress)}
        for i, a in enumerate(areas):
            row[f'Area{i+1}'] = a
        rows.append(row)

    road_distress_df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=['File_name', 'Road Distress'])

    # Merge with GPS from frame_data
    saf_df = pd.DataFrame(frame_data)[['og_file', 'latitude', 'longitude']]

    print(f"DEBUG: road_distress_df shape: {road_distress_df.shape}")
    print(f"DEBUG: saf_df shape: {saf_df.shape}")

    if not road_distress_df.empty:
        road_distress_df['file_key'] = road_distress_df['File_name'].apply(normalize_filename)
    if not saf_df.empty:
        saf_df['file_key'] = saf_df['og_file'].apply(normalize_filename)

    if not road_distress_df.empty and 'file_key' in road_distress_df.columns and not saf_df.empty:
        merged_df = pd.merge(road_distress_df, saf_df, on='file_key', how='inner')
        print(f"DEBUG: merged_df shape: {merged_df.shape}")
        if merged_df.empty:
            road_keys = set(road_distress_df['file_key'].unique())
            saf_keys  = set(saf_df['file_key'].unique())
            print(f"DEBUG: Common keys: {len(road_keys.intersection(saf_keys))}")
    else:
        merged_df = pd.DataFrame()

    # Fallback: build from frame_data inference_info
    if merged_df.empty or len(merged_df) == 0:
        print("WARNING: merged_df empty. Building from frame_data inference_info...")
        road_defect_rows = []
        for frame in frame_data:
            if frame.get('inference_info') and len(frame['inference_info']) > 0:
                defect_labels = [inf.get('label', '') for inf in frame['inference_info'] if inf.get('label')]
                if defect_labels:
                    road_defect_rows.append({
                        'Latitude':      frame.get('latitude', ''),
                        'Longitude':     frame.get('longitude', ''),
                        'Road Distress': ', '.join(defect_labels),
                        'File_name':     frame.get('og_file', ''),
                        'File_URL':      ''
                    })
        final_df_road_defect = (
            pd.DataFrame(road_defect_rows) if road_defect_rows
            else pd.DataFrame(columns=['Latitude', 'Longitude', 'Road Distress', 'File_name', 'File_URL'])
        )
    else:
        merged_df.rename(columns={'latitude': 'Latitude', 'longitude': 'Longitude'}, inplace=True)
        area_cols   = [c for c in merged_df.columns if c.startswith('Area')]
        keep_cols   = ['Latitude', 'Longitude', 'Road Distress'] + area_cols + ['File_name']
        keep_cols   = [c for c in keep_cols if c in merged_df.columns]
        final_df_road_defect = merged_df[keep_cols].copy()
        final_df_road_defect['File_URL'] = ''

    # ============================================================================
    # 16. UPLOAD DEFECT IMAGES TO GCS (roadvisionai1)
    # ============================================================================
    print(f"[*] Uploading defect images to gs://{NEW_BUCKET_GCS}/Defects/{CODEBUILD_BUILD_ID}/...")

    if not final_df_road_defect.empty and 'File_name' in final_df_road_defect.columns:
        for index, row in final_df_road_defect.iterrows():
            s3_file_path = row.get('File_name', '')
            if not s3_file_path or pd.isna(s3_file_path):
                continue
            filename       = os.path.basename(str(s3_file_path))
            local_file_path = f"predict/{filename}"
            blob_path       = f"Defects/{CODEBUILD_BUILD_ID}/{filename}"

            if not os.path.exists(local_file_path):
                alt_ext  = '.jpg' if local_file_path.lower().endswith('.png') else '.png'
                alt_path = os.path.splitext(local_file_path)[0] + alt_ext
                if os.path.exists(alt_path):
                    local_file_path = alt_path
                    blob_path       = f"Defects/{CODEBUILD_BUILD_ID}/{os.path.basename(alt_path)}"
                else:
                    print(f"WARNING: {local_file_path} not found locally")
                    continue

            public_url = upload_gcs_file(NEW_BUCKET_GCS, blob_path, local_file_path)
            if public_url:
                final_df_road_defect.at[index, 'File_URL'] = public_url
                print(f"Uploaded: {blob_path}")

    # Cleanup
    if 'File_name' in final_df_road_defect.columns:
        final_df_road_defect.drop(columns=['File_name'], inplace=True, errors='ignore')
    if 'file_key' in final_df_road_defect.columns:
        final_df_road_defect.drop(columns=['file_key'], inplace=True, errors='ignore')
    area_cols = [c for c in final_df_road_defect.columns if c.startswith('Area')]
    final_df_road_defect.drop(columns=area_cols, inplace=True, errors='ignore')

    final_df_road_defect.to_csv('road_defect.csv', index=False)
    print(f"road_defect.csv created: {len(final_df_road_defect)} rows")
    final_df_road_defect.to_csv('inspection.csv', index=False)

    # ============================================================================
    # 17. NHAI APPENDIX-IV (report_4_key)
    # ============================================================================
    def load_nhai_project_metadata(config_data=None):
        if config_data is None:
            config_data = full_config_data
        if not config_data:
            config_data = {}
        print("[*] Loading NHAI project metadata")
        return {
            'nh_number':    config_data.get('nh_number', ''),
            'project_name': config_data.get('project_name', ''),
            'upc_code':     config_data.get('upc_code', ''),
            'state':        config_data.get('state', ''),
            'ro_name':      config_data.get('ro_name', ''),
            'piu_name':     config_data.get('piu_name', ''),
            'survey_date':  config_data.get('survey_date', datetime.now().strftime('%Y-%m-%d'))
        }


    def get_asset_type(category_id, cat_mapping):
        road_defect_keywords = [
            'Pothole', 'Cracking', 'Rutting', 'Stripping', 'Delamination',
            'Pavement Joint', 'Pavement Damage', 'Unsealed Road', 'Settlement',
            'Shoulder', 'Rain Cuts', 'Edge Drop', 'Unevenness', 'Water Stagnation', 'Drain'
        ]
        for group_name, cat_ids in cat_mapping.items():
            if str(category_id) in [str(c) for c in cat_ids]:
                for kw in road_defect_keywords:
                    if kw.lower() in group_name.lower():
                        return 'Road Defect'
                return 'Road Asset'
        return 'Road Defect'


    def create_nhai_report_4(dedup_tracker_obj, cat_info, nhai_meta, fd, cat_mapping, frame_df):
        if nhai_meta is None:
            nhai_meta = {k: '' for k in ['nh_number', 'project_name', 'upc_code', 'state', 'ro_name', 'piu_name']}
            nhai_meta['survey_date'] = datetime.now().strftime('%Y-%m-%d')

        print("[*] Creating NHAI APPENDIX-IV format report_4_key...")
        rows = []
        s_no = 1
        chainage_sources = {'defect': 0, 'df_fallback': 0, 'gps_fallback': 0, 'failed': 0}

        for cat_id, defects in dedup_tracker_obj.global_tracker.unique_defects.items():
            for defect in defects:
                try:
                    defect_description = cat_info.get(int(cat_id), f'Category_{cat_id}')
                except (ValueError, TypeError):
                    defect_description = f'Category_{cat_id}'

                # Get defect image URL
                frame_index = defect.get('best_frame_index', -1)
                defect_image = ''
                if 0 <= frame_index < len(fd):
                    og_file = fd[frame_index].get('og_file', '')
                    if og_file:
                        fn = os.path.basename(str(og_file))
                        defect_image = (
                            f"https://storage.googleapis.com/{NEW_BUCKET_GCS}"
                            f"/Defects/{CODEBUILD_BUILD_ID}/{fn}"
                        )

                # Get chainage
                chainage_str = ''
                chainage_num = defect.get('chainage', None)
                chainage_source = 'defect' if chainage_num is not None else None

                if chainage_num is None:
                    chainage_source = 'df_fallback'
                    if 0 <= frame_index < len(frame_df):
                        try:
                            chainage_num = frame_df.iloc[frame_index].get('chainage', None)
                        except:
                            chainage_num = None

                if chainage_num is None or (isinstance(chainage_num, float) and pd.isna(chainage_num)):
                    chainage_source = 'gps_fallback'
                    defect_lat = defect.get('lat', 0)
                    defect_lon = defect.get('lon', 0)
                    if len(frame_df) > 0 and 'latitude' in frame_df.columns and 'chainage' in frame_df.columns:
                        try:
                            distances = (frame_df['latitude'] - defect_lat).abs() + \
                                        (frame_df['longitude'] - defect_lon).abs()
                            chainage_num = frame_df.loc[distances.idxmin(), 'chainage']
                        except:
                            chainage_num = None
                            chainage_source = 'failed'

                if chainage_num is not None and not (isinstance(chainage_num, float) and pd.isna(chainage_num)):
                    try:
                        chainage_num = int(chainage_num)
                        chainage_str = f"{chainage_num * 100}-{(chainage_num + 1) * 100}m"
                        if chainage_source:
                            chainage_sources[chainage_source] = chainage_sources.get(chainage_source, 0) + 1
                    except:
                        chainage_sources['failed'] = chainage_sources.get('failed', 0) + 1
                else:
                    chainage_sources['failed'] = chainage_sources.get('failed', 0) + 1

                rows.append({
                    'S_No':              s_no,
                    'Reporting_Date':    nhai_meta.get('survey_date', ''),
                    'Asset_Type':        get_asset_type(cat_id, cat_mapping),
                    'Defect_Description': defect_description,
                    'Side':              '',
                    'Chainage':          chainage_str,
                    'Latitude':          defect.get('lat', ''),
                    'Longitude':         defect.get('lon', ''),
                    'Defect_Image':      defect_image,
                    'NH_Number':         nhai_meta.get('nh_number', ''),
                    'Project_Name':      nhai_meta.get('project_name', ''),
                    'UPC_Code':          nhai_meta.get('upc_code', ''),
                    'State':             nhai_meta.get('state', ''),
                    'RO_Name':           nhai_meta.get('ro_name', ''),
                    'PIU_Name':          nhai_meta.get('piu_name', ''),
                    'Survey_Date':       nhai_meta.get('survey_date', ''),
                })
                s_no += 1

        nhai_columns = [
            'S_No', 'Reporting_Date', 'Asset_Type', 'Defect_Description', 'Side',
            'Chainage', 'Latitude', 'Longitude', 'Defect_Image',
            'NH_Number', 'Project_Name', 'UPC_Code', 'State', 'RO_Name', 'PIU_Name', 'Survey_Date'
        ]
        nhai_df = pd.DataFrame(rows, columns=nhai_columns) if rows else pd.DataFrame(columns=nhai_columns)
        print(f"[*] NHAI report_4_key: {len(nhai_df)} rows")
        print(f"    Chainage sources: {chainage_sources}")
        return nhai_df


    nhai_metadata    = load_nhai_project_metadata()
    updated_data_asset = create_nhai_report_4(
        dedup_tracker, category_information, nhai_metadata,
        frame_data, categories_mapping, df
    )

    # ============================================================================
    # 18. BACKEND UPDATE
    # ============================================================================
    ANNOTATION_BUILD_ID = os.environ.get('ANNOTATION_BUILD_ID', CODEBUILD_BUILD_ID)
    print(f"DEBUG: ANNOTATION_BUILD_ID = {ANNOTATION_BUILD_ID}")
    updated_frame_data = {
        "frame_list_data":        frame_data,
        "category_information":   category_information,
        "CODEBUILD_BUILD_ID":     CODEBUILD_BUILD_ID,
        # "NEW_CODEBUILD_BUILD_ID": os.environ.get('CODEBUILD_BUILD_ID', CODEBUILD_BUILD_ID).split(":")[-1],
        "NEW_CODEBUILD_BUILD_ID": ANNOTATION_BUILD_ID,
        "report_1_key":           defect_details_df.to_csv(index=False),
        "report_2_key":           df_grouped.to_csv(),
        "report_3_key":           final_df_road_defect.to_csv(index=False),
        "report_4_key":           updated_data_asset.to_csv(index=False),
        "road_length":            road_length,
        "road_rating":            road_rating,
        "dashboard_df_csv":       df_serail.to_csv(),
        "data_submitted":         data_submitted,
    }

    print("Updated json data")
    print(json.dumps(updated_frame_data))

    data_object = {
        "updated_data":    json.dumps(updated_frame_data),
        "show_inference":  True,
        "inference_id":    inference_id,
        "plot_data":       json.dumps({'plots': plot_data}, cls=NpEncoder),
        "secret_key_value": "sleeksitescodepipeline"
    }

    update_result = requests.post(BASE_URL + "webApi/project/update_inference_data", data_object)
    if json.loads(update_result.text).get("status") == "success":
        print("✅ Inferences Updated Successfully")
    else:
        print(f"Backend update status: {update_result.status_code}")

    # ============================================================================
    # 19. STANDALONE EXCEL (post pipeline)
    # ============================================================================
    try:
        if STANDALONE_GENERATOR_AVAILABLE:
            try:
                from standalone_report_generator import generate_excel_from_report_4_key
                _excel_result = generate_excel_from_report_4_key(
                    report_4_csv=updated_data_asset.to_csv(index=False),
                    upload_to_s3=False  # GCP: handle separately if needed
                )
                if _excel_result:
                    print(f"[STANDALONE] Excel generated: {_excel_result}")
            except Exception as _e:
                print(f"[STANDALONE] Excel generation failed (ignored): {_e}")
    except:
        pass

    # ============================================================================
    # 20. UPLOAD FRAMES & PREDICT TO GenerateAnotatedImages (MATCHING AWS STRUCTURE)
    # ============================================================================
    print("[*] Uploading frames and predict to GenerateAnotatedImages path (matching AWS)...")

    import glob as _glob

    TARGET_BUCKET = "codepipeline-ap-south-1-1510246084881"
    # base_path = f"GenerateAnotatedImages/{CODEBUILD_BUILD_ID}/GenerateAnotatedImages"

    # ANNOTATION_BUILD_ID = os.environ.get('BUILD_ID', CODEBUILD_BUILD_ID)
    # print(f"DEBUG: BUILD_ID env = {os.environ.get('BUILD_ID')}")
    # print(f"DEBUG: CODEBUILD_BUILD_ID = {CODEBUILD_BUILD_ID}")
    # print(f"DEBUG: ANNOTATION_BUILD_ID = {ANNOTATION_BUILD_ID}")
    # base_path = f"GenerateAnotatedImages/{ANNOTATION_BUILD_ID}/GenerateAnotatedImages"

    ANNOTATION_BUILD_ID = os.environ.get('ANNOTATION_BUILD_ID', CODEBUILD_BUILD_ID)
    print(f"DEBUG: ANNOTATION_BUILD_ID = {ANNOTATION_BUILD_ID}")
    base_path = f"GenerateAnotatedImages/{ANNOTATION_BUILD_ID}/GenerateAnotatedImages"


    # Upload predict folder (annotated frames with bounding boxes)
    predict_count = 0
    for file_path in _glob.glob("predict/*"):
        if not os.path.isfile(file_path):
            continue
        filename = os.path.basename(file_path)
        blob_path = f"{base_path}/predict/{filename}"
        try:
            storage_client.bucket(TARGET_BUCKET).blob(blob_path).upload_from_filename(file_path)
            predict_count += 1
        except Exception as e:
            print(f"Failed predict upload {filename}: {e}")
    print(f"[✓] Uploaded {predict_count} files to predict/")

    # Upload frames folder (original frames)
    frames_count = 0
    for file_path in _glob.glob("frames/*"):
        if not os.path.isfile(file_path):
            continue
        filename = os.path.basename(file_path)
        blob_path = f"{base_path}/frames/{filename}"
        try:
            storage_client.bucket(TARGET_BUCKET).blob(blob_path).upload_from_filename(file_path)
            frames_count += 1
        except Exception as e:
            print(f"Failed frame upload {filename}: {e}")
    print(f"[✓] Uploaded {frames_count} files to frames/")

    # Upload result.json into predict folder (matching AWS structure)
    if os.path.exists("result.json"):
        try:
            storage_client.bucket(TARGET_BUCKET).blob(f"{base_path}/predict/result.json").upload_from_filename("result.json")
            print("[✓] Uploaded result.json to predict/")
        except Exception as e:
            print(f"Failed result.json upload: {e}")

    print("[✓] GenerateAnotatedImages upload complete!")

    merge_file.close()

    # ============================================================================
    # 21. VIDEO GENERATION & UPLOAD TO GCS (nhvideobucket)
    # ============================================================================
    print("\n[*] Starting video generation from annotated frames...")


    def create_video_from_frames(uid):
        import glob
        fps = 1.0
        try:
            with open(result_consolidated, 'r') as f:
                result_data = json.loads(f.read())
                if isinstance(result_data, list) and len(result_data) > 0:
                    fps = result_data[0].get('info', {}).get('fps', 1.0)
                elif isinstance(result_data, dict):
                    fps = result_data.get('info', {}).get('fps', 1.0)
            print(f"[*] Video FPS: {fps}")
        except Exception as e:
            print(f"[WARNING] Could not read FPS: {e}")

        frame_files = sorted(
            glob.glob('predict/frame_*.[jp][pn]g'),
            key=lambda x: int(re.findall(r'\d+', os.path.basename(x))[0])
        )
        if not frame_files:
            print("[WARNING] No frames found. Skipping video generation.")
            return None

        first_frame = cv2.imread(frame_files[0])
        if first_frame is None:
            return None
        height, width = first_frame.shape[:2]
        output_path = f'{uid}.mp4'
        file_ext    = 'jpg' if frame_files[0].endswith('.jpg') else 'png'
        input_pattern = f'predict/frame_%05d.{file_ext}'

        try:
            import subprocess
            ffmpeg_cmd = [
                'ffmpeg', '-y', '-framerate', str(fps), '-i', input_pattern,
                '-c:v', 'libx264', '-profile:v', 'baseline', '-level', '3.0',
                '-preset', 'veryfast', '-crf', '28', '-pix_fmt', 'yuv420p',
                '-movflags', '+faststart', '-vf', f'scale={width}:{height}', output_path
            ]
            result_proc = subprocess.run(ffmpeg_cmd, capture_output=True, text=True, timeout=300)
            if result_proc.returncode == 0:
                print(f"[✓] Video created (H.264): {output_path}")
                return output_path
            raise Exception(result_proc.stderr)
        except Exception as e:
            print(f"[WARNING] ffmpeg failed: {e}. Falling back to OpenCV...")
            fourcc = cv2.VideoWriter_fourcc(*'mp4v')
            writer = cv2.VideoWriter(output_path, fourcc, fps, (width, height))
            for ff in frame_files:
                frame = cv2.imread(ff)
                if frame is not None:
                    writer.write(frame)
            writer.release()
            print(f"[✓] Video created (OpenCV): {output_path}")
            return output_path


    def upload_video_to_gcs(video_path, uid):
        """Upload annotated video to gs://nhvideobucket/videos2/{uid}.mp4"""
        try:
            blob_name = f"videos2/{uid}.mp4"
            bucket = storage_client.bucket(VIDEO_BUCKET)
            blob   = bucket.blob(blob_name)
            blob.upload_from_filename(video_path, content_type="video/mp4")
            video_url = f"https://storage.googleapis.com/{VIDEO_BUCKET}/{blob_name}"
            print(f"[✓] Video uploaded: {video_url}")
        except Exception as e:
            print(f"[ERROR] Video upload failed: {e}")


    # Extract UID
    uid = None
    try:
        with open(result_consolidated, 'r') as f:
            result_data = json.loads(f.read())
            if isinstance(result_data, list) and len(result_data) > 0:
                uid = result_data[0].get('info', {}).get('uid')
            elif isinstance(result_data, dict):
                uid = result_data.get('info', {}).get('uid')
        if uid:
            print(f"[*] UID from result.json: {uid}")
    except Exception as e:
        print(f"[WARNING] Could not extract UID: {e}")

    if not uid and 'GCS_KEY' in os.environ:
        uid = os.environ["GCS_KEY"].split('/')[0]
        print(f"[*] UID from GCS_KEY: {uid}")

    if uid:
        video_path = create_video_from_frames(uid)
        if video_path and os.path.exists(video_path):
            upload_video_to_gcs(video_path, uid)
        else:
            print("[WARNING] Video generation failed. Skipping upload.")
    else:
        print("[WARNING] Could not extract UID. Skipping video generation.")

    print("[✓] GCP Annotation Pipeline Completed!")
