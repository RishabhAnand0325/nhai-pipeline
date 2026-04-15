# Video Processing Pipeline — Flow

## Overview

```
GCS Bucket (videos + GPX)
  │
  ▼
trigger_builds.py (Orchestrator)
  │  Lists videos, detects Road ID, launches parallel workers
  │
  ▼ (subprocess per video)
┌─────────────────────────────────────────────────┐
│  Pipeline 1: pipeline1.py                       │
│  ├─ Download video + GPX from GCS               │
│  ├─ Extract frames at 10m GPS intervals (FFmpeg) │
│  ├─ Upload frames to GCS                        │
│  ├─ Run YOLO inference (TensorRT FP16 on GPU)   │
│  └─ Save result.json to GCS                     │
│                                                 │
│  Pipeline 2: main.py (AnnotationPipeline)       │
│  ├─ Process defects (chainage, PCI, IRC)        │
│  ├─ Draw bounding boxes on frames               │
│  ├─ Generate 4 reports                          │
│  ├─ Generate annotated video (FFmpeg)           │
│  ├─ Upload everything to GCS                    │
│  └─ Save to MongoDB                             │
└─────────────────────────────────────────────────┘
```

---

## Detailed Flow

### Step 0: trigger_builds.py (Orchestrator)

```
trigger_builds.py --road_id R525275 --parallel 4
  │
  ├─ List MP4s from gs://datanh11/video-processing-pipelines-data/{road_id}/
  │
  ├─ Detect Road ID per video:
  │   ├─ From directory name (if in road ID subdirectory) — instant
  │   └─ From GPX matching against nhai_roads.json — fallback
  │
  ├─ Generate UUID (run_id) for each video
  │
  └─ Launch N parallel workers (subprocess per video):
      env: GCS_URI, ROAD_ID, RUN_ID, MODEL_WEIGHTS
      cmd: python main.py
      cwd: workdir/{run_id}/  (isolated per video)
```

### Step 1: Pipeline 1 — Frame Extraction & Inference (pipeline1.py)

```
RoadvisionPipeline.run()
  │
  ├─ 1. Setup directories
  │     frame_data/frames/, frame_data/predict/, video/
  │
  ├─ 2. Download from GCS
  │     ├─ video/file.mp4  (from GCS_URI)
  │     └─ video/data.gpx → video/data.json  (GPX → position details)
  │
  ├─ 3. Read GPS metadata
  │     300 trackpoints → frame_timestamps[]
  │
  ├─ 4. Compute 10m GPS timestamps
  │     Walk GPS points, find every 10m milestone via haversine
  │     → target_timestamps[] (one per 10m of road)
  │
  ├─ 5. Extract frames (parallel FFmpeg)
  │     ├─ Copy video to /dev/shm/{uuid}.mp4 (RAM)
  │     ├─ Split into 8 segments
  │     ├─ 8 parallel FFmpeg processes extract target frames
  │     ├─ Upload frames to GCS (pipelined, 32 threads)
  │     └─ Copy to frame_data/predict/
  │
  ├─ 6. YOLO inference (GPU)
  │     ├─ Load nhai_best.engine (TensorRT FP16) or .pt (PyTorch)
  │     ├─ Batch inference on all frames
  │     ├─ Convert xyxy → COCO [x,y,w,h] format
  │     └─ Populate json_pavement (images, annotations, categories)
  │
  ├─ 7. Save & upload result.json to GCS
  │     gs://datanh11/processed-data/{ROAD_ID}/{UUID}/result.json
  │
  └─ 8. Post frame data to backend API
        POST /flutterApi/auth/add_data
```

### Step 2: Pipeline 2 — Annotation & Reports (main.py)

```
AnnotationPipeline.run()
  │
  ├─ 1. Process defects
  │     ├─ Calculate total road distance (haversine)
  │     ├─ Assign frames to 100m chainage groups
  │     ├─ Aggregate defect counts/severity per chainage
  │     ├─ Calculate IRC rating, PCI
  │     ├─ Reverse geocode start/end addresses (Google Maps API)
  │     └─ Output: df, df_grouped, df_serial, road_length, road_rating
  │
  ├─ 2. Draw bounding boxes on frames
  │     ├─ For each frame with detections:
  │     │   ├─ Draw bbox + label on image (OpenCV)
  │     │   └─ Update frame defect_state + inference_info
  │     └─ Upload annotated frames to GCS
  │
  ├─ 3. Generate severity pie chart data
  │     {Not Defected, High, Medium, Low} percentages
  │
  ├─ 4. Generate Report 1 (defect_details_df)
  │     Per-defect-type summary with severity levels
  │     Columns: name, level, value, start, end, roadLength, roadRating, defect, unique_value
  │
  ├─ 5. Generate Report 3 (road distress)
  │     Per-frame defect areas with real-world homography calculation
  │     Columns: Latitude, Longitude, Road Distress, Area1..N, File_URL
  │
  ├─ 6. Generate Report 4 (per detection)
  │     One row per detection instance
  │     Columns: S_No, Reporting_Date, Defect_Description, Chainage, Lat, Lon, Defect_Image
  │
  ├─ 7. Generate annotated video
  │     ├─ Load annotated frames from frame_data/predict/
  │     ├─ Create H.264 MP4 at 1 FPS (FFmpeg)
  │     └─ Upload to GCS: processed-data/{ROAD_ID}/{UUID}/annotated_video.mp4
  │
  ├─ 8. Post to backend API
  │     POST /webApi/project/update_inference_data
  │     Payload: frame_list_data, reports 1-4, road_length, road_rating, plot_data
  │
  ├─ 9. Upload report CSVs to GCS
  │     processed-data/{ROAD_ID}/{UUID}/
  │       ├─ report_1_defect_details.csv
  │       ├─ report_2_chainage_grouped.csv
  │       ├─ report_3_road_distress.csv
  │       ├─ report_4_per_detection.csv
  │       ├─ result.json
  │       └─ chainage_report.csv
  │
  └─ 10. Save to MongoDB
        ├─ annotation_segments collection:
        │   uuid, road_id, survey_no, total_frames, total_defects,
        │   road_length_km, road_rating, report_1-4 (CSV strings),
        │   severity_distribution, category_information,
        │   frame_list_data (with GeoJSON location per frame)
        │
        └─ roads collection:
            road_id, road_name, road_type, full_route (GeoJSON LineString),
            surveys[] (push new survey entry with length, frames, rating)
```

---

## GCS Directory Structure

```
gs://datanh11/
├── video-processing-pipelines-data/
│   ├── R525275/                          ← Input videos organized by road
│   │   ├── 20251126155933_000113A.MP4
│   │   ├── 20251126155933_000113A.gpx
│   │   └── ...
│   ├── R843035/
│   └── R000101/
│
└── processed-data/
    └── R525275/
        └── {UUID}/                        ← Output per video run
            ├── frame_data/
            │   ├── frames/                ← Original extracted frames
            │   └── predict/               ← Annotated frames (with bboxes)
            ├── result.json                ← COCO format detections
            ├── chainage_report.csv
            ├── report_1_defect_details.csv
            ├── report_2_chainage_grouped.csv
            ├── report_3_road_distress.csv
            ├── report_4_per_detection.csv
            └── annotated_video.mp4        ← 1 FPS video with bboxes
```

---

## MongoDB Schema

### annotation_segments
```json
{
  "uuid": "a1b2c3d4-...",
  "road_id": "R525275",
  "survey_no": 1,
  "total_frames": 92,
  "total_defects": 173,
  "road_length_km": 0.97,
  "road_rating": 83.09,
  "report_1_key": "name,level,value,...\nPotholes,Low,0,...",
  "report_2_key": ",Chainage,start_address,...\n0,0-100m,...",
  "report_3_key": "Latitude,Longitude,Road Distress,...",
  "report_4_key": "S_No,Reporting_Date,...",
  "severity_distribution": {"counts": [...], "labels": [...]},
  "frame_list_data": [
    {
      "latitude": 17.22, "longitude": 80.15,
      "location": {"type": "Point", "coordinates": [80.15, 17.22]},
      "chainage_km": 0.01,
      "defect_state": "high",
      "inference_info": [{"label": "Potholes", "bbox": [...], "severity": "high"}]
    }
  ]
}
```

### roads
```json
{
  "road_id": "R525275",
  "road_name": "Unknown-R525275",
  "road_type": "highway",
  "full_route": {"type": "LineString", "coordinates": [[80.14, 17.21], ...]},
  "surveys": [
    {
      "survey_no": 1,
      "total_length_km": 0.97,
      "total_frames": 92,
      "road_rating": 83.09,
      "survey_date": "2026-04-13T..."
    }
  ]
}
```

---

## Commands

```bash
cd /home/shubham/video-processing-pipeline

# Process one road (4 parallel):
venv/bin/python3 trigger_builds.py --road_id R525275 --parallel 4

# Process all roads:
venv/bin/python3 trigger_builds.py --parallel 4

# Single video:
GCS_URI="gs://datanh11/video-processing-pipelines-data/R525275/video.MP4" \
ROAD_ID="R525275" venv/bin/python3 main.py

# Re-process annotations from updated result.json:
venv/bin/python3 reprocess_annotations.py --road_id R525275 --uuid <uuid>

# Watch for result.json changes:
venv/bin/python3 reprocess_annotations.py --watch --road_id R525275 --uuid <uuid>
```

---

## Key Specs

| Spec | Value |
|------|-------|
| GPU | Nvidia Tesla T4 (15 GB VRAM) |
| Inference | TensorRT FP16 (nhai_best.engine) ~38 fps |
| Model | YOLOv8 — 34 defect classes |
| Frame interval | Every 10m of road (GPS-based) |
| Chainage grouping | 100m segments |
| Video output | H.264, 1 FPS |
| Max parallel | 4 (recommended for T4) |
| Geospatial | 2dsphere indexes on roads.full_route + annotation_segments.frame_list_data.location |
