"""
reprocess_annotations.py
========================
Re-run ONLY Pipeline 2 (Annotation) from an updated result.json in GCS.
Downloads frames + result.json from GCS, regenerates annotated frames,
reports, annotated video, and updates MongoDB.

Use cases:
  - Manually corrected annotations in result.json
  - Re-run reports after fixing category mappings
  - Regenerate annotated video after bbox adjustments

Usage:
    # Re-process a specific run by UUID and road ID:
    python reprocess_annotations.py --road_id R525275 --uuid <run-uuid>

    # Re-process from a direct GCS path to result.json:
    python reprocess_annotations.py --gcs_result gs://datanh11/processed-data/R525275/<uuid>/result.json

    # Watch for changes (polls every 60s for updated result.json):
    python reprocess_annotations.py --watch --road_id R525275 --uuid <run-uuid>
"""

import argparse
import json
import os
import sys
import shutil
import time

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, 'annotation-pipeline-bak'))
sys.path.insert(0, os.path.join(current_dir, 'annotation-pipeline'))

from google.cloud import storage as gcs_storage


def download_run_data(gcs_client, bucket_name, road_id, run_uuid, work_dir):
    """Download result.json and predict frames from GCS for a given run."""
    bucket = gcs_client.bucket(bucket_name)
    base_path = f"processed-data/{road_id}/{run_uuid}"

    # Download result.json
    result_blob = bucket.blob(f"{base_path}/result.json")
    if not result_blob.exists():
        print(f"[!] result.json not found at gs://{bucket_name}/{base_path}/result.json")
        sys.exit(1)

    os.makedirs(work_dir, exist_ok=True)
    result_path = os.path.join(work_dir, "result.json")
    result_blob.download_to_filename(result_path)
    file_size = os.path.getsize(result_path) / 1024
    print(f"[✓] Downloaded result.json ({file_size:.1f} KB)")

    # Download predict frames
    predict_dir = os.path.join(work_dir, "frame_data", "predict")
    frames_dir = os.path.join(work_dir, "frame_data", "frames")
    os.makedirs(predict_dir, exist_ok=True)
    os.makedirs(frames_dir, exist_ok=True)

    frame_count = 0
    for blob in gcs_client.list_blobs(bucket_name, prefix=f"{base_path}/frame_data/predict/"):
        fname = blob.name.split("/")[-1]
        if fname.endswith(".jpg"):
            blob.download_to_filename(os.path.join(predict_dir, fname))
            # Also copy to frames/ (needed by some report steps)
            blob.download_to_filename(os.path.join(frames_dir, fname))
            frame_count += 1

    print(f"[✓] Downloaded {frame_count} frames to frame_data/predict/")
    return result_path, frame_count


def build_frame_data_from_result(result_path):
    """Reconstruct frame_data list from result.json (images + GPS metadata)."""
    with open(result_path, "r") as f:
        data = json.load(f)

    json_pavement = data[0] if isinstance(data, list) else data

    # Build frame_data from images — extract frame index from filename
    images_sorted = sorted(json_pavement["images"], key=lambda x: x["id"])
    frame_data = []
    for img in images_sorted:
        frame_idx = img["id"]
        frame_name = img["file_name"].split("/")[-1]
        frame_data.append({
            "timeElapsed": frame_idx,
            "latitude": 0.0,
            "longitude": 0.0,
            "speed": 0,
            "speedAccuracy": 0,
            "orientation": "portraitUp",
            "og_file": f"frame_data/frames/{frame_name}",
        })

    # Try to load GPS data from chainage_report.csv if it exists alongside result.json
    chainage_path = os.path.join(os.path.dirname(result_path), "chainage_report.csv")
    if os.path.exists(chainage_path):
        import pandas as pd
        try:
            ch_df = pd.read_csv(chainage_path)
            if "start_latitude" in ch_df.columns:
                print(f"[*] Enriching frame GPS from chainage_report.csv")
        except Exception:
            pass

    return frame_data, json_pavement


def run_annotation_pipeline(frame_data, json_pavement, run_uuid, road_id, work_dir):
    """Run Pipeline 2 (AnnotationPipeline) in the given work directory."""
    original_dir = os.getcwd()
    os.chdir(work_dir)

    try:
        os.environ["ROAD_ID"] = road_id
        os.environ["RUN_ID"] = run_uuid

        from main import AnnotationPipeline
        p2 = AnnotationPipeline(frame_data, json_pavement, run_uuid)
        p2.run()
        return True
    except Exception as e:
        print(f"[!] Annotation pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        os.chdir(original_dir)


def reprocess(args):
    """Main reprocessing logic."""
    gcs_client = gcs_storage.Client()
    bucket_name = args.bucket

    # Resolve road_id and uuid from args
    if args.gcs_result:
        # Parse: gs://bucket/processed-data/ROAD_ID/UUID/result.json
        parts = args.gcs_result.replace("gs://", "").split("/")
        bucket_name = parts[0]
        road_id = parts[2]
        run_uuid = parts[3]
    else:
        road_id = args.road_id
        run_uuid = args.uuid

    if not road_id or not run_uuid:
        print("[!] Must provide --road_id + --uuid or --gcs_result")
        sys.exit(1)

    print(f"\n{'═'*60}")
    print(f"  Re-processing Annotations")
    print(f"  Road ID : {road_id}")
    print(f"  UUID    : {run_uuid}")
    print(f"  Bucket  : {bucket_name}")
    print(f"{'═'*60}\n")

    # Set up isolated work directory
    work_dir = os.path.join(current_dir, "workdir", f"reprocess-{run_uuid}")
    if os.path.exists(work_dir):
        shutil.rmtree(work_dir)
    os.makedirs(work_dir)

    # Symlink pipeline source files
    for item in ["main.py", "pipeline1.py", "annotation-pipeline", "annotation-pipeline-bak"]:
        src = os.path.join(current_dir, item)
        dst = os.path.join(work_dir, item)
        if not os.path.exists(dst):
            os.symlink(src, dst)

    # Step 1: Download data from GCS
    print("[*] Downloading run data from GCS...")
    result_path, frame_count = download_run_data(
        gcs_client, bucket_name, road_id, run_uuid, work_dir
    )

    # Also download chainage_report.csv if it exists
    bucket = gcs_client.bucket(bucket_name)
    base_path = f"processed-data/{road_id}/{run_uuid}"
    ch_blob = bucket.blob(f"{base_path}/chainage_report.csv")
    if ch_blob.exists():
        ch_blob.download_to_filename(os.path.join(work_dir, "chainage_report.csv"))

    # Step 2: Build frame_data from result.json
    print("[*] Building frame data from result.json...")
    frame_data, json_pavement = build_frame_data_from_result(result_path)

    # Enrich frame_data with GPS from json_pavement images if available
    # The images in result.json don't contain GPS, but the original frame_data
    # had it. We need to check if there's frame metadata in MongoDB.
    try:
        from pymongo import MongoClient
        mongo_client = MongoClient(
            os.environ.get("MONGO_URI",
                           "mongodb+srv://tech_db_user:IK96qWD8AvtbpOHe@cluster0.nm6pkfg.mongodb.net/roadvision?retryWrites=true&w=majority")
        )
        db = mongo_client["roadvision"]
        existing = db.annotation_segments.find_one({"uuid": run_uuid})
        if existing and existing.get("frame_list_data"):
            print(f"[*] Enriching frame GPS from MongoDB (existing annotation_segment)")
            for i, stored_frame in enumerate(existing["frame_list_data"]):
                if i < len(frame_data):
                    frame_data[i]["latitude"] = stored_frame.get("latitude", 0)
                    frame_data[i]["longitude"] = stored_frame.get("longitude", 0)
                    frame_data[i]["speed"] = stored_frame.get("speed", 0)
                    frame_data[i]["speedAccuracy"] = stored_frame.get("speedAccuracy", 0)
                    frame_data[i]["orientation"] = stored_frame.get("orientation", "portraitUp")
                    frame_data[i]["timeElapsed"] = stored_frame.get("timeElapsed", i)
            print(f"    ✅ Enriched {len(frame_data)} frames with GPS data")
    except Exception as e:
        print(f"[⚠] Could not enrich GPS from MongoDB: {e}")

    print(f"[✓] Frame data: {len(frame_data)} frames, {len(json_pavement.get('annotations', []))} annotations")

    # Step 3: Run annotation pipeline
    print("\n[*] Running Annotation Pipeline...")
    start = time.time()
    success = run_annotation_pipeline(frame_data, json_pavement, run_uuid, road_id, work_dir)
    elapsed = time.time() - start

    if success:
        print(f"\n✅ Re-processing complete in {elapsed:.1f}s ({elapsed/60:.1f} min)")
    else:
        print(f"\n❌ Re-processing failed after {elapsed:.1f}s")

    # Cleanup temp files but keep log
    for item in ["frame_data", "video", "predict", "frames", "saf.csv",
                 "road_distress.csv", "initial_merged_road_distress_data.csv",
                 "updated_merged_road_defect_data.csv"]:
        path = os.path.join(work_dir, item)
        if os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)
        elif os.path.isfile(path):
            os.remove(path)

    return success


def watch_and_reprocess(args):
    """Poll GCS for changes to result.json and re-process when updated."""
    gcs_client = gcs_storage.Client()
    bucket_name = args.bucket
    road_id = args.road_id
    run_uuid = args.uuid
    interval = args.watch_interval

    base_path = f"processed-data/{road_id}/{run_uuid}/result.json"
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(base_path)

    print(f"[*] Watching gs://{bucket_name}/{base_path} every {interval}s...")

    last_updated = None
    if blob.exists():
        blob.reload()
        last_updated = blob.updated
        print(f"    Current version: {last_updated}")

    while True:
        time.sleep(interval)
        try:
            blob.reload()
            current_updated = blob.updated
            if current_updated != last_updated:
                print(f"\n[!] result.json changed: {last_updated} → {current_updated}")
                last_updated = current_updated
                reprocess(args)
                print(f"\n[*] Resuming watch every {interval}s...")
        except Exception as e:
            print(f"[⚠] Watch error: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Re-run annotation pipeline from updated result.json in GCS"
    )
    parser.add_argument("--road_id", default=None, help="Road ID (e.g. R525275)")
    parser.add_argument("--uuid", default=None, help="Run UUID")
    parser.add_argument("--gcs_result", default=None,
                        help="Full GCS path to result.json (e.g. gs://datanh11/processed-data/R525275/<uuid>/result.json)")
    parser.add_argument("--bucket", default="datanh11", help="GCS bucket name")
    parser.add_argument("--watch", action="store_true",
                        help="Watch for changes to result.json and auto-reprocess")
    parser.add_argument("--watch_interval", type=int, default=60,
                        help="Polling interval in seconds for --watch mode (default: 60)")
    args = parser.parse_args()

    if args.watch:
        if not args.road_id or not args.uuid:
            print("[!] --watch requires --road_id and --uuid")
            sys.exit(1)
        watch_and_reprocess(args)
    else:
        success = reprocess(args)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
