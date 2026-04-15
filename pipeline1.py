"""
RoadvisionPipeline — GCP Version
==================================
Migrated from AWS (SageMaker + S3) to GCP (Vertex AI + GCS).
Flow matches AWS pipeline exactly:
  - Frames + Predict  → codepipeline-ap-south-1-1510246084881/videoModelOut/{BUILD_ID}/artifacts/frame_data/
  - chainage_report.csv → codepipeline-ap-south-1-1510246084881/videoModelOut/{BUILD_ID}/artifacts/
  - result.json        → codepipeline-ap-south-1-1510246084881/videoModelOut/{BUILD_ID}/artifacts/
  - result.json (copy) → roadvisionvideoframes1/{BUILD_ID}/result.json  ← triggers annotation pipeline
  - Frame data         → API Dashboard via POST
"""

import os
import re
import cv2
import json
import shutil
import uuid
import pandas as pd
import numpy as np
import requests
import time
import logging
from datetime import datetime
import queue
import threading
import sys
import subprocess
import glob

from google.cloud import storage as gcs_storage

sys.path.append(os.path.join(os.path.dirname(__file__), 'annotation-pipeline'))

# =========================================================================
# LOGGING SETUP
# =========================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("RoadvisionPipeline")


def log_section(title):
    log.info("=" * 60)
    log.info(f"  {title}")
    log.info("=" * 60)


class RoadvisionPipeline:
    def __init__(self):
        log_section("INITIALIZING ROADVISION PIPELINE")

        # GCS Buckets — all outputs go to datanh11
        self.BUCKET_NAME      = "datanh11"
        self.BUCKET_NAME_JSON = "datanh11"
        self.UPLOAD_BUCKET_NAME = "datanh11"

        # Road ID + output prefix: processed-data/{ROAD_ID}/{BUILD_ID}/
        self.ROAD_ID       = os.environ.get("ROAD_ID", "R000000")
        self.BUCKET_PREFIX = f"processed-data/{self.ROAD_ID}"

        self.BASE_URL = "https://roadvision-backend-new-505717192876.asia-south1.run.app/"

        # Local YOLO model for GPU inference
        # Prefer TensorRT engine (.engine) for ~3-5x faster inference on T4
        # Falls back to PyTorch weights (.pt) if engine not found
        default_engine = "/home/shubham/nhai_best.engine"
        default_pt = "/home/shubham/nhai_best.pt"
        env_weights = os.environ.get("MODEL_WEIGHTS", "")
        if env_weights:
            self.MODEL_WEIGHTS = env_weights
        elif os.path.exists(default_engine):
            self.MODEL_WEIGHTS = default_engine
        else:
            self.MODEL_WEIGHTS = default_pt

        self.USE_TENSORRT = self.MODEL_WEIGHTS.endswith(".engine")

        log.info(f"Input Bucket    : {self.BUCKET_NAME}")
        log.info(f"Frames Bucket   : {self.BUCKET_NAME_JSON}/{self.BUCKET_PREFIX}")
        log.info(f"Result Bucket   : {self.UPLOAD_BUCKET_NAME}")
        log.info(f"API URL         : {self.BASE_URL}")
        log.info(f"Model Weights   : {self.MODEL_WEIGHTS}")
        log.info(f"TensorRT        : {'YES (FP16)' if self.USE_TENSORRT else 'NO (PyTorch)'}")

        log.info("Connecting to GCS...")
        self.gcs_client = gcs_storage.Client()
        log.info("✅ GCS connected.")

        # Load YOLO model on GPU
        log.info("Loading YOLO model on GPU...")
        from ultralytics import YOLO
        self.yolo_model = YOLO(self.MODEL_WEIGHTS, task="detect")
        if not self.USE_TENSORRT:
            self.yolo_model.to("cuda")
        log.info(f"✅ YOLO model loaded on GPU ({'TensorRT FP16' if self.USE_TENSORRT else 'CUDA PyTorch'}).")

        # Read GCS_URI for the video to process
        gcs_uri = os.environ.get("GCS_URI")
        if not gcs_uri:
            log.warning("⚠️ GCS_URI env variable NOT SET — using DUMMY path for testing")
            gcs_uri = "gs://datanh11/video-processing-pipelines-data/20251126155933_000113A.MP4"
        else:
            log.info(f"✅ GCS_URI received from environment: {gcs_uri}")

        # UID = filename stem, e.g. "20251126155933_000113A"
        self.UID = gcs_uri.split("/")[-1].rsplit(".", 1)[0]
        # Generate a UUID instead of using Cloud Build ID
        self.CODEBUILD_ID = os.environ.get("RUN_ID", str(uuid.uuid4()))
        key_parts = gcs_uri.replace("gs://", "")
        self.file_key = "/".join(key_parts.split("/")[1:])
        self.job_number = "frame_data"

        log.info("-" * 40)
        log.info(f"UID             : {self.UID}")
        log.info(f"RUN_ID          : {self.CODEBUILD_ID}")
        log.info(f"file_key        : {self.file_key}")
        log.info(f"BUCKET          : {self.BUCKET_NAME}")
        log.info("-" * 40)

        self.info_dataframe = pd.DataFrame(
            columns=[
                "Frame Name", "CODEBUILD_BUILD_ID",
                "Transverse Crack (D00)", "Longitudinal Crack (D10)",
                "Alligator Crack (D20)", "Potholes (D40)", "Other Corruption (D43)",
            ]
        )
        self.frame_timestamps = []
        self.frame_data = []
        self.annotation_id_pavement = 0
        self.temp_frame_data = []
        self.dataframe_rows = []
        self.images_added = set()

        # Load categories directly from the YOLO model (matches nhai_best.pt classes)
        self.pavement_categories = {name: idx for idx, name in self.yolo_model.names.items()}
        log.info(f"Pavement categories loaded from model: {len(self.pavement_categories)} classes")

        self.json_pavement = {
            "images": [],
            "categories": [{"id": v, "name": k} for k, v in self.pavement_categories.items()],
            "annotations": [],
            "info": {
                "year": datetime.now().year, "version": "1.0",
                "description": "Pavement Details", "contributor": "Roadvision",
                "url": "", "date_created": datetime.now().isoformat(),
                "fps": 1.0, "uid": self.UID,
            },
        }
        log.info("✅ Pipeline initialized successfully.")

    # =========================================================================
    # STEP 1: SETUP DIRECTORIES
    # =========================================================================
    def setup_directories(self):
        log_section("SETTING UP DIRECTORIES")
        paths = [f"{self.job_number}/frames/", f"{self.job_number}/predict/", "video"]
        for path in paths:
            if os.path.exists(path):
                shutil.rmtree(path)
                log.info(f"🗑️  Cleared existing: {path}")
            os.makedirs(path)
            log.info(f"📁 Created: {path}")
        log.info("✅ Directories set up.")

    # =========================================================================
    # STEP 2: DOWNLOAD FROM GCS
    # =========================================================================
    def download_s3_files(self):
        log_section("DOWNLOADING FILES FROM GCS")
        try:
            log.info(f"Bucket : {self.BUCKET_NAME}")
            log.info(f"Key    : {self.file_key}")

            bucket = self.gcs_client.bucket(self.BUCKET_NAME)

            log.info("📥 Downloading video file...")
            start = time.time()
            bucket.blob(self.file_key).download_to_filename("video/file.mp4")
            elapsed = time.time() - start
            size_mb = os.path.getsize("video/file.mp4") / (1024 * 1024)
            log.info(f"✅ Video downloaded: {size_mb:.1f} MB in {elapsed:.1f}s")

            # Derive GPX key from the MP4 key (same name, .gpx extension)
            gpx_key = self.file_key.rsplit(".", 1)[0] + ".gpx"
            log.info(f"📥 Downloading GPX metadata ({gpx_key})...")
            bucket.blob(gpx_key).download_to_filename("video/data.gpx")
            log.info("✅ GPX downloaded.")

            # Convert GPX → positionDetails JSON expected by read_metadata()
            self._convert_gpx_to_metadata("video/data.gpx", "video/data.json")

        except Exception as e:
            log.error(f"❌ GCS Download Error: {e}")
            exit(1)

    def _convert_gpx_to_metadata(self, gpx_path, out_path):
        """Parse a .gpx file and write a data.json with positionDetails list."""
        import xml.etree.ElementTree as ET
        from math import radians, sin, cos, sqrt, atan2 as _atan2
        from datetime import datetime as _dt

        def _haversine(lat1, lon1, lat2, lon2):
            R = 6371000.0
            p1, p2 = radians(lat1), radians(lat2)
            dp = radians(lat2 - lat1)
            dl = radians(lon2 - lon1)
            a = sin(dp / 2) ** 2 + cos(p1) * cos(p2) * sin(dl / 2) ** 2
            return 2 * R * _atan2(sqrt(a), sqrt(1 - a))

        tree = ET.parse(gpx_path)
        root = tree.getroot()

        # Support both namespaced and bare GPX
        ns = ''
        if root.tag.startswith('{'):
            ns = root.tag.split('}')[0] + '}'

        trkpts = root.findall(f'.//{ns}trkpt')
        if not trkpts:
            log.error("❌ No trackpoints found in GPX file")
            exit(1)

        # Parse timestamps once for speed calculation
        def _parse_time(pt):
            t = pt.findtext(f'{ns}time') or ''
            for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S"):
                try:
                    return _dt.strptime(t.strip(), fmt)
                except ValueError:
                    continue
            return None

        times = [_parse_time(pt) for pt in trkpts]

        position_details = []
        for i, pt in enumerate(trkpts):
            lat = float(pt.attrib['lat'])
            lon = float(pt.attrib['lon'])

            speed = 0.0
            if i > 0:
                prev = trkpts[i - 1]
                prev_lat = float(prev.attrib['lat'])
                prev_lon = float(prev.attrib['lon'])
                if times[i] and times[i - 1]:
                    dt_sec = (times[i] - times[i - 1]).total_seconds()
                    if dt_sec > 0:
                        speed = _haversine(prev_lat, prev_lon, lat, lon) / dt_sec

            # timeElapsed: actual seconds from start (falls back to 1-based index)
            if times[i] and times[0]:
                time_elapsed = (times[i] - times[0]).total_seconds()
            else:
                time_elapsed = float(i + 1)

            position_details.append([{
                "timeElapsed":   time_elapsed,
                "latitude":      lat,
                "longitude":     lon,
                "speed":         speed,
                "speedAccuracy": speed,
                "orientation":   "portraitUp",
            }])

        with open(out_path, "w") as f:
            json.dump({"positionDetails": position_details}, f)
        log.info(f"✅ GPX converted: {len(position_details)} trackpoints → {out_path}")

    # =========================================================================
    # STEP 3: READ METADATA
    # =========================================================================
    def read_metadata(self):
        log_section("READING METADATA")
        try:
            with open("video/data.json", "r") as json_file:
                data = json.load(json_file)
                positional_data = data["positionDetails"]
            self.frame_timestamps = [loc_data[0] for loc_data in positional_data]
            log.info(f"✅ Metadata loaded: {len(self.frame_timestamps)} position entries found")
            if self.frame_timestamps:
                log.info(f"   Sample entry: {self.frame_timestamps[0]}")
        except Exception as e:
            log.error(f"❌ Error reading metadata file: {e}")
            exit(1)

    # =========================================================================
    # STEP 3b: COMPUTE 10m GPS-BASED FRAME TIMESTAMPS
    # =========================================================================
    def compute_10m_frame_timestamps(self):
        """
        Walk GPS points from frame_timestamps and find video offsets at every 10m
        along the route using haversine distance + linear interpolation.
        Returns list of dicts: {video_offset_sec, latitude, longitude, orientation, timeElapsed}
        """
        from math import radians, cos, sin, sqrt, atan2

        def haversine(lat1, lon1, lat2, lon2):
            R = 6371000.0
            phi1, phi2 = radians(lat1), radians(lat2)
            dphi = radians(lat2 - lat1)
            dlambda = radians(lon2 - lon1)
            a = sin(dphi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(dlambda / 2) ** 2
            return 2 * R * atan2(sqrt(a), sqrt(1 - a))

        points = self.frame_timestamps
        if not points:
            log.warning("⚠️ No GPS points found — cannot compute 10m timestamps")
            return []

        INTERVAL = 10.0  # metres between frames
        result = []
        cumulative = 0.0
        next_milestone = 0.0

        for i, p in enumerate(points):
            if i == 0:
                # Capture the very start (0m)
                result.append({
                    "video_offset_sec": p["timeElapsed"],
                    "latitude": p["latitude"],
                    "longitude": p["longitude"],
                    "orientation": p.get("orientation", "landscapeLeft"),
                    "timeElapsed": p["timeElapsed"],
                })
                next_milestone = INTERVAL
                continue

            prev = points[i - 1]
            seg_dist = haversine(
                prev["latitude"], prev["longitude"],
                p["latitude"],    p["longitude"]
            )
            if seg_dist == 0:
                continue

            # One segment can span multiple 10m milestones
            while cumulative + seg_dist >= next_milestone:
                frac = (next_milestone - cumulative) / seg_dist
                lat = prev["latitude"]  + frac * (p["latitude"]  - prev["latitude"])
                lon = prev["longitude"] + frac * (p["longitude"] - prev["longitude"])
                t   = prev["timeElapsed"] + frac * (p["timeElapsed"] - prev["timeElapsed"])
                result.append({
                    "video_offset_sec": t,
                    "latitude": lat,
                    "longitude": lon,
                    "orientation": p.get("orientation", "landscapeLeft"),
                    "timeElapsed": t,
                })
                next_milestone += INTERVAL

            cumulative += seg_dist

        log.info(f"✅ 10m GPS timestamps computed: {len(result)} frames "
                 f"(total distance ≈ {cumulative:.0f}m)")
        return result

    # =========================================================================
    # STEP 4: PHASE 1 - FRAME EXTRACTION (10m GPS-based)
    # =========================================================================
    def extract_all_frames_only(self):
        log_section("PHASE 1: FRAME EXTRACTION (10m GPS-based)")

        video_path   = "video/file.mp4"
        frames_dir   = f"{self.job_number}/frames"
        target_height = 640

        # ── Copy video to RAM for fast parallel seeks ──────────────────────────
        ram_video_path = f"/dev/shm/{self.CODEBUILD_ID}.mp4"
        try:
            size_mb = os.path.getsize(video_path) / (1024 * 1024)
            log.info(f"📋 Copying {size_mb:.1f} MB video to RAM (/dev/shm)...")
            t0 = time.time()
            shutil.copy2(video_path, ram_video_path)
            log.info(f"✅ Video in RAM in {time.time()-t0:.1f}s")
            video_path = ram_video_path
        except Exception as e:
            log.warning(f"⚠️ Could not copy to /dev/shm ({e}) — using disk path")

        # ── Video properties ───────────────────────────────────────────────────
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            log.error(f"❌ Could not open video: {video_path}")
            exit(1)
        width       = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height      = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        video_fps   = float(cap.get(cv2.CAP_PROP_FPS))
        cap.release()
        duration_sec = frame_count / video_fps

        log.info(f"Video Resolution : {width}x{height}")
        log.info(f"Video FPS        : {video_fps:.2f}")
        log.info(f"Total Frames     : {frame_count}")
        log.info(f"Duration         : {duration_sec:.2f}s ({duration_sec/60:.1f} min)")

        # ── Compute 10m GPS-based timestamps ──────────────────────────────────
        target_timestamps = self.compute_10m_frame_timestamps()

        # Clamp timestamps to video duration and filter out-of-range
        target_timestamps = [
            ts for ts in target_timestamps
            if 0 <= ts["video_offset_sec"] <= duration_sec
        ]

        # ── Fallback: GPS had no movement (all same coords or < 2 frames) ──────
        # Extract one frame every TIME_INTERVAL_SEC seconds using GPS coords
        # from the first valid trackpoint for all frames.
        TIME_INTERVAL_SEC = 5.0
        if len(target_timestamps) < 2:
            log.warning(
                f"⚠️ GPS-based extraction produced only {len(target_timestamps)} frame(s) "
                f"(total distance ≈ 0m). Falling back to 1 frame every {TIME_INTERVAL_SEC}s."
            )
            ref_lat = self.frame_timestamps[0]["latitude"]  if self.frame_timestamps else 0.0
            ref_lon = self.frame_timestamps[0]["longitude"] if self.frame_timestamps else 0.0
            ref_ori = self.frame_timestamps[0].get("orientation", "portraitUp") if self.frame_timestamps else "portraitUp"
            target_timestamps = [
                {
                    "video_offset_sec": t,
                    "latitude":  ref_lat,
                    "longitude": ref_lon,
                    "orientation": ref_ori,
                    "timeElapsed": t,
                }
                for t in [
                    i * TIME_INTERVAL_SEC
                    for i in range(int(duration_sec / TIME_INTERVAL_SEC) + 1)
                    if i * TIME_INTERVAL_SEC <= duration_sec
                ]
            ]
            log.info(f"↳ Fallback timestamps: {len(target_timestamps)} frames over {duration_sec:.1f}s")

        if not target_timestamps:
            log.error("❌ No valid timestamps to extract frames from!")
            exit(1)

        num_frames = len(target_timestamps)
        log.info(f"Frames to extract: {num_frames} (one per 10m of road)")

        import concurrent.futures as _cf
        import threading as _threading

        # ── GCS upload pool (pipelined) ────────────────────────────────────────
        import requests.adapters as _ra
        _ra_adapter = _ra.HTTPAdapter(pool_connections=32, pool_maxsize=32)
        self.gcs_client._http.mount("https://", _ra_adapter)
        self.gcs_client._http.mount("http://",  _ra_adapter)

        _base_gcs_path   = f"{self.BUCKET_PREFIX}/{self.CODEBUILD_ID}/frame_data"
        _upload_pool     = _cf.ThreadPoolExecutor(max_workers=32)
        _upload_futures  = []
        _upload_counters = {"frames": 0, "predict": 0, "failed": 0}
        _upload_lock     = _threading.Lock()

        def _gcs_upload(local_path, blob_name):
            try:
                self.gcs_client.bucket(self.BUCKET_NAME_JSON).blob(blob_name).upload_from_filename(
                    local_path, content_type="image/jpeg"
                )
                with _upload_lock:
                    if "/frames/" in blob_name:
                        _upload_counters["frames"] += 1
                    else:
                        _upload_counters["predict"] += 1
            except Exception as e:
                log.error(f"❌ Upload failed {os.path.basename(local_path)}: {e}")
                with _upload_lock:
                    _upload_counters["failed"] += 1

        # ── Parallel FFmpeg segment extraction (H.265 compatible) ────────────
        # Splits the video into NUM_WORKERS time segments and runs one FFmpeg
        # process per segment in parallel. Each process decodes only 1/NUM_WORKERS
        # of the video (8× speedup on E2_HIGHCPU_8).
        #
        # Uses timestamp-based select (lte(abs(t-T),eps)) instead of frame-number
        # select — works correctly after keyframe seek regardless of H.264/H.265 codec.
        import tempfile as _tempfile

        extract_start = time.time()
        NUM_WORKERS   = min(os.cpu_count() or 4, 8)
        seg_dur       = duration_sec / NUM_WORKERS
        eps           = 0.5 / video_fps        # half-frame window for timestamp match

        # Group target timestamps into segments by their video offset
        segments = [[] for _ in range(NUM_WORKERS)]
        for i, ts in enumerate(target_timestamps):
            seg_idx = min(int(ts["video_offset_sec"] / seg_dur), NUM_WORKERS - 1)
            segments[seg_idx].append((i, ts))

        extracted_ok   = 0
        extracted_lock = _threading.Lock()

        def _run_segment(seg_idx):
            nonlocal extracted_ok
            items = segments[seg_idx]
            if not items:
                return

            seg_start = seg_idx * seg_dur
            seg_end   = min((seg_idx + 1) * seg_dur + eps, duration_sec)

            # After -ss seek, FFmpeg resets t=0 at the seek point.
            # So use t relative to seg_start for the select expression.
            sorted_items = sorted(items, key=lambda x: x[1]["video_offset_sec"])
            conditions = "+".join(
                f"lte(abs(t-{ts['video_offset_sec'] - seg_start:.6f}),{eps:.6f})"
                for _, ts in sorted_items
            )

            # Use frames_dir directly as tmp (same filesystem) to avoid cross-device rename
            tmp_dir = _tempfile.mkdtemp(prefix=f"seg{seg_idx}_", dir=frames_dir)
            out_pat = os.path.join(tmp_dir, "f_%05d.jpg")

            cmd = [
                "ffmpeg", "-y",
                "-ss", f"{seg_start:.6f}",
                "-to", f"{seg_end:.6f}",
                "-i", video_path,
                "-vf", f"select='{conditions}',scale=-1:{target_height}",
                "-vsync", "0",
                "-q:v", "4",
                out_pat
            ]
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if result.returncode != 0:
                log.error(f"❌ Segment {seg_idx} FFmpeg failed:\n{result.stderr[-400:]}")
                shutil.rmtree(tmp_dir, ignore_errors=True)
                return

            # FFmpeg outputs frames in chronological order → match to sorted items
            extracted_files = sorted(glob.glob(os.path.join(tmp_dir, "f_*.jpg")))

            count = 0
            for j, (orig_idx, _) in enumerate(sorted_items):
                if j >= len(extracted_files):
                    log.warning(f"⚠️ Seg {seg_idx}: missing frame for orig_idx={orig_idx}")
                    continue
                dst_path = os.path.join(frames_dir, f"frame_{orig_idx:05d}.jpg")
                shutil.move(extracted_files[j], dst_path)  # same filesystem → atomic
                blob_name = f"{_base_gcs_path}/frames/frame_{orig_idx:05d}.jpg"
                _upload_futures.append(_upload_pool.submit(_gcs_upload, dst_path, blob_name))
                count += 1

            with extracted_lock:
                extracted_ok += count

            shutil.rmtree(tmp_dir, ignore_errors=True)
            log.info(f"   Segment {seg_idx}: {count}/{len(items)} frames extracted")

        log.info(f"🎬 Parallel FFmpeg extraction — {NUM_WORKERS} segments × "
                 f"~{seg_dur:.0f}s each ({num_frames} target frames)...")
        with _cf.ThreadPoolExecutor(max_workers=NUM_WORKERS) as pool:
            list(pool.map(_run_segment, range(NUM_WORKERS)))

        log.info(f"✅ Frames extracted in {time.time()-extract_start:.1f}s "
                 f"({extracted_ok}/{num_frames} ok)")

        # ── Build metadata + copy to predict/ ─────────────────────────────────
        self.temp_frame_data = [None] * num_frames
        self.dataframe_rows  = [None] * num_frames
        self.json_pavement["info"]["fps"] = 1.0  # 1fps playback for annotated video

        missing_lock   = _threading.Lock()
        missing_frames = 0

        def _copy_and_build(i):
            nonlocal missing_frames
            filename = f"frame_{i:05d}.jpg"
            src_path = f"{self.job_number}/frames/{filename}"
            dst_path = f"{self.job_number}/predict/{filename}"
            if not os.path.exists(src_path):
                with missing_lock:
                    missing_frames += 1
                return
            shutil.copy(src_path, dst_path)
            # Submit predict/ upload immediately (pipelined)
            blob_name = f"{_base_gcs_path}/predict/{filename}"
            _upload_futures.append(_upload_pool.submit(_gcs_upload, dst_path, blob_name))
            # Use interpolated GPS data at the exact 10m mark
            ts = target_timestamps[i]
            timestamp_data = {
                "timeElapsed":   ts["timeElapsed"],
                "latitude":      ts["latitude"],
                "longitude":     ts["longitude"],
                "orientation":   ts["orientation"],
                "og_file":       src_path,
            }
            self.temp_frame_data[i] = timestamp_data
            self.dataframe_rows[i]  = [filename, self.CODEBUILD_ID, "NO", "NO", "NO", "NO", "NO"]

        with _cf.ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(_copy_and_build, range(num_frames)))

        log.info(f"✅ PHASE 1 COMPLETE: {num_frames - missing_frames}/{num_frames} frames organized")
        if missing_frames:
            log.warning(f"⚠️ {missing_frames} frames were missing")

        # ── Free RAM copy ──────────────────────────────────────────────────────
        if os.path.exists(ram_video_path):
            try:
                os.remove(ram_video_path)
                log.info("🗑️  RAM video copy freed.")
            except Exception:
                pass

        # ── Wait for any remaining in-flight GCS uploads ──────────────────────
        upload_wait_start = time.time()
        log.info(f"⏳ Waiting for {len(_upload_futures)} pipelined GCS uploads to finish...")
        _cf.wait(_upload_futures)
        _upload_pool.shutdown(wait=False)
        log.info(f"✅ Pipelined GCS upload complete in {time.time() - upload_wait_start:.1f}s extra wait")
        log.info(f"   frames/  uploaded : {_upload_counters['frames']}")
        log.info(f"   predict/ uploaded : {_upload_counters['predict']}")
        log.info(f"   failed            : {_upload_counters['failed']}")
        self._pipelined_upload_done     = True
        self._pipelined_upload_counters = _upload_counters

        return num_frames

    # =========================================================================
    # STEP 5: UPLOAD FRAMES TO GCS
    # =========================================================================
    def upload_frames_to_gcs(self, num_frames):
        import concurrent.futures
        import threading
        import requests.adapters as _ra

        log_section("UPLOADING FRAMES TO GCS")

        # If pipelining already uploaded all frames during extraction, skip.
        if getattr(self, '_pipelined_upload_done', False):
            c = self._pipelined_upload_counters
            log.info("✅ Frames already uploaded during extraction (pipelined) — skipping.")
            log.info(f"   frames/  uploaded : {c['frames']}")
            log.info(f"   predict/ uploaded : {c['predict']}")
            log.info(f"   failed            : {c['failed']}")
            return

        # Expand the urllib3 connection pool to match the number of upload workers.
        # Default pool size is 10; with 32 workers the excess connections are discarded
        # and recreated each time, causing the "Connection pool is full" warning.
        _adapter = _ra.HTTPAdapter(pool_connections=32, pool_maxsize=32)
        self.gcs_client._http.mount("https://", _adapter)
        self.gcs_client._http.mount("http://",  _adapter)

        base_gcs_path = f"{self.BUCKET_PREFIX}/{self.CODEBUILD_ID}/frame_data"

        log.info(f"Destination: gs://{self.BUCKET_NAME_JSON}/{base_gcs_path}/")
        log.info(f"Uploading {num_frames} frames (frames/ + predict/) with 32 parallel threads...")

        # Build task list: (local_path, gcs_blob_name)
        tasks = []
        for i in range(num_frames):
            filename = f"frame_{i:05d}.jpg"
            frames_local   = f"{self.job_number}/frames/{filename}"
            predict_local  = f"{self.job_number}/predict/{filename}"
            if os.path.exists(frames_local):
                tasks.append((frames_local,  f"{base_gcs_path}/frames/{filename}"))
            if os.path.exists(predict_local):
                tasks.append((predict_local, f"{base_gcs_path}/predict/{filename}"))

        counters = {"frames": 0, "predict": 0, "failed": 0}
        lock = threading.Lock()

        def _upload(local_path, blob_name):
            # Each thread gets its own bucket reference (GCS client is thread-safe)
            try:
                self.gcs_client.bucket(self.BUCKET_NAME_JSON).blob(blob_name).upload_from_filename(
                    local_path, content_type="image/jpeg"
                )
                with lock:
                    if "/frames/" in blob_name:
                        counters["frames"] += 1
                    else:
                        counters["predict"] += 1
            except Exception as e:
                log.error(f"❌ Failed to upload {os.path.basename(local_path)}: {e}")
                with lock:
                    counters["failed"] += 1

        start = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
            futs = [pool.submit(_upload, lp, bn) for lp, bn in tasks]
            done = 0
            for _ in concurrent.futures.as_completed(futs):
                done += 1
                if done % 50 == 0 or done == len(tasks):
                    log.info(f"   Upload progress: {done}/{len(tasks)} blobs done")

        elapsed = time.time() - start
        log.info(f"✅ Frames upload complete in {elapsed:.1f}s")
        log.info(f"   frames/  uploaded : {counters['frames']}")
        log.info(f"   predict/ uploaded : {counters['predict']}")
        log.info(f"   failed            : {counters['failed']}")

    # =========================================================================
    # STEP 6: LOCAL YOLO INFERENCE (GPU)
    # =========================================================================
    def run_inference_on_extracted_frames(self, num_frames):
        engine_label = "TensorRT FP16" if self.USE_TENSORRT else "PyTorch CUDA"
        log_section(f"PHASE 2: RUNNING LOCAL YOLO INFERENCE ({engine_label})")

        # Collect valid frame paths
        frame_paths = []
        frame_indices = []
        for i in range(num_frames):
            if self.temp_frame_data[i] is None:
                continue
            path = f"{self.job_number}/predict/frame_{i:05d}.jpg"
            if os.path.exists(path):
                frame_paths.append(path)
                frame_indices.append(i)

        log.info(f"Total frames to infer : {len(frame_paths)}")
        log.info(f"Engine                : {engine_label}")
        inference_start = time.time()

        # TensorRT engine was built with fixed batch=1, so we must infer one frame at a time.
        # PyTorch can handle larger batches.
        if self.USE_TENSORRT:
            BATCH_SIZE = 1
        else:
            BATCH_SIZE = int(os.environ.get("INFERENCE_BATCH_SIZE", "32"))
        log.info(f"Batch size            : {BATCH_SIZE}")

        processed_count = 0
        for batch_start in range(0, len(frame_paths), BATCH_SIZE):
            batch_paths = frame_paths[batch_start:batch_start + BATCH_SIZE]
            batch_indices = frame_indices[batch_start:batch_start + BATCH_SIZE]

            results = self.yolo_model.predict(
                source=batch_paths,
                conf=0.25,
                device="cuda",
                half=self.USE_TENSORRT,
                verbose=False,
            )

            for result, frame_index in zip(results, batch_indices):
                frame_name = f"frame_{frame_index:05d}.jpg"
                gcs_file_path = f"gs://{self.BUCKET_NAME_JSON}/{self.BUCKET_PREFIX}/{self.CODEBUILD_ID}/frame_data/predict/{frame_name}"
                h, w = result.orig_shape

                if frame_index not in self.images_added:
                    self.json_pavement["images"].append(
                        {"width": w, "height": h, "id": frame_index, "file_name": gcs_file_path}
                    )
                    self.images_added.add(frame_index)

                boxes = result.boxes
                if boxes is not None and len(boxes) > 0:
                    for box in boxes:
                        cls_id = int(box.cls.item())
                        conf = float(box.conf.item())
                        # Convert xyxy to COCO format [x, y, width, height]
                        x1, y1, x2, y2 = box.xyxy[0].tolist()
                        bbox = [x1, y1, x2 - x1, y2 - y1]

                        self.json_pavement["annotations"].append({
                            "id": self.annotation_id_pavement,
                            "image_id": frame_index,
                            "category_id": cls_id,
                            "segmentation": [],
                            "bbox": bbox,
                            "ignore": 0, "iscrowd": 0,
                            "area": bbox[2] * bbox[3],
                            "confidence": conf,
                            "ai_model": "YOLO-Local-GPU"
                        })
                        self.annotation_id_pavement += 1

            processed_count += len(batch_paths)
            if processed_count % 50 == 0 or processed_count == len(frame_paths):
                elapsed = time.time() - inference_start
                rate = processed_count / elapsed if elapsed > 0 else 0
                log.info(f"   🤖 YOLO GPU: {processed_count}/{len(frame_paths)} frames | "
                         f"{rate:.1f} fps | {self.annotation_id_pavement} annotations")

        elapsed = time.time() - inference_start
        log.info(f"✅ PHASE 2 COMPLETE in {elapsed:.1f}s")
        log.info(f"   Total frames processed  : {processed_count}")
        log.info(f"   Total annotations found : {self.annotation_id_pavement}")

        for i in range(num_frames):
            if self.dataframe_rows[i]:
                self.info_dataframe.loc[len(self.info_dataframe)] = self.dataframe_rows[i]
        self.frame_data = [fd for fd in self.temp_frame_data if fd is not None]

    # =========================================================================
    # STEP 7: SAVE FRAME INFO CSV
    # =========================================================================
    def save_frame_info(self):
        log_section("SAVING FRAME INFO CSV")
        csv_path = f"{self.job_number}/frame_info.csv"
        self.info_dataframe.to_csv(csv_path, index=False)
        log.info(f"✅ frame_info.csv saved: {len(self.info_dataframe)} rows → {csv_path}")

    # =========================================================================
    # STEP 8: SAVE AND UPLOAD result.json
    # ✅ CHANGED: Upload 2 now uses {BUILD_ID}/result.json (was bare {BUILD_ID})
    #             GCS trigger pattern **/result.json will now fire automatically
    # =========================================================================
    def save_json(self, json_filename="result.json"):
        log_section("SAVING AND UPLOADING RESULT JSON")

        with open(json_filename, "w") as json_file:
            json.dump([self.json_pavement], json_file, indent=4)

        size_kb = os.path.getsize(json_filename) / 1024
        log.info(f"result.json saved locally: {size_kb:.1f} KB")

        # Upload: datanh11/processed-data/{ROAD_ID}/{BUILD_ID}/result.json
        artifacts_path = f"{self.BUCKET_PREFIX}/{self.CODEBUILD_ID}/result.json"
        log.info(f"📤 Uploading to gs://{self.BUCKET_NAME_JSON}/{artifacts_path}...")
        bucket_json = self.gcs_client.bucket(self.BUCKET_NAME_JSON)
        bucket_json.blob(artifacts_path).upload_from_filename(json_filename, content_type="application/json")
        log.info(f"✅ result.json uploaded to gs://{self.BUCKET_NAME_JSON}/{artifacts_path}")

    # =========================================================================
    # STEP 9: UPLOAD chainage_report.csv
    # =========================================================================
    def upload_chainage_report(self):
        log_section("UPLOADING CHAINAGE REPORT")
        chainage_file = "chainage_report.csv"

        if not os.path.exists(chainage_file):
            log.warning(f"⚠️ {chainage_file} not found — skipping upload")
            return

        gcs_path = f"{self.BUCKET_PREFIX}/{self.CODEBUILD_ID}/{chainage_file}"
        log.info(f"📤 Uploading to gs://{self.BUCKET_NAME_JSON}/{gcs_path}...")
        bucket = self.gcs_client.bucket(self.BUCKET_NAME_JSON)
        bucket.blob(gcs_path).upload_from_filename(chainage_file, content_type="text/csv")
        log.info(f"✅ chainage_report.csv uploaded to gs://{self.BUCKET_NAME_JSON}/{gcs_path}")

    # =========================================================================
    # STEP 10: POST TO API
    # =========================================================================
    def post_api(self):
        log_section("POSTING DATA TO API")
        for frame in self.frame_data:
            frame["inference_info"] = []
            frame["inference_image"] = frame["og_file"].replace("frames", "predict")

        final_output_for_API = {
            "uid": self.UID,
            "data": json.dumps({
                "frame_list_data": self.frame_data,
                "CODEBUILD_BUILD_ID": self.CODEBUILD_ID,
            }),
        }

        log.info(f"API URL    : {self.BASE_URL}flutterApi/auth/add_data")
        log.info(f"UID        : {self.UID}")
        log.info(f"Frame data : {len(self.frame_data)} frames")

        try:
            response = requests.post(self.BASE_URL + "flutterApi/auth/add_data", data=final_output_for_API)
            resp_json = response.json()
            if resp_json.get("status") == "success":
                log.info("✅ Data added successfully to API!")
                shutil.rmtree("video")
                log.info("🗑️  Cleaned up video directory.")
            else:
                # Non-fatal: video may not be registered in the app — pipeline continues
                log.warning(f"⚠️ API post skipped (video not registered in app): {resp_json.get('result', resp_json)}")
        except Exception as e:
            log.warning(f"⚠️ API post failed (non-fatal): {e}")

    # =========================================================================
    # MAIN RUN
    # =========================================================================
    def run(self):
        pipeline_start = time.time()
        log_section("PIPELINE STARTED")
        log.info(f"Start time: {datetime.now().isoformat()}")

        self.setup_directories()
        self.download_s3_files()
        self.read_metadata()
        total_frames = self.extract_all_frames_only()
        self.upload_frames_to_gcs(total_frames)
        self.run_inference_on_extracted_frames(total_frames)
        self.save_frame_info()
        self.save_json()          # ← uploads {BUILD_ID}/result.json → triggers annotation pipeline
        self.upload_chainage_report()
        self.post_api()
         # Change this order!
        # self.post_api()           # 1. Save to database FIRST
        # self.save_json()          # 2. Upload result.json LAST (triggers Pipeline 2)
        # self.upload_chainage_report()   

        elapsed = time.time() - pipeline_start
        log_section("PIPELINE COMPLETED")
        log.info(f"Total time : {elapsed:.1f}s ({elapsed/60:.1f} min)")
        log.info(f"Frames     : {total_frames}")
        log.info(f"Annotations: {self.annotation_id_pavement}")
        log.info(f"UID        : {self.UID}")
        log.info(f"Run ID     : {self.CODEBUILD_ID}")
        log.info("")
        log.info("📦 Final GCS locations:")
        log.info(f"   Frames   → gs://{self.BUCKET_NAME_JSON}/{self.BUCKET_PREFIX}/{self.CODEBUILD_ID}/frame_data/frames/")
        log.info(f"   Predict  → gs://{self.BUCKET_NAME_JSON}/{self.BUCKET_PREFIX}/{self.CODEBUILD_ID}/frame_data/predict/")
        log.info(f"   JSON     → gs://{self.BUCKET_NAME_JSON}/{self.BUCKET_PREFIX}/{self.CODEBUILD_ID}/result.json")
        log.info(f"   Chainage → gs://{self.BUCKET_NAME_JSON}/{self.BUCKET_PREFIX}/{self.CODEBUILD_ID}/chainage_report.csv")


if __name__ == "__main__":
    pipeline = RoadvisionPipeline()
    pipeline.run()
