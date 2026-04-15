"""
trigger_builds.py
=================
Orchestrator: lists every .MP4 in the source GCS bucket, auto-detects
the correct Road ID for each video by matching its GPX start/end coords
against the road database, then runs the pipeline locally on this VM
using the GPU (Nvidia T4) for YOLO inference.

Modes:
  1. One-shot: process all videos currently in the bucket, then exit
  2. Watch:    monitor bucket for new uploads, process in batches

Usage:
    # One-shot — process all videos for a road:
    python trigger_builds.py --road_id R525275 --parallel 4

    # Watch mode — monitor bucket, process in batches of 8:
    python trigger_builds.py --watch --road_id R000101 --parallel 8 --batch_size 8

    # Watch all roads:
    python trigger_builds.py --watch --parallel 8 --batch_size 8
"""

import argparse
import concurrent.futures
import csv
import json as _json
import os
import shutil
import sys
import uuid
import time
import subprocess
import threading
import xml.etree.ElementTree as ET
from datetime import datetime
from math import radians, sin, cos, sqrt, atan2

from google.cloud import storage


# ── Routing log helpers ────────────────────────────────────────────────────

def open_routing_log(log_dir: str = "logs") -> tuple:
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path  = os.path.join(log_dir, f"routing_{timestamp}.csv")
    fh = open(log_path, "w", newline="", encoding="utf-8")
    writer = csv.writer(fh)
    writer.writerow(["timestamp", "video_file", "road_id", "run_id", "output_path", "status"])
    fh.flush()
    print(f"[*] Routing log: {os.path.abspath(log_path)}")
    return fh, writer


def log_routing(writer, fh, video_file, road_id, run_id, output_path, status):
    writer.writerow([
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        video_file,
        road_id,
        run_id,
        output_path,
        status,
    ])
    fh.flush()


# ── Constants ──────────────────────────────────────────────────────────────

MATCH_THRESHOLD_M = 2000.0


# ── Haversine ──────────────────────────────────────────────────────────────

def haversine_m(lat1, lon1, lat2, lon2) -> float:
    R = 6371000.0
    p1, p2 = radians(lat1), radians(lat2)
    dp = radians(lat2 - lat1)
    dl = radians(lon2 - lon1)
    a  = sin(dp / 2) ** 2 + cos(p1) * cos(p2) * sin(dl / 2) ** 2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))


# ── GPX helpers ────────────────────────────────────────────────────────────

def get_gpx_endpoints(gpx_bytes: bytes):
    try:
        root = ET.fromstring(gpx_bytes)
        ns   = ''
        if root.tag.startswith('{'):
            ns = root.tag.split('}')[0] + '}'
        trkpts = root.findall(f'.//{ns}trkpt')
        if not trkpts:
            return None
        valid = [
            pt for pt in trkpts
            if not (float(pt.attrib['lat']) == 0.0 and float(pt.attrib['lon']) == 0.0)
        ]
        if not valid:
            return None
        start = (float(valid[0].attrib['lat']),  float(valid[0].attrib['lon']))
        end   = (float(valid[-1].attrib['lat']), float(valid[-1].attrib['lon']))
        return start, end
    except Exception as e:
        print(f"    [!] GPX parse error: {e}")
        return None


# ── Road ID helpers ────────────────────────────────────────────────────────

def generate_road_id(start_lat, start_lon, end_lat, end_lon) -> str:
    fingerprint = f"{start_lat}-{start_lon}-{end_lat}-{end_lon}"
    h = 0
    for ch in fingerprint:
        h = ((h << 5) - h) + ord(ch)
        h &= 0xFFFFFFFF
        if h >= 0x80000000:
            h -= 0x100000000
    unique_num = abs(h) % 1_000_000
    return f"R{unique_num:06d}"


def load_roads_from_json(path: str) -> list[dict]:
    import json
    with open(path, "r") as f:
        roads = json.load(f)
    normalized = []
    for r in roads:
        try:
            normalized.append({
                "road_id":         r["road_id"],
                "road_name":       r.get("road_name", ""),
                "start_latitude":  r["start_point"]["latitude"],
                "start_longitude": r["start_point"]["longitude"],
                "end_latitude":    r["end_point"]["latitude"],
                "end_longitude":   r["end_point"]["longitude"],
            })
        except (KeyError, TypeError):
            continue
    return normalized


def point_to_segment_distance(p_lat, p_lon, a_lat, a_lon, b_lat, b_lon) -> float:
    abx, aby = b_lon - a_lon, b_lat - a_lat
    apx, apy = p_lon - a_lon, p_lat - a_lat
    ab2 = abx * abx + aby * aby
    if ab2 == 0:
        return haversine_m(p_lat, p_lon, a_lat, a_lon)
    t = max(0.0, min(1.0, (apx * abx + apy * aby) / ab2))
    return haversine_m(p_lat, p_lon, a_lat + t * aby, a_lon + t * abx)


def match_road_id(video_start, video_end, roads, threshold_m=MATCH_THRESHOLD_M):
    best_id    = None
    best_score = float("inf")

    for road in roads:
        try:
            r_start = (float(road["start_latitude"]),  float(road["start_longitude"]))
            r_end   = (float(road["end_latitude"]),    float(road["end_longitude"]))
        except (KeyError, TypeError, ValueError):
            continue

        score = (
            point_to_segment_distance(*video_start, *r_start, *r_end) +
            point_to_segment_distance(*video_end,   *r_start, *r_end)
        )

        if score < best_score:
            best_score = score
            best_id    = road.get("road_id")

    if best_score <= threshold_m * 2:
        return best_id, best_score
    return None, best_score


def detect_road_id_for_video(gcs_client, bucket_name, mp4_blob_name, roads, fallback_road_id):
    filename = mp4_blob_name.split("/")[-1]

    if fallback_road_id:
        print(f"    [{filename}] → Using forced road_id: {fallback_road_id}")
        return fallback_road_id

    gpx_blob_name = mp4_blob_name.rsplit(".", 1)[0] + ".gpx"
    print(f"    [{filename}] Downloading GPX: {gpx_blob_name}")
    try:
        gpx_bytes = gcs_client.bucket(bucket_name).blob(gpx_blob_name).download_as_bytes()
    except Exception as e:
        print(f"    [{filename}] ⚠ GPX download failed: {e}")
        return "R000000"

    endpoints = get_gpx_endpoints(gpx_bytes)
    if not endpoints:
        print(f"    [{filename}] ⚠ Could not parse GPX endpoints")
        return "R000000"

    video_start, video_end = endpoints
    print(f"    [{filename}] GPS endpoints → start={video_start}, end={video_end}")

    if roads:
        matched, best_score = match_road_id(video_start, video_end, roads)
        print(f"    [{filename}] Best match: {matched}  score={best_score:.0f}m  (threshold={MATCH_THRESHOLD_M*2:.0f}m)")
        if matched:
            print(f"    [{filename}] ✅ Matched road ID: {matched}")
            return matched
        print(f"    [{filename}] ⚠ No road matched — closest was {best_score:.0f}m away")

    computed = generate_road_id(*video_start, *video_end)
    print(f"    [{filename}] ⚠ Computed road ID from GPS (no match): {computed}")
    return computed


# ── Local pipeline runner ──────────────────────────────────────────────────

def run_pipeline_locally(gcs_uri, road_id, run_id, model_weights, log_prefix=""):
    """
    Run main.py as a subprocess with the required environment variables.
    Each process gets its own working directory (workdir/{run_id}/) to avoid
    file conflicts when running multiple videos in parallel.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    main_script = os.path.join(script_dir, "main.py")

    # Create isolated working directory for this run
    work_dir = os.path.join(script_dir, "workdir", run_id)
    os.makedirs(work_dir, exist_ok=True)

    # Symlink the pipeline source files into the work directory
    for item in ["main.py", "pipeline1.py", "annotation-pipeline", "annotation-pipeline-bak"]:
        src = os.path.join(script_dir, item)
        dst = os.path.join(work_dir, item)
        if not os.path.exists(dst):
            os.symlink(src, dst)

    env = os.environ.copy()
    env["GCS_URI"] = gcs_uri
    env["ROAD_ID"] = road_id
    env["RUN_ID"] = run_id
    env["MODEL_WEIGHTS"] = model_weights

    print(f"{log_prefix}🚀 Launching pipeline (workdir: workdir/{run_id[:8]}...)")

    # Write logs to a file so parallel output doesn't interleave
    log_file = os.path.join(work_dir, "pipeline.log")
    with open(log_file, "w") as lf:
        result = subprocess.run(
            [sys.executable, main_script],
            env=env,
            cwd=work_dir,
            stdout=lf,
            stderr=subprocess.STDOUT,
        )

    status = "SUCCESS" if result.returncode == 0 else "FAILED"

    # Keep only pipeline.log, clean up everything else
    for item in os.listdir(work_dir):
        if item == "pipeline.log":
            continue
        path = os.path.join(work_dir, item)
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path, ignore_errors=True)
        elif os.path.isfile(path) or os.path.islink(path):
            os.remove(path)

    if status == "FAILED":
        print(f"{log_prefix}❌ FAILED — see log: {log_file}")

    return status


# ── Processing tracker ──────────────────────────────────────────────────

TRACKER_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "processing_tracker.json")

def load_tracker():
    """Load the processing tracker JSON file."""
    if os.path.exists(TRACKER_FILE):
        with open(TRACKER_FILE, "r") as f:
            return _json.load(f)
    return {}

def save_tracker(tracker):
    """Save the processing tracker JSON file."""
    with open(TRACKER_FILE, "w") as f:
        _json.dump(tracker, f, indent=2)

def is_video_tracked(tracker, road_id, fname):
    """Check if a video has already been processed or is processing."""
    return fname in tracker.get(road_id, {})

def track_video(tracker, road_id, fname, run_id, status="processing"):
    """Add or update a video in the tracker."""
    if road_id not in tracker:
        tracker[road_id] = {}
    tracker[road_id][fname] = {
        "uuid": run_id,
        "status": status,
        "started": datetime.now().isoformat(),
        "finished": None,
    }
    save_tracker(tracker)

def update_tracker_status(tracker, road_id, fname, status):
    """Update the status of a tracked video."""
    if road_id in tracker and fname in tracker[road_id]:
        tracker[road_id][fname]["status"] = status
        tracker[road_id][fname]["finished"] = datetime.now().isoformat()
        save_tracker(tracker)


# ── Batch processing helper ────────────────────────────────────────────

def process_batch(jobs, num_parallel, model_weights, source_bucket, log_writer, log_fh, tracker):
    """Process a batch of jobs and return results."""
    log_lock = threading.Lock()

    def safe_log(writer, fh, *row_args):
        with log_lock:
            log_routing(writer, fh, *row_args)

    all_results = []
    wall_clock_start = time.time()
    total = len(jobs)

    if num_parallel <= 1:
        for idx, job in enumerate(jobs, start=1):
            output_path = f"gs://{source_bucket}/processed-data/{job['road_id']}/{job['run_id']}/"
            print(f"\n{'═'*60}")
            print(f"  Video {idx}/{total}: {job['fname']}")
            print(f"  Road ID : {job['road_id']}")
            print(f"  Run ID  : {job['run_id']}")
            print(f"  Output  : {output_path}")
            print(f"{'═'*60}")

            safe_log(log_writer, log_fh, job['fname'], job['road_id'], job['run_id'], output_path, "STARTED")
            track_video(tracker, job['road_id'], job['fname'], job['run_id'], "processing")

            start_time = time.time()
            status = run_pipeline_locally(
                job['gcs_uri'], job['road_id'], job['run_id'], model_weights,
                log_prefix=f"    ",
            )
            elapsed = time.time() - start_time

            icon = "✅" if status == "SUCCESS" else "❌"
            print(f"    {icon} {job['fname']}  [{status}] in {elapsed:.1f}s ({elapsed/60:.1f} min)")
            safe_log(log_writer, log_fh, job['fname'], job['road_id'], job['run_id'], output_path, status)
            update_tracker_status(tracker, job['road_id'], job['fname'], status.lower())
            all_results.append((job['fname'], job['road_id'], job['run_id'], status, elapsed))
    else:
        print(f"\n[*] Processing {total} video(s) with {num_parallel} parallel workers (GPU shared)...")
        print(f"    Logs for each video are saved to workdir/<run_id>/pipeline.log")

        for job in jobs:
            output_path = f"gs://{source_bucket}/processed-data/{job['road_id']}/{job['run_id']}/"
            print(f"    📋 {job['fname']}  road={job['road_id']}  run={job['run_id'][:8]}...")
            safe_log(log_writer, log_fh, job['fname'], job['road_id'], job['run_id'], output_path, "QUEUED")
            track_video(tracker, job['road_id'], job['fname'], job['run_id'], "processing")

        def _process_one(job):
            output_path = f"gs://{source_bucket}/processed-data/{job['road_id']}/{job['run_id']}/"
            tag = f"[{job['fname']}] "
            print(f"{tag}▶ Starting...")
            safe_log(log_writer, log_fh, job['fname'], job['road_id'], job['run_id'], output_path, "STARTED")

            start_time = time.time()
            status = run_pipeline_locally(
                job['gcs_uri'], job['road_id'], job['run_id'], model_weights,
                log_prefix=tag,
            )
            elapsed = time.time() - start_time

            icon = "✅" if status == "SUCCESS" else "❌"
            print(f"{tag}{icon} [{status}] in {elapsed:.1f}s ({elapsed/60:.1f} min)")
            safe_log(log_writer, log_fh, job['fname'], job['road_id'], job['run_id'], output_path, status)
            update_tracker_status(tracker, job['road_id'], job['fname'], status.lower())
            return (job['fname'], job['road_id'], job['run_id'], status, elapsed)

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_parallel) as pool:
            futures = {pool.submit(_process_one, job): job for job in jobs}
            for future in concurrent.futures.as_completed(futures):
                all_results.append(future.result())

    wall_time = time.time() - wall_clock_start
    passed = [r for r in all_results if r[3] == "SUCCESS"]
    failed = [r for r in all_results if r[3] != "SUCCESS"]
    total_time = sum(r[4] for r in all_results)
    print(f"\n{'─'*60}")
    print(f"  Batch     : {len(all_results)} videos")
    print(f"  Parallel  : {num_parallel}")
    print(f"  Passed    : {len(passed)}")
    print(f"  Failed    : {len(failed)}")
    print(f"  Wall time : {wall_time:.1f}s ({wall_time/60:.1f} min)")
    print(f"  CPU time  : {total_time:.1f}s ({total_time/60:.1f} min)")
    if failed:
        print("  Failed videos:")
        for fname, road_id, run_id, status, elapsed in failed:
            print(f"    {fname}  road={road_id}  run={run_id}  [{status}]")
    print(f"{'─'*60}")
    return all_results


# ── Watch mode ─────────────────────────────────────────────────────────

def list_new_videos(gcs_client, source_bucket, search_prefix, source_prefix, tracker, roads, forced_road_id):
    """List MP4s in GCS that haven't been tracked yet, with road ID detection."""
    blobs = gcs_client.list_blobs(source_bucket, prefix=search_prefix)
    mp4_blobs = [b.name for b in blobs if b.name.lower().endswith(".mp4")]

    new_jobs = []
    for blob_name in mp4_blobs:
        fname = blob_name.split("/")[-1]

        # Detect road ID from directory or GPX
        parts = blob_name.replace(source_prefix, "").split("/")
        if len(parts) >= 2 and parts[0].startswith("R") and parts[0][1:].isdigit():
            road_id = parts[0]
        elif forced_road_id:
            road_id = forced_road_id
        else:
            road_id = detect_road_id_for_video(
                gcs_client=gcs_client,
                bucket_name=source_bucket,
                mp4_blob_name=blob_name,
                roads=roads,
                fallback_road_id=None,
            )

        # Skip if already tracked
        if is_video_tracked(tracker, road_id, fname):
            continue

        # Check GPX exists
        gpx_blob_name = blob_name.rsplit(".", 1)[0] + ".gpx"
        gpx_exists = gcs_client.bucket(source_bucket).blob(gpx_blob_name).exists()
        if not gpx_exists:
            continue

        new_jobs.append({
            "blob_name": blob_name,
            "gcs_uri": f"gs://{source_bucket}/{blob_name}",
            "road_id": road_id,
            "run_id": str(uuid.uuid4()),
            "fname": fname,
        })

    return new_jobs


def watch_mode(args):
    """Monitor GCS bucket for new videos and process them in batches."""
    gcs_client = storage.Client()
    tracker = load_tracker()

    # Load roads for GPX matching
    if args.road_id:
        roads = []
    else:
        roads = load_roads_from_json(args.roads_json)

    if args.road_id:
        search_prefix = f"{args.source_prefix}{args.road_id}/"
    else:
        search_prefix = args.source_prefix

    batch_size = args.batch_size
    poll_interval = args.watch_interval
    settle_time = args.settle_time

    log_fh, log_writer = open_routing_log()

    print(f"\n{'═'*60}")
    print(f"  WATCH MODE")
    print(f"  Bucket    : gs://{args.source_bucket}/{search_prefix}")
    print(f"  Batch size: {batch_size}")
    print(f"  Parallel  : {args.parallel}")
    print(f"  Poll every: {poll_interval}s")
    print(f"  Settle    : {settle_time}s (wait for uploads to finish)")
    print(f"  Tracker   : {TRACKER_FILE}")
    print(f"{'═'*60}")

    total_processed = 0
    batch_num = 0

    try:
        while True:
            # Scan for new videos
            new_jobs = list_new_videos(
                gcs_client, args.source_bucket, search_prefix,
                args.source_prefix, tracker, roads, args.road_id
            )

            if not new_jobs:
                print(f"\r[*] Watching... {total_processed} processed | "
                      f"waiting for new videos ({datetime.now().strftime('%H:%M:%S')})", end="", flush=True)
                time.sleep(poll_interval)
                continue

            # Found new videos — wait for settle_time to let uploads finish
            found_count = len(new_jobs)
            print(f"\n[!] Found {found_count} new video(s) — waiting {settle_time}s for uploads to settle...")
            time.sleep(settle_time)

            # Re-scan to catch any videos uploaded during settle time
            new_jobs = list_new_videos(
                gcs_client, args.source_bucket, search_prefix,
                args.source_prefix, tracker, roads, args.road_id
            )

            if not new_jobs:
                continue

            # If we have enough for a batch, or if count stabilized
            if len(new_jobs) >= batch_size:
                # Take exactly batch_size
                batch_jobs = new_jobs[:batch_size]
            else:
                # Wait one more settle period to see if more are coming
                prev_count = len(new_jobs)
                print(f"[*] Found {prev_count} videos (need {batch_size}) — "
                      f"waiting {settle_time}s more for remaining uploads...")
                time.sleep(settle_time)

                new_jobs = list_new_videos(
                    gcs_client, args.source_bucket, search_prefix,
                    args.source_prefix, tracker, roads, args.road_id
                )

                if len(new_jobs) == prev_count:
                    # No new videos arrived — process what we have
                    print(f"[*] No more uploads detected — processing {len(new_jobs)} video(s)")
                    batch_jobs = new_jobs
                elif len(new_jobs) >= batch_size:
                    batch_jobs = new_jobs[:batch_size]
                else:
                    batch_jobs = new_jobs

            batch_num += 1
            print(f"\n{'═'*60}")
            print(f"  BATCH {batch_num}: {len(batch_jobs)} video(s)")
            print(f"{'═'*60}")

            for job in batch_jobs:
                print(f"    📋 {job['fname']}  road={job['road_id']}  run={job['run_id'][:8]}...")

            # Process the batch
            results = process_batch(
                batch_jobs, args.parallel, args.model_weights,
                args.source_bucket, log_writer, log_fh, tracker
            )
            total_processed += len(results)

            print(f"\n[*] Batch {batch_num} complete — resuming watch "
                  f"({total_processed} total processed)\n")

    except KeyboardInterrupt:
        print(f"\n\n[*] Watch mode stopped by user.")
        print(f"    Total batches   : {batch_num}")
        print(f"    Total processed : {total_processed}")
        print(f"    Tracker file    : {TRACKER_FILE}")
        log_fh.close()
        sys.exit(0)


# ── main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Run video processing pipeline locally on VM with GPU"
    )
    parser.add_argument("--road_id",       default=None,
                        help="Force a specific Road ID for ALL videos (skips auto-detection)")
    parser.add_argument("--source_bucket", default="datanh11")
    parser.add_argument("--source_prefix", default="video-processing-pipelines-data/")
    parser.add_argument("--roads_json",    default="nhai_roads.json",
                        help="Path to NHAI roads JSON file (default: nhai_roads.json)")
    parser.add_argument("--model_weights", default="/home/shubham/nhai_best.engine",
                        help="Path to YOLO model weights — .engine for TensorRT, .pt for PyTorch")
    parser.add_argument("--parallel",      type=int, default=1,
                        help="Number of videos to process in parallel (default: 1)")
    parser.add_argument("--watch",         action="store_true",
                        help="Watch mode — monitor bucket for new uploads and process in batches")
    parser.add_argument("--batch_size",    type=int, default=8,
                        help="Number of videos per batch in watch mode (default: 8)")
    parser.add_argument("--watch_interval", type=int, default=10,
                        help="Polling interval in seconds for watch mode (default: 10)")
    parser.add_argument("--settle_time",   type=int, default=60,
                        help="Seconds to wait after detecting new uploads before processing (default: 60)")
    args = parser.parse_args()

    # ── Watch mode ─────────────────────────────────────────────────────
    if args.watch:
        watch_mode(args)
        return

    # ── One-shot mode (original behavior) ──────────────────────────────
    tracker = load_tracker()
    log_fh, log_writer = open_routing_log()

    # Load NHAI roads
    if args.road_id:
        print(f"[*] --road_id forced: {args.road_id}")
        roads = []
    else:
        roads = load_roads_from_json(args.roads_json)
        print(f"[✓] {len(roads)} road(s) loaded from {args.roads_json}")

    gcs_client = storage.Client()

    # List MP4s
    if args.road_id:
        search_prefix = f"{args.source_prefix}{args.road_id}/"
    else:
        search_prefix = args.source_prefix
    print(f"[*] Listing MP4s in gs://{args.source_bucket}/{search_prefix}")
    blobs = gcs_client.list_blobs(args.source_bucket, prefix=search_prefix)
    mp4_blobs = [b.name for b in blobs if b.name.lower().endswith(".mp4")]

    if not mp4_blobs:
        print("[!] No MP4 files found. Exiting.")
        sys.exit(1)

    print(f"[✓] Found {len(mp4_blobs)} video(s)")

    # Detect road IDs
    print("\n[*] Detecting Road ID for each video...")
    jobs = []
    for blob_name in mp4_blobs:
        fname = blob_name.split("/")[-1]
        parts = blob_name.replace(args.source_prefix, "").split("/")
        if len(parts) >= 2 and parts[0].startswith("R") and parts[0][1:].isdigit():
            road_id = parts[0]
            print(f"    [{fname}] ✅ Road ID from directory: {road_id}")
        else:
            road_id = detect_road_id_for_video(
                gcs_client=gcs_client, bucket_name=args.source_bucket,
                mp4_blob_name=blob_name, roads=roads, fallback_road_id=args.road_id,
            )
        jobs.append({
            "blob_name": blob_name,
            "gcs_uri": f"gs://{args.source_bucket}/{blob_name}",
            "road_id": road_id,
            "run_id": str(uuid.uuid4()),
            "fname": fname,
        })

    print(f"\n[*] Video → Road ID mapping:")
    for job in jobs:
        print(f"    {job['fname']}  →  {job['road_id']}")

    # Process
    results = process_batch(
        jobs, args.parallel, args.model_weights,
        args.source_bucket, log_writer, log_fh, tracker
    )

    log_fh.close()
    failed = [r for r in results if r[3] != "SUCCESS"]
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
