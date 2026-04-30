"""
pipeline_v2.py — Roadvision Video Processing Pipeline, V2 (self-contained)
==========================================================================

End-to-end preprocessing for a single road, defined by an optional KML/KMZ
polyline plus a folder of MP4+GPX video pairs in GCS. Produces a single
consolidated `inference_data` record (organisation/city scoped) plus a
GCS layout that is byte-compatible with the existing IBI dashboard.

This file is INTENTIONALLY a single script. It does not import from V1
(`/home/shubham/video-processing-pipeline/`) and does not shell out to
`main.py` / `pipeline1.py`. Every step a developer needs to understand the
flow is in here.

────────────────────────────────────────────────────────────────────────────
HIGH-LEVEL FLOW
────────────────────────────────────────────────────────────────────────────

    GCS:  video-processing-pipelines-data/<road_id>/{*.MP4, *.gpx, *.kmz?}
                          │
                          ▼
        ┌──────────────────────────────────────────────────────────┐
        │ Phase A    GPX → KML projection (per video, optional)    │
        │            • Auto-detects .kml/.kmz inside the road's    │
        │              GCS folder. Caller can override with --kml. │
        │            • Each GPX trackpoint is projected onto the   │
        │              road polyline; lat/lon snap to road, off-   │
        │              route GPS noise (>--max-perp-m) is dropped. │
        │            • Rewritten GPX uploaded as <stem>.kml.gpx.   │
        │            • Without a KML, Phase A is silently skipped  │
        │              and Phase B reads the raw GPX.              │
        └──────────────────────────────────────────────────────────┘
                          │
                          ▼
        ┌──────────────────────────────────────────────────────────┐
        │ Phase B    Per-video processing (sequential, GPU-bound)  │
        │            For each (mp4, gpx) pair:                     │
        │              1. download MP4 + GPX from GCS              │
        │              2. parse GPX → trackpoints with timestamps  │
        │              3. walk trackpoints with 10 m haversine →   │
        │                 emit a "milestone" timestamp per 10 m    │
        │              4. parallel ffmpeg (8 segments) extracts    │
        │                 the corresponding frames from /dev/shm   │
        │              5. YOLO TensorRT engine runs over every     │
        │                 frame → per-frame {label, bbox, conf}    │
        │              6. raw frames uploaded to                   │
        │                 processed-data/<road>/<uuid>/             │
        │                   annotated_frames/frames/                │
        │              7. each frame's bboxes drawn ON the raw     │
        │                 image with the 52-label palette →        │
        │                 annotated_frames/predict/<file>.jpg      │
        │              8. per-UUID annotated_video.mp4 stitched    │
        │                 from the predict frames (1 fps H.264)    │
        │              9. result.json (COCO format) saved          │
        │             10. annotation_segments doc written to Mongo │
        │                 with frame_list_data / inference_info    │
        └──────────────────────────────────────────────────────────┘
                          │
                          ▼
        ┌──────────────────────────────────────────────────────────┐
        │ Phase C    Consolidation (always runs, in-memory merge)  │
        │            • Loads every annotation_segments for the     │
        │              road_id; concatenates frame_list_data.      │
        │            • Sorts merged frames by along-KML chainage   │
        │              (when KML present) or by source MP4 +       │
        │              frame index (raw GPX fallback). Robust to   │
        │              videos recorded out of spatial order.       │
        │            • Re-stamps chainage_km cumulatively across   │
        │              the merged sequence.                        │
        │            • Optionally REPLACES inference_info with     │
        │              Label Studio bboxes (--ls-export-dir) for   │
        │              orgs with sub-category taxonomies (Kota's   │
        │              52-label / 17-category set).                │
        │            • Re-tags severity per IBI Guideline.         │
        │            • Builds report_1_key, report_2_key,          │
        │              dashboard_df_csv, chainage_report.csv,      │
        │              plot_data.plots.pie_chart2 in the IBI       │
        │              shape every backend endpoint expects.       │
        │            • Builds a merged COCO result.json.           │
        │            • Uploads combined CSVs + result.json to      │
        │              processed-data/<road>/<uid>/                 │
        │            • Builds                                      │
        │                processed-data/<road>/merged_frames_<tag>/│
        │                processed-data/<road>/merged_predict_<tag>│
        │              (raw + bboxed, sequence-numbered 10 m).     │
        │            • Stitches consolidated raw +                 │
        │              annotated MP4s into                         │
        │                processed-data/<road>/videos/             │
        │            • Upserts inference_data + video_upload.      │
        └──────────────────────────────────────────────────────────┘

────────────────────────────────────────────────────────────────────────────
THREE OPERATING MODES
────────────────────────────────────────────────────────────────────────────

  ONE-SHOT (default)
      A single pass over the road's GCS prefix. Every (mp4, gpx) pair
      currently in the folder is processed once: Phase A → Phase B →
      Phase C → merged frames → videos → exit. Use this when all videos
      for the road are already uploaded.

  WATCH MODE  (--watch)
      Long-running poll loop for surveys whose videos arrive
      incrementally (slow WAN uploads, batched uploads from multiple
      field crews, live streaming surveys). Mirrors the contract that
      V1's trigger_builds.py --watch used to provide.

      every --watch-interval seconds:
        scan GCS prefix → drop already-tracked → list "new"
        none new       → log idle, sleep, continue
        new arrived    → settle for --settle-time, re-scan
        ≥ batch-size   → take batch_size, dispatch
        < batch-size   → settle once more; if count stable, dispatch
                         what we have (avoids stalling when uploads stop)

      per dispatched batch:
        Phase A on the new pairs (if KML available)
        Phase B per video — honours --parallel:
                =1 → serial in-process, one shared YOLO model
                >1 → ProcessPoolExecutor (spawn), each worker loads
                     its own YOLO. ~8 saturates a T4 GPU.
        Phase C re-consolidates ALL annotation_segments — dashboard
                updates after every batch
        merged_frames + consolidated videos rebuilt incrementally

      State persists in `processing_tracker.json` next to this script:
        { "<road_id>": { "<filename>.MP4": {
              "uuid": "...", "status": "processing|done|failed",
              "started": "...", "finished": "..."
        } } }

      Survives Ctrl+C and process restart — videos already marked
      `done` or `failed` are skipped on re-scan.

  REPROCESS MODE  (--reprocess)
      Re-run ONLY Phase C from the merged COCO result.json that lives at
      processed-data/<road_id>/<uid>/result.json. NO frame extraction,
      NO YOLO inference. Used when:
        • annotations were manually corrected in result.json (post-LS
          round, post-human-review)
        • severity mappings changed and reports need re-derivation
        • bboxes were adjusted and the merged_predict frames + the
          consolidated annotated video need to be regenerated

      What it does:
        1. download merged result.json from GCS
        2. reconstruct frame_list_data from images + annotations
        3. re-tag severity per IBI Guideline (or override)
        4. re-build report_1..4 / chainage_report.csv / pie_chart2
        5. re-upload combined reports + result.json to GCS
        6. re-render every merged_predict_<tag>/<seq>.jpg by drawing
           the (possibly updated) bboxes on the corresponding raw frame
        7. re-stitch the consolidated raw + annotated videos
        8. update inference_data with the new reports + URLs

      Combine with --watch to poll the result.json's `updated`
      timestamp and re-trigger automatically whenever it changes
      (mirrors reprocess_annotations.py --watch).

      Mirrors V1's reprocess_annotations.py contract; the only
      difference is the source — V1 ran per UUID, V2 runs per UID
      (the consolidated record).

────────────────────────────────────────────────────────────────────────────
GCS LAYOUT after a complete run
────────────────────────────────────────────────────────────────────────────

  gs://datanh11/processed-data/{road_id}/
  ├── {uuid}/                              ← per-video Phase B output
  │   ├── annotated_frames/
  │   │   ├── frames/                      ← raw extracted frames
  │   │   └── predict/                     ← bboxes drawn (52-label palette)
  │   ├── annotated_video.mp4              ← 1 fps stitch of predict/
  │   └── result.json                      ← per-video COCO detections
  ├── {uid}/                               ← Phase C combined per-road
  │   ├── report_1.csv ... report_4.csv    ← IBI shape, per-frame-unique
  │   ├── chainage_report.csv
  │   └── result.json                      ← merged COCO across all UUIDs
  ├── merged_frames_{tag}/  000000.jpg…    ← raw, 10 m sequence numbering
  ├── merged_predict_{tag}/ 000000.jpg…    ← bboxed, dashboard "AI Analyzed"
  └── videos/
      ├── {uid}_raw.mp4                    ← consolidated raw video
      └── {uid}_annotated.mp4              ← consolidated bboxed video

────────────────────────────────────────────────────────────────────────────
MONGO COLLECTIONS WRITTEN
────────────────────────────────────────────────────────────────────────────

  annotation_segments  one doc per (road_id, uuid):
                       uuid, road_id, total_frames, total_defects,
                       road_length_km, road_rating,
                       frame_list_data[*] = {timeElapsed, latitude,
                         longitude, location (GeoJSON), chainage_km,
                         orientation, og_file, inference_image,
                         defect_state, inference_info[*] = {label, bbox,
                         severity}}, severity_distribution

  inference_data       one doc per (uid, organization):
                       uid = "{road_id}_{uid_suffix}"
                       data.report_1_key … report_4_key, dashboard_df_csv,
                       data.frame_list_data, data.total_defects,
                       data.road_length, data.road_rating,
                       data.video_url_raw / video_url_annotated,
                       plot_data.plots.pie_chart2,
                       video_url (top-level) = annotated,
                       video_url_rhs (top-level) = raw

  video_upload         one doc per uid — joined by Video Library page

────────────────────────────────────────────────────────────────────────────
USAGE
────────────────────────────────────────────────────────────────────────────

  ONE-SHOT — process whatever's in the GCS prefix right now and exit:

    /home/shubham/video-processing-pipeline/venv/bin/python pipeline_v2.py \\
        --gcs-prefix    "video-processing-pipelines-data/<road_id>/" \\
        --road-id       <road_id> \\
        --uid-suffix    day \\
        --organization  VaranasiOrg \\
        --city          Varanasi \\
        --project-title "Display name" \\
        --start-address "..."  --end-address "..."

  WATCH — long-running poll loop, processes batches as videos arrive:

    /home/shubham/video-processing-pipeline/venv/bin/python pipeline_v2.py \\
        --gcs-prefix    "video-processing-pipelines-data/<road_id>/" \\
        --road-id       <road_id> \\
        --uid-suffix    day --organization VaranasiOrg --city Varanasi \\
        --project-title "..." \\
        --start-address "..."  --end-address "..." \\
        --watch \\
        --watch-interval 60   # poll GCS every N seconds when idle
        --settle-time    60   # wait N seconds for in-progress uploads to finish
        --batch-size      8   # accumulate up to N new videos before dispatching
        --parallel        8   # process the batch with N concurrent YOLO workers

  REPROCESS — Phase-C-only re-run from the merged result.json:

    # one-shot: re-run after manual annotation corrections
    /home/shubham/video-processing-pipeline/venv/bin/python pipeline_v2.py \\
        --gcs-prefix    "video-processing-pipelines-data/<road_id>/" \\
        --road-id       <road_id> --uid-suffix day \\
        --organization  VaranasiOrg --city Varanasi \\
        --project-title "..." \\
        --start-address "..."  --end-address "..." \\
        --reprocess

    # watch: auto-trigger whenever the result.json is edited externally
    /home/shubham/video-processing-pipeline/venv/bin/python pipeline_v2.py \\
        --gcs-prefix    "..." --road-id <road_id> --uid-suffix day \\
        --organization  VaranasiOrg --city Varanasi \\
        --project-title "..." \\
        --start-address "..."  --end-address "..." \\
        --reprocess --watch --watch-interval 60

  Useful re-entry flags:
    --skip-phase-a          re-use earlier <stem>.kml.gpx in GCS
    --skip-phase-b          rebuild Phase C from existing annotation_segments
    --skip-merged-frames    skip the GCS frame-folder copy
    --skip-videos           skip the consolidated video stitch
    --kml /path/file.kmz    explicit KML override (else auto-detect in GCS)
    --ls-export-dir <path>  override YOLO inference_info with LS sub-labels
    --severity-map-json p   org-specific {label: severity} overrides

  Watch mode flags (ignored in one-shot mode):
    --watch                 enable long-running poll loop
    --watch-interval SEC    seconds between idle GCS scans (default 30)
    --settle-time    SEC    upload-finalisation wait (default 30)
    --batch-size     N      videos to accumulate before dispatch (default 8)
    --parallel       N      worker count INSIDE each batch (also one-shot);
                            1=serial, >1=ProcessPoolExecutor (default 1)

  Reprocess mode flags:
    --reprocess                       run Phase C only, from merged result.json
    --reprocess-result-path GS_URL    explicit gs:// path override
    --reprocess-skip-frames           don't redraw merged_predict frames
    --reprocess-skip-videos           don't re-stitch consolidated videos
    (combine --reprocess with --watch + --watch-interval to auto-retrigger)

  Tracker file (watch mode only):
    /home/shubham/video-processing-pipline-V2/processing_tracker.json
    Records every video this watch process has picked up. Delete to
    force re-processing of all videos on the next watch run.

────────────────────────────────────────────────────────────────────────────
WHY THIS FILE IS SELF-CONTAINED
────────────────────────────────────────────────────────────────────────────

V1 historically split the pipeline across `pipeline1.py`, `main.py`,
`annotation-pipeline*/`, `ImageProcessor`, `ConfigManager`,
`S3Manager`, and `trigger_builds.py`. This V2 is one file because:

  • Anyone debugging the chainage / report / merged-frame pipeline can
    follow the entire flow top-to-bottom without chasing imports.
  • There's no subprocess fan-out — every step (frame extraction, YOLO
    inference, bbox drawing, GCS upload, Mongo write, watch loop) runs
    in the same Python interpreter, sharing one loaded YOLO model in
    GPU memory.
  • The previously-buggy spots in V1 (en-dash labels colliding with the
    colour palette → invisible bboxes; per-video timeElapsed reset
    interleaving merged frames; KML parser only reading the first
    LineString; etc.) are all addressed once, here, in plain code.
  • Watch mode + tracker are inlined too — no separate orchestrator
    process required for incremental-upload surveys.

Dependencies: pymongo, google-cloud-storage, ultralytics (TensorRT),
opencv-python, Pillow, numpy. All present in V1's venv.

────────────────────────────────────────────────────────────────────────────
SECTION INDEX (search for "Section N —" to jump)
────────────────────────────────────────────────────────────────────────────

   1. Imports + constants
   2. Logging
   3. Geo helpers (haversine, KML parsing, polyline projection)
   4. GPX parsing + Phase A KML projection
   5. GCS helpers
   6. Label normalisation + IBI severity map
   7. 52-label colour palette + bbox drawer
   8. Frame extraction (10 m haversine walk + parallel ffmpeg)
   9. YOLO inference (TensorRT engine, ultralytics)
  10. Per-video Phase B (process_one_video)
  11. Label Studio JSON ingestion
  12. IBI-format report builders (report_1, report_2, chainage, pie_chart2)
  13. Combined reports + merged COCO result.json
  14. Merged frames folder + consolidated raw/annotated videos
  15. Phase C consolidation + Mongo upserts
  16. Watch mode (tracker + poll loop)
  17. Reprocess (Phase-C-only re-run from merged result.json)
  18. CLI orchestrator (parse_args, main)
"""
from __future__ import annotations

# ─────────────────────────────────────────────────────────────────────────────
# Section 1 — Imports + constants
# ─────────────────────────────────────────────────────────────────────────────
import argparse
import csv
import datetime as _dt
import glob
import io
import json
import logging
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid as uuid_mod
import xml.etree.ElementTree as ET
import zipfile
from collections import Counter, OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import cv2
import numpy as np
from google.cloud import storage
from pymongo import MongoClient

# GCS
GCS_BUCKET       = "datanh11"
PROCESSED_PREFIX = "processed-data"

# Mongo — single connection string used everywhere
MONGO_URI = (
    "mongodb+srv://tech_db_user:IK96qWD8AvtbpOHe"
    "@cluster0.nm6pkfg.mongodb.net/roadvision?retryWrites=true&w=majority"
)

# YOLO weights — TensorRT engine preferred for ~3-5× speedup on T4
DEFAULT_ENGINE_PATH = "/home/shubham/nhai_best.engine"
DEFAULT_PT_PATH     = "/home/shubham/nhai_best.pt"

GPX_NS = "{http://www.topografix.com/GPX/1/1}"

# Frame extraction tunables
EXTRACT_INTERVAL_M     = 10.0   # one frame per 10 m of GPX-walked distance
EXTRACT_TARGET_HEIGHT  = 640    # ffmpeg downscale (preserves aspect)
EXTRACT_FFMPEG_WORKERS = 8      # parallel ffmpeg processes per video
EXTRACT_FALLBACK_SEC   = 5.0    # one frame per 5 s when GPS has zero motion


# ─────────────────────────────────────────────────────────────────────────────
# Section 2 — Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline_v2")


# ── Per-uid log files (mirrors V1's per-UUID workdir/<run_id>/pipeline.log)
# V1 wrote one log file per UUID because each UUID was processed by a fresh
# subprocess whose stdout was redirected into the file. V2's unit of
# completion is the consolidated UID (= "{road_id}_{uid_suffix}"), not the
# per-video UUID, so we write one log per UID at:
#     {workdir}/{road_id}/pipeline.log
# Survives ProcessPoolExecutor parallelism: each --parallel worker subprocess
# adds its OWN FileHandler pointing at the same path. POSIX `O_APPEND`
# guarantees atomic line writes, so multi-worker output interleaves cleanly
# without corruption.
def road_log_path(workdir_root: Path, road_id: str) -> Path:
    """Per-uid log file location. Caller ensures the parent dir exists."""
    log_dir = workdir_root / road_id
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / "pipeline.log"


def attach_road_log_handler(log_path: Path) -> logging.FileHandler:
    """Add a FileHandler to the module logger writing to `log_path`.
    Caller must call detach_log_handler() when done."""
    handler = logging.FileHandler(str(log_path), mode="a", encoding="utf-8")
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    log.addHandler(handler)
    return handler


def detach_log_handler(handler: logging.Handler | None) -> None:
    if handler is None:
        return
    try:
        log.removeHandler(handler)
        handler.close()
    except Exception:
        pass


class road_log_session:
    """Context manager. Use around any per-road processing block:

        with road_log_session(workdir_root, road_id):
            # Phase B / Phase C / merged frames / videos
            # all log lines also tee'd to {workdir}/{road_id}/pipeline.log
    """
    def __init__(self, workdir_root: Path, road_id: str):
        self.path: Path = road_log_path(workdir_root, road_id)
        self._h: logging.FileHandler | None = None
        self.road_id = road_id

    def __enter__(self):
        self._h = attach_road_log_handler(self.path)
        log.info("─── log session for %s → %s ───", self.road_id, self.path)
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc:
            log.error("─── log session for %s ended with %s ───",
                      self.road_id, exc_type.__name__)
        else:
            log.info("─── log session for %s ended ───", self.road_id)
        detach_log_handler(self._h)


def _log_section(title: str) -> None:
    """Visual separator for major flow stages — easy to scan in the log."""
    bar = "─" * max(8, 70 - len(title) - 2)
    log.info("─── %s %s", title, bar)


# ─────────────────────────────────────────────────────────────────────────────
# Section 3 — Geo helpers (haversine, KML parsing, polyline projection)
# ─────────────────────────────────────────────────────────────────────────────
def _haversine_m(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Great-circle distance between two (lat, lon) points in metres."""
    R = 6_371_000.0
    (la1, lo1), (la2, lo2) = a, b
    dlat = math.radians(la2 - la1)
    dlon = math.radians(lo2 - lo1)
    h = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(la1)) * math.cos(math.radians(la2))
         * math.sin(dlon / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(h))


def parse_kml(path: str) -> list[tuple[float, float]]:
    """
    Parse .kml or .kmz and return the road polyline as (lat, lon) pairs.

    KMLs exported from a GPX trace are commonly split into many <Placemark>
    LineStrings — we walk every one and concatenate them, dropping the seam
    vertex when one segment's last point equals the next segment's first.
    Reading only the first LineString (a long-standing bug) would massively
    under-report road length and cause Phase A to drop most of the GPX as
    "off-road".
    """
    p = Path(path)
    if p.suffix.lower() == ".kmz":
        with zipfile.ZipFile(p) as z:
            kml_name = next((n for n in z.namelist() if n.endswith(".kml")), None)
            if not kml_name:
                raise ValueError(f"No .kml entry inside {path}")
            kml_text = z.read(kml_name).decode("utf-8", errors="ignore")
    else:
        kml_text = p.read_text(encoding="utf-8", errors="ignore")

    # Strip default xmlns so findall works without namespace juggling.
    kml_text = re.sub(r'\sxmlns="[^"]+"', "", kml_text, count=1)
    root = ET.fromstring(kml_text)
    line_strings = root.findall(".//LineString/coordinates")
    if not line_strings:
        raise ValueError(f"No <LineString><coordinates> in {path}")

    pts: list[tuple[float, float]] = []
    for coord_el in line_strings:
        if not (coord_el is not None and coord_el.text):
            continue
        seg: list[tuple[float, float]] = []
        for tok in coord_el.text.strip().split():
            parts = tok.split(",")
            if len(parts) < 2:
                continue
            seg.append((float(parts[1]), float(parts[0])))   # KML is lon,lat
        if not seg:
            continue
        if pts and seg[0] == pts[-1]:
            seg = seg[1:]
        pts.extend(seg)

    if len(pts) < 2:
        raise ValueError(f"KML polyline has < 2 vertices: {path}")
    log.info("KML polyline: %d LineStrings, %d vertices, length ≈ %.2f km",
             len(line_strings), len(pts), _polyline_length_m(pts) / 1000.0)
    return pts


def _polyline_length_m(poly: list[tuple[float, float]]) -> float:
    return sum(_haversine_m(poly[i], poly[i + 1]) for i in range(len(poly) - 1))


def precompute_polyline_chainage(poly: list[tuple[float, float]]) -> list[float]:
    """Cumulative along-polyline distance in metres at each vertex."""
    chain = [0.0]
    for i in range(1, len(poly)):
        chain.append(chain[-1] + _haversine_m(poly[i - 1], poly[i]))
    return chain


def parse_kml_via_points(path: str) -> list[dict]:
    """
    Extract named Point waypoints from a KML/KMZ. Used by Phase C to
    populate RoadData.via_points — the dashboard shows these as
    intermediate stops along the road. Each Placemark with a <Point>
    geometry (and an optional <name>) becomes one entry:
        {"name": ..., "latitude": ..., "longitude": ...}
    LineString placemarks (the road geometry itself) are ignored.
    """
    p = Path(path)
    if p.suffix.lower() == ".kmz":
        with zipfile.ZipFile(p) as z:
            kml_name = next((n for n in z.namelist() if n.endswith(".kml")), None)
            if not kml_name:
                return []
            kml_text = z.read(kml_name).decode("utf-8", errors="ignore")
    else:
        kml_text = p.read_text(encoding="utf-8", errors="ignore")
    kml_text = re.sub(r'\sxmlns="[^"]+"', "", kml_text, count=1)
    root = ET.fromstring(kml_text)
    out: list[dict] = []
    for pm in root.findall(".//Placemark"):
        pt = pm.find("Point")
        if pt is None:
            continue
        coord_el = pt.find("coordinates")
        if coord_el is None or not coord_el.text:
            continue
        parts = coord_el.text.strip().split(",")
        if len(parts) < 2:
            continue
        try:
            lng = float(parts[0])
            lat = float(parts[1])
        except ValueError:
            continue
        name_el = pm.find("name")
        out.append({
            "name":      (name_el.text or "").strip() if name_el is not None else "",
            "latitude":  lat,
            "longitude": lng,
        })
    log.info("KML via_points: %d named waypoints", len(out))
    return out


# ── Reverse geocoding ────────────────────────────────────────────────────
# Used to fill RoadData.starting_address / ending_address (and any
# via_points missing names) when only lat/lon are available. Uses
# OpenStreetMap Nominatim — free, no API key required, rate-limited to
# ~1 req/sec for the public endpoint, so we sleep between calls.
_GEOCODE_CACHE: dict[tuple[float, float], str] = {}


def reverse_geocode(lat: float | None, lng: float | None,
                    cache: dict | None = None) -> str:
    """
    Return a human-readable address for (lat, lng). Returns '' when
    coords are missing or the request fails. Caches by 5-decimal-rounded
    coords to avoid duplicate requests within a single run.
    """
    if lat is None or lng is None:
        return ""
    if cache is None:
        cache = _GEOCODE_CACHE
    key = (round(float(lat), 5), round(float(lng), 5))
    if key in cache:
        return cache[key]

    import urllib.request, urllib.parse
    url = ("https://nominatim.openstreetmap.org/reverse?format=json"
           f"&lat={lat}&lon={lng}&zoom=14&accept-language=en")
    addr = ""
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "pipeline_v2 (Roadvision)"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        addr = (data.get("display_name") or "").strip()
    except Exception as e:
        log.warning("[geocode] reverse failed for (%s, %s): %s", lat, lng, e)

    cache[key] = addr
    # Nominatim public-API courtesy throttle (~1 req/sec)
    time.sleep(1.0)
    if addr:
        log.info("[geocode] (%.5f, %.5f) → %s", lat, lng, addr[:70])
    return addr


def _is_placeholder_address(s: str | None) -> bool:
    """Treat empty / whitespace / TBD as 'no address provided'."""
    if not s:
        return True
    return s.strip().lower() in ("", "tbd", "n/a", "na", "-", "unknown")


def _project_point_on_segment(
    p: tuple[float, float],
    a: tuple[float, float],
    b: tuple[float, float],
) -> tuple[tuple[float, float], float, float]:
    """
    Project p onto segment a→b using a flat-Earth approximation around a's
    latitude (good enough for road surveys; KML segments are tens of metres).

    Returns (projected_lat_lon, perpendicular_distance_m, t_along_segment_0_1).
    """
    M_PER_DEG_LAT  = 111_320.0
    m_per_deg_lon  = 111_320.0 * math.cos(math.radians(a[0]))

    bx = (b[1] - a[1]) * m_per_deg_lon
    by = (b[0] - a[0]) * M_PER_DEG_LAT
    px = (p[1] - a[1]) * m_per_deg_lon
    py = (p[0] - a[0]) * M_PER_DEG_LAT

    seg_len_sq = bx * bx + by * by
    if seg_len_sq == 0:
        return a, _haversine_m(p, a), 0.0
    t = max(0.0, min(1.0, (px * bx + py * by) / seg_len_sq))

    proj_lon = a[1] + (bx * t) / m_per_deg_lon
    proj_lat = a[0] + (by * t) / M_PER_DEG_LAT
    proj = (proj_lat, proj_lon)
    return proj, _haversine_m(p, proj), t


def project_to_polyline(
    p: tuple[float, float],
    poly: list[tuple[float, float]],
    cumdist: list[float],
) -> tuple[tuple[float, float], float, float]:
    """
    Project p onto the closest segment of the polyline.

    Returns (projected_lat_lon, perpendicular_distance_m, chainage_m_along_polyline).
    """
    best: tuple[float, int, float, tuple[float, float]] | None = None
    for i in range(len(poly) - 1):
        proj, d, t = _project_point_on_segment(p, poly[i], poly[i + 1])
        if best is None or d < best[0]:
            best = (d, i, t, proj)
    perp_d, idx, t, proj = best
    seg_len = cumdist[idx + 1] - cumdist[idx]
    return proj, perp_d, cumdist[idx] + seg_len * t


# ─────────────────────────────────────────────────────────────────────────────
# Section 4 — GPX parsing + Phase A KML projection
# ─────────────────────────────────────────────────────────────────────────────
def parse_gpx_trackpoints(gpx_text: str) -> list[dict]:
    """
    Return [{lat, lon, time_str, idx}] from a GPX file's text. Some surveys
    ship a malformed `<? xml ...?>` declaration — we normalise it before
    passing to the XML parser.
    """
    gpx_text = re.sub(r"<\?\s*xml[^?]*\?>",
                      '<?xml version="1.0" encoding="UTF-8"?>',
                      gpx_text, count=1)
    root = ET.fromstring(gpx_text)
    out: list[dict] = []
    for i, pt in enumerate(root.findall(f".//{GPX_NS}trkpt")):
        try:
            lat = float(pt.attrib["lat"])
            lon = float(pt.attrib["lon"])
        except (KeyError, ValueError):
            continue
        t_el = pt.find(f"{GPX_NS}time")
        ts = t_el.text.strip() if t_el is not None and t_el.text else None
        out.append({"lat": lat, "lon": lon, "time_str": ts, "idx": i})
    return out


def rewrite_gpx_with_kml_projection(
    gpx_text: str,
    poly: list[tuple[float, float]],
    cumdist: list[float],
    max_perp_m: float = 75.0,
) -> tuple[str, dict]:
    """
    Project every GPX trackpoint onto the KML polyline; replace its lat/lon
    with the projected coordinate; stamp a <chainage_m> extension for any
    downstream consumer; drop trackpoints further than `max_perp_m` from the
    polyline (typically GPS noise from the surveyor pulling over).
    """
    gpx_text = re.sub(r"<\?\s*xml[^?]*\?>",
                      '<?xml version="1.0" encoding="UTF-8"?>',
                      gpx_text, count=1)
    ET.register_namespace("", "http://www.topografix.com/GPX/1/1")
    root = ET.fromstring(gpx_text)

    kept = dropped = 0
    for trkseg in root.findall(f".//{GPX_NS}trkseg"):
        for pt in list(trkseg.findall(f"{GPX_NS}trkpt")):
            try:
                lat = float(pt.attrib["lat"])
                lon = float(pt.attrib["lon"])
            except (KeyError, ValueError):
                trkseg.remove(pt); dropped += 1; continue
            proj, perp, chainage_m = project_to_polyline((lat, lon), poly, cumdist)
            if perp > max_perp_m:
                trkseg.remove(pt); dropped += 1; continue
            pt.set("lat", f"{proj[0]:.7f}")
            pt.set("lon", f"{proj[1]:.7f}")
            existing = pt.find(f"{GPX_NS}chainage_m")
            if existing is not None:
                existing.text = f"{chainage_m:.3f}"
            else:
                ch = ET.SubElement(pt, f"{GPX_NS}chainage_m")
                ch.text = f"{chainage_m:.3f}"
            kept += 1
    return ET.tostring(root, encoding="unicode", xml_declaration=False), \
        {"kept": kept, "dropped": dropped}


def phase_a_project_all_gpx(
    pairs: list[tuple[str, str]],
    poly: list[tuple[float, float]],
    cumdist: list[float],
    max_perp_m: float,
) -> list[tuple[str, str]]:
    """For every (mp4, gpx) pair, rewrite the GPX with KML-projected
    coordinates and upload it as <stem>.kml.gpx in the same prefix.
    Returns the list of (mp4, projected_gpx) pairs to feed into Phase B."""
    out: list[tuple[str, str]] = []
    for mp4_blob, gpx_blob in pairs:
        log.info("[Phase A] projecting %s", gpx_blob)
        try:
            text = gcs_download_text(gpx_blob)
        except Exception as e:
            log.warning("  download failed: %s — skipping pair", e); continue
        try:
            rewritten, stats = rewrite_gpx_with_kml_projection(
                text, poly, cumdist, max_perp_m=max_perp_m)
        except Exception as e:
            log.warning("  rewrite failed: %s — skipping pair", e); continue
        kml_gpx_blob = gpx_blob[:-4] + ".kml.gpx"
        gcs_upload_text(kml_gpx_blob, rewritten,
                        content_type="application/gpx+xml")
        log.info("  kept=%d  dropped=%d → %s",
                 stats["kept"], stats["dropped"], kml_gpx_blob)
        out.append((mp4_blob, kml_gpx_blob))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Section 5 — GCS helpers
# ─────────────────────────────────────────────────────────────────────────────
_GCS_CLIENT: storage.Client | None = None
def gcs_client() -> storage.Client:
    """Process-wide GCS client. Created on first use, cached thereafter."""
    global _GCS_CLIENT
    if _GCS_CLIENT is None:
        _GCS_CLIENT = storage.Client()
    return _GCS_CLIENT


def gcs_find_kml(prefix: str) -> str | None:
    """
    Auto-fallback: look for a .kml or .kmz inside the road's GCS folder and
    download to /tmp. Lets a batch run process roads with mixed KML
    availability without per-road CLI flags.
    """
    cli = gcs_client()
    candidates = [b.name for b in cli.list_blobs(GCS_BUCKET, prefix=prefix)
                  if b.name.lower().endswith((".kmz", ".kml"))]
    if not candidates:
        return None
    candidates.sort(key=lambda n: (0 if n.lower().endswith(".kmz") else 1, n))
    chosen = candidates[0]
    local = Path(tempfile.gettempdir()) / f"v2_kml_{Path(chosen).name}"
    cli.bucket(GCS_BUCKET).blob(chosen).download_to_filename(str(local))
    log.info("auto-detected KML in GCS prefix: gs://%s/%s → %s",
             GCS_BUCKET, chosen, local)
    return str(local)


def gcs_list_pairs(prefix: str) -> list[tuple[str, str]]:
    """List (mp4_blob, gpx_blob) pairs by stem under prefix.

    Skips already-projected `.kml.gpx` files so re-runs don't pair them as
    fresh inputs."""
    cli = gcs_client()
    by_stem: dict[str, dict[str, str]] = defaultdict(dict)
    for blob in cli.list_blobs(GCS_BUCKET, prefix=prefix):
        name = blob.name
        if name.lower().endswith(".mp4"):
            by_stem[name[:-4]]["mp4"] = name
        elif name.lower().endswith(".gpx"):
            stem = name[:-4]
            if stem.endswith(".kml"):
                continue
            by_stem[stem]["gpx"] = name
    out: list[tuple[str, str]] = []
    for stem, kinds in sorted(by_stem.items()):
        if "mp4" in kinds and "gpx" in kinds:
            out.append((kinds["mp4"], kinds["gpx"]))
        else:
            log.warning("incomplete pair (missing %s) for stem=%s",
                        "gpx" if "gpx" not in kinds else "mp4", stem)
    return out


def gcs_download_text(blob_name: str) -> str:
    return gcs_client().bucket(GCS_BUCKET).blob(blob_name).download_as_text()


def gcs_download_bytes(blob_name: str) -> bytes:
    return gcs_client().bucket(GCS_BUCKET).blob(blob_name).download_as_bytes()


def gcs_upload_text(blob_name: str, body: str, content_type: str = "text/plain") -> None:
    gcs_client().bucket(GCS_BUCKET).blob(blob_name).upload_from_string(
        body, content_type=content_type)


def gcs_upload_bytes(blob_name: str, body: bytes, content_type: str = "application/octet-stream") -> None:
    gcs_client().bucket(GCS_BUCKET).blob(blob_name).upload_from_string(
        body, content_type=content_type)


def gcs_upload_file(blob_name: str, local_path: str | Path,
                    content_type: str = "application/octet-stream") -> None:
    gcs_client().bucket(GCS_BUCKET).blob(blob_name).upload_from_filename(
        str(local_path), content_type=content_type)


def _split_gcs_url(src: str) -> tuple[str, str] | None:
    """Accept gs://bucket/path or https://storage.googleapis.com/bucket/path."""
    if not src:
        return None
    m = re.match(r"https?://storage\.googleapis\.com/([^/]+)/(.+)$", src)
    if m:
        return m.group(1), m.group(2).split("?")[0]
    if src.startswith("gs://"):
        _, _, rest = src.partition("gs://")
        b, _, p = rest.partition("/")
        return b, p
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Section 6 — Label normalisation + IBI severity map
# ─────────────────────────────────────────────────────────────────────────────
# Why the normalisation matters: the YOLO model's class names ship with
# en-dashes ("Cleanliness – Debris"), but the colour palette + severity map
# below use plain hyphens. Without normalisation, every lookup falls through
# and (a) bbox draw paints invisible white-on-light, (b) Phase C retag wipes
# severities to 'none'. Normalise once, at every label ingestion site.
def normalize_label(s: str) -> str:
    if not s:
        return ""
    return (s.replace("–", "-")    # en-dash
             .replace("—", "-")    # em-dash
             .replace("‐", "-").replace("‑", "-")
             .strip())


# IBI Guideline severity table — maps each defect/asset name to one of
# {high, medium, low, none}. Keys are lower-case + plain-hyphen; lookup
# normalises the input before matching.
SEVERITY_MAP: dict[str, str] = {
    # Defect classes
    "pavement defects": "high", "traffic behaviour": "high",
    "encroachment": "medium", "illegal parking": "low",
    "natural": "low", "infrastructure": "low",
    # Assets — presence alone is not a defect
    "road surface": "none", "road structure": "none",
    "pavement marking": "none", "median": "none",
    "refuge island": "none", "kerb ramp": "none",
    "foot over bridge (fob)": "none", "pedestrian crossing": "none",
    "pelican control signal": "none", "pedestrian signal": "none",
    "speed limit signages": "none", "school zone signage": "none",
    "pedestrian signage": "none", "metro signages": "none",
    "speed hump": "none", "rumble strip": "none",
    "street light": "none", "flood light (high mast)": "none",
    # Pavement Defects sub-labels
    "pothole": "high", "rutting": "high",
    "faulting at joints": "high", "blow-up (buckling)": "high",
    "cracking": "medium", "corner break (concrete)": "medium",
    "delamination": "low", "patch": "none",
    # State sub-labels
    "damaged": "high", "non-functional": "high",
    "poor": "high", "poor/faded": "high", "wrong side driving": "high",
    "missing": "medium", "paint faded": "medium",
    "damaged/faded": "medium", "not accessible": "medium",
    "poor/worn out": "medium", "poor/damaged": "medium",
    "barricading": "medium", "vendor": "medium", "shop spillover": "medium",
    "visibly obstructed": "low", "obstructed": "low",
    "overhanging branches": "low", "vegetation overgrowth": "low",
    "electric pole/infrastructure": "low", "traffic kiosk": "low",
    "good": "none", "bituminous": "none", "concrete": "none",
    "paver blocks": "none", "unpaved": "none",
    "flyover": "none", "underpass": "none",
    "footpath": "none", "road": "none",
}
_SEV_RANK  = {"high": 3, "medium": 2, "low": 1, "none": 0}
_RANK_NAME = {3: "high", 2: "medium", 1: "low", 0: "none"}


def severity_for(label: str, override_map: dict | None = None) -> str:
    """Resolve severity. override_map (e.g. Kota) wins; else IBI Guideline.
    Substring fallback handles slight label variants."""
    n = normalize_label(label).lower()
    if override_map and n in override_map:
        return override_map[n]
    if n in SEVERITY_MAP:
        return SEVERITY_MAP[n]
    for key, sev in SEVERITY_MAP.items():
        if key in n or n in key:
            return sev
    return "none"


# ─────────────────────────────────────────────────────────────────────────────
# Section 7 — 52-label colour palette + bbox drawer
# ─────────────────────────────────────────────────────────────────────────────
# Palette mirrors V1's annotation-pipeline/annotations.py (regular-hyphen
# keys, RGB tuples). Used both by the per-video Phase B drawer that produces
# annotated_frames/predict/* and by the consolidated merged_predict_<tag>/*
# stitch in Phase C. Labels not in the palette fall back to yellow so they
# remain visible.
LABEL_COLORS_RGB: dict[str, tuple[int, int, int]] = {
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
_FALLBACK_RGB = (255, 255, 0)   # yellow — labels not in the palette


def _rgb_to_bgr(rgb: tuple[int, int, int]) -> tuple[int, int, int]:
    return (rgb[2], rgb[1], rgb[0])


def draw_overlays_on_jpeg(
    jpeg_bytes: bytes,
    bboxes: list[dict],
    *,
    line_thickness: int = 4,
    font_scale: float = 0.7,
) -> bytes | None:
    """
    Decode JPEG → draw <label, bbox> rectangles + label band → re-encode.
    Returns new JPEG bytes, or None if there's nothing drawable (caller
    should then upload the raw bytes verbatim — saves a re-encode).
    """
    drawable = [b for b in bboxes
                if (b.get("label") or "").strip() and b.get("bbox")]
    if not drawable:
        return None
    arr = np.frombuffer(jpeg_bytes, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None:
        return None
    h_img, w_img = img.shape[:2]

    for bb in drawable:
        label = normalize_label(bb["label"])
        try:
            x, y, w, h = (float(v) for v in bb["bbox"][:4])
        except Exception:
            continue
        x1, y1 = max(0, int(x)), max(0, int(y))
        x2, y2 = min(w_img - 1, int(x + w)), min(h_img - 1, int(y + h))
        if x2 <= x1 or y2 <= y1:
            continue
        colour = _rgb_to_bgr(LABEL_COLORS_RGB.get(label, _FALLBACK_RGB))
        cv2.rectangle(img, (x1, y1), (x2, y2), colour, line_thickness)
        (tw, th), _ = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX,
                                      font_scale, 2)
        ty = max(th + 6, y1)
        cv2.rectangle(img, (x1, ty - th - 6), (x1 + tw + 6, ty),
                      colour, thickness=-1)
        b_, g_, r_ = colour
        text_colour = (0, 0, 0) if (0.299 * r_ + 0.587 * g_ + 0.114 * b_) > 140 else (255, 255, 255)
        cv2.putText(img, label, (x1 + 3, ty - 3),
                    cv2.FONT_HERSHEY_SIMPLEX, font_scale, text_colour, 2,
                    cv2.LINE_AA)

    ok, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 92])
    return bytes(buf) if ok else None


# ─────────────────────────────────────────────────────────────────────────────
# Section 8 — Frame extraction (10 m haversine walk + parallel ffmpeg)
# ─────────────────────────────────────────────────────────────────────────────
def compute_10m_milestones(trackpoints: list[dict]) -> list[dict]:
    """
    Walk the GPX trackpoints with a haversine accumulator, emitting one
    "milestone" every EXTRACT_INTERVAL_M (10 m) along the surveyor's path.
    Linear-interpolate lat/lon and timestamp at each milestone. Returns a
    list of dicts shaped for downstream ffmpeg + frame_list_data builders.

    Each milestone:
      {
        "video_offset_sec": float,    # for ffmpeg -ss
        "timeElapsed":      float,    # echoed in frame_list_data
        "latitude":         float,
        "longitude":        float,
        "orientation":      str,      # cosmetic, defaults to landscapeLeft
      }
    """
    if not trackpoints:
        return []
    # Translate GPX time strings into video-offset seconds. Surveyors
    # without a fixed start tag get a synthetic "1 second per trackpoint"
    # fallback — pipeline1 used 1 Hz GPX in practice, this is matched.
    base_t: float | None = None
    out_pts: list[dict] = []
    for i, p in enumerate(trackpoints):
        t_off: float
        if p.get("time_str"):
            try:
                t = _dt.datetime.fromisoformat(
                    p["time_str"].replace("Z", "+00:00"))
                if base_t is None:
                    base_t = t.timestamp()
                t_off = t.timestamp() - base_t
            except Exception:
                t_off = float(i)
        else:
            t_off = float(i)
        out_pts.append({"lat": p["lat"], "lon": p["lon"], "t": t_off})

    INTERVAL = EXTRACT_INTERVAL_M
    milestones: list[dict] = []
    cumulative   = 0.0
    next_milestone = 0.0

    for i, p in enumerate(out_pts):
        if i == 0:
            milestones.append({
                "video_offset_sec": p["t"],
                "timeElapsed":      p["t"],
                "latitude":         p["lat"],
                "longitude":        p["lon"],
                "orientation":      "landscapeLeft",
            })
            next_milestone = INTERVAL
            continue
        prev = out_pts[i - 1]
        seg = _haversine_m((prev["lat"], prev["lon"]), (p["lat"], p["lon"]))
        if seg == 0:
            continue
        while cumulative + seg >= next_milestone:
            frac = (next_milestone - cumulative) / seg
            milestones.append({
                "video_offset_sec": prev["t"]   + frac * (p["t"]   - prev["t"]),
                "timeElapsed":      prev["t"]   + frac * (p["t"]   - prev["t"]),
                "latitude":         prev["lat"] + frac * (p["lat"] - prev["lat"]),
                "longitude":        prev["lon"] + frac * (p["lon"] - prev["lon"]),
                "orientation":      "landscapeLeft",
            })
            next_milestone += INTERVAL
        cumulative += seg

    log.info("    10 m milestones: %d  (path length ≈ %.0f m)",
             len(milestones), cumulative)
    return milestones


def extract_frames_parallel_ffmpeg(
    video_path: str,
    milestones: list[dict],
    out_dir: str,
    *,
    target_height: int = EXTRACT_TARGET_HEIGHT,
    num_workers: int = EXTRACT_FFMPEG_WORKERS,
) -> int:
    """
    Splits the video into `num_workers` time segments and runs one ffmpeg
    process per segment in parallel. Each process decodes only 1/N of the
    video — major speedup on multi-core machines.

    Uses timestamp-based select (lte(abs(t-T),eps)) instead of frame-number
    select, so it works correctly after keyframe seek for both H.264 and
    H.265. After -ss seek, ffmpeg resets t=0 at the seek point, so we
    express conditions relative to seg_start.

    Each successfully-extracted frame is renamed to
    `{out_dir}/frame_{i:05d}.jpg` where `i` is its index in `milestones`.

    Returns the number of frames actually extracted.
    """
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video {video_path}")
    width  = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    nf     = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps    = float(cap.get(cv2.CAP_PROP_FPS))
    cap.release()
    duration = nf / fps if fps > 0 else 0
    log.info("    video %sx%s  fps=%.2f  frames=%d  dur=%.1fs",
             width, height, fps, nf, duration)

    # Clamp to video bounds — surveyors sometimes overshoot.
    milestones = [m for m in milestones
                  if 0 <= m["video_offset_sec"] <= duration]
    if not milestones:
        log.warning("    no in-range milestones — nothing to extract")
        return 0

    # Distribute milestones across `num_workers` time-segments.
    num_workers = min(num_workers, max(1, os.cpu_count() or 4))
    seg_dur = duration / num_workers
    eps = 0.5 / max(fps, 1.0)
    segments: list[list[tuple[int, dict]]] = [[] for _ in range(num_workers)]
    for i, m in enumerate(milestones):
        seg_idx = min(int(m["video_offset_sec"] / seg_dur), num_workers - 1)
        segments[seg_idx].append((i, m))

    extracted = 0

    def _run_segment(seg_idx: int) -> int:
        items = segments[seg_idx]
        if not items:
            return 0
        seg_start = seg_idx * seg_dur
        seg_end   = min((seg_idx + 1) * seg_dur + eps, duration)
        items_sorted = sorted(items, key=lambda x: x[1]["video_offset_sec"])
        # Build a single -vf select filter that ORs all milestones in this segment.
        conditions = "+".join(
            f"lte(abs(t-{m['video_offset_sec'] - seg_start:.6f}),{eps:.6f})"
            for _, m in items_sorted)

        tmp_dir = tempfile.mkdtemp(prefix=f"seg{seg_idx}_", dir=out_dir)
        out_pat = os.path.join(tmp_dir, "f_%05d.jpg")
        cmd = [
            "ffmpeg", "-y",
            "-ss", f"{seg_start:.6f}",
            "-to", f"{seg_end:.6f}",
            "-i",  video_path,
            "-vf", f"select='{conditions}',scale=-1:{target_height}",
            "-vsync", "0",
            "-q:v", "4",
            out_pat,
        ]
        result = subprocess.run(cmd,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True)
        if result.returncode != 0:
            log.warning("    seg %d ffmpeg failed: %s", seg_idx, result.stderr[-200:])
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return 0

        # ffmpeg outputs in chronological order — match to sorted items.
        produced = sorted(glob.glob(os.path.join(tmp_dir, "f_*.jpg")))
        count = 0
        for j, (orig_idx, _) in enumerate(items_sorted):
            if j >= len(produced):
                continue
            dst = os.path.join(out_dir, f"frame_{orig_idx:05d}.jpg")
            shutil.move(produced[j], dst)
            count += 1
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return count

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        for c in pool.map(_run_segment, range(num_workers)):
            extracted += c
    log.info("    extracted %d / %d frames", extracted, len(milestones))
    return extracted


# ─────────────────────────────────────────────────────────────────────────────
# Section 9 — YOLO inference
# ─────────────────────────────────────────────────────────────────────────────
_YOLO_MODEL = None
_YOLO_TENSORRT = False
_YOLO_CLASSES: dict[int, str] = {}


def load_yolo_model(weights_path: str) -> None:
    """Load the model exactly once per process. TensorRT engine preferred —
    ~3-5× speedup on T4 vs PyTorch."""
    global _YOLO_MODEL, _YOLO_TENSORRT, _YOLO_CLASSES
    if _YOLO_MODEL is not None:
        return
    use_trt = weights_path.endswith(".engine")
    log.info("loading YOLO model (%s) → %s",
             "TensorRT FP16" if use_trt else "PyTorch CUDA", weights_path)
    from ultralytics import YOLO  # imported lazily — fast startup for help/dry-run
    model = YOLO(weights_path, task="detect")
    if not use_trt:
        model.to("cuda")
    # Class names are normalised at load — every downstream consumer (palette
    # lookup, severity_for, report builders) uses plain-hyphen names.
    _YOLO_CLASSES = {idx: normalize_label(name) for idx, name in model.names.items()}
    _YOLO_MODEL = model
    _YOLO_TENSORRT = use_trt
    log.info("✅ YOLO model loaded — %d classes", len(_YOLO_CLASSES))


def yolo_infer_frames(
    frame_paths: list[str],
    *,
    confidence: float = 0.25,
    batch_size: int | None = None,
) -> list[list[dict]]:
    """
    Run YOLO over a list of frame paths. Returns a list (same length and
    order) where each entry is the list of detected bboxes for that frame:
        [{"label": str, "bbox": [x, y, w, h], "confidence": float}, ...]

    TensorRT engines built with batch=1 must be inferred one frame at a
    time; PyTorch can take larger batches.
    """
    if _YOLO_MODEL is None:
        raise RuntimeError("YOLO model not loaded — call load_yolo_model() first")
    if batch_size is None:
        batch_size = 1 if _YOLO_TENSORRT else int(os.environ.get("INFERENCE_BATCH_SIZE", "32"))

    results_per_frame: list[list[dict]] = [[] for _ in frame_paths]
    if not frame_paths:
        return results_per_frame

    start = time.time()
    annotations_total = 0
    for batch_start in range(0, len(frame_paths), batch_size):
        batch = frame_paths[batch_start: batch_start + batch_size]
        results = _YOLO_MODEL.predict(
            source=batch, conf=confidence, device="cuda",
            half=_YOLO_TENSORRT, verbose=False,
        )
        for offset, res in enumerate(results):
            local_bboxes: list[dict] = []
            boxes = res.boxes
            if boxes is not None and len(boxes) > 0:
                for box in boxes:
                    cls_id = int(box.cls.item())
                    conf   = float(box.conf.item())
                    x1, y1, x2, y2 = box.xyxy[0].tolist()
                    local_bboxes.append({
                        "label":      _YOLO_CLASSES.get(cls_id, f"class_{cls_id}"),
                        "category_id": cls_id,
                        "bbox":       [x1, y1, x2 - x1, y2 - y1],
                        "confidence": conf,
                    })
            results_per_frame[batch_start + offset] = local_bboxes
            annotations_total += len(local_bboxes)
        elapsed = time.time() - start
        if elapsed > 0:
            log.info("    YOLO: %d/%d frames | %.1f fps | %d annotations",
                     min(batch_start + batch_size, len(frame_paths)),
                     len(frame_paths),
                     min(batch_start + batch_size, len(frame_paths)) / elapsed,
                     annotations_total)
    return results_per_frame


# ─────────────────────────────────────────────────────────────────────────────
# Section 10 — Per-video Phase B (replaces V1's main.py subprocess)
# ─────────────────────────────────────────────────────────────────────────────
# Concurrency model
# -----------------
# Per-video work is dominated by I/O (GCS download/upload, ffmpeg) — only a
# small fraction is GPU-bound (YOLO ≈ 12s for a 200-frame video at 17 fps
# vs ~80s of I/O). Sequential-per-video is therefore much slower than
# trigger_builds.py --parallel N. To match V1's throughput we expose a
# `dispatch_phase_b(jobs, parallel=N)` helper that:
#   • parallel == 1  → in-process serial loop, ONE shared YOLO model
#                      (cheapest startup; best for single-video roads)
#   • parallel  > 1  → ProcessPoolExecutor with `spawn` (CUDA-safe), each
#                      worker loads its own YOLO model into its own CUDA
#                      context. ~50 MB GPU per worker → on a 15 GB T4 we
#                      can run ~30 workers before OOM, but realistically
#                      8 is the GPU-saturation sweet spot.
# Both modes preserve idempotency: a successful run upserts annotation_segments
# on uuid; a failed run leaves the partial workdir for inspection.
def process_one_video(
    *,
    mp4_blob:        str,
    gpx_blob:        str,
    road_id:         str,
    run_id:          str,
    workdir:         Path,
    severity_override: dict | None = None,
    fast:            bool = False,
) -> dict | None:
    """
    Runs the full per-video Phase B in-process:
        download → GPX parse → 10 m milestones → ffmpeg extract → YOLO →
        bbox draw → upload {raw,annotated}_frames + result.json + per-UUID
        annotated_video.mp4 → annotation_segments doc.

    Returns a dict with summary metadata (uuid, mp4_blob, frame count,
    defect total) or None on hard failure. Idempotent for re-runs against
    the same `run_id`: existing GCS blobs are overwritten, the Mongo doc
    is upserted on (uuid).
    """
    workdir.mkdir(parents=True, exist_ok=True)
    _log_section(f"video {Path(mp4_blob).name}  uuid={run_id}")

    # 1) Download MP4 + GPX into the per-UUID workdir
    local_mp4 = workdir / "video.mp4"
    local_gpx = workdir / "data.gpx"
    log.info("    downloading MP4 + GPX from GCS")
    gcs_client().bucket(GCS_BUCKET).blob(mp4_blob).download_to_filename(str(local_mp4))
    gcs_client().bucket(GCS_BUCKET).blob(gpx_blob).download_to_filename(str(local_gpx))

    # 2) Parse GPX trackpoints
    trackpoints = parse_gpx_trackpoints(local_gpx.read_text(encoding="utf-8", errors="ignore"))
    log.info("    GPX trackpoints: %d", len(trackpoints))
    if not trackpoints:
        log.warning("    GPX has no trackpoints — skipping video")
        return None

    # 3) Compute 10 m milestones along the GPX walk
    milestones = compute_10m_milestones(trackpoints)
    if not milestones:
        log.warning("    no milestones — skipping video"); return None

    # 4) Copy MP4 to RAM disk for fast parallel ffmpeg seeks
    raw_dir = workdir / "raw_frames"; raw_dir.mkdir(parents=True, exist_ok=True)
    ann_dir = workdir / "annotated_frames"; ann_dir.mkdir(parents=True, exist_ok=True)

    ram_mp4 = Path("/dev/shm") / f"v2_{run_id}.mp4"
    try:
        shutil.copy2(str(local_mp4), str(ram_mp4))
        ext_video = str(ram_mp4)
        log.info("    video copied to /dev/shm")
    except Exception as e:
        log.warning("    /dev/shm copy failed (%s) — using disk video", e)
        ext_video = str(local_mp4)

    # 5) Extract frames in parallel
    extracted = extract_frames_parallel_ffmpeg(ext_video, milestones, str(raw_dir))
    if extracted == 0:
        if ram_mp4.exists():
            ram_mp4.unlink()
        log.warning("    no frames extracted — skipping video"); return None

    if ram_mp4.exists():
        try: ram_mp4.unlink()
        except Exception: pass

    # 6) Build the list of frame paths actually present (some milestones may
    #    have failed extraction at segment boundaries)
    raw_paths: list[str] = []
    raw_indices: list[int] = []
    for i in range(len(milestones)):
        p = raw_dir / f"frame_{i:05d}.jpg"
        if p.exists():
            raw_paths.append(str(p))
            raw_indices.append(i)
    log.info("    %d raw frames ready for inference", len(raw_paths))

    # 7) YOLO inference
    detections = yolo_infer_frames(raw_paths)

    # 8) Build COCO result.json (per-video) — same schema V1's pipeline1 used
    base_gcs = f"{PROCESSED_PREFIX}/{road_id}/{run_id}/annotated_frames"
    json_pavement: dict = {
        "info": {
            "year": _dt.datetime.utcnow().year, "version": "1.0",
            "description": "Pavement Details", "contributor": "Roadvision",
            "date_created": _dt.datetime.utcnow().isoformat(),
            "fps": 1.0, "uid": Path(mp4_blob).stem,
        },
        "images": [],
        "categories": [{"id": cid, "name": name}
                       for cid, name in sorted(_YOLO_CLASSES.items())],
        "annotations": [],
    }
    ann_id = 0
    detection_lookup: dict[int, list[dict]] = {}
    for j, frame_idx in enumerate(raw_indices):
        local_path = raw_paths[j]
        h_img, w_img = cv2.imread(local_path).shape[:2]
        json_pavement["images"].append({
            "id":        frame_idx,
            "file_name": f"gs://{GCS_BUCKET}/{base_gcs}/predict/frame_{frame_idx:05d}.jpg",
            "width":     w_img,
            "height":    h_img,
        })
        bboxes = detections[j]
        detection_lookup[frame_idx] = bboxes
        for det in bboxes:
            json_pavement["annotations"].append({
                "id":          ann_id,
                "image_id":    frame_idx,
                "category_id": det["category_id"],
                "bbox":        det["bbox"],
                "iscrowd":     0,
                "ignore":      0,
                "segmentation": [],
                "area":        det["bbox"][2] * det["bbox"][3],
                "confidence":  det["confidence"],
                "ai_model":    "YOLO-Local-GPU",
            })
            ann_id += 1
    log.info("    %d total detections", ann_id)

    # 9) Draw bboxes on raw frames → annotated frames; severity per-bbox
    for j, frame_idx in enumerate(raw_indices):
        bboxes = detection_lookup.get(frame_idx) or []
        for det in bboxes:
            det["severity"] = severity_for(det["label"], severity_override)
        raw_path = raw_paths[j]
        ann_path = ann_dir / f"frame_{frame_idx:05d}.jpg"
        if not bboxes:
            shutil.copy(raw_path, ann_path)
            continue
        with open(raw_path, "rb") as fh:
            raw_bytes = fh.read()
        out_bytes = draw_overlays_on_jpeg(raw_bytes, bboxes)
        if out_bytes is None:
            shutil.copy(raw_path, ann_path)
        else:
            ann_path.write_bytes(out_bytes)

    # 10) Upload raw + annotated frames to GCS
    log.info("    uploading frames to GCS")
    upload_tasks: list[tuple[str, str]] = []
    for frame_idx in raw_indices:
        fname = f"frame_{frame_idx:05d}.jpg"
        upload_tasks.append((str(raw_dir / fname), f"{base_gcs}/frames/{fname}"))
        upload_tasks.append((str(ann_dir / fname), f"{base_gcs}/predict/{fname}"))

    def _upload(t):
        gcs_upload_file(t[1], t[0], content_type="image/jpeg")
    with ThreadPoolExecutor(max_workers=32) as pool:
        list(pool.map(_upload, upload_tasks))
    log.info("    %d images uploaded", len(upload_tasks))

    # 11) Upload per-UUID result.json (skipped under --fast; Phase C builds
    #     the merged result.json from segments anyway, so this duplicate
    #     is just network load. ~3s per video × 8 = ~24s saved per batch.)
    if not fast:
        result_blob = f"{PROCESSED_PREFIX}/{road_id}/{run_id}/result.json"
        gcs_upload_text(result_blob, json.dumps([json_pavement], default=str),
                        content_type="application/json")

    # 12) Stitch per-UUID annotated_video.mp4 (skipped under --fast; the
    #     consolidated annotated video that Phase C builds is the only
    #     one anyone reads, and ffmpeg + upload here costs ~10s × 8 = 80s
    #     per batch.)
    if not fast:
        av_local = workdir / "annotated_video.mp4"
        av_blob  = f"{PROCESSED_PREFIX}/{road_id}/{run_id}/annotated_video.mp4"
        cmd = [
            "ffmpeg", "-y",
            "-framerate", "1",
            "-i", str(ann_dir / "frame_%05d.jpg"),
            "-c:v", "libx264", "-pix_fmt", "yuv420p",
            "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
            "-r", "30",
            str(av_local),
        ]
        try:
            subprocess.run(cmd, check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            gcs_upload_file(av_blob, str(av_local), content_type="video/mp4")
            log.info("    annotated_video.mp4 uploaded")
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            log.warning("    annotated_video.mp4 build failed: %s", e)

    # 13) Build frame_list_data + severity distribution + Mongo annotation_segments
    severity_rank = _SEV_RANK
    frame_list_data: list[dict] = []
    cum_km = 0.0
    prev_pt: tuple[float, float] | None = None
    severity_totals = {"high": 0, "medium": 0, "low": 0, "none": 0}
    total_defects = 0
    for j, frame_idx in enumerate(raw_indices):
        m = milestones[frame_idx]
        cur = (m["latitude"], m["longitude"])
        if prev_pt is not None:
            cum_km += _haversine_m(prev_pt, cur) / 1000.0
        prev_pt = cur
        bboxes = detection_lookup.get(frame_idx) or []
        worst = "none"
        for b in bboxes:
            sev = b.get("severity", "none")
            severity_totals[sev] = severity_totals.get(sev, 0) + 1
            if severity_rank.get(sev, 0) > severity_rank.get(worst, 0):
                worst = sev
        total_defects += len(bboxes)
        frame_list_data.append({
            "timeElapsed":     m["timeElapsed"],
            "latitude":        m["latitude"],
            "longitude":       m["longitude"],
            "location":        {"type": "Point",
                                "coordinates": [m["longitude"], m["latitude"]]},
            "chainage_km":     round(cum_km, 3),
            "orientation":     m["orientation"],
            "og_file":         f"frame_data/frames/frame_{frame_idx:05d}.jpg",
            "inference_image": f"frame_data/predict/frame_{frame_idx:05d}.jpg",
            "defect_state":    worst,
            "inference_info":  bboxes,
        })

    # Per-frame PCI advisory (matches V1: PCI = max(0, 100 - 10·H - 4·M - L))
    road_length_km = round(cum_km, 3)
    H = severity_totals["high"]; M = severity_totals["medium"]; L = severity_totals["low"]
    road_rating = max(0, 100 - 10 * H - 4 * M - 1 * L) if (H or M or L) else 100

    db = MongoClient(MONGO_URI)["roadvision"]
    db.annotation_segments.update_one(
        {"uuid": run_id},
        {"$set": {
            "uuid":               run_id,
            "road_id":            road_id,
            "source_mp4":         mp4_blob,         # used by Phase C sort fallback
            "total_frames":       len(frame_list_data),
            "total_defects":      total_defects,
            "road_length_km":     road_length_km,
            "road_rating":        float(road_rating),
            "severity_distribution": severity_totals,
            "frame_list_data":    frame_list_data,
            "category_information": {str(c["id"]): c["name"]
                                     for c in json_pavement["categories"]},
            "data_submitted":     _dt.date.today().strftime("%d-%m-%Y"),
            "last_updated":       _dt.datetime.utcnow(),
        }},
        upsert=True,
    )
    log.info("    annotation_segments upserted  uuid=%s  frames=%d  defects=%d",
             run_id, len(frame_list_data), total_defects)

    return {
        "uuid":          run_id,
        "mp4_blob":      mp4_blob,
        "frames":        len(frame_list_data),
        "defects":       total_defects,
        "road_length_km": road_length_km,
    }


def _phase_b_worker(
    mp4_blob:          str,
    gpx_blob:          str,
    road_id:           str,
    run_id:            str,
    workdir_str:       str,
    severity_override: dict | None,
    model_weights:     str,
    fast:              bool = False,
    log_file_path:     str | None = None,
    log_workdir_root:  str | None = None,
) -> dict | None:
    """
    Top-level (picklable) entry point used by ProcessPoolExecutor.

    Each pool worker is a fresh `spawn`ed Python process — it imports
    pipeline_v2 fresh, then loads the YOLO model into ITS OWN CUDA
    context. The TensorRT engine takes ~50 MB GPU per worker; on a 15 GB
    T4 this is fine for parallel ≤ 30, with ~8 being the practical
    GPU-saturation sweet spot for the typical 10 m frame density.

    When `log_file_path` is supplied, the worker also tees its log
    output to that file (POSIX append → safe across workers).

    Cleans up its own workdir on exit so parallel runs don't accumulate
    gigabytes of intermediate files.
    """
    # Resolve which log file the worker should append to. Two paths:
    #   1. log_file_path  → per-uid mode (single road per V2 invocation)
    #   2. log_workdir_root + road_id → per-road derivation in --all-roads
    file_handler: logging.FileHandler | None = None
    resolved_log: Path | None = None
    if log_file_path:
        resolved_log = Path(log_file_path)
    elif log_workdir_root:
        resolved_log = road_log_path(Path(log_workdir_root), road_id)
    if resolved_log is not None:
        file_handler = attach_road_log_handler(resolved_log)
    workdir = Path(workdir_str)
    try:
        load_yolo_model(model_weights)
        return process_one_video(
            mp4_blob=mp4_blob, gpx_blob=gpx_blob,
            road_id=road_id, run_id=run_id, workdir=workdir,
            severity_override=severity_override,
            fast=fast,
        )
    except Exception:
        log.exception("[Phase B worker] %s failed", mp4_blob)
        return None
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
        detach_log_handler(file_handler)


def dispatch_phase_b(
    *,
    jobs:              list[dict],
    parallel:          int,
    model_weights:     str,
    severity_override: dict | None,
    on_done_callback=None,
    fast:              bool = False,
    log_file_path:     str | None = None,
    log_workdir_root:  str | None = None,
) -> int:
    """
    Run Phase B over a list of (mp4, gpx, road_id, run_id, workdir) jobs.

    parallel == 1 → serial loop, ONE shared YOLO model in this process.
                    Cheapest startup; best when a road has 1–2 videos.
    parallel  > 1 → ProcessPoolExecutor (spawn context — CUDA-safe), each
                    worker loads its own YOLO model. Wall-clock scales
                    near-linearly until GPU saturation because per-video
                    work is dominated by I/O.

    `on_done_callback(job, result_or_None)` fires after each job completes
    in BOTH modes. Used by watch mode to update the tracker. The callback
    is called from the main process — workers don't touch the tracker.

    Returns the number of jobs that returned a non-None result.
    """
    if not jobs:
        return 0
    t0 = time.time()
    log.info("[Phase B] dispatching %d videos (parallel=%d)", len(jobs), parallel)

    ok = 0
    if parallel <= 1:
        load_yolo_model(model_weights)
        for j in jobs:
            res = None
            try:
                res = process_one_video(
                    mp4_blob=j["mp4_blob"], gpx_blob=j["gpx_blob"],
                    road_id=j["road_id"], run_id=j["run_id"],
                    workdir=j["workdir"],
                    severity_override=severity_override,
                    fast=fast,
                )
                if res:
                    ok += 1
            except Exception as e:
                log.exception("[Phase B] %s failed: %s", j["mp4_blob"], e)
            finally:
                shutil.rmtree(j["workdir"], ignore_errors=True)
            if on_done_callback is not None:
                on_done_callback(j, res)
    else:
        # CUDA contexts can't survive fork() — must use spawn.
        import multiprocessing as _mp
        from concurrent.futures import ProcessPoolExecutor as _PPE
        ctx = _mp.get_context("spawn")
        with _PPE(max_workers=parallel, mp_context=ctx) as pool:
            futs = {
                pool.submit(
                    _phase_b_worker,
                    j["mp4_blob"], j["gpx_blob"], j["road_id"],
                    j["run_id"], str(j["workdir"]),
                    severity_override, model_weights, fast,
                    log_file_path,        # explicit per-uid path (overrides road_id derivation)
                    log_workdir_root,     # else derive {root}/{road_id}/pipeline.log per worker
                ): j
                for j in jobs
            }
            for fut in as_completed(futs):
                j = futs[fut]
                try:
                    res = fut.result()
                except Exception as e:
                    log.exception("[Phase B] worker for %s raised: %s",
                                  j["mp4_blob"], e)
                    res = None
                if res:
                    ok += 1
                if on_done_callback is not None:
                    on_done_callback(j, res)

    elapsed = time.time() - t0
    log.info("[Phase B] %d/%d succeeded in %.1fs (%.1fs/video)",
             ok, len(jobs), elapsed, elapsed / max(1, len(jobs)))
    return ok


# ─────────────────────────────────────────────────────────────────────────────
# Section 11 — Label Studio JSON ingestion (sub-category override for Phase C)
# ─────────────────────────────────────────────────────────────────────────────
# The deployed YOLO model emits its 34 trained NHAI classes. Orgs that need
# 52-leaf sub-category granularity (Kota / Varanasi) get those richer labels
# from a human Label Studio annotation pass. Each LS task points at a frame
# URL via data.image; result entries are either:
#   * type=rectanglelabels, label string already complete, OR
#   * type=rectanglelabels (main) PAIRED with type=choices (sub-state) on a
#     sibling result that shares the same `id` → "<Main> – <Sub>" composite.
#
# When --ls-export-dir is supplied to V2, every frame's inference_info is
# REPLACED by the LS bboxes for that frame BEFORE Phase C builds reports.
def _ls_pair_results(results: list[dict]) -> list[dict]:
    by_id: dict[str, dict] = defaultdict(dict)
    standalone: list[dict] = []
    for r in results:
        rid = r.get("id")
        rtype = r.get("type")
        v = r.get("value") or {}
        if rtype == "rectanglelabels":
            rect = {
                "main": (v.get("rectanglelabels") or [None])[0],
                "x":    float(v.get("x", 0)),
                "y":    float(v.get("y", 0)),
                "w":    float(v.get("width", 0)),
                "h":    float(v.get("height", 0)),
                "ow":   float(r.get("original_width") or 0),
                "oh":   float(r.get("original_height") or 0),
            }
            if rid:
                by_id[rid]["rect"] = rect
            else:
                standalone.append({**rect, "sub": None})
        elif rtype == "choices":
            sub = (v.get("choices") or [None])[0]
            if rid and sub:
                by_id[rid]["sub"] = sub

    out: list[dict] = []
    for entry in by_id.values():
        rect = entry.get("rect")
        if not rect or not rect["main"]:
            continue
        sub = entry.get("sub")
        label = f"{rect['main']} – {sub}" if sub else rect["main"]
        out.append({"label": label.strip(),
                    "x_pct": rect["x"], "y_pct": rect["y"],
                    "w_pct": rect["w"], "h_pct": rect["h"],
                    "ow":    rect["ow"], "oh":    rect["oh"]})
    for r in standalone:
        if r["main"]:
            out.append({"label": r["main"].strip(),
                        "x_pct": r["x"], "y_pct": r["y"],
                        "w_pct": r["w"], "h_pct": r["h"],
                        "ow":    r["ow"], "oh":    r["oh"]})
    return out


def load_ls_export_dir(ls_root: str) -> dict[str, list[dict]]:
    """
    Build a lookup keyed by trailing two segments of each task's image URL
    (e.g. "20260321095111_000100F/frame_00036.jpg") → list of bboxes in
    pixel coords with severity placeholder. Phase C's retag fills severity.
    """
    root = Path(ls_root)
    if not root.is_dir():
        raise SystemExit(f"--ls-export-dir not found: {ls_root}")
    lookup: dict[str, list[dict]] = {}
    n_bboxes = 0; label_hits: Counter[str] = Counter()
    for folder in sorted(root.iterdir()):
        if not folder.is_dir():
            continue
        ann_path = folder / "annotations.json"
        if not ann_path.exists():
            continue
        tasks = json.load(open(ann_path))
        for t in tasks:
            image = t.get("data", {}).get("image") or ""
            if not image:
                continue
            parts = image.strip("/").split("/")
            if len(parts) < 2:
                continue
            tail = "/".join(parts[-2:])
            for ann in t.get("annotations", []):
                paired = _ls_pair_results(ann.get("result", []) or [])
                bboxes = []
                for p in paired:
                    if p["ow"] <= 0 or p["oh"] <= 0:
                        continue
                    bboxes.append({
                        "label":    p["label"],
                        "bbox":     [p["x_pct"] * p["ow"] / 100.0,
                                     p["y_pct"] * p["oh"] / 100.0,
                                     p["w_pct"] * p["ow"] / 100.0,
                                     p["h_pct"] * p["oh"] / 100.0],
                        "severity": "none",
                    })
                    label_hits[p["label"]] += 1
                if bboxes:
                    lookup.setdefault(tail, []).extend(bboxes)
                    n_bboxes += len(bboxes)
    log.info("[LS] %d frames with bboxes, %d total bboxes, %d distinct labels",
             len(lookup), n_bboxes, len(label_hits))
    return lookup


def apply_ls_to_frames(
    frames: list[dict],
    ls_lookup: dict[str, list[dict]],
) -> tuple[int, int]:
    """Replace each frame's inference_info with the LS bboxes when the
    frame's og_file's trailing two path segments match a key in
    `ls_lookup`. Frames without an LS match keep whatever YOLO emitted."""
    replaced = no_match = 0
    for f in frames:
        og = f.get("og_file") or f.get("inference_image") or ""
        parts = og.strip("/").split("/")
        if len(parts) < 2:
            no_match += 1; continue
        tail = "/".join(parts[-2:]).split("?")[0]
        bboxes = ls_lookup.get(tail)
        if bboxes is None:
            no_match += 1; continue
        f["inference_info"] = [dict(b) for b in bboxes]
        replaced += 1
    log.info("[LS] applied to %d frames (%d had no LS task)", replaced, no_match)
    return replaced, no_match


# ─────────────────────────────────────────────────────────────────────────────
# Section 12 — IBI-format report builders
# ─────────────────────────────────────────────────────────────────────────────
# All builders apply the per-frame-unique counting rule (Option 1 confirmed
# with the user): if a single frame contains N bboxes of the same label,
# they collapse to one defect at the WORST severity. Avoids inflated counts
# when YOLO fires repeatedly on a single asset within one frame.
def _cap_level(level: str) -> str:
    l = (level or "none").lower()
    return {"high": "High", "medium": "Medium", "low": "Low"}.get(l, "Low")


def _severity_str(rank: int) -> str:
    return {3: "High", 2: "Medium", 1: "Low"}.get(rank, "Nill")


def build_report_1_key(
    frames: list[dict],
    road_length_km: float,
    road_rating: float,
    start_addr: str,
    end_addr: str,
) -> tuple[str, int]:
    """Per-defect-type summary CSV, IBI shape.
    Cols: name,level,value,start,end,roadLength,roadRating,defect,unique_value
    Returns (csv_text, total_defects_per_frame_unique)."""
    per_bucket: Counter[tuple[str, str]] = Counter()
    label_frames: dict[str, set[int]] = defaultdict(set)
    all_labels: set[str] = set()
    total_defects = 0
    for i, f in enumerate(frames):
        worst_rank_per_label: dict[str, int] = {}
        for inf in (f.get("inference_info") or []):
            label = (inf.get("label") or "").strip()
            if not label:
                continue
            r = _SEV_RANK.get((inf.get("severity") or "none").lower(), 0)
            if r > worst_rank_per_label.get(label, -1):
                worst_rank_per_label[label] = r
        for label, rank in worst_rank_per_label.items():
            level = _cap_level(_RANK_NAME.get(rank, "none"))
            per_bucket[(label, level)] += 1
            label_frames[label].add(i)
            all_labels.add(label)
            total_defects += 1

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["name", "level", "value", "start", "end",
                "roadLength", "roadRating", "defect", "unique_value"])
    for label in sorted(all_labels):
        unique_val = len(label_frames[label])
        for level in ("Low", "Medium", "High"):
            count = per_bucket.get((label, level), 0)
            w.writerow([label, level, float(count), start_addr, end_addr,
                        road_length_km, road_rating, total_defects, unique_val])
    return buf.getvalue(), total_defects


def build_report_2_key(frames: list[dict]) -> tuple[str, list[str]]:
    """Per-100 m chainage CSV (also stored as data.dashboard_df_csv).
    Cols: SerialNumber, Chainage, start_lat, start_lng, end_lat, end_lng,
          PCI, {label}_Count, {label}_Severity, {label}%   per label."""
    bucket_frames: dict[int, list[dict]] = {}
    for f in frames:
        km = float(f.get("chainage_km") or 0)
        bucket_frames.setdefault(int(km // 0.1), []).append(f)
    sorted_buckets = sorted(bucket_frames.keys())

    label_set: set[str] = set()
    for flist in bucket_frames.values():
        for f in flist:
            for inf in (f.get("inference_info") or []):
                lbl = (inf.get("label") or "").strip()
                if lbl:
                    label_set.add(lbl)
    labels = sorted(label_set)

    base_cols = ["Serial Number", "Chainage", "start_latitude", "start_longitude",
                 "end_latitude", "end_longitude", "PCI"]
    label_cols = []
    for lbl in labels:
        label_cols += [f"{lbl}_Count", f"{lbl}_Severity", f"{lbl}%"]

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(base_cols + label_cols)

    for idx, bucket in enumerate(sorted_buckets):
        flist = bucket_frames[bucket]
        # Per-frame-unique inside the bucket (same rule as build_report_1_key)
        label_counts: Counter[str] = Counter()
        label_sev_rank: dict[str, int] = {}
        for f in flist:
            worst_rank_per_label: dict[str, int] = {}
            for inf in (f.get("inference_info") or []):
                lbl = (inf.get("label") or "").strip()
                if not lbl:
                    continue
                r = _SEV_RANK.get((inf.get("severity") or "none").lower(), 0)
                if r > worst_rank_per_label.get(lbl, -1):
                    worst_rank_per_label[lbl] = r
            for lbl, rank in worst_rank_per_label.items():
                label_counts[lbl] += 1
                if rank > label_sev_rank.get(lbl, 0):
                    label_sev_rank[lbl] = rank

        # PCI: max(0, 100 − 10·H − 4·M − 1·L) — V1 advisory formula.
        sev_totals = {3: 0, 2: 0, 1: 0, 0: 0}
        for lbl, rank in label_sev_rank.items():
            sev_totals[rank] += label_counts[lbl]
        pci = max(0, 100 - sev_totals[3] * 10 - sev_totals[2] * 4 - sev_totals[1] * 1)

        row = [idx + 1, bucket * 100,
               flist[0].get("latitude", 0),  flist[0].get("longitude", 0),
               flist[-1].get("latitude", 0), flist[-1].get("longitude", 0),
               pci]
        for lbl in labels:
            count = label_counts.get(lbl, 0)
            row += [count, _severity_str(label_sev_rank.get(lbl, 0)),
                    round(count / 10.0, 3)]
        w.writerow(row)
    return buf.getvalue(), labels


def build_chainage_report_csv(frames: list[dict],
                              start_addr: str, end_addr: str) -> str:
    """Same shape as report_2_key plus start_address/end_address tail
    (only on the first / last bucket — the IBI Reports page expects this)."""
    bucket_frames: dict[int, list[dict]] = {}
    for f in frames:
        km = float(f.get("chainage_km") or 0)
        bucket_frames.setdefault(int(km // 0.1), []).append(f)
    sorted_buckets = sorted(bucket_frames.keys())

    label_set: set[str] = set()
    for flist in bucket_frames.values():
        for f in flist:
            for inf in (f.get("inference_info") or []):
                lbl = (inf.get("label") or "").strip()
                if lbl:
                    label_set.add(lbl)
    labels = sorted(label_set)

    base_cols = ["Serial Number", "Chainage", "start_latitude", "start_longitude",
                 "end_latitude", "end_longitude", "PCI"]
    label_cols: list[str] = []
    for lbl in labels:
        label_cols += [f"{lbl}_Count", f"{lbl}_Severity", f"{lbl}%"]
    tail_cols = ["start_address", "end_address"]

    buf = io.StringIO()
    w = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    w.writerow(base_cols + label_cols + tail_cols)

    for idx, bucket in enumerate(sorted_buckets):
        flist = bucket_frames[bucket]
        label_counts: Counter[str] = Counter()
        label_sev_rank: dict[str, int] = {}
        for f in flist:
            worst_rank_per_label: dict[str, int] = {}
            for inf in (f.get("inference_info") or []):
                lbl = (inf.get("label") or "").strip()
                if not lbl:
                    continue
                r = _SEV_RANK.get((inf.get("severity") or "none").lower(), 0)
                if r > worst_rank_per_label.get(lbl, -1):
                    worst_rank_per_label[lbl] = r
            for lbl, rank in worst_rank_per_label.items():
                label_counts[lbl] += 1
                if rank > label_sev_rank.get(lbl, 0):
                    label_sev_rank[lbl] = rank
        sev_totals = {3: 0, 2: 0, 1: 0, 0: 0}
        for lbl, rank in label_sev_rank.items():
            sev_totals[rank] += label_counts[lbl]
        pci = max(0, 100 - sev_totals[3] * 10 - sev_totals[2] * 4 - sev_totals[1] * 1)

        row = [idx + 1, f"{bucket * 100}-{(bucket + 1) * 100}m",
               flist[0].get("latitude", 0),  flist[0].get("longitude", 0),
               flist[-1].get("latitude", 0), flist[-1].get("longitude", 0),
               pci]
        for lbl in labels:
            count = label_counts.get(lbl, 0)
            row += [count, _severity_str(label_sev_rank.get(lbl, 0)),
                    round(count / 10.0, 3)]
        row += [start_addr if idx == 0 else "",
                end_addr   if idx == len(sorted_buckets) - 1 else ""]
        w.writerow(row)
    return buf.getvalue()


def build_pie_chart2(frames: list[dict]) -> dict:
    """plot_data.plots.pie_chart2 — required by the /project/get_plot_data
    backend filter (it skips docs without `plot_data.plots`). Per-frame
    severity, percentages summing to 100."""
    counts = {"high": 0, "medium": 0, "low": 0, "none": 0}
    for f in frames:
        worst = "none"
        for inf in (f.get("inference_info") or []):
            sev = (inf.get("severity") or "none").lower()
            if _SEV_RANK.get(sev, 0) > _SEV_RANK[worst]:
                worst = sev
        counts[worst] += 1
    total = max(1, len(frames))
    def pct(n): return round(n * 100.0 / total, 2)
    return {
        "severity_counts": [pct(counts["none"]),  pct(counts["high"]),
                            pct(counts["medium"]), pct(counts["low"])],
        "category_names":  ["Not Defected", "High Severity",
                            "Medium Severity", "Low Severity"],
    }


# ── Report 3 + Report 4 (per-frame distress + per-detection summary) ─────
# V1's main.py generated four reports per UUID (1–4). V2's Phase B per-UUID
# only stores enough data for Phase C to rebuild reports 1, 2 and the
# chainage CSV. Reports 3 + 4 used to be concatenated from per-UUID
# `report_3_key` / `report_4_key` strings on annotation_segments, but those
# fields are empty on V2-produced segments — the concat returned "" and
# upload_combined_reports skipped them. The functions below regenerate
# both directly from merged_frames + inference_info, matching V1's exact
# column schemas so the dashboard / IBI / NHAI consumers see no difference.

# Homography for Report 3 — same constants V1 uses. Image-plane → real-
# world (metres) projection for converting bbox area to ground area. The
# defaults assume a dashcam mounted ~1.4 m above the road centre, fitting
# the previously-trained NHAI model's typical view; orgs with different
# mountings should provide their own homography in the future.
_REPORT3_IMAGE_POINTS = np.array(
    [[700, 100], [700, 850], [520, 300], [720, 400]], dtype=np.float32)
_REPORT3_REAL_POINTS = np.array(
    [[0, 0], [0, 3.5], [1, 0], [1, 3.5]], dtype=np.float32)
_REPORT3_AREA_SCALE = 300.764   # empirical scaling factor V1 uses
_REPORT3_HOMOGRAPHY: np.ndarray | None = None


def _real_world_area_m2(bbox: list | tuple) -> float:
    """Project an image-plane bbox onto the road plane via homography
    and return the resulting rectangle's area in square metres
    (× V1's empirical scaling factor)."""
    global _REPORT3_HOMOGRAPHY
    if _REPORT3_HOMOGRAPHY is None:
        _REPORT3_HOMOGRAPHY, _ = cv2.findHomography(
            _REPORT3_IMAGE_POINTS, _REPORT3_REAL_POINTS)
    H = _REPORT3_HOMOGRAPHY
    try:
        x, y, w, h = (float(v) for v in bbox[:4])
    except Exception:
        return 0.0
    corners = [(x, y), (x + w, y), (x + w, y + h), (x, y + h)]
    rw: list[tuple[float, float]] = []
    for cx, cy in corners:
        p = np.array([cx, cy, 1.0]).reshape((3, 1))
        t = H @ p
        if t[2, 0] == 0:
            return 0.0
        t = t / t[2, 0]
        rw.append((float(t[0, 0]), float(t[1, 0])))
    width  = float(np.linalg.norm(np.array(rw[0]) - np.array(rw[1])))
    height = float(np.linalg.norm(np.array(rw[0]) - np.array(rw[3])))
    return width * height * _REPORT3_AREA_SCALE


def build_report_3_key(frames: list[dict]) -> str:
    """
    Per-frame road-distress CSV with real-world bbox areas.
    Columns: Latitude, Longitude, Road Distress, Area1..AreaN, File_URL.
    One row per frame that has at least one detection. Areas[i] aligns
    with the i-th label in the comma-joined "Road Distress" cell.
    """
    rows: list[dict] = []
    max_areas = 0
    for f in frames:
        bboxes = [b for b in (f.get("inference_info") or [])
                  if (b.get("label") or "").strip()]
        if not bboxes:
            continue
        labels: list[str] = []
        areas:  list[float] = []
        for b in bboxes:
            labels.append(normalize_label(b["label"]))
            areas.append(round(_real_world_area_m2(b.get("bbox") or [0, 0, 0, 0]), 3))
        max_areas = max(max_areas, len(areas))
        rows.append({
            "Latitude":      f.get("latitude"),
            "Longitude":     f.get("longitude"),
            "Road Distress": ", ".join(labels),
            "_areas":        areas,
            "File_URL":      f.get("inference_image") or f.get("og_file") or "",
        })

    cols = (["Latitude", "Longitude", "Road Distress"]
            + [f"Area{i + 1}" for i in range(max_areas)]
            + ["File_URL"])
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(cols)
    for r in rows:
        row = [r["Latitude"], r["Longitude"], r["Road Distress"]]
        row.extend(r["_areas"] + [""] * (max_areas - len(r["_areas"])))
        row.append(r["File_URL"])
        w.writerow(row)
    return buf.getvalue()


def build_report_4_key(frames: list[dict],
                       reporting_date: str | None = None) -> str:
    """
    Per-detection summary CSV. One row per bbox.
    Columns: S_No, Reporting_Date, Asset_Type, Defect_Description, Side,
             Chainage, Latitude, Longitude, Defect_Image,
             NH_Number, Project_Name, UPC_Code, State, RO_Name, PIU_Name,
             Survey_Date  (the trailing 6 fields are placeholders V1
             leaves blank; downstream IBI / NHAI tooling fills them).
    """
    if reporting_date is None:
        reporting_date = _dt.date.today().strftime("%d-%m-%Y")

    cols = ["S_No", "Reporting_Date", "Asset_Type", "Defect_Description",
            "Side", "Chainage", "Latitude", "Longitude", "Defect_Image",
            "NH_Number", "Project_Name", "UPC_Code", "State", "RO_Name",
            "PIU_Name", "Survey_Date"]
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(cols)
    s_no = 0
    for f in frames:
        for b in (f.get("inference_info") or []):
            label = (b.get("label") or "").strip()
            if not label:
                continue
            s_no += 1
            ch_km = float(f.get("chainage_km") or 0)
            ch_m = int(ch_km * 1000)
            # bucket-aligned label like "1300-1400m" (matches V1)
            bucket = (ch_m // 100) * 100
            chainage_str = f"{bucket}-{bucket + 100}m"
            w.writerow([
                s_no, reporting_date, "Road Defect",
                normalize_label(label), "",     # Side blank
                chainage_str,
                f.get("latitude"), f.get("longitude"),
                f.get("inference_image") or f.get("og_file") or "",
                "", "", "", "", "", "",        # NH_Number etc.
                reporting_date,
            ])
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# Section 13 — Combined reports + merged COCO result.json
# ─────────────────────────────────────────────────────────────────────────────
def _concat_csv_strings(csvs: list[str]) -> str:
    """Merge multiple CSV strings into one — keep first non-empty header,
    append every body row."""
    header: str | None = None
    body: list[str] = []
    for raw in csvs:
        if not raw:
            continue
        lines = [l for l in raw.replace("\r\n", "\n").rstrip("\n").split("\n") if l]
        if not lines:
            continue
        if header is None:
            header = lines[0]
            body.extend(lines[1:])
        else:
            body.extend(lines[1:] if lines[0] == header else lines)
    if header is None:
        return ""
    return header + "\n" + "\n".join(body) + "\n"


def upload_combined_reports(
    *,
    road_id: str,
    uid: str,
    segments: list[dict],
    report_1: str,
    report_2: str,
    report_3: str,
    report_4: str,
    chainage_csv: str,
) -> dict[str, str]:
    """Upload the four consolidated CSVs + chainage_report.csv to GCS at
    processed-data/{road_id}/{uid}/. Caller already produced every body —
    we just upload them. Returns a {filename: public_url} map.

    `segments` is kept for API symmetry; older callers that wanted us
    to concat per-UUID report_3/4 strings can still pass them but the
    correct path now is to build report_3 + report_4 from merged_frames
    via build_report_3_key / build_report_4_key.
    """
    files = {
        "report_1.csv":        report_1,
        "report_2.csv":        report_2,
        "report_3.csv":        report_3,
        "report_4.csv":        report_4,
        "chainage_report.csv": chainage_csv,
    }
    urls: dict[str, str] = {}
    for name, body in files.items():
        if not body:
            continue
        blob = f"{PROCESSED_PREFIX}/{road_id}/{uid}/{name}"
        gcs_upload_text(blob, body, content_type="text/csv")
        urls[name] = f"https://storage.googleapis.com/{GCS_BUCKET}/{blob}"
    log.info("[Phase C] %d combined report files uploaded", len(urls))
    return urls


def build_merged_result_json(merged_frames: list[dict], merged_tag: str) -> dict:
    """Single COCO-format result.json from merged frame_list_data — same
    schema as V1's per-UUID result.json so existing tooling
    (label-studio import, reprocess scripts) works against the merged set.
    Image file_names point at the merged_frames_<tag>/<seq:06d>.jpg path
    (10 m sequence numbering)."""
    labels: dict[str, int] = {}
    for f in merged_frames:
        for inf in (f.get("inference_info") or []):
            lbl = (inf.get("label") or "").strip()
            if lbl and lbl not in labels:
                labels[lbl] = len(labels)
    categories = [{"id": cid, "name": lbl} for lbl, cid in labels.items()]

    images: list[dict] = []; annotations: list[dict] = []; ann_id = 0
    for i, f in enumerate(merged_frames):
        seq = i * 10
        images.append({
            "id":          i,
            "file_name":   f"merged_frames_{merged_tag}/{seq:06d}.jpg",
            "width":       int(f.get("width") or 0),
            "height":      int(f.get("height") or 0),
            "latitude":    f.get("latitude"),
            "longitude":   f.get("longitude"),
            "chainage_km": f.get("chainage_km"),
            "_uuid":       f.get("_uuid"),
        })
        for inf in (f.get("inference_info") or []):
            lbl = (inf.get("label") or "").strip()
            if not lbl:
                continue
            cid = labels.get(lbl)
            if cid is None:
                continue
            bbox = list(inf.get("bbox") or [0, 0, 0, 0])
            annotations.append({
                "id":          ann_id,
                "image_id":    i,
                "category_id": cid,
                "bbox":        bbox,
                "iscrowd":     0,
                "area":        float(bbox[2] * bbox[3]) if len(bbox) >= 4 else 0,
                "severity":    inf.get("severity", "none"),
            })
            ann_id += 1
    return {
        "info": {
            "description":  f"Consolidated COCO result for {merged_tag}",
            "version":      "1.0",
            "date_created": _dt.datetime.utcnow().isoformat(),
            "merged_frames_count":      len(images),
            "merged_annotations_count": len(annotations),
        },
        "images":      images,
        "annotations": annotations,
        "categories":  categories,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Section 14 — Merged frames folder + consolidated raw/annotated videos
# ─────────────────────────────────────────────────────────────────────────────
def build_merged_frames_folder(road_id: str, uid: str, merged_tag: str) -> None:
    """
    Copy each consolidated frame's raw + annotated source from
    processed-data/<road>/<uuid>/annotated_frames/{frames,predict}/* into:
        processed-data/<road>/merged_frames_<tag>/<seq:06d>.jpg   (raw)
        processed-data/<road>/merged_predict_<tag>/<seq:06d>.jpg  (bboxed)
    Names use 6-digit zero-padded sequence × 10 (matches IBI's "10 m steps"
    naming convention). Stamps the resulting URLs onto each frame's
    og_file / inference_image fields on the inference_data doc.
    """
    db = MongoClient(MONGO_URI)["roadvision"]
    cli = gcs_client()
    dest_bucket = cli.bucket(GCS_BUCKET)

    inf = db.inference_data.find_one({"uid": uid})
    if not inf:
        log.warning("[merged] no inference_data for uid=%s", uid); return
    frames = (inf.get("data") or {}).get("frame_list_data") or []
    if not frames:
        log.warning("[merged] uid=%s has no frames", uid); return

    raw_prefix = f"{PROCESSED_PREFIX}/{road_id}/merged_frames_{merged_tag}"
    ann_prefix = f"{PROCESSED_PREFIX}/{road_id}/merged_predict_{merged_tag}"
    log.info("[merged] %d frames → gs://%s/%s/  +  %s/",
             len(frames), GCS_BUCKET, raw_prefix, ann_prefix)

    bucket_cache: dict[str, storage.Bucket] = {GCS_BUCKET: dest_bucket}

    def _copy(src_url: str, dest_name: str) -> str | None:
        sp = _split_gcs_url(src_url)
        if not sp:
            return None
        src_bucket_name, src_path = sp
        if src_bucket_name not in bucket_cache:
            bucket_cache[src_bucket_name] = cli.bucket(src_bucket_name)
        src_blob = bucket_cache[src_bucket_name].blob(src_path)
        try:
            bucket_cache[src_bucket_name].copy_blob(src_blob, dest_bucket, dest_name)
        except Exception as e:
            log.debug("[merged] copy fail %s → %s: %s", src_url, dest_name, e)
            return None
        return f"https://storage.googleapis.com/{GCS_BUCKET}/{dest_name}"

    def _resolve(src: str | None, frame: dict, *, kind: str) -> str | None:
        """Build the absolute GCS URL for the {kind} version of this frame.
        Always points at annotated_frames/{kind}/, never at frame_data/.
        kind ∈ {'frames', 'predict'}."""
        if not src:
            return None
        if src.startswith("http") or src.startswith("gs://"):
            return src
        seg_uuid = frame.get("_uuid") or ""
        if not seg_uuid:
            return None
        filename = Path(src).name
        return (f"https://storage.googleapis.com/{GCS_BUCKET}/"
                f"{PROCESSED_PREFIX}/{road_id}/{seg_uuid}/"
                f"annotated_frames/{kind}/{filename}")

    def _copy_pair(idx_frame):
        idx, frame = idx_frame
        seq = idx * 10
        raw_dest = f"{raw_prefix}/{seq:06d}.jpg"
        ann_dest = f"{ann_prefix}/{seq:06d}.jpg"
        raw_src = _resolve(frame.get("og_file") or frame.get("inference_image"),
                           frame, kind="frames")
        ann_src = _resolve(frame.get("inference_image") or frame.get("og_file"),
                           frame, kind="predict")
        raw_url = _copy(raw_src, raw_dest) if raw_src else None
        ann_url = _copy(ann_src, ann_dest) if ann_src else None
        return idx, raw_url, ann_url

    raw_ok = ann_ok = 0
    with ThreadPoolExecutor(max_workers=32) as pool:
        futs = [pool.submit(_copy_pair, (i, f)) for i, f in enumerate(frames)]
        for fut in as_completed(futs):
            idx, raw_url, ann_url = fut.result()
            if raw_url:
                frames[idx]["og_file"] = raw_url; raw_ok += 1
            if ann_url:
                frames[idx]["inference_image"] = ann_url; ann_ok += 1

    db.inference_data.update_one(
        {"_id": inf["_id"]},
        {"$set": {"data.frame_list_data": frames}},
    )
    log.info("[merged] %d raw + %d annotated copied; URLs rewritten",
             raw_ok, ann_ok)


def _list_merged_frame_blobs(road_id: str, kind: str, merged_tag: str) -> list[str]:
    prefix = f"{PROCESSED_PREFIX}/{road_id}/merged_{kind}_{merged_tag}/"
    return sorted(b.name for b in gcs_client().list_blobs(GCS_BUCKET, prefix=prefix)
                  if b.name.lower().endswith((".jpg", ".jpeg", ".png")))


def _build_one_video(blob_names: list[str], out_blob: str, *,
                     fps: int = 1, label: str) -> str | None:
    """Download merged frames, ffmpeg-stitch into MP4 (1 fps H.264 yuv420p),
    upload to GCS. Returns public URL or None.

    Encoder: libx264 with `-preset ultrafast -crf 28`. At 1 fps preview
    rate the visual quality is indistinguishable from `medium` but the
    encode is ~5–10× faster (~10s vs ~140s for a 1338-frame road).
    """
    if not blob_names:
        log.warning("[video][%s] no frames found", label); return None
    cli = gcs_client(); bucket = cli.bucket(GCS_BUCKET)
    with tempfile.TemporaryDirectory(prefix="v2_video_") as td:
        local_paths: list[str] = []
        def _dl(idx_name):
            i, name = idx_name
            local = Path(td) / f"{i:06d}.jpg"
            try:
                bucket.blob(name).download_to_filename(str(local))
                return str(local)
            except Exception:
                return None
        with ThreadPoolExecutor(max_workers=32) as pool:
            for p in pool.map(_dl, enumerate(blob_names)):
                if p:
                    local_paths.append(p)
        if not local_paths:
            log.warning("[video][%s] all downloads failed", label); return None
        log.info("[video][%s] downloaded %d → ffmpeg", label, len(local_paths))
        out_local = Path(td) / "out.mp4"
        cmd = [
            "ffmpeg", "-y",
            "-framerate", str(fps),
            "-i", str(Path(td) / "%06d.jpg"),
            "-c:v", "libx264", "-pix_fmt", "yuv420p",
            "-preset", "ultrafast", "-crf", "28",
            "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
            str(out_local),
        ]
        try:
            subprocess.run(cmd, check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            log.warning("[video][%s] ffmpeg failed: %s", label, e); return None
        bucket.blob(out_blob).upload_from_filename(str(out_local),
                                                   content_type="video/mp4")
        url = f"https://storage.googleapis.com/{GCS_BUCKET}/{out_blob}"
        log.info("[video][%s] uploaded → %s (%d bytes)",
                 label, url, out_local.stat().st_size)
        return url


def build_consolidated_videos(road_id: str, uid: str, merged_tag: str,
                              fps: int = 1) -> dict[str, str]:
    """Stitch raw + annotated videos from merged frame folders into
    processed-data/<road>/videos/."""
    out: dict[str, str] = {}
    for kind, label, suffix in (("frames", "raw", "raw"),
                                ("predict", "annotated", "annotated")):
        blobs = _list_merged_frame_blobs(road_id, kind, merged_tag)
        out_blob = f"{PROCESSED_PREFIX}/{road_id}/videos/{uid}_{suffix}.mp4"
        url = _build_one_video(blobs, out_blob, fps=fps, label=label)
        if url:
            out[label] = url
    return out


def fast_finalize_frames_and_videos(
    road_id:    str,
    uid:        str,
    merged_tag: str,
    fps:        int = 1,
) -> dict[str, str]:
    """
    Fast-path replacement for build_merged_frames_folder + build_consolidated_videos.
    Skips the merged_frames_<tag>/ + merged_predict_<tag>/ blob-copy loop
    entirely (the slow part — ~135s for a 1338-frame road) and instead:

      1. Stamps each frame's og_file / inference_image URL on the
         inference_data doc to point AT the per-UUID
         annotated_frames/{frames,predict}/ paths directly. The dashboard's
         frame slider already reads URLs from the doc — no copy needed.

      2. Builds the consolidated raw + annotated videos by streaming
         frames from per-UUID paths in chainage order into a local /tmp
         dir, ffmpeg-stitching, uploading the MP4. Same end product as
         the old path, half the GCS round-trips (no per-frame upload).

    Net savings on a typical 1338-frame road: ~120s.
    """
    db = MongoClient(MONGO_URI)["roadvision"]
    inf = db.inference_data.find_one({"uid": uid})
    if not inf:
        log.warning("[fast-finalize] no inference_data for uid=%s", uid)
        return {}
    frames = (inf.get("data") or {}).get("frame_list_data") or []
    if not frames:
        return {}

    # --- 1) URL stamping ----------------------------------------------------
    # Each frame's og_file / inference_image is a relative path like
    # "frame_data/frames/frame_00000.jpg" stamped by Phase B. Resolve to
    # the actual GCS public URL under annotated_frames/{kind}/.
    def _resolve(rel: str | None, kind: str, frame: dict) -> str | None:
        if not rel:
            return None
        if rel.startswith(("http", "gs://")):
            return rel
        seg_uuid = frame.get("_uuid") or ""
        if not seg_uuid:
            return None
        return (f"https://storage.googleapis.com/{GCS_BUCKET}/"
                f"{PROCESSED_PREFIX}/{road_id}/{seg_uuid}/"
                f"annotated_frames/{kind}/{Path(rel).name}")

    for f in frames:
        raw_url = _resolve(f.get("og_file") or f.get("inference_image"),
                           "frames", f)
        ann_url = _resolve(f.get("inference_image") or f.get("og_file"),
                           "predict", f)
        if raw_url:
            f["og_file"] = raw_url
        if ann_url:
            f["inference_image"] = ann_url
    db.inference_data.update_one(
        {"_id": inf["_id"]},
        {"$set": {"data.frame_list_data": frames}},
    )
    log.info("[fast-finalize] %d frame URLs stamped (no copy)", len(frames))

    # --- 2) Build videos by streaming per-UUID frames in chainage order ----
    cli = gcs_client()
    out: dict[str, str] = {}
    for kind, label, suffix in (("frames", "raw", "raw"),
                                ("predict", "annotated", "annotated")):
        # Build the source URL list from the merged frames in their final
        # (chainage-sorted) order.
        urls: list[tuple[str, str]] = []
        for f in frames:
            src = (f.get("og_file") if kind == "frames"
                   else f.get("inference_image"))
            sp = _split_gcs_url(src) if src else None
            if sp:
                urls.append(sp)
        if not urls:
            log.warning("[fast-finalize][%s] no source URLs", label); continue

        with tempfile.TemporaryDirectory(prefix="v2_fastvideo_") as td:
            local_paths: list[str] = []

            def _dl(idx_url):
                i, (b, p) = idx_url
                local = Path(td) / f"{i:06d}.jpg"
                try:
                    cli.bucket(b).blob(p).download_to_filename(str(local))
                    return str(local)
                except Exception:
                    return None
            with ThreadPoolExecutor(max_workers=64) as pool:
                for p in pool.map(_dl, enumerate(urls)):
                    if p:
                        local_paths.append(p)
            if not local_paths:
                log.warning("[fast-finalize][%s] all downloads failed", label)
                continue
            log.info("[fast-finalize][%s] downloaded %d → ffmpeg",
                     label, len(local_paths))

            out_local = Path(td) / "out.mp4"
            # ultrafast preset + CRF 28: ~5–10× faster encode than `medium`
            # with imperceptible quality loss at 1 fps preview rate. Output
            # at native source fps (no 30 fps duplication) — file is 1/30
            # the size and the dashboard's HTML5 player handles it fine.
            cmd = [
                "ffmpeg", "-y",
                "-framerate", str(fps),
                "-i", str(Path(td) / "%06d.jpg"),
                "-c:v", "libx264", "-pix_fmt", "yuv420p",
                "-preset", "ultrafast", "-crf", "28",
                "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
                str(out_local),
            ]
            try:
                subprocess.run(cmd, check=True,
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE)
            except (subprocess.CalledProcessError, FileNotFoundError) as e:
                log.warning("[fast-finalize][%s] ffmpeg failed: %s", label, e)
                continue
            out_blob = f"{PROCESSED_PREFIX}/{road_id}/videos/{uid}_{suffix}.mp4"
            cli.bucket(GCS_BUCKET).blob(out_blob).upload_from_filename(
                str(out_local), content_type="video/mp4")
            out[label] = f"https://storage.googleapis.com/{GCS_BUCKET}/{out_blob}"
            log.info("[fast-finalize][%s] uploaded → %s",
                     label, out[label])
    return out


def stamp_video_urls(uid: str, urls: dict[str, str]) -> None:
    """Mirror IBI's URL placement on the inference doc:
        data.video_url_raw       = urls['raw']
        data.video_url_annotated = urls['annotated']
        video_url     (top-level) = annotated     (dashboard primary)
        video_url_rhs (top-level) = raw           (right-hand viewer)
    """
    db = MongoClient(MONGO_URI)["roadvision"]
    update: dict[str, str] = {}
    if urls.get("raw"):
        update["data.video_url_raw"] = urls["raw"]
        update["video_url_rhs"]      = urls["raw"]
    if urls.get("annotated"):
        update["data.video_url_annotated"] = urls["annotated"]
        update["video_url"]                = urls["annotated"]
    if not update:
        return
    db.inference_data.update_one({"uid": uid}, {"$set": update})
    log.info("[video] URLs stamped on inference_data.uid=%s", uid)


# ─────────────────────────────────────────────────────────────────────────────
# Section 15 — Phase C consolidation + Mongo upserts
# ─────────────────────────────────────────────────────────────────────────────
def consolidate_and_rebuild(
    *,
    road_id:           str,
    uid_suffix:        str,
    organization:      str,
    city:              str,
    project_title:     str,
    start_addr:        str,
    end_addr:          str,
    severity_override: dict | None = None,
    ls_lookup:         dict[str, list[dict]] | None = None,
    uuid_to_mp4:       dict[str, str] | None = None,
    merged_tag:        str | None = None,
    polyline:          list[tuple[float, float]] | None = None,
    cumdist:           list[float] | None = None,
    kml_path:          str | None = None,
) -> str:
    """
    Phase C, end to end:
      1. Load every annotation_segments doc for road_id.
      2. Concatenate frame_list_data; tag each frame with its source UUID
         + source MP4 (so the merged-frames copier can resolve relative
         paths and so we can fall back to MP4-based sort when no KML).
      3. Sort merged frames into surveyor's true along-road order.
         Strategies (in order of preference):
           a) project each frame onto the KML polyline → sort by along-
              road chainage. Robust to videos recorded out of spatial
              sequence (the typical case for multi-segment surveys).
           b) sort by source MP4 basename + per-video frame index.
              Fine for single-video roads or roads where MP4 filename
              order matches driving order.
      4. Re-stamp chainage_km cumulatively across the sorted sequence
         so it's monotonic from 0 → road_length.
      5. Optional: replace inference_info per frame with LS-sourced
         bboxes (when --ls-export-dir was supplied).
      6. Re-tag severity per IBI Guideline (or org override).
      7. Build report_1_key, report_2_key, dashboard_df_csv,
         pie_chart2 (per-frame-unique counting throughout).
      8. Build chainage_report.csv (with address tail).
      9. Upload combined report_{1..4}.csv + chainage + result.json
         to processed-data/<road>/<uid>/.
     10. Upsert inference_data + video_upload.
    Returns the consolidated uid.
    """
    db = MongoClient(MONGO_URI)["roadvision"]
    segments = list(db.annotation_segments.find({"road_id": road_id}))
    if not segments:
        raise SystemExit(f"No annotation_segments for road_id={road_id}")
    log.info("[Phase C] loaded %d annotation_segments for %s", len(segments), road_id)

    # ── 1+2. Merge frames; stamp UUID + MP4 source on every frame. ────────
    merged_frames: list[dict] = []
    total_frames = 0
    weighted_rating = 0.0
    total_road_length = 0.0
    for seg in segments:
        frames = seg.get("frame_list_data") or []
        if not frames:
            continue
        seg_uuid = seg.get("uuid") or ""
        # Resolve the source MP4 for this segment in priority order:
        # explicit V2 mapping → segment's own stamped source_mp4 → segment _id
        src_mp4 = ""
        if uuid_to_mp4 and seg_uuid in uuid_to_mp4:
            src_mp4 = uuid_to_mp4[seg_uuid]
        elif seg.get("source_mp4"):
            src_mp4 = seg["source_mp4"]
        elif seg.get("gcs_uri"):
            src_mp4 = seg["gcs_uri"]
        seg_order_key = src_mp4 or str(seg.get("_id", ""))
        for f in frames:
            f.setdefault("_uuid", seg_uuid)
            f.setdefault("_source_mp4", src_mp4)
            f.setdefault("_seg_order", seg_order_key)
        merged_frames.extend(frames)
        total_frames += seg.get("total_frames") or len(frames)
        total_road_length += float(seg.get("road_length_km") or 0)
        weighted_rating += float(seg.get("road_rating") or 0) * (seg.get("total_frames") or len(frames))
    if not merged_frames:
        raise SystemExit(f"All segments empty for {road_id}")

    # ── 3. Sort along-road. KML projection wins when available. ──────────
    def _frame_index(f):
        og = f.get("og_file") or ""
        m = re.search(r"frame[_-]?(\d+)", og)
        return int(m.group(1)) if m else 0

    if polyline and cumdist:
        log.info("[Phase C] sorting %d frames by KML-projected chainage",
                 len(merged_frames))
        for f in merged_frames:
            lat, lon = f.get("latitude"), f.get("longitude")
            if lat is None or lon is None:
                f["_kml_chainage_m"] = float("inf")
                continue
            _, _perp, ch_m = project_to_polyline((lat, lon), polyline, cumdist)
            f["_kml_chainage_m"] = ch_m
        def _sort_key(f):
            return (f.get("_kml_chainage_m", float("inf")),
                    f.get("_seg_order", ""),
                    _frame_index(f))
    else:
        def _sort_key(f):
            return (f.get("_seg_order", ""),
                    _frame_index(f),
                    float(f.get("timeElapsed") or 0))
    merged_frames.sort(key=_sort_key)

    # ── 4. Re-stamp chainage_km cumulatively across the sorted merge. ─────
    cum_km = 0.0; prev = None
    for f in merged_frames:
        cur = (f.get("latitude"), f.get("longitude"))
        if prev is not None and None not in cur and None not in prev:
            cum_km += _haversine_m(prev, cur) / 1000.0
        f["chainage_km"] = round(cum_km, 3)
        prev = cur if None not in cur else prev

    road_length_km = round(cum_km if cum_km > 0 else total_road_length, 2)
    road_rating = round((weighted_rating / total_frames) if total_frames else 0.0, 2)
    log.info("[Phase C] merged frames=%d  road_length=%.2f km  road_rating=%.2f",
             len(merged_frames), road_length_km, road_rating)

    # ── 5. Optional LS override BEFORE severity retag. ────────────────────
    if ls_lookup:
        apply_ls_to_frames(merged_frames, ls_lookup)

    # ── 6. Re-tag severity per IBI Guideline (or override). ───────────────
    sev_totals = {"high": 0, "medium": 0, "low": 0, "none": 0}
    for f in merged_frames:
        worst = "none"
        for inf in (f.get("inference_info") or []):
            sev = severity_for(inf.get("label", ""), severity_override)
            inf["severity"] = sev
            sev_totals[sev] += 1
            if _SEV_RANK[sev] > _SEV_RANK[worst]:
                worst = sev
        f["defect_state"] = worst
    log.info("[Phase C] severity totals: %s", sev_totals)

    # ── 7. Reports + 8. chainage CSV ──────────────────────────────────────
    report_1, total_defects = build_report_1_key(
        merged_frames, road_length_km, road_rating, start_addr, end_addr)
    report_2, labels = build_report_2_key(merged_frames)
    report_3 = build_report_3_key(merged_frames)
    report_4 = build_report_4_key(merged_frames)
    pie_chart2 = build_pie_chart2(merged_frames)
    chainage_csv = build_chainage_report_csv(merged_frames, start_addr, end_addr)

    uid = f"{road_id}_{uid_suffix}"

    # ── 9. Combined reports → GCS, plus the merged result.json. ───────────
    upload_combined_reports(
        road_id=road_id, uid=uid, segments=segments,
        report_1=report_1, report_2=report_2,
        report_3=report_3, report_4=report_4,
        chainage_csv=chainage_csv,
    )
    log.info("[Phase C] reports uploaded; report_2 = %d labels", len(labels))

    merged_result = build_merged_result_json(merged_frames,
                                             merged_tag or uid_suffix)
    result_blob = f"{PROCESSED_PREFIX}/{road_id}/{uid}/result.json"
    gcs_upload_text(result_blob, json.dumps(merged_result, default=str),
                    content_type="application/json")
    log.info("[Phase C] merged result.json → gs://%s/%s  (%d images, %d annotations)",
             GCS_BUCKET, result_blob,
             len(merged_result["images"]), len(merged_result["annotations"]))

    # ── 10. Upsert inference_data + video_upload. ─────────────────────────
    inference_doc = {
        "uid":                   uid,
        "road_id":               road_id,
        "organization":          organization,
        "city":                  city,
        "project_title_display": project_title,
        "start_add":             {"add": start_addr},
        "end_add":               {"add": end_addr},
        "showInference":         True,
        "is_deleted":            False,
        "created":               _dt.datetime.utcnow(),
        "meta_data":             {},
        "plot_data":             {"plots": {"pie_chart2": pie_chart2}},
        "data": {
            "road_id":            road_id,
            "road_length":        road_length_km,
            "road_rating":        road_rating,
            "data_submitted":     _dt.date.today().strftime("%d-%m-%Y"),
            "total_defects":      total_defects,
            "frame_list_data":    merged_frames,
            "category_information": {},
            "report_1_key":       report_1,
            "report_2_key":       report_2,
            "dashboard_df_csv":   report_2,        # IBI keeps these in sync
            "report_3_key":       report_3,
            "report_4_key":       report_4,
            "CODEBUILD_BUILD_ID":     uid,
            "NEW_CODEBUILD_BUILD_ID": uid,
            "project_title_display":  project_title,
        },
    }
    db.inference_data.update_one(
        {"uid": uid, "organization": organization},
        {"$set": inference_doc},
        upsert=True,
    )
    log.info("[Phase C] inference_data upserted  uid=%s", uid)

    db.video_upload.update_one(
        {"video_uid": uid},
        {"$set": {
            "video_uid":          uid,
            "road_id":            road_id,
            "organization":       organization,
            "city":               city,
            "mobile_user_name":   "pipeline_v2",
            "mobile_user_email":  "pipeline@roadvision.ai",
            "project_title":      project_title,
            "status":             "Completed",
            "meta_data":          {"source": "pipeline_v2",
                                   "uuids": [s.get("uuid") for s in segments]},
            "created":            _dt.datetime.utcnow(),
        }},
        upsert=True,
    )
    log.info("[Phase C] video_upload upserted    uid=%s", uid)

    # ── 11. roads + RoadData (V1 dashboard schemas) ───────────────────────
    # Two more collections the dashboard reads:
    #   • roads     — V1's per-road shape with full_route GeoJSON +
    #                 surveys[]. Used by the Road Surveys page.
    #   • RoadData  — Kota/IBI-style start/end metadata + authority +
    #                 web_user scoping. Read by the dashboard's road
    #                 list filter.
    # Both indexed by road_id. We populate from the merged frames so the
    # full_route follows the actual surveyed path (post-KML projection),
    # not just two endpoints.
    full_route_coords = [
        [f.get("longitude"), f.get("latitude")]
        for f in merged_frames
        if f.get("longitude") is not None and f.get("latitude") is not None
    ]
    first = merged_frames[0]
    last  = merged_frames[-1]
    survey_date = _dt.datetime.utcnow()

    db.roads.update_one(
        {"road_id": road_id},
        {"$set": {
            "road_id":   road_id,
            "road_name": project_title,
            "road_type": "Urban Street",
            "full_route": {
                "type": "LineString",
                "coordinates": full_route_coords,
            },
            # Replace the surveys[] array entirely on every Phase C re-run
            # — V2's consolidated record IS the survey, so we don't try to
            # keep history (which would drift on each rebuild). Use $set
            # rather than $push so re-runs are idempotent.
            "surveys": [{
                "survey_no":       1,
                "total_length_km": road_length_km,
                "total_frames":    len(merged_frames),
                "road_rating":     road_rating,
                "survey_date":     survey_date,
            }],
        }},
        upsert=True,
    )
    log.info("[Phase C] roads upserted          road_id=%s", road_id)

    # Look up the org's user record to populate organization_id +
    # web_user_email on RoadData (the dashboard filters by these).
    user_doc = db.web_user.find_one({"organization": organization}) or {}

    # Reverse-geocode start/end when the caller passed a placeholder
    # (default for --all-roads watch since per-road addresses come from
    # _road_meta.json, which often lacks them). Cached, throttled.
    start_lat, start_lng = first.get("latitude"), first.get("longitude")
    end_lat,   end_lng   = last.get("latitude"),  last.get("longitude")
    if _is_placeholder_address(start_addr):
        geocoded = reverse_geocode(start_lat, start_lng)
        if geocoded:
            start_addr = geocoded
    if _is_placeholder_address(end_addr):
        geocoded = reverse_geocode(end_lat, end_lng)
        if geocoded:
            end_addr = geocoded

    # via_points from the KML <Placemark><Point> entries (named waypoints
    # the surveyor marked along the road). Empty when no KML is present.
    via_points: list[dict] = []
    if kml_path:
        try:
            via_points = parse_kml_via_points(kml_path)
        except Exception as e:
            log.warning("[Phase C] KML via_points parse failed: %s", e)

    db.RoadData.update_one(
        {"road_id": road_id, "organization": organization},
        {"$set": {
            "road_id":           road_id,
            "road_name":         project_title,
            "start_latitude":    start_lat,
            "start_longitude":   start_lng,
            "starting_address":  start_addr,
            "end_latitude":      end_lat,
            "end_longitude":     end_lng,
            "ending_address":    end_addr,
            "via_points":        via_points,
            "concerned_officer": "",
            "authority":         "",
            "road_type":         "Urban Street",
            "organization":      organization,
            "organization_id":   user_doc.get("organization_id", ""),
            "web_user_email":    user_doc.get("email", ""),
            "road_length":       road_length_km,
            "material":          "Bituminous",
            "last_updated":      survey_date,
        }},
        upsert=True,
    )
    log.info("[Phase C] RoadData upserted       road_id=%s  via_points=%d",
             road_id, len(via_points))
    # Echo the resolved addresses back to the inference doc so the
    # dashboard sees the geocoded values too.
    if not _is_placeholder_address(start_addr) or not _is_placeholder_address(end_addr):
        db.inference_data.update_one(
            {"uid": uid, "organization": organization},
            {"$set": {
                "start_add": {"add": start_addr},
                "end_add":   {"add": end_addr},
            }},
        )
    return uid


# ─────────────────────────────────────────────────────────────────────────────
# Section 16 — Watch mode: persistent tracker + GCS-poll loop
# ─────────────────────────────────────────────────────────────────────────────
# Mirrors trigger_builds.py's --watch contract for surveys where MP4s are
# uploaded to GCS incrementally over time (live streaming surveys, slow
# WAN uploads, batched uploads from multiple field crews). The orchestrator
# polls the road's GCS prefix, debounces upload completion via a settle
# window, accumulates videos until --batch-size are queued (or no more are
# arriving), processes them, then re-consolidates Phase C so the dashboard
# updates after each batch. Runs forever; Ctrl+C exits cleanly.
#
# State lives in `processing_tracker.json` next to this script — survives
# process restarts so an interrupted V2 picks up where it left off without
# re-processing already-done videos.
TRACKER_FILE = Path(__file__).resolve().parent / "processing_tracker.json"


def load_tracker() -> dict:
    """Read the on-disk tracker. Empty dict on first run."""
    if TRACKER_FILE.exists():
        try:
            with open(TRACKER_FILE) as f:
                return json.load(f)
        except Exception as e:
            log.warning("[watch] tracker file unreadable (%s) — starting fresh", e)
    return {}


def save_tracker(tracker: dict) -> None:
    """Atomic-ish write — temp file + rename, so a crash mid-write doesn't
    leave a corrupted tracker."""
    tmp = TRACKER_FILE.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        json.dump(tracker, f, indent=2, default=str)
    tmp.replace(TRACKER_FILE)


def is_video_tracked(tracker: dict, road_id: str, fname: str) -> bool:
    """A video is considered tracked once it's been queued or finished —
    skipped on subsequent polls regardless of success or failure."""
    return fname in tracker.get(road_id, {})


def track_video(tracker: dict, road_id: str, fname: str, run_id: str,
                status: str = "processing") -> None:
    """Record a video being picked up. Persists immediately so a crash
    between this call and process_one_video() doesn't lose state."""
    tracker.setdefault(road_id, {})[fname] = {
        "uuid":     run_id,
        "status":   status,
        "started":  _dt.datetime.utcnow().isoformat(),
        "finished": None,
    }
    save_tracker(tracker)


def update_tracker_status(tracker: dict, road_id: str, fname: str,
                          status: str) -> None:
    """Mark a tracked video done/failed."""
    if road_id in tracker and fname in tracker[road_id]:
        tracker[road_id][fname]["status"] = status
        tracker[road_id][fname]["finished"] = _dt.datetime.utcnow().isoformat()
        save_tracker(tracker)


def _list_new_pairs(prefix: str, road_id: str, tracker: dict
                    ) -> list[tuple[str, str]]:
    """List (mp4, gpx) pairs in the GCS prefix that aren't tracked yet."""
    pairs = gcs_list_pairs(prefix)
    return [(mp4, gpx) for mp4, gpx in pairs
            if not is_video_tracked(tracker, road_id, Path(mp4).name)]


def _process_one_with_tracker(
    *,
    mp4_blob: str,
    gpx_blob: str,
    road_id:  str,
    workdir_root: Path,
    tracker:  dict,
    severity_override: dict | None,
    uuid_to_mp4: dict[str, str],
) -> bool:
    """Phase B for one video, with tracker bookkeeping. Returns True on success."""
    fname  = Path(mp4_blob).name
    run_id = str(uuid_mod.uuid4())
    track_video(tracker, road_id, fname, run_id, status="processing")
    workdir = workdir_root / run_id
    try:
        res = process_one_video(
            mp4_blob=mp4_blob, gpx_blob=gpx_blob,
            road_id=road_id, run_id=run_id, workdir=workdir,
            severity_override=severity_override,
        )
        if res:
            update_tracker_status(tracker, road_id, fname, "done")
            uuid_to_mp4[run_id] = mp4_blob
            return True
        update_tracker_status(tracker, road_id, fname, "failed")
        return False
    except Exception as e:
        log.exception("[watch] %s failed: %s", fname, e)
        update_tracker_status(tracker, road_id, fname, "failed")
        return False
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def watch_mode(
    args: argparse.Namespace,
    severity_override: dict | None,
    ls_lookup: dict | None,
    poly: list[tuple[float, float]] | None,
    cumdist: list[float] | None,
    kml_path: str | None = None,
) -> None:
    """
    Long-running poll loop. Behaviour mirrors trigger_builds.py --watch:

      every --watch-interval seconds:
        scan GCS prefix → drop already-tracked videos → list "new"
        if new is empty:           continue (just keep watching)
        if new arrived:            sleep --settle-time so any in-progress
                                    uploads can finalise → re-scan
        if new ≥ --batch-size:     dispatch the first batch_size as a batch
        if new < --batch-size:     wait one more --settle-time; if count
                                    didn't grow, dispatch what we have
                                    (avoids stalling when uploads stop)

      For each dispatched batch:
        1. Phase A KML projection (if KML available) on the new pairs
        2. Per-video Phase B (sequential, one YOLO model loaded once)
        3. Phase C re-consolidation across ALL annotation_segments —
           dashboard sees the latest state after every batch
        4. merged_frames + consolidated videos rebuilt (skip with
           --skip-merged-frames / --skip-videos)

    Ctrl+C exits cleanly — the tracker file holds the state, so a restart
    resumes from where the loop left off.
    """
    prefix = args.gcs_prefix.rstrip("/") + "/"
    workdir_root = Path(args.workdir) / args.road_id
    workdir_root.mkdir(parents=True, exist_ok=True)
    merged_tag = args.merged_tag or args.uid_suffix

    # Per-uid log file — opens for the lifetime of this watch session;
    # every batch's events plus parallel worker output append to it.
    log_path = road_log_path(Path(args.workdir), args.road_id)
    file_handler = attach_road_log_handler(log_path)

    log.info("─── WATCH MODE  road_id=%s ───", args.road_id)
    log.info("  prefix       : gs://%s/%s", GCS_BUCKET, prefix)
    log.info("  batch_size   : %d", args.batch_size)
    log.info("  poll every   : %ds", args.watch_interval)
    log.info("  settle time  : %ds", args.settle_time)
    log.info("  tracker file : %s", TRACKER_FILE)
    log.info("  log file     : %s", log_path)
    log.info("  KML present  : %s", "yes" if poly else "no")
    log.info("  Ctrl+C to stop. Tracker survives restarts.")

    # Load YOLO once for the lifetime of the watch process
    load_yolo_model(args.model_weights)

    tracker = load_tracker()
    uuid_to_mp4: dict[str, str] = {
        meta["uuid"]: f"{prefix}{fname}"
        for road_entries in [tracker.get(args.road_id, {})]
        for fname, meta in road_entries.items() if meta.get("uuid")
    }
    batch_num = 0

    try:
        while True:
            # 1) Scan for fresh pairs
            new_pairs = _list_new_pairs(prefix, args.road_id, tracker)
            if not new_pairs:
                log.info("[watch] idle — %d videos tracked, none new (next scan in %ds)",
                         len(tracker.get(args.road_id, {})), args.watch_interval)
                time.sleep(args.watch_interval)
                continue

            # 2) Settle wait — let in-progress uploads finalise
            log.info("[watch] %d new video(s) — settling for %ds",
                     len(new_pairs), args.settle_time)
            time.sleep(args.settle_time)
            new_pairs = _list_new_pairs(prefix, args.road_id, tracker)
            if not new_pairs:
                continue

            # 3) Decide batch size
            if len(new_pairs) >= args.batch_size:
                batch = new_pairs[: args.batch_size]
            else:
                # one more settle to see if more arrive before processing partial
                prev_count = len(new_pairs)
                log.info("[watch] %d found (need %d) — waiting %ds for more",
                         prev_count, args.batch_size, args.settle_time)
                time.sleep(args.settle_time)
                new_pairs = _list_new_pairs(prefix, args.road_id, tracker)
                if len(new_pairs) == prev_count:
                    log.info("[watch] no further uploads — processing %d", prev_count)
                    batch = new_pairs
                elif len(new_pairs) >= args.batch_size:
                    batch = new_pairs[: args.batch_size]
                else:
                    batch = new_pairs

            batch_num += 1
            _log_section(f"BATCH #{batch_num}  ({len(batch)} videos)")
            for mp4, _ in batch:
                log.info("    📋 %s", Path(mp4).name)

            # 4) Phase A (per-batch — only project the new pairs)
            if poly and cumdist:
                batch = phase_a_project_all_gpx(batch, poly, cumdist, args.max_perp_m)

            # 5) Phase B — honour --parallel via dispatch_phase_b. The
            #    tracker is updated to "processing" BEFORE dispatch (so a
            #    crash mid-batch doesn't lose the queue), then bumped to
            #    done/failed via on_done_callback as each worker finishes.
            jobs = []
            for mp4_blob, gpx_blob in batch:
                fname  = Path(mp4_blob).name
                run_id = str(uuid_mod.uuid4())
                track_video(tracker, args.road_id, fname, run_id,
                            status="processing")
                uuid_to_mp4[run_id] = mp4_blob
                jobs.append({
                    "mp4_blob": mp4_blob, "gpx_blob": gpx_blob,
                    "road_id":  args.road_id, "run_id": run_id,
                    "workdir":  workdir_root / run_id,
                    "_fname":   fname,
                })

            def _on_done(j, res):
                update_tracker_status(
                    tracker, args.road_id, j["_fname"],
                    "done" if res else "failed",
                )

            ok = dispatch_phase_b(
                jobs=jobs, parallel=args.parallel,
                model_weights=args.model_weights,
                severity_override=severity_override,
                on_done_callback=_on_done,
                fast=args.fast,
                log_file_path=str(log_path),
            )
            log.info("[watch] batch #%d Phase B: %d/%d succeeded",
                     batch_num, ok, len(batch))
            if ok == 0:
                log.warning("[watch] batch had 0 successes — skipping Phase C")
                continue

            # 6) Phase C re-consolidation across ALL segments (incl. older ones)
            uid = consolidate_and_rebuild(
                road_id=args.road_id,
                uid_suffix=args.uid_suffix,
                organization=args.organization,
                city=args.city,
                project_title=args.project_title,
                start_addr=args.start_address,
                end_addr=args.end_address,
                severity_override=severity_override,
                ls_lookup=ls_lookup,
                uuid_to_mp4=uuid_to_mp4,
                merged_tag=merged_tag,
                polyline=poly,
                cumdist=cumdist,
                kml_path=kml_path,
            )

            # 7) Merged frames + consolidated videos (incremental rebuild)
            if not args.skip_merged_frames:
                if args.fast:
                    urls = fast_finalize_frames_and_videos(
                        args.road_id, uid, merged_tag, fps=args.video_fps)
                    if urls:
                        stamp_video_urls(uid, urls)
                else:
                    build_merged_frames_folder(args.road_id, uid, merged_tag)
                    if not args.skip_videos:
                        urls = build_consolidated_videos(args.road_id, uid, merged_tag,
                                                         fps=args.video_fps)
                        if urls:
                            stamp_video_urls(uid, urls)

            log.info("[watch] batch #%d done — resuming watch", batch_num)
    except KeyboardInterrupt:
        log.info("[watch] Ctrl+C — exiting cleanly. Tracker preserved at %s",
                 TRACKER_FILE)
    finally:
        detach_log_handler(file_handler)


# ─────────────────────────────────────────────────────────────────────────────
# Multi-road watch (mirrors trigger_builds.py with no --road_id)
# ─────────────────────────────────────────────────────────────────────────────
# When `--all-roads` is set, V2 scans the parent prefix (default
# `video-processing-pipelines-data/`) for every road subfolder, accumulates
# new (mp4, gpx) pairs ACROSS roads into a single batch, dispatches the
# whole batch through one shared --parallel pool, then runs Phase C once
# per road that had a successful video. Same tracker file as the per-road
# watch — entries are namespaced by road_id, so the two modes can co-exist
# without collision.
#
# Per-road metadata (project_title, addresses) is read from each folder's
# _road_meta.json if present (created by setup_varanasi_roads.py). When
# absent, falls back to the road_id as project title and "TBD" addresses
# — fields the user can override later via direct Mongo update.
def list_road_folders(root_prefix: str) -> dict[str, str]:
    """
    Return {road_id: folder_prefix} for every immediate subfolder of
    `root_prefix` whose name matches the R\\d{6} convention. GCS doesn't
    have real folders — we discover them via the `delimiter='/'` listing
    which surfaces the immediate prefixes of objects within the root.
    """
    cli = gcs_client()
    found: dict[str, str] = {}
    iterator = cli.list_blobs(GCS_BUCKET, prefix=root_prefix, delimiter="/")
    for page in iterator.pages:
        for prefix in page.prefixes:
            road_id = prefix.rstrip("/").split("/")[-1]
            if re.match(r"^R\d{6}$", road_id):
                found[road_id] = prefix
    return found


def load_road_meta_from_gcs(prefix: str, road_id: str) -> dict:
    """
    Pull the optional `_road_meta.json` setup_varanasi_roads.py uploads
    next to a road's MP4s. Used to fill --project-title / --start-address
    / --end-address per-road in --all-roads mode. Missing fields default
    to road_id / "TBD".
    """
    try:
        text = gcs_download_text(f"{prefix}_road_meta.json")
        meta = json.loads(text)
        return {
            "project_title": meta.get("folder") or meta.get("project_title") or road_id,
            "start_address": meta.get("start_address")
                              or meta.get("starting_address")
                              or "TBD",
            "end_address":   meta.get("end_address")
                              or meta.get("ending_address")
                              or "TBD",
        }
    except Exception:
        return {"project_title": road_id, "start_address": "TBD", "end_address": "TBD"}


def watch_all_roads_mode(
    args:              argparse.Namespace,
    severity_override: dict | None,
    ls_lookup:         dict | None,
) -> None:
    """
    Multi-road watch loop. Single Python process, shared --parallel pool,
    per-road tracker entries. Mirrors V1's `trigger_builds.py --watch`
    (no --road_id) but with V2's Phase C re-consolidation after every
    batch — so each road's dashboard updates as soon as its videos
    process.

    Decision tree per scan tick:
      • scan ROOT_PREFIX → list every road subfolder
      • for each road → drop already-tracked videos → list "new"
      • flatten across roads → total_new
      • total_new == 0 → log idle, sleep --watch-interval
      • total_new  > 0 → settle --settle-time → re-flatten
                          if  total_new ≥ batch_size → take first batch_size
                          if  total_new < batch_size → settle once more;
                              stable count → process what we have
                              still growing → take batch_size or wait

      For each batch:
        Phase A per-pair → cache KMLs per road, project the GPX once
        Phase B → dispatch_phase_b (shared --parallel pool across roads)
        Phase C → run for each road that had ≥1 successful video; uses
                  the road's _road_meta.json metadata if present
    """
    root_prefix = args.gcs_prefix_root.rstrip("/") + "/"
    workdir_root = Path(args.workdir) / "all_roads"
    workdir_root.mkdir(parents=True, exist_ok=True)
    merged_tag = args.merged_tag or args.uid_suffix

    log.info("─── WATCH ALL ROADS  root=%s ───", root_prefix)
    log.info("  batch_size  : %d", args.batch_size)
    log.info("  parallel    : %d", args.parallel)
    log.info("  poll every  : %ds", args.watch_interval)
    log.info("  settle time : %ds", args.settle_time)
    log.info("  tracker     : %s", TRACKER_FILE)
    log.info("  Ctrl+C to stop. Tracker survives restarts.")

    # Pre-load YOLO once when running serially; --parallel >1 workers
    # load their own.
    if args.parallel <= 1:
        load_yolo_model(args.model_weights)

    tracker = load_tracker()
    # cache: road_id → (poly, cumdist, kml_path) | None — kml_path is also
    # passed into Phase C so RoadData.via_points can be populated.
    kml_cache: dict[str, tuple[list, list, str] | None] = {}
    meta_cache: dict[str, dict] = {}

    def _get_kml(road_id: str, prefix: str):
        if road_id not in kml_cache:
            kml_path = gcs_find_kml(prefix)
            if kml_path:
                poly = parse_kml(kml_path)
                cumdist = precompute_polyline_chainage(poly)
                kml_cache[road_id] = (poly, cumdist, kml_path)
            else:
                kml_cache[road_id] = None
        return kml_cache[road_id]

    def _get_meta(road_id: str, prefix: str):
        if road_id not in meta_cache:
            meta_cache[road_id] = load_road_meta_from_gcs(prefix, road_id)
        return meta_cache[road_id]

    def _scan_all() -> dict[str, list[tuple[str, str, str]]]:
        """Return {road_id: [(mp4, gpx, prefix), ...]} of NEW pairs only."""
        roads = list_road_folders(root_prefix)
        out: dict[str, list[tuple[str, str, str]]] = {}
        for rid, prefix in roads.items():
            new_pairs = _list_new_pairs(prefix, rid, tracker)
            if new_pairs:
                out[rid] = [(mp4, gpx, prefix) for mp4, gpx in new_pairs]
        return out

    batch_num = 0
    try:
        while True:
            # 1) Scan all roads
            new_by_road = _scan_all()
            total_new = sum(len(v) for v in new_by_road.values())
            if total_new == 0:
                tracked = sum(len(v) for v in tracker.values())
                log.info("[watch-all] idle — %d videos tracked across %d road(s); "
                         "next scan in %ds", tracked, len(tracker), args.watch_interval)
                time.sleep(args.watch_interval)
                continue

            # 2) Settle for in-progress uploads
            log.info("[watch-all] %d new video(s) across %d road(s) — settling for %ds",
                     total_new, len(new_by_road), args.settle_time)
            time.sleep(args.settle_time)
            new_by_road = _scan_all()
            total_new = sum(len(v) for v in new_by_road.values())
            if total_new == 0:
                continue

            # 3) Batch decision
            if total_new < args.batch_size:
                prev = total_new
                log.info("[watch-all] %d found (need %d) — waiting %ds for more",
                         prev, args.batch_size, args.settle_time)
                time.sleep(args.settle_time)
                new_by_road = _scan_all()
                total_new = sum(len(v) for v in new_by_road.values())
                if total_new == prev:
                    log.info("[watch-all] uploads stable — processing %d", prev)

            # 4) Flatten + cap at batch_size (round-robin across roads so a
            #    single road's flood can't starve the others)
            flat: list[tuple[str, str, str, str]] = []
            roads_cycle = list(new_by_road.keys())
            road_idx = 0
            while len(flat) < args.batch_size and any(new_by_road.values()):
                rid = roads_cycle[road_idx % len(roads_cycle)]
                if new_by_road[rid]:
                    mp4, gpx, prefix = new_by_road[rid].pop(0)
                    flat.append((rid, mp4, gpx, prefix))
                road_idx += 1
                if road_idx > len(roads_cycle) * args.batch_size:
                    break  # safety
            roads_in_batch = sorted(set(j[0] for j in flat))

            batch_num += 1
            _log_section(f"BATCH #{batch_num}  ({len(flat)} videos across "
                         f"{len(roads_in_batch)} road(s): {', '.join(roads_in_batch)})")
            for rid, mp4, _, _ in flat:
                log.info("    📋 %s  (road %s)", Path(mp4).name, rid)

            # 5) Phase A per pair (cache KML poly/cumdist per road)
            phase_b_jobs = []
            for rid, mp4_blob, gpx_blob, prefix in flat:
                kml = _get_kml(rid, prefix)
                if kml is not None:
                    poly, cumdist = kml
                    try:
                        text = gcs_download_text(gpx_blob)
                        rewritten, _stats = rewrite_gpx_with_kml_projection(
                            text, poly, cumdist, args.max_perp_m)
                        new_gpx = gpx_blob[:-4] + ".kml.gpx"
                        gcs_upload_text(new_gpx, rewritten,
                                        content_type="application/gpx+xml")
                        gpx_blob = new_gpx
                    except Exception as e:
                        log.warning("[Phase A] %s skipped: %s", gpx_blob, e)
                fname  = Path(mp4_blob).name
                run_id = str(uuid_mod.uuid4())
                track_video(tracker, rid, fname, run_id, status="processing")
                phase_b_jobs.append({
                    "mp4_blob": mp4_blob, "gpx_blob": gpx_blob,
                    "road_id":  rid,      "run_id":   run_id,
                    "workdir":  workdir_root / run_id,
                    "_fname":   fname, "_road_id": rid,
                })

            def _on_done(j, res):
                update_tracker_status(tracker, j["_road_id"], j["_fname"],
                                      "done" if res else "failed")

            ok = dispatch_phase_b(
                jobs=phase_b_jobs, parallel=args.parallel,
                model_weights=args.model_weights,
                severity_override=severity_override,
                on_done_callback=_on_done,
                fast=args.fast,
                # Each worker derives its own log path from this root + its
                # job's road_id, so a multi-road batch's parallel workers
                # tee into the right per-uid log file (not one global file).
                log_workdir_root=str(Path(args.workdir)),
            )
            log.info("[watch-all] batch #%d Phase B: %d/%d succeeded",
                     batch_num, ok, len(phase_b_jobs))
            if ok == 0:
                log.warning("[watch-all] batch had 0 successes — skipping Phase C")
                continue

            # 6) Phase C per affected road — per-road log session so all
            #    Phase C events for that road land in its pipeline.log
            for rid in roads_in_batch:
                meta = _get_meta(rid, list_road_folders(root_prefix).get(rid, ""))
                kml = _get_kml(rid, list_road_folders(root_prefix).get(rid, ""))
                if kml:
                    poly, cumdist, kml_path_for_road = kml
                else:
                    poly, cumdist, kml_path_for_road = None, None, None
                try:
                    with road_log_session(Path(args.workdir), rid):
                        uid = consolidate_and_rebuild(
                            road_id=rid,
                            uid_suffix=args.uid_suffix,
                            organization=args.organization,
                            city=args.city,
                            project_title=meta["project_title"],
                            start_addr=meta["start_address"],
                            end_addr=meta["end_address"],
                            severity_override=severity_override,
                            ls_lookup=ls_lookup,
                            merged_tag=merged_tag,
                            polyline=poly, cumdist=cumdist,
                            kml_path=kml_path_for_road,
                        )
                        if not args.skip_merged_frames:
                            if args.fast:
                                urls = fast_finalize_frames_and_videos(
                                    rid, uid, merged_tag, fps=args.video_fps)
                                if urls:
                                    stamp_video_urls(uid, urls)
                            else:
                                build_merged_frames_folder(rid, uid, merged_tag)
                                if not args.skip_videos:
                                    urls = build_consolidated_videos(rid, uid, merged_tag,
                                                                     fps=args.video_fps)
                                    if urls:
                                        stamp_video_urls(uid, urls)
                except Exception as e:
                    log.exception("[watch-all] Phase C for %s failed: %s", rid, e)

            log.info("[watch-all] batch #%d done — resuming watch", batch_num)
    except KeyboardInterrupt:
        log.info("[watch-all] Ctrl+C — exiting cleanly. Tracker preserved at %s",
                 TRACKER_FILE)


# ─────────────────────────────────────────────────────────────────────────────
# Reset — wipe a road's state before re-processing (--reset)
# ─────────────────────────────────────────────────────────────────────────────
# Defends against cross-pollinated re-runs (the bug where stale segments
# from an earlier test under a different GCS prefix carried the same
# road_id and got merged into Phase C, producing impossibly long roads).
# When --reset is passed:
#   1. Mongo: delete every annotation_segments / inference_data /
#      video_upload / roads doc that references the road_id (or uid).
#   2. GCS: delete everything under processed-data/<road_id>/ — per-UUID
#      Phase B output, combined per-uid CSVs, merged_frames_*, videos.
#      Source data (video-processing-pipelines-data/<road_id>/) is NOT
#      touched — that's the input MP4 + GPX which we want to re-process.
#   3. Tracker: drop every entry under road_id so the watch loop will
#      re-pick the videos on its next scan.
# Idempotent — safe to re-run.
def _gcs_delete_prefix(prefix: str) -> int:
    """Parallel-delete every blob under a GCS prefix. Returns count."""
    cli = gcs_client()
    blobs = list(cli.list_blobs(GCS_BUCKET, prefix=prefix))
    if not blobs:
        return 0
    def _del(blob):
        try:
            blob.delete()
            return 1
        except Exception:
            return 0
    with ThreadPoolExecutor(max_workers=32) as pool:
        return sum(pool.map(_del, blobs))


def reset_road_state(
    road_id:      str,
    uid:          str,
    organization: str,
    *,
    tracker:      dict | None = None,
) -> dict:
    """
    Wipe Mongo + GCS + tracker state for a single road. Returns a
    dict of deleted counts keyed by collection / location, so the
    caller can log a summary.
    """
    db = MongoClient(MONGO_URI)["roadvision"]
    counts: dict[str, int] = {}
    counts["annotation_segments"] = db.annotation_segments.delete_many(
        {"road_id": road_id}).deleted_count
    counts["inference_data"]      = db.inference_data.delete_many(
        {"$or": [{"uid": uid, "organization": organization},
                 {"road_id": road_id, "organization": organization}]}
    ).deleted_count
    counts["video_upload"]        = db.video_upload.delete_many(
        {"$or": [{"video_uid": uid}, {"road_id": road_id}]}
    ).deleted_count
    counts["roads"]               = db.roads.delete_many(
        {"road_id": road_id}).deleted_count
    counts["gcs_blobs"]           = _gcs_delete_prefix(
        f"{PROCESSED_PREFIX}/{road_id}/")
    if tracker is not None and road_id in tracker:
        counts["tracker_entries"] = len(tracker[road_id])
        del tracker[road_id]
        save_tracker(tracker)
    log.info("[reset] %s: %s", road_id,
             ", ".join(f"{k}={v}" for k, v in counts.items()))
    return counts


# ─────────────────────────────────────────────────────────────────────────────
# Section 17 — Reprocess (Phase-C-only re-run from the merged result.json)
# ─────────────────────────────────────────────────────────────────────────────
# Mirrors the contract of V1's reprocess_annotations.py, but adapted for V2's
# consolidated layout: instead of operating on a single per-UUID result.json,
# this works on the MERGED COCO result.json that Phase C uploads to
#       processed-data/<road_id>/<uid>/result.json

# Use cases (same as V1's script):
#   1. Annotations were manually corrected in the merged result.json
#      (post-LS round, post-human-review, etc.)
#   2. Severity mappings changed and reports need re-derivation
#   3. Bboxes were adjusted and the merged_predict frames + the
#      consolidated annotated video need to be regenerated

# What this does (NO YOLO, NO frame extraction):
#   1. Download the merged result.json from GCS
#   2. Reconstruct an in-memory frame_list_data from images + annotations
#      (lat/lng/chainage_km/_uuid are stored on each image by Phase C, so
#      no Mongo round-trip is needed)
#   3. Re-tag severity per IBI Guideline (or org override)
#   4. Re-build report_1 / report_2 / dashboard_df_csv / chainage_report.csv
#      / pie_chart2 — IBI-format, per-frame-unique counts
#   5. Re-upload the combined report files and merged result.json
#   6. Re-render every merged_predict_<tag>/<seq>.jpg by drawing the
#      (possibly updated) bboxes onto the corresponding raw frame
#   7. Re-stitch the consolidated annotated video from the redrawn
#      merged_predict frames; also re-build the consolidated raw video
#      (cheap; videos are 1 fps H.264)
#   8. Update inference_data with the new reports + URLs

# CLI:
#   --reprocess                                 single re-run, then exit
#   --reprocess --watch                         poll merged result.json,
#                                               re-run on every change
#   --reprocess-result-path "gs://bucket/path"  override the default location
#   --reprocess-skip-frames                     skip merged_predict redraw
#                                               (just rebuild reports + Mongo)
#   --reprocess-skip-videos                     skip consolidated video stitch

  
def download_merged_result_json(road_id: str, uid: str,
                                explicit_gs_url: str | None = None) -> dict:
    """
    Download and parse the merged result.json. Returns the raw COCO dict
    (info + images + annotations + categories). Raises SystemExit if the
    file is missing — re-running before Phase C has ever been run is a
    user error.
    """
    if explicit_gs_url:
        sp = _split_gcs_url(explicit_gs_url)
        if not sp:
            raise SystemExit(f"--reprocess-result-path not a gs:// URL: {explicit_gs_url}")
        src_bucket, src_path = sp
    else:
        src_bucket = GCS_BUCKET
        src_path = f"{PROCESSED_PREFIX}/{road_id}/{uid}/result.json"

    blob = gcs_client().bucket(src_bucket).blob(src_path)
    if not blob.exists():
        raise SystemExit(f"merged result.json not found at gs://{src_bucket}/{src_path}\n"
                         f"  → run V2 normally at least once before reprocessing.")
    raw = blob.download_as_text()
    log.info("[reprocess] downloaded gs://%s/%s (%d bytes)",
             src_bucket, src_path, len(raw))
    return json.loads(raw)


def reconstruct_frames_from_result(coco: dict) -> list[dict]:
    """
    Rebuild a list of frame_list_data-shaped dicts from the merged COCO
    result.json. The image entries already carry lat/lng/chainage_km/_uuid
    (stamped by build_merged_result_json), so no Mongo round-trip is
    needed. Annotations are grouped by image_id and translated back into
    the inference_info schema the Phase C report builders consume.
    """
    images = sorted(coco.get("images") or [], key=lambda x: x.get("id", 0))
    annotations = coco.get("annotations") or []
    categories = {c["id"]: c["name"] for c in (coco.get("categories") or [])}

    anns_by_image: dict[int, list[dict]] = defaultdict(list)
    for ann in annotations:
        anns_by_image[ann.get("image_id")].append(ann)

    merged_frames: list[dict] = []
    for img in images:
        bboxes: list[dict] = []
        for ann in anns_by_image.get(img.get("id"), []):
            label = categories.get(ann.get("category_id"), "")
            if not label:
                continue
            bboxes.append({
                "label":       label,
                "bbox":        list(ann.get("bbox") or [0, 0, 0, 0]),
                "severity":    ann.get("severity", "none"),
                "category_id": ann.get("category_id"),
                "confidence":  ann.get("confidence", 1.0),
            })
        # file_name is "merged_frames_<tag>/<seq:06d>.jpg" → that's
        # already the raw URL we'll download from for redrawing.
        rel = img.get("file_name") or ""
        merged_frames.append({
            "_uuid":           img.get("_uuid"),
            "latitude":        img.get("latitude"),
            "longitude":       img.get("longitude"),
            "chainage_km":     img.get("chainage_km") or 0.0,
            "location":        {"type": "Point",
                                "coordinates": [img.get("longitude"),
                                                img.get("latitude")]},
            "og_file":         rel,         # filled with absolute URL below
            "inference_image": rel,
            "inference_info":  bboxes,
            "defect_state":    "none",      # filled by retag below
            "timeElapsed":     img.get("id", 0),
            "orientation":     "landscapeLeft",
        })
    log.info("[reprocess] reconstructed %d frames, %d annotations",
             len(merged_frames), sum(len(f["inference_info"]) for f in merged_frames))
    return merged_frames


def redraw_merged_predict_from_frames(
    road_id: str,
    uid: str,
    merged_tag: str,
    frames: list[dict],
) -> int:
    """
    Re-render merged_predict_<tag>/<seq:06d>.jpg using each frame's CURRENT
    inference_info. The raw source is read from merged_frames_<tag>/<seq>.jpg
    (built by an earlier Phase C run), so we never need to chase per-UUID
    annotated_frames URLs.

    Returns the number of frames redrawn.
    """
    raw_prefix = f"{PROCESSED_PREFIX}/{road_id}/merged_frames_{merged_tag}"
    ann_prefix = f"{PROCESSED_PREFIX}/{road_id}/merged_predict_{merged_tag}"
    cli = gcs_client()
    bucket = cli.bucket(GCS_BUCKET)

    redrawn = 0
    def _redraw_one(idx_frame):
        nonlocal redrawn
        idx, frame = idx_frame
        seq = idx * 10
        raw_blob = f"{raw_prefix}/{seq:06d}.jpg"
        ann_blob = f"{ann_prefix}/{seq:06d}.jpg"
        try:
            raw_bytes = bucket.blob(raw_blob).download_as_bytes()
        except Exception as e:
            log.debug("[reprocess][redraw] missing raw %s: %s", raw_blob, e)
            return
        bboxes = frame.get("inference_info") or []
        out_bytes = draw_overlays_on_jpeg(raw_bytes, bboxes)
        if out_bytes is None:
            out_bytes = raw_bytes   # no detections → identical copy
        bucket.blob(ann_blob).upload_from_string(out_bytes,
                                                 content_type="image/jpeg")
        redrawn += 1

    log.info("[reprocess][redraw] %d frames → gs://%s/%s/",
             len(frames), GCS_BUCKET, ann_prefix)
    with ThreadPoolExecutor(max_workers=32) as pool:
        list(pool.map(_redraw_one, enumerate(frames)))
    log.info("[reprocess][redraw] %d frames redrawn", redrawn)
    return redrawn


def reprocess_from_merged_result_json(
    *,
    road_id:           str,
    uid_suffix:        str,
    organization:      str,
    city:              str,
    project_title:     str,
    start_addr:        str,
    end_addr:          str,
    severity_override: dict | None = None,
    merged_tag:        str | None = None,
    explicit_gs_url:   str | None = None,
    redraw_frames:     bool = True,
    rebuild_videos:    bool = True,
    video_fps:         int = 1,
) -> str:
    """
    Phase C, but the data source is the merged result.json instead of the
    per-UUID annotation_segments collection. See section banner for the
    full motivation. Returns the consolidated uid.
    """
    uid = f"{road_id}_{uid_suffix}"
    merged_tag = merged_tag or uid_suffix

    # 1) Download + parse + reconstruct frames
    coco = download_merged_result_json(road_id, uid, explicit_gs_url)
    merged_frames = reconstruct_frames_from_result(coco)
    if not merged_frames:
        raise SystemExit("merged result.json had no images — nothing to reprocess")

    # 2) Severity retag (in case mappings changed since the last run)
    sev_totals = {"high": 0, "medium": 0, "low": 0, "none": 0}
    for f in merged_frames:
        worst = "none"
        for inf in (f.get("inference_info") or []):
            sev = severity_for(inf.get("label", ""), severity_override)
            inf["severity"] = sev
            sev_totals[sev] += 1
            if _SEV_RANK[sev] > _SEV_RANK[worst]:
                worst = sev
        f["defect_state"] = worst
    log.info("[reprocess] severity totals: %s", sev_totals)

    # 3) Re-build IBI reports
    cum_km = max((float(f.get("chainage_km") or 0) for f in merged_frames), default=0)
    road_length_km = round(cum_km, 2)
    # Use V1's PCI advisory: max(0, 100 - 10·H - 4·M - 1·L) over per-frame totals
    H = sev_totals["high"]; M = sev_totals["medium"]; L = sev_totals["low"]
    road_rating = max(0.0, 100.0 - 10 * H - 4 * M - 1 * L) if (H or M or L) else 100.0

    report_1, total_defects = build_report_1_key(
        merged_frames, road_length_km, road_rating, start_addr, end_addr)
    report_2, labels = build_report_2_key(merged_frames)
    report_3 = build_report_3_key(merged_frames)
    report_4 = build_report_4_key(merged_frames)
    pie_chart2 = build_pie_chart2(merged_frames)
    chainage_csv = build_chainage_report_csv(merged_frames, start_addr, end_addr)

    # 4) Re-upload combined reports + the merged result.json (in case the
    #    user's edits didn't include the latest schema additions)
    db = MongoClient(MONGO_URI)["roadvision"]
    segments = list(db.annotation_segments.find({"road_id": road_id}))
    upload_combined_reports(
        road_id=road_id, uid=uid, segments=segments,
        report_1=report_1, report_2=report_2,
        report_3=report_3, report_4=report_4,
        chainage_csv=chainage_csv,
    )
    log.info("[reprocess] reports re-uploaded; report_2 = %d labels", len(labels))

    merged_result = build_merged_result_json(merged_frames, merged_tag)
    result_blob = f"{PROCESSED_PREFIX}/{road_id}/{uid}/result.json"
    gcs_upload_text(result_blob, json.dumps(merged_result, default=str),
                    content_type="application/json")
    log.info("[reprocess] merged result.json refreshed (%d images, %d annotations)",
             len(merged_result["images"]), len(merged_result["annotations"]))

    # 5) Upsert inference_data (preserve top-level video URLs the dashboard reads)
    inference_doc = {
        "uid":                   uid,
        "road_id":               road_id,
        "organization":          organization,
        "city":                  city,
        "project_title_display": project_title,
        "start_add":             {"add": start_addr},
        "end_add":               {"add": end_addr},
        "showInference":         True,
        "is_deleted":            False,
        "meta_data":             {"reprocessed_at": _dt.datetime.utcnow().isoformat()},
        "plot_data":             {"plots": {"pie_chart2": pie_chart2}},
        "data": {
            "road_id":              road_id,
            "road_length":          road_length_km,
            "road_rating":          road_rating,
            "data_submitted":       _dt.date.today().strftime("%d-%m-%Y"),
            "total_defects":        total_defects,
            "frame_list_data":      merged_frames,
            "category_information": {},
            "report_1_key":         report_1,
            "report_2_key":         report_2,
            "dashboard_df_csv":     report_2,
            "report_3_key":         report_3,
            "report_4_key":         report_4,
            "CODEBUILD_BUILD_ID":     uid,
            "NEW_CODEBUILD_BUILD_ID": uid,
            "project_title_display":  project_title,
        },
    }
    db.inference_data.update_one(
        {"uid": uid, "organization": organization},
        {"$set": inference_doc},
        upsert=True,
    )
    log.info("[reprocess] inference_data refreshed  uid=%s", uid)

    # 6) Re-draw merged_predict frames (if labels/bboxes changed)
    if redraw_frames:
        redraw_merged_predict_from_frames(road_id, uid, merged_tag, merged_frames)

    # 7) Re-stitch consolidated videos
    if rebuild_videos:
        urls = build_consolidated_videos(road_id, uid, merged_tag, fps=video_fps)
        if urls:
            stamp_video_urls(uid, urls)

    log.info("✅ reprocess complete  uid=%s", uid)
    return uid


def reprocess_watch_mode(args: argparse.Namespace,
                         severity_override: dict | None) -> None:
    """
    Poll the merged result.json on GCS; whenever its `updated` timestamp
    changes (someone edited it externally), re-run reprocess_from_merged_result_json.

    Same loop semantics as V1's reprocess_annotations.py --watch.
    Ctrl+C exits cleanly.
    """
    uid = f"{args.road_id}_{args.uid_suffix}"
    merged_tag = args.merged_tag or args.uid_suffix

    if args.reprocess_result_path:
        sp = _split_gcs_url(args.reprocess_result_path)
        if not sp:
            raise SystemExit(f"--reprocess-result-path not a gs:// URL: {args.reprocess_result_path}")
        src_bucket, src_path = sp
    else:
        src_bucket = GCS_BUCKET
        src_path = f"{PROCESSED_PREFIX}/{args.road_id}/{uid}/result.json"

    blob = gcs_client().bucket(src_bucket).blob(src_path)
    log.info("─── REPROCESS WATCH  uid=%s ───", uid)
    log.info("  source       : gs://%s/%s", src_bucket, src_path)
    log.info("  poll every   : %ds", args.watch_interval)
    log.info("  Ctrl+C to stop.")

    last_updated = None
    if blob.exists():
        blob.reload()
        last_updated = blob.updated
        log.info("  current ver  : %s", last_updated)
    else:
        log.warning("  result.json doesn't exist yet — will trigger on first appearance")

    try:
        while True:
            time.sleep(args.watch_interval)
            try:
                if not blob.exists():
                    continue
                blob.reload()
                if blob.updated == last_updated:
                    log.info("[reprocess-watch] no change (next check in %ds)",
                             args.watch_interval)
                    continue
                log.info("[reprocess-watch] result.json changed: %s → %s",
                         last_updated, blob.updated)
                last_updated = blob.updated
                reprocess_from_merged_result_json(
                    road_id=args.road_id, uid_suffix=args.uid_suffix,
                    organization=args.organization, city=args.city,
                    project_title=args.project_title,
                    start_addr=args.start_address, end_addr=args.end_address,
                    severity_override=severity_override,
                    merged_tag=merged_tag,
                    explicit_gs_url=args.reprocess_result_path,
                    redraw_frames=not args.reprocess_skip_frames,
                    rebuild_videos=not args.reprocess_skip_videos,
                    video_fps=args.video_fps,
                )
                log.info("[reprocess-watch] resuming watch")
            except Exception as e:
                log.exception("[reprocess-watch] error: %s", e)
    except KeyboardInterrupt:
        log.info("[reprocess-watch] Ctrl+C — exiting cleanly")


# ─────────────────────────────────────────────────────────────────────────────
# Section 18 — CLI orchestrator (Phase A → Phase B → Phase C → merged frames + videos)
# ─────────────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Roadvision V2 — single-script end-to-end preprocessing pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--gcs-prefix",   default=None,
                    help='GCS prefix holding {stem}.MP4 + {stem}.gpx (e.g. '
                         '"video-processing-pipelines-data/R045861/"). '
                         "Required for single-road mode; ignored when "
                         "--all-roads is set (use --gcs-prefix-root then).")
    ap.add_argument("--road-id",      default=None,
                    help="Road identifier (also the GCS subfolder). "
                         "Required for single-road mode; ignored when "
                         "--all-roads is set (each subfolder of "
                         "--gcs-prefix-root becomes a road).")
    ap.add_argument("--uid-suffix",   default="day",
                    help='Suffix for the consolidated uid, default "day". '
                         'Final uid = "{road_id}_{uid_suffix}".')
    ap.add_argument("--organization", required=True,
                    help="Organization the inference_data doc is scoped to "
                         '(e.g. "BangaloreOrg", "kotaOrg", "VaranasiOrg")')
    ap.add_argument("--city",         required=True)
    ap.add_argument("--project-title", default=None,
                    help="Display name shown on the dashboard. "
                         "Single-road mode: required. "
                         "--all-roads: read from each road's _road_meta.json "
                         "(falls back to road_id).")
    ap.add_argument("--start-address", default=None,
                    help='Start address. --all-roads: read from _road_meta.json '
                         '(falls back to "TBD").')
    ap.add_argument("--end-address",   default=None,
                    help='End address. --all-roads: read from _road_meta.json '
                         '(falls back to "TBD").')

    # ── Multi-road watch (mirrors trigger_builds.py with no --road_id) ────
    ap.add_argument("--all-roads", action="store_true",
                    help="Watch ALL road subfolders under --gcs-prefix-root. "
                         "One Python process, shared --parallel pool, "
                         "Phase C runs per-road as videos finish. "
                         "Mirrors V1 trigger_builds.py --watch (no --road_id). "
                         "Only meaningful with --watch.")
    ap.add_argument("--gcs-prefix-root", default="video-processing-pipelines-data/",
                    help="Parent prefix to scan when --all-roads is set "
                         "(default video-processing-pipelines-data/). Each "
                         "immediate Rxxxxxx subfolder is treated as a road.")
    ap.add_argument("--fast", action="store_true",
                    help="Skip outputs that Phase C rebuilds anyway. "
                         "Drops per-UUID annotated_video.mp4 (~10s/video), "
                         "per-UUID result.json (~3s/video), AND the "
                         "merged_frames_<tag>/* + merged_predict_<tag>/* "
                         "copy_blob loop in Phase C (~135s/road for a "
                         "1338-frame road). Instead: stamps per-UUID "
                         "frame URLs directly on inference_data, and "
                         "builds the consolidated raw + annotated videos "
                         "by streaming frames from per-UUID paths into "
                         "a local tmp dir → ffmpeg → upload. Net wall "
                         "time for an 8-video batch on a T4: ~290s → "
                         "~80–120s. Recommended for any non-IBI workflow.")
    ap.add_argument("--reset", action="store_true",
                    help="DESTRUCTIVE: before processing, wipe all existing "
                         "state for the targeted road(s) — Mongo "
                         "(annotation_segments / inference_data / "
                         "video_upload / roads) and GCS "
                         "(processed-data/<road_id>/* — keeps the source "
                         "MP4 + GPX). Tracker entries for those roads are "
                         "also cleared so a watch run picks up every video "
                         "again. With --all-roads, every discovered road is "
                         "reset. Use to recover from cross-pollinated re-runs "
                         "(stale segments under the same road_id) or to "
                         "force a clean rebuild after annotation changes.")

    ap.add_argument("--kml", default=None,
                    help="Optional explicit path to a .kml/.kmz polyline. "
                         "When omitted V2 auto-detects a KML/KMZ inside "
                         "--gcs-prefix; if neither is present Phase A is "
                         "silently skipped and Phase B reads the raw GPX.")

    ap.add_argument("--model-weights", default=DEFAULT_ENGINE_PATH,
                    help=f"YOLO weights. TensorRT .engine preferred (default "
                         f"{DEFAULT_ENGINE_PATH}); .pt fallback works too.")
    ap.add_argument("--parallel", type=int, default=1,
                    help="Concurrent per-video Phase B workers. 1 (default) "
                         "= serial in-process; one shared YOLO model. "
                         ">1 = ProcessPoolExecutor with `spawn` (CUDA-safe), "
                         "each worker loads its own YOLO model into its "
                         "own CUDA context. I/O dominates per-video time "
                         "(GCS download/upload + ffmpeg), so wall-time "
                         "scales near-linearly until GPU saturation. ~8 "
                         "is the practical sweet spot on a T4 (~50 MB GPU "
                         "per worker for the TensorRT engine). Honoured by "
                         "both one-shot and watch modes.")
    ap.add_argument("--max-perp-m", type=float, default=75.0,
                    help="Drop GPX points further than this many metres off "
                         "the KML during Phase A (default 75)")
    ap.add_argument("--severity-map-json", default=None,
                    help="Optional JSON file with org-specific {label: "
                         "severity} overrides used by Phase C retag.")
    ap.add_argument("--ls-export-dir", default=None,
                    help='Optional Label Studio export root. Each frame\'s '
                         'inference_info is REPLACED by the LS task\'s '
                         'bboxes (paired rectanglelabels+choices → '
                         '"<Main> – <Sub>" labels). Use for orgs with '
                         'sub-category taxonomies (Kota / Varanasi).')

    ap.add_argument("--skip-phase-a",       action="store_true",
                    help="Skip GPX→KML projection (re-use existing .kml.gpx)")
    ap.add_argument("--skip-phase-b",       action="store_true",
                    help="Skip per-video processing (annotation_segments must "
                         "already exist)")
    ap.add_argument("--skip-merged-frames", action="store_true",
                    help="Skip building the chronological merged frames folder")
    ap.add_argument("--skip-videos",        action="store_true",
                    help="Skip stitching consolidated raw + annotated videos")
    ap.add_argument("--video-fps", type=int, default=1,
                    help="FPS for consolidated MP4s (default 1, matches IBI)")
    ap.add_argument("--merged-tag", default=None,
                    help='Suffix for merged_frames_<tag>/ + merged_predict_<tag>/. '
                         'Defaults to --uid-suffix.')
    ap.add_argument("--workdir",
                    default=str(Path(__file__).resolve().parent / "workdir"),
                    help="Per-run isolated workdirs root. Default: a "
                         "`workdir/` subfolder next to pipeline_v2.py "
                         "(so per-uid logs persist across reboots and "
                         "live alongside the script). Set to "
                         "/dev/shm/pipeline_v2 if you'd rather keep "
                         "intermediate per-UUID frames on RAM-backed "
                         "tmpfs (slightly faster but cleared on reboot).")

    # ── Watch mode (mirrors trigger_builds.py --watch) ────────────────────
    ap.add_argument("--watch", action="store_true",
                    help="Run forever, polling --gcs-prefix for new "
                         "(mp4, gpx) pairs. Each batch of new videos is "
                         "Phase-B processed and Phase-C re-consolidated, "
                         "incrementally updating the dashboard. Tracker "
                         "state is persisted next to this script — survives "
                         "process restarts. Ctrl+C exits cleanly.")
    ap.add_argument("--watch-interval", type=int, default=30,
                    help="Seconds between GCS scans when no new videos are "
                         "present (default 30)")
    ap.add_argument("--settle-time", type=int, default=30,
                    help="After detecting new uploads, wait this many "
                         "seconds for in-progress uploads to finalise "
                         "before scanning again (default 30)")
    ap.add_argument("--batch-size", type=int, default=8,
                    help="Trigger Phase B once this many new videos have "
                         "accumulated (or sooner if uploads stop arriving). "
                         "Smaller batches run Phase C more frequently — "
                         "the dashboard updates sooner but each Phase C "
                         "rebuild costs ~30 s. Default 8.")

    # ── Reprocess mode (mirrors reprocess_annotations.py contract) ────────
    ap.add_argument("--reprocess", action="store_true",
                    help="Re-run only Phase C from the merged result.json "
                         "in GCS (skips Phase A and B entirely). Use when "
                         "annotations have been manually corrected, "
                         "severity mappings changed, or bbox overlays / "
                         "the consolidated video need to be regenerated. "
                         "Combine with --watch to re-trigger automatically "
                         "whenever the result.json is updated externally.")
    ap.add_argument("--reprocess-result-path", default=None,
                    help='Optional explicit "gs://bucket/path/result.json" '
                         "override. Defaults to "
                         "gs://datanh11/processed-data/{road_id}/{uid}/result.json")
    ap.add_argument("--reprocess-skip-frames", action="store_true",
                    help="Reprocess: skip redrawing merged_predict frames "
                         "(reports + Mongo only — fastest path)")
    ap.add_argument("--reprocess-skip-videos", action="store_true",
                    help="Reprocess: skip stitching consolidated raw + "
                         "annotated videos")
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    severity_override = None
    if args.severity_map_json:
        with open(args.severity_map_json) as f:
            severity_override = {k.lower(): v for k, v in json.load(f).items()}
        log.info("severity_override loaded: %d entries", len(severity_override))

    ls_lookup = None
    if args.ls_export_dir:
        ls_lookup = load_ls_export_dir(args.ls_export_dir)

    # ── RESET — destructive, runs BEFORE any mode dispatch so the rest of
    #            the run sees a clean slate. Reads optional tracker file
    #            so its entries get wiped too. Mutually exclusive with
    #            --reprocess (reprocess needs the existing data to read).
    if args.reset:
        if args.reprocess:
            raise SystemExit("--reset and --reprocess are mutually exclusive")
        tracker = load_tracker() if TRACKER_FILE.exists() else None
        if args.all_roads:
            root = args.gcs_prefix_root.rstrip("/") + "/"
            roads = list_road_folders(root)
            if not roads:
                log.warning("[reset] no roads discovered under gs://%s/%s",
                            GCS_BUCKET, root)
            log.info("[reset] all-roads mode: wiping %d road(s)", len(roads))
            for rid in roads:
                uid = f"{rid}_{args.uid_suffix}"
                reset_road_state(rid, uid, args.organization, tracker=tracker)
        else:
            if not args.road_id:
                raise SystemExit("--reset without --all-roads requires --road-id")
            uid = f"{args.road_id}_{args.uid_suffix}"
            reset_road_state(args.road_id, uid, args.organization, tracker=tracker)
        log.info("[reset] complete — proceeding to processing")

    # ── ALL-ROADS WATCH — scans every road subfolder, no per-road args ───
    # Highest-priority dispatch — short-circuits the per-road validation
    # below since each road's metadata is read from its own _road_meta.json.
    if args.all_roads:
        if not args.watch:
            raise SystemExit("--all-roads only makes sense with --watch")
        watch_all_roads_mode(args, severity_override, ls_lookup)
        return

    # ── Per-road validation: --gcs-prefix and --road-id are mandatory now ─
    if not args.gcs_prefix or not args.road_id:
        raise SystemExit("--gcs-prefix and --road-id are required "
                         "(or pass --all-roads --watch for multi-road mode)")
    if not args.project_title or not args.start_address or not args.end_address:
        raise SystemExit("--project-title, --start-address and --end-address "
                         "are required in single-road mode "
                         "(or pass --all-roads --watch which reads from "
                         "each road's _road_meta.json)")

    # ── KML resolution (shared by single-road watch + one-shot modes) ────
    prefix_with_slash = args.gcs_prefix.rstrip("/") + "/"
    kml_path: str | None = args.kml or gcs_find_kml(prefix_with_slash)
    poly: list[tuple[float, float]] | None = None
    cumdist: list[float] | None = None
    if kml_path is not None:
        poly = parse_kml(kml_path)
        cumdist = precompute_polyline_chainage(poly)

    # ── REPROCESS MODE — Phase C only, from merged result.json ───────────
    if args.reprocess:
        merged_tag = args.merged_tag or args.uid_suffix
        if args.watch:
            reprocess_watch_mode(args, severity_override)
        else:
            reprocess_from_merged_result_json(
                road_id=args.road_id, uid_suffix=args.uid_suffix,
                organization=args.organization, city=args.city,
                project_title=args.project_title,
                start_addr=args.start_address, end_addr=args.end_address,
                severity_override=severity_override,
                merged_tag=merged_tag,
                explicit_gs_url=args.reprocess_result_path,
                redraw_frames=not args.reprocess_skip_frames,
                rebuild_videos=not args.reprocess_skip_videos,
                video_fps=args.video_fps,
            )
        return

    # ── WATCH MODE — long-running upload-poll loop, exits on Ctrl+C ──────
    if args.watch:
        watch_mode(args, severity_override, ls_lookup, poly, cumdist, kml_path)
        return

    # ── ONE-SHOT MODE: PHASE A — KML/GPX projection ──────────────────────
    _log_section("PHASE A — KML / GPX projection")
    pairs = gcs_list_pairs(prefix_with_slash)
    if not pairs:
        raise SystemExit(f"No (mp4, gpx) pairs at gs://{GCS_BUCKET}/{args.gcs_prefix}")
    log.info("Found %d video pairs", len(pairs))

    if kml_path is None:
        log.info("Phase A skipped — no KML/KMZ for %s; running on raw GPX",
                 args.road_id)
        kml_pairs = pairs
    elif args.skip_phase_a:
        kml_pairs = [(mp4, gpx[:-4] + ".kml.gpx") for mp4, gpx in pairs]
        log.info("Phase A skipped — reusing existing .kml.gpx files")
    else:
        kml_pairs = phase_a_project_all_gpx(pairs, poly, cumdist, args.max_perp_m)
        log.info("Phase A complete: %d pairs ready", len(kml_pairs))

    # ── PHASE B + C wrapped in a per-uid log session so every event lands
    #     in {workdir}/{road_id}/pipeline.log alongside the per-UUID
    #     temporary subdirs (which auto-delete on success). The session
    #     handler is also passed to ProcessPoolExecutor workers so their
    #     output appends to the same file.
    workdir_root = Path(args.workdir) / args.road_id
    workdir_root.mkdir(parents=True, exist_ok=True)
    log_path = road_log_path(Path(args.workdir), args.road_id)

    with road_log_session(Path(args.workdir), args.road_id):
        # ── PHASE B — per-video YOLO + frame upload + Mongo ───────────────
        # Honours --parallel: serial when 1 (one shared YOLO model in-process),
        # ProcessPoolExecutor with `spawn` when >1 (each worker loads its own
        # YOLO model). I/O dominates per-video time → near-linear speedup
        # under parallelism up to GPU saturation (~8 workers on a T4).
        _log_section("PHASE B — per-video processing")
        uuid_to_mp4: dict[str, str] = {}
        if not args.skip_phase_b:
            jobs = []
            for mp4_blob, gpx_blob in kml_pairs:
                run_id = str(uuid_mod.uuid4())
                uuid_to_mp4[run_id] = mp4_blob
                jobs.append({
                    "mp4_blob": mp4_blob, "gpx_blob": gpx_blob,
                    "road_id":  args.road_id, "run_id": run_id,
                    "workdir":  workdir_root / run_id,
                })
            ok = dispatch_phase_b(
                jobs=jobs, parallel=args.parallel,
                model_weights=args.model_weights,
                severity_override=severity_override,
                fast=args.fast,
                log_file_path=str(log_path),
            )
            if ok == 0:
                raise SystemExit("Phase B produced no successful runs")
        else:
            # When --skip-phase-b is on, recover (uuid → mp4) mapping from each
            # segment's stamped source_mp4 field. Falls back to Mongo _id order
            # × sorted-MP4 pairing for older segments without source_mp4.
            try:
                db = MongoClient(MONGO_URI)["roadvision"]
                seg_docs = list(db.annotation_segments.find(
                    {"road_id": args.road_id},
                    {"uuid": 1, "_id": 1, "source_mp4": 1}).sort("_id", 1))
                for s in seg_docs:
                    if s.get("source_mp4"):
                        uuid_to_mp4[s["uuid"]] = s["source_mp4"]
                missing = [s for s in seg_docs if not s.get("source_mp4")]
                if missing:
                    # Fallback for old V1 segments — alphabetical pairing.
                    sorted_mp4s = sorted(mp4 for mp4, _ in kml_pairs)
                    missing_uuids = [s["uuid"] for s in missing]
                    for u, mp4 in zip(missing_uuids, sorted_mp4s):
                        uuid_to_mp4.setdefault(u, mp4)
                log.info("Phase B skipped — recovered %d UUID→MP4 entries",
                         len(uuid_to_mp4))
            except Exception as e:
                log.warning("Phase B skipped — UUID→MP4 recovery failed: %s", e)

        # ── PHASE C — consolidate + IBI-format reports ───────────────────
        _log_section("PHASE C — consolidation")
        merged_tag = args.merged_tag or args.uid_suffix
        uid = consolidate_and_rebuild(
            road_id=args.road_id,
            uid_suffix=args.uid_suffix,
            organization=args.organization,
            city=args.city,
            project_title=args.project_title,
            start_addr=args.start_address,
            end_addr=args.end_address,
            severity_override=severity_override,
            ls_lookup=ls_lookup,
            uuid_to_mp4=uuid_to_mp4,
            merged_tag=merged_tag,
            polyline=poly,
            cumdist=cumdist,
            kml_path=kml_path,
        )

        # ── Merged frames + consolidated videos ─────────────────────────
        if not args.skip_merged_frames:
            _log_section("merged frames + consolidated videos")
            if args.fast:
                urls = fast_finalize_frames_and_videos(
                    args.road_id, uid, merged_tag, fps=args.video_fps)
                if urls:
                    stamp_video_urls(uid, urls)
            else:
                build_merged_frames_folder(args.road_id, uid, merged_tag)
                if not args.skip_videos:
                    urls = build_consolidated_videos(args.road_id, uid, merged_tag,
                                                     fps=args.video_fps)
                    if urls:
                        stamp_video_urls(uid, urls)

        log.info("✅ Pipeline V2 complete  uid=%s  road_id=%s  log=%s",
                 uid, args.road_id, log_path)


if __name__ == "__main__":
    main()
