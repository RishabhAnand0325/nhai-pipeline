"""
pipeline_v3.py — Roadvision Video Processing Pipeline, V3 NHAI (self-contained)
===============================================================================

End-to-end preprocessing for a single road, defined by an optional KML/KMZ
polyline plus a folder of MP4+GPX video pairs in GCS. Produces consolidated
`inference_data` records (one per road × period × time × direction × sub-
road), plus a 3-tab xlsx workbook bundling each direction's sub-roads.

V3 (this file) ONLY supports the NHAI directory schema. For the older
flat / Survey-N layouts (R074598-style), use pipeline_v2.py.

This file is INTENTIONALLY a single script — every step a developer needs
to understand the flow is in here.

────────────────────────────────────────────────────────────────────────────
NHAI DIRECTORY SCHEMA (v3 — the only layout supported by this script)
────────────────────────────────────────────────────────────────────────────

  gs://nhai-upload/<road_id>/
   └── Week-{N}-{Mon}-{YYYY}/
        ├── day/
        │    ├── LHS/
        │    │    ├── MCW/           ← Main Carriage Way   (*.MP4 + *.gpx)
        │    │    ├── service-road/  ← Service Road        (*.MP4 + *.gpx)
        │    │    └── slip-road/     ← Slip Road           (*.MP4 + *.gpx)
        │    └── RHS/ { MCW, service-road, slip-road }
        └── night/{LHS,RHS}/{MCW, service-road, slip-road}

  Each leaf folder holds one or more (mp4, gpx) pairs. Direction (LHS/RHS)
  and sub-road (MCW/service-road/slip-road) are first-class dimensions —
  the pipeline keeps their frame_list_data, reports, and dashboard rows
  fully separated, then bundles each direction's three sub-roads into a
  single xlsx deliverable.

────────────────────────────────────────────────────────────────────────────
HIGH-LEVEL FLOW
────────────────────────────────────────────────────────────────────────────

    GCS:  nhai-upload/<road_id>/Week-<N>-<Mon>-<YYYY>/<day|night>/
                                 <LHS|RHS>/<MCW|service-road|slip-road>/
                                                    {*.MP4, *.gpx, *.kmz?}
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
        │ Phase B    Per-video processing (multi-GPU + NVDEC)      │
        │            For each (mp4, gpx) pair:                     │
        │              1. download MP4 + GPX from GCS              │
        │              2. parse GPX → trackpoints with timestamps  │
        │              3. walk trackpoints with 10 m haversine →   │
        │                 emit a "milestone" timestamp per 10 m    │
        │              4. ffmpeg extracts the milestone frames:    │
        │                 • NVDEC (default when GPUs detected) —   │
        │                   one ffmpeg/video, h264_cuvid decode,   │
        │                   ~3× faster on 4K, frees CPU. Auto-    │
        │                   falls back to CPU on Novatek-style    │
        │                   files NVDEC refuses.                   │
        │                 • CPU split (--ffmpeg-hwaccel cpu) —    │
        │                   classic 8-segment parallel ffmpeg.     │
        │              5. YOLO TensorRT engine runs over every     │
        │                 frame → per-frame {label, bbox, conf}.   │
        │                 Worker is pinned to one GPU via          │
        │                 CUDA_VISIBLE_DEVICES so a parallel pool  │
        │                 spreads round-robin across all --num-    │
        │                 gpus T4s (default: auto-detected).       │
        │              6. raw frames uploaded to                   │
        │                 processed-data/<road>/<uuid>/            │
        │                   annotated_frames/frames/               │
        │              7. each frame's bboxes drawn ON the raw     │
        │                 image with the 67-label palette →        │
        │                 annotated_frames/predict/<file>.jpg      │
        │              8. per-UUID annotated_video.mp4 stitched    │
        │                 from the predict frames (1 fps; uses     │
        │                 h264_nvenc when GPUs present, else       │
        │                 libx264 ultrafast)                       │
        │              9. result.json (COCO format) saved          │
        │             10. annotation_segments doc written to Mongo │
        │                 with frame_list_data / inference_info /  │
        │                 period / period_label / uid_suffix /     │
        │                 direction / subroad / subroad_code       │
        └──────────────────────────────────────────────────────────┘
                          │
                          ▼
        ┌──────────────────────────────────────────────────────────┐
        │ Phase C    Consolidation (one run per uid_key)           │
        │            • Loads annotation_segments matching the      │
        │              uid_key = "{road}_W{N}{Mon}{YYYY}_{day|     │
        │              night}_{LHS|RHS}_{MCW|SR|SL}"               │
        │              (one consolidate per sub-road).             │
        │            • Concatenates frame_list_data; tags each     │
        │              frame with its source UUID + source MP4.    │
        │            • Sorts merged frames by along-KML chainage   │
        │              (when KML present) or by source MP4 +       │
        │              frame index (raw GPX fallback). Robust to   │
        │              videos recorded out of spatial order.       │
        │            • Re-stamps chainage_km cumulatively across   │
        │              the merged sequence (per-segment ground     │
        │              truth, offset by prior segment lengths).    │
        │            • Optionally REPLACES inference_info with     │
        │              Label Studio bboxes (--ls-export-dir or     │
        │              the road's annotations.json in GCS).        │
        │            • Re-tags severity per IBI Guideline.         │
        │            • Builds report_1_key, report_2_key,          │
        │              dashboard_df_csv, chainage_report.csv,      │
        │              nhai_report.csv (NHAI RFP format), and      │
        │              plot_data.plots.pie_chart2.                 │
        │            • Builds a merged COCO result.json.           │
        │            • Uploads combined CSVs + result.json to      │
        │              processed-data/<road>/<uid>/                 │
        │            • Builds                                      │
        │                processed-data/<road>/merged_frames_<tag>/│
        │                processed-data/<road>/merged_predict_<tag>│
        │              (raw + bboxed, sequence-numbered 10 m).     │
        │            • Stitches consolidated raw + annotated MP4s  │
        │              (h264_nvenc when GPU available, libx264     │
        │              fallback) into                              │
        │                processed-data/<road>/videos/             │
        │            • Upserts inference_data (per uid),           │
        │              video_upload (per uid), roads.surveys[],    │
        │              RoadData (per road).                        │
        │            • Bundles the just-finished sub-road's        │
        │              dashboard_df_csv into the 3-tab xlsx for    │
        │              its (road × period × time × direction) —    │
        │              missing sub-roads get "No data" placeholder │
        │              tabs that refill on the next consolidate.   │
        │            • Optional Phase D: --create-ls-project       │
        │              creates LS project "{road}-{W…}-{day|       │
        │              night}-{LHS|RHS}-{MCW|SR|SL}", attaches GCS │
        │              source storage, uploads YOLO bboxes as      │
        │              predictions.                                │
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
        Phase B per video — honours --parallel + --num-gpus:
                =1 → serial in-process, one shared YOLO model
                >1 → ProcessPoolExecutor (spawn), workers pinned
                     round-robin across --num-gpus T4s via
                     CUDA_VISIBLE_DEVICES. NVDEC frame extract
                     (~3× CPU). ~8 workers saturate one T4 →
                     --parallel 32 with 4 T4s is the sweet spot.
        Phase C re-consolidates per uid_key (one run per
                survey × time × direction in the batch) — dashboard
                updates after every batch
        merged_frames + consolidated videos rebuilt incrementally
        Phase D (--create-ls-project) per uid_key

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
  │   │   └── predict/                     ← bboxes drawn (67-label palette)
  │   ├── annotated_video.mp4              ← 1 fps stitch of predict/
  │   └── result.json                      ← per-video COCO detections
  ├── {uid}/                               ← Phase C combined per-uid
  │   ├── report_1.csv ... report_4.csv    ← IBI shape, per-frame-unique
  │   ├── chainage_report.csv
  │   ├── nhai_report.csv                  ← NHAI Report Format (RFP based)
  │   ├── result.json                      ← merged COCO across all UUIDs
  │   └── annotations.json                 ← uploaded by RV Studio (optional)
  ├── merged_frames_{tag}/  000000.jpg…    ← raw, 10 m sequence numbering
  ├── merged_predict_{tag}/ 000000.jpg…    ← bboxed, dashboard "AI Analyzed"
  ├── videos/
  │   ├── {uid}_raw.mp4                    ← consolidated raw video
  │   └── {uid}_annotated.mp4              ← consolidated bboxed video
  └── {period_label}/                      ← v3 NHAI xlsx deliverable
      ├── day_LHS_report.xlsx              ← 3 tabs: MCW / Service Roads / Slip Roads
      ├── day_RHS_report.xlsx
      ├── night_LHS_report.xlsx
      └── night_RHS_report.xlsx

  uid format:   "{road}_W{N}{Mon}{YYYY}_{day|night}_{LHS|RHS}_{MCW|SR|SL}"
                e.g. R186871_W1Jan2026_day_LHS_MCW
  merged_tag:   "{day|night}_{LHS|RHS}_{MCW|SR|SL}"
                e.g. day_LHS_MCW  (one merged_frames_<tag>/ folder per uid)

────────────────────────────────────────────────────────────────────────────
MONGO COLLECTIONS WRITTEN
────────────────────────────────────────────────────────────────────────────

  annotation_segments  one doc per (road_id, uuid):
                       uuid, road_id, total_frames, total_defects,
                       road_length_km, road_rating,
                       period, period_label, uid_suffix, direction,
                       subroad, subroad_code, uid_key,
                       frame_list_data[*] = {timeElapsed, latitude,
                         longitude, location (GeoJSON), chainage_km,
                         orientation, og_file, inference_image,
                         defect_state, inference_info[*] = {label, bbox,
                         severity}}, severity_distribution

                       period       = "W1Jan2026"        (compact, uid-friendly)
                       period_label = "Week-1-Jan-2026"  (raw GCS folder)
                       subroad      = "MCW" | "service-road" | "slip-road"
                       subroad_code = "MCW" | "SR" | "SL"

  inference_data       one doc per (uid, organization):
                       uid = build_uid(road, period=..., uid_suffix=...,
                                       direction=..., subroad=...)
                            e.g. "R186871_W1Jan2026_day_LHS_MCW"
                       period, period_label, uid_suffix, direction,
                       subroad, subroad_code,
                       data.report_1_key … report_4_key, dashboard_df_csv,
                       data.nhai_report_csv (NHAI RFP format),
                       data.frame_list_data, data.total_defects,
                       data.road_length, data.road_rating,
                       data.video_url_raw / video_url_annotated,
                       plot_data.plots.pie_chart2,
                       video_url (top-level) = annotated,
                       video_url_rhs (top-level) = raw

  video_upload         one doc per uid (carries the same period / direction /
                       subroad fields) — joined by Video Library page.

  roads                one doc per road_id; surveys[] array enumerates every
                       (uid_key, period, period_label, uid_suffix, direction,
                       subroad, subroad_code) combo. Used by the Survey Cycle
                       + sub-road dropdowns on the dashboard.

  RoadData             one doc per (road_id, organization) — start/end
                       address, road_length, via_points (from KML), used
                       by the NHAI dashboard.

NHAI deliverable
────────────────────────────────────────────────────────────────────────────
  Per (road_id × period × time × direction), Phase C also uploads:

    gs://datanh11/processed-data/<road_id>/<period_label>/
                                          <uid_suffix>_<direction>_report.xlsx

  3 tabs (MCW / Service Roads / Slip Roads). Each tab is that sub-road's
  per-100 m chainage report (data.dashboard_df_csv). Re-runs overwrite;
  missing sub-roads emit "No data" placeholder tabs that refill once the
  next sub-road's consolidate finishes.

────────────────────────────────────────────────────────────────────────────
USAGE
────────────────────────────────────────────────────────────────────────────

  Most per-road metadata (organization, city, project_title, addresses,
  optional NHAI block) is read from road_metadata.json next to this
  script — keyed by road_id. CLI flags still win when explicitly set.

  ONE-SHOT — process whatever's in the GCS prefix right now and exit:

    /home/shubham/video-processing-pipeline/venv/bin/python pipeline_v3.py \\
        --source-bucket  nhai-upload \\
        --gcs-prefix     "R186871/" \\
        --road-id        R186871 \\
        --create-ls-project          # optional: provision LS project + preds

  WATCH (single road) — long-running poll loop:

    /home/shubham/video-processing-pipeline/venv/bin/python pipeline_v3.py \\
        --source-bucket  nhai-upload \\
        --gcs-prefix     "R186871/" \\
        --road-id        R186871 \\
        --watch \\
        --watch-interval 60          # idle scan interval
        --settle-time    60          # upload-finalisation wait
        --batch-size     32          # accumulate before dispatch
        --parallel       32          # workers per batch (8 per T4 × 4 T4s)
        --create-ls-project

  WATCH ALL ROADS — multi-road, single VM, shared pool:

    /home/shubham/video-processing-pipeline/venv/bin/python pipeline_v3.py \\
        --source-bucket    nhai-upload \\
        --gcs-prefix-root  ""        # empty = bucket root
        --all-roads --watch \\
        --parallel 32 --batch-size 32 \\
        --create-ls-project

  REPROCESS — Phase-C-only re-run (no YOLO, no frame extraction):

    /home/shubham/video-processing-pipeline/venv/bin/python pipeline_v3.py \\
        --source-bucket  nhai-upload \\
        --gcs-prefix     "R186871/" \\
        --road-id        R186871 \\
        --uid-suffix     W1Jan2026_day_LHS_MCW \\
        --merged-tag     day_LHS_MCW \\
        --reprocess

    # watch the merged result.json's `updated` timestamp:
    ... --reprocess --watch --watch-interval 60

  Source layout parsed by parse_source_path() (v3 — only one supported):
    <road>/Week-{N}-{Mon}-{YYYY}/{day|night}/{LHS|RHS}/
                                 {MCW|service-road|slip-road}/*.MP4
    e.g. R186871/Week-1-Jan-2026/day/LHS/MCW/file.mp4
    Files outside this shape (no Week-N folder, missing direction, or an
    unrecognised sub-road name) are silently skipped — they parse to
    period=None and gcs_list_pairs filters them out.

  Useful re-entry flags:
    --skip-phase-a          re-use earlier <stem>.kml.gpx in GCS
    --skip-phase-b          rebuild Phase C from existing annotation_segments
    --skip-merged-frames    skip the GCS frame-folder copy
    --skip-videos           skip the consolidated video stitch
    --kml /path/file.kmz    explicit KML override (else auto-detect in GCS)
    --ls-export-dir <path>  override YOLO inference_info with LS sub-labels
    --severity-map-json p   org-specific {label: severity} overrides
    --metadata-json    p    override path to road_metadata.json
    --reset                 wipe Mongo + processed-data/ for the road first

  Watch mode flags (ignored in one-shot mode):
    --watch                 enable long-running poll loop
    --watch-interval SEC    seconds between idle GCS scans (default 30)
    --settle-time    SEC    upload-finalisation wait (default 30)
    --batch-size     N      videos to accumulate before dispatch (default 8)
    --parallel       N      worker count INSIDE each batch (also one-shot);
                            1=serial, >1=ProcessPoolExecutor (default 1).
                            Sweet spot: 32 with 4 T4s.

  GPU acceleration flags (default ON when nvidia-smi reports >0 GPUs):
    --num-gpus       N      spread Phase B workers round-robin across N GPUs
                            via CUDA_VISIBLE_DEVICES. Auto-detect when unset.
    --ffmpeg-hwaccel cuda|cpu   frame-extract backend. cuda → one ffmpeg/video
                            with NVDEC (~3× faster on 4K, frees CPU). Falls
                            back to cpu on per-file NVDEC errors. Default
                            cuda when --num-gpus > 0.
    --no-nvenc              force libx264 for video stitches even on GPU VMs.

  Bucket / source flags:
    --source-bucket    NAME    GCS bucket to read MP4/GPX/KML from (defaults
                               to the output bucket 'datanh11'). Use to point
                               watch at an ingest-only bucket like nhai-upload.
    --gcs-prefix-root  PATH    parent prefix scanned by --all-roads (default
                               'video-processing-pipelines-data/'; use ""
                               for nhai-upload's bucket-root layout).

  Reprocess mode flags:
    --reprocess                       run Phase C only, from merged result.json
    --reprocess-result-path GS_URL    explicit gs:// path override
    --reprocess-skip-frames           don't redraw merged_predict frames
    --reprocess-skip-videos           don't re-stitch consolidated videos
    (combine --reprocess with --watch + --watch-interval to auto-retrigger)

  Phase D (Label Studio) flags:
    --create-ls-project           after Phase C, create LS project named
                                  uid (e.g. "R186871-W1Jan2026-day-LHS-MCW"),
                                  attach GCS source storage, upload YOLO
                                  bboxes as predictions. Idempotent.
    --ls-credentials   PATH       override path to label_studio_credentials.json

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
       — includes 11b/13b: build_subroad_xlsx_workbook (3-tab NHAI deliverable)
  14. Merged frames folder + consolidated raw/annotated videos
  15. Phase C consolidation + Mongo upserts (per uid_key including subroad)
  16. Watch mode (tracker + poll loop, group key = period × time × dir × subroad)
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
GCS_BUCKET       = "datanh11"           # OUTPUT: processed-data, merged frames, videos
SOURCE_BUCKET    = GCS_BUCKET            # INPUT: MP4 + GPX + KML — overridable via --source-bucket
PROCESSED_PREFIX = "processed-data"

# Mongo — single connection string used everywhere
MONGO_URI = (
    "mongodb+srv://tech_db_user:IK96qWD8AvtbpOHe"
    "@cluster0.nm6pkfg.mongodb.net/roadvision?retryWrites=true&w=majority"
)

# YOLO weights — TensorRT engine preferred for ~3-5× speedup on T4
DEFAULT_ENGINE_PATH = "/home/shubham/epoch270.engine"
DEFAULT_PT_PATH     = "/home/shubham/epoch270.pt"

GPX_NS = "{http://www.topografix.com/GPX/1/1}"

# Frame extraction tunables
EXTRACT_INTERVAL_M     = 10.0   # one frame per 10 m of GPX-walked distance
EXTRACT_TARGET_HEIGHT  = 640    # ffmpeg downscale (preserves aspect)
EXTRACT_FFMPEG_WORKERS = 8      # parallel ffmpeg processes per video (CPU mode)
EXTRACT_FALLBACK_SEC   = 5.0    # one frame per 5 s when GPS has zero motion

# GPU acceleration. Defaults are tuned for the rv-processing VM (4× T4)
# but stay safe on single-GPU / no-GPU machines via auto-detection in main().
# Override via --num-gpus / --ffmpeg-hwaccel / --no-nvenc CLI flags.
#   NUM_GPUS       — workers read this to pin CUDA_VISIBLE_DEVICES round-robin
#   FFMPEG_HWACCEL — "cuda" → one ffmpeg per video using NVDEC (~3× faster on
#                    4K dashcam, frees CPU). "cpu" → classic 8-segment split.
#   USE_NVENC      — True swaps libx264 for h264_nvenc on video stitches.
NUM_GPUS       = 0               # 0 = auto-detect; main() resolves to live count
FFMPEG_HWACCEL = "cuda"          # "cpu" | "cuda"
USE_NVENC      = True

# OCR fallback for trackpoints with corrupted GPS (lat=0 AND lon=0).
# Phase B always tries this when it sees a corrupted trackpoint — the
# OSD overlay burned into the frame is usually still correct. Costs
# ~50-200ms per recovered point (Tesseract). No CLI flag because it's
# a silent fallback: if tesseract isn't installed or the OSD doesn't
# parse, the corrupted point is left alone and the downstream perp-distance
# filter drops it. Capped at OCR_MAX_PER_VIDEO so one totally junk GPX
# can't dominate per-batch wall time.
OCR_STRIP_PCT      = 0.15        # bottom 15% of the frame holds the OSD
OCR_MAX_PER_VIDEO  = 60          # safety cap on attempts per video


def detect_num_gpus() -> int:
    """Count NVIDIA GPUs via `nvidia-smi -L`. Returns 0 if nvidia-smi is
    missing or no GPUs visible. main() uses this to default --num-gpus
    so the same code runs unchanged on dev laptops + the 4-T4 VM."""
    try:
        r = subprocess.run(["nvidia-smi", "-L"], capture_output=True,
                           text=True, timeout=5)
        if r.returncode != 0:
            return 0
        return sum(1 for L in r.stdout.splitlines() if L.startswith("GPU "))
    except Exception:
        return 0


def video_encoder_args(*, gpu_device: int = 0) -> list[str]:
    """Return the ffmpeg encoder args for stitching frames into MP4.

    NVENC (h264_nvenc) replaces libx264 when USE_NVENC is on, dropping
    per-video stitch time from ~10s → ~3s on a T4. NVENC's quality at the
    1 fps / fast preset we use is visually indistinguishable from libx264.

    Caller is expected to follow these args with `-vf <filter> <out_path>`.
    """
    if USE_NVENC:
        return [
            "-c:v", "h264_nvenc",
            "-gpu", str(gpu_device),
            "-preset", "p1",        # p1=fastest, p7=slowest. p1 ≈ libx264 ultrafast
            "-pix_fmt", "yuv420p",
            "-rc", "constqp", "-qp", "28",
        ]
    return [
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-preset", "ultrafast",
        "-crf", "28",
    ]


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
        self.path:        Path = road_log_path(workdir_root, road_id)
        self.timing_path: Path = timing_log_path(workdir_root, road_id)
        self._h:        logging.FileHandler | None = None
        self._timing_h: logging.FileHandler | None = None
        self.road_id = road_id
        self._t0     = 0.0

    def __enter__(self):
        self._h        = attach_road_log_handler(self.path)
        self._timing_h = attach_timing_handler(self.timing_path)
        self._t0 = time.perf_counter()
        log.info("─── log session for %s → %s ───", self.road_id, self.path)
        timing_log.info("=== %s session start (timelog → %s) ===",
                        self.road_id, self.timing_path)
        return self

    def __exit__(self, exc_type, exc, tb):
        total = time.perf_counter() - self._t0
        if exc:
            log.error("─── log session for %s ended with %s ───",
                      self.road_id, exc_type.__name__)
            timing_log.info("=== %s SESSION TOTAL (failed: %s)  %.3fs ===",
                            self.road_id, exc_type.__name__, total)
        else:
            log.info("─── log session for %s ended ───", self.road_id)
            timing_log.info("=== %s SESSION TOTAL                     %.3fs ===",
                            self.road_id, total)
        detach_log_handler(self._h)
        detach_timing_handler(self._timing_h)


def _log_section(title: str) -> None:
    """Visual separator for major flow stages — easy to scan in the log."""
    bar = "─" * max(8, 70 - len(title) - 2)
    log.info("─── %s %s", title, bar)
    timing_log.info("─── %s ───", title)


# ── Per-road timing log (workdir/{road_id}/timelog.log) ───────────────────
# Separate from pipeline.log: contains ONLY timing entries — phase totals
# and per-function durations — so a developer can `cat timelog.log` and
# eyeball the wall-time profile without scrolling through every YOLO line.
# Format:  "<HH:MM:SS>  <label padded to 42>  <seconds with 3dp>".
# Logger has propagate=False so nothing here echoes into pipeline.log or
# the console; the only sink is the per-road FileHandler attached by
# road_log_session (and re-attached inside ProcessPoolExecutor workers).
timing_log = logging.getLogger("pipeline_v2.timing")
timing_log.setLevel(logging.INFO)
timing_log.propagate = False

def timing_log_path(workdir_root: Path, road_id: str) -> Path:
    log_dir = workdir_root / road_id
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / "timelog.log"

def attach_timing_handler(path: Path) -> logging.FileHandler:
    handler = logging.FileHandler(str(path), mode="a", encoding="utf-8")
    handler.setFormatter(logging.Formatter(
        "%(asctime)s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
    ))
    timing_log.addHandler(handler)
    return handler

def detach_timing_handler(handler: logging.Handler | None) -> None:
    if handler is None:
        return
    try:
        timing_log.removeHandler(handler)
        handler.close()
    except Exception:
        pass


class timed:
    """Context manager that records elapsed time to timing_log.

        with timed("yolo_infer_frames"):
            yolo_infer_frames(...)

    No-op when timing_log has no handlers attached (i.e. running outside
    a road_log_session context — the timing line is silently dropped).
    """
    __slots__ = ("label", "_t0")

    def __init__(self, label: str):
        self.label = label
        self._t0 = 0.0

    def __enter__(self):
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        dt = time.perf_counter() - self._t0
        if timing_log.handlers:
            timing_log.info(f"  {self.label:<42} {dt:>9.3f}s")
        return False


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

    # Parse each LineString into its own seg first. Concatenation order
    # gets fixed below — KMLs exported from GPX-stitching tools sometimes
    # store LineStrings in file order rather than driving order, which
    # introduces phantom edges between seg boundaries (e.g. R074598's
    # lucknow_rhs.kmz has 6 LineStrings whose physical-driving order is
    # LS2→LS3→LS1→LS4→LS5→LS6; in-file LS1→LS2 alone adds an 11 km
    # phantom edge that inflates road_length).
    raw_segs: list[list[tuple[float, float]]] = []
    for coord_el in line_strings:
        if not (coord_el is not None and coord_el.text):
            continue
        seg: list[tuple[float, float]] = []
        for tok in coord_el.text.strip().split():
            parts = tok.split(",")
            if len(parts) < 2:
                continue
            seg.append((float(parts[1]), float(parts[0])))   # KML is lon,lat
        if seg:
            raw_segs.append(seg)

    # ── Greedy endpoint-chain reorder ─────────────────────────────────────
    # Walk segs as a chain: pick a starting seg whose first vertex no
    # other seg ends near, then repeatedly append the unused seg whose
    # first vertex is closest to the current last. Threshold is what
    # the actual seam can be (parse_gpx → kml exports often have 50–150 m
    # boundary gaps). If the chain can't cover every seg, fall back to
    # original file order so we don't make things worse.
    STITCH_THRESHOLD_M = 200.0
    def _chain(segs: list[list[tuple[float, float]]]) -> list[list[tuple[float, float]]]:
        n = len(segs)
        if n <= 1:
            return segs
        firsts = [s[0] for s in segs]
        lasts  = [s[-1] for s in segs]
        start = None
        for i in range(n):
            preceded = any(j != i and _haversine_m(lasts[j], firsts[i]) <= STITCH_THRESHOLD_M
                           for j in range(n))
            if not preceded:
                start = i; break
        if start is None:
            return segs   # cyclic or ambiguous — leave original order alone
        order = [start]; used = {start}
        while len(order) < n:
            cur_last = lasts[order[-1]]
            best_j, best_d = None, float("inf")
            for j in range(n):
                if j in used:
                    continue
                d = _haversine_m(cur_last, firsts[j])
                if d < best_d:
                    best_d, best_j = d, j
            if best_j is None or best_d > STITCH_THRESHOLD_M:
                return segs   # gap too large — abandon reorder, keep file order
            order.append(best_j); used.add(best_j)
        return [segs[i] for i in order]

    ordered = _chain(raw_segs)
    if ordered is not raw_segs:
        log.info("KML reorder: %d LineStrings reordered into driving sequence "
                 "(file order had non-contiguous seams)", len(raw_segs))

    # Concatenate, dropping the seam vertex when adjacent segs touch.
    pts: list[tuple[float, float]] = []
    for seg in ordered:
        if pts and seg and (seg[0] == pts[-1] or
                            _haversine_m(pts[-1], seg[0]) <= STITCH_THRESHOLD_M):
            seg = seg[1:] if seg[0] == pts[-1] else seg
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
_GPX_FMT_PATH = Path(__file__).parent / "gpx.fmt"


def _ddmm_to_decimal(x: float) -> float:
    """Convert NMEA-style DDMM.MMMM to decimal degrees. Used by the
    RedTiger binary parser — RedTiger stores lat/lon in this legacy
    NMEA-0183 format inside the proprietary YOUQINGGPS atom."""
    deg = int(x / 100)
    minutes = x - (deg * 100)
    return deg + (minutes / 60)


def _extract_gpx_redtiger_native(mp4_path: str, out_gpx_path: str) -> bool:
    """
    RedTiger F-series dashcams (e.g. F7N) embed GPS in a proprietary
    'YOUQINGGPS' atom that exiftool can't decode. This native parser
    memory-maps the MP4, scans for the byte signature, and unpacks each
    GPS record from the documented offsets.

    Returns True iff ≥1 trackpoint was decoded. Returns False instantly
    when the marker is absent (so it's cheap to call as a "is this a
    RedTiger file?" probe before falling back to exiftool).

    Source: /home/shubham/video-processing-pipline-V2/redtiger_native_extractor.py
    """
    import mmap, struct
    points = []
    try:
        with open(mp4_path, "rb") as f:
            try:
                mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            except (OSError, ValueError):
                return False
            with mm:
                pos = 0
                while True:
                    pos = mm.find(b"YOUQINGGPS", pos)
                    if pos == -1:
                        break
                    try:
                        lat_raw = struct.unpack("<f", mm[pos+0x18:pos+0x1c])[0]
                        lon_raw = struct.unpack("<f", mm[pos+0x1c:pos+0x20])[0]
                        if lat_raw == 0.0 and lon_raw == 0.0:
                            pos += 10; continue
                        speed_kmh  = struct.unpack("<f", mm[pos+0x50:pos+0x54])[0]
                        utc_hour   = struct.unpack("<I", mm[pos+0x20:pos+0x24])[0]
                        utc_minute = struct.unpack("<I", mm[pos+0x24:pos+0x28])[0]
                        utc_second = struct.unpack("<I", mm[pos+0x28:pos+0x2c])[0]
                        utc_year   = struct.unpack("<I", mm[pos+0x2c:pos+0x30])[0]
                        utc_month  = struct.unpack("<I", mm[pos+0x30:pos+0x34])[0]
                        utc_day    = struct.unpack("<I", mm[pos+0x34:pos+0x38])[0]
                        # RedTiger stores 2-digit year offset from century;
                        # current cameras emit 24..30 → 2024..2030.
                        ts = (f"20{utc_year:02d}-{utc_month:02d}-{utc_day:02d}"
                              f"T{utc_hour:02d}:{utc_minute:02d}:{utc_second:02d}Z")
                        points.append({
                            "lat":   f"{_ddmm_to_decimal(lat_raw):.6f}",
                            "lon":   f"{_ddmm_to_decimal(lon_raw):.6f}",
                            "time":  ts,
                            "speed": f"{speed_kmh:.2f}",
                        })
                        pos += 100      # next record sits ~100 bytes later
                    except Exception:
                        pos += 10
    except Exception as e:
        log.debug("    redtiger native parse failed: %s", e)
        return False

    if not points:
        return False

    # Build the GPX with the same structure the RedTiger DVPlayer emits,
    # so any downstream tooling that recognises it stays compatible.
    NS = "http://www.topografix.com/GPX/1/1"
    ET.register_namespace("", NS)
    root = ET.Element(f"{{{NS}}}gpx", attrib={"version":"1.1", "creator":"DVPlayer"})
    md = ET.SubElement(root, f"{{{NS}}}metadata")
    ET.SubElement(md, f"{{{NS}}}name").text = " Produced by RedTiger Player  version 1.72"
    trk = ET.SubElement(root, f"{{{NS}}}trk")
    ET.SubElement(trk, f"{{{NS}}}name").text = f"Track Start Time: {points[0]['time']} "
    trkseg = ET.SubElement(trk, f"{{{NS}}}trkseg")
    for p in points:
        trkpt = ET.SubElement(trkseg, f"{{{NS}}}trkpt",
                              attrib={"lat": p["lat"], "lon": p["lon"]})
        ET.SubElement(trkpt, f"{{{NS}}}ele").text   = "0.0"
        ET.SubElement(trkpt, f"{{{NS}}}time").text  = p["time"]
        ET.SubElement(trkpt, f"{{{NS}}}speed").text = p["speed"]
    tree = ET.ElementTree(root)
    try:
        ET.indent(tree, space="  ", level=0)
    except AttributeError:
        pass    # Python < 3.9 — indent is cosmetic only
    try:
        tree.write(out_gpx_path, xml_declaration=True, encoding="utf-8")
    except Exception as e:
        log.warning("    redtiger native write failed: %s", e)
        return False
    log.info("    gpx extraction: RedTiger native parser → %d trkpts from %s",
             len(points), Path(mp4_path).name)
    return True


def _extract_gpx_exiftool(mp4_path: str, out_gpx_path: str,
                          timeout_sec: int = 600) -> bool:
    """
    CP Plus / Novatek / Viofo / generic dashcam extractor: exiftool with
    the gpx.fmt template walks the embedded GPS stream and emits a GPX.

    Requires:
      • exiftool ≥ 13 in $PATH (12.x silently produces 0 trkpts on Novatek
        atom layouts — same hard-won lesson as gps_test_dashboard.py).
      • gpx.fmt sitting next to pipeline_v2.py.

    No -if filter: that flag evaluates at file level and would always
    fail for dashcams whose GPS lives only in per-frame embedded streams.
    """
    if not _GPX_FMT_PATH.exists():
        log.warning("    gpx extraction: gpx.fmt missing at %s", _GPX_FMT_PATH)
        return False
    cmd = [
        "exiftool", "-ee",
        "-api", "LargeFileSupport=1",
        "-p", str(_GPX_FMT_PATH),
        str(mp4_path),
    ]
    try:
        with open(out_gpx_path, "w") as fh:
            subprocess.run(cmd, stdout=fh, stderr=subprocess.PIPE,
                           text=True, timeout=timeout_sec)
    except subprocess.TimeoutExpired:
        log.warning("    gpx extraction: exiftool timed out on %s",
                    Path(mp4_path).name)
        return False
    except Exception as e:
        log.warning("    gpx extraction: exiftool failed (%s)", e)
        return False
    try:
        size = os.path.getsize(out_gpx_path)
    except OSError:
        return False
    if size == 0:
        try: os.remove(out_gpx_path)
        except OSError: pass
        return False
    try:
        with open(out_gpx_path) as fh:
            head = fh.read(8192)
        if "<trkpt" not in head:
            return False
    except Exception:
        return False
    log.info("    gpx extraction: exiftool → %d KB GPX from %s",
             size // 1024, Path(mp4_path).name)
    return True


def extract_gpx_from_mp4(mp4_path: str, out_gpx_path: str,
                         timeout_sec: int = 600) -> bool:
    """
    Pull GPS out of a dashcam MP4. Tries vendor-specific extractors in
    cheap-first order:

      1. RedTiger native — instant binary scan for `YOUQINGGPS`. Returns
         False in microseconds when the marker isn't present, so it's
         safe to always try first.
      2. exiftool + gpx.fmt — handles CP Plus / Novatek / Viofo / Azdome
         and any other camera whose GPS exiftool understands.

    Returns True iff one of the extractors produced a GPX with ≥1 trkpt.
    Logs which extractor succeeded so failure modes are debuggable.
    """
    # Strategy 1: RedTiger F-series (proprietary YOUQINGGPS atom)
    if _extract_gpx_redtiger_native(mp4_path, out_gpx_path):
        return True

    # Strategy 2: exiftool on standard embedded GPS streams (CP Plus etc.)
    if _extract_gpx_exiftool(mp4_path, out_gpx_path, timeout_sec):
        return True

    log.warning("    gpx extraction: no extractor recognised %s "
                "(tried RedTiger native + exiftool — check exiftool ≥ 13 "
                "and that the camera vendor is supported)",
                Path(mp4_path).name)
    return False


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


# ─────────────────────────────────────────────────────────────────────────────
# Section 6b — OCR fallback for corrupted GPS trackpoints
# ─────────────────────────────────────────────────────────────────────────────
# When a trackpoint has lat=0 AND lon=0 (typical Null-Island sentinel for
# "GPS not yet locked" or corrupted records), we extract the corresponding
# video frame, OCR the bottom strip (where the dashcam burns its OSD —
# date, time, lat/lon, speed), parse the text, and patch the trackpoint.
#
# Per-trackpoint cost: ~50-100ms on Tesseract (single ffmpeg seek + one
# OCR call). Bounded per-video by OCR_MAX_PER_VIDEO so a fully-broken GPX
# doesn't blow the batch wall time. Always-on fallback — silently no-ops
# when there's nothing corrupted, when tesseract isn't installed, or when
# the OSD won't parse.

# Common dashcam OSD coordinate formats:
#   N25.094327 / 25.094327N / N:25.094327
#   N 25 5.66 (DMS-ish, very rare)
#   25:05:39.61N (DMS, less common)
# Most modern dashcams emit decimal-degrees with a hemisphere letter.
_OCR_LAT_RE = re.compile(
    r"""
    \b
    (?P<hemi_pre>[NS])?\s*[:.]?\s*           # optional N/S prefix
    (?P<deg>\d{1,3}\.\d{3,8})                # 25.094327 — decimal mandatory
    \s*°?\s*                                 # (integer-only matches the seconds
    (?P<hemi_post>[NS])?                     #  field of an adjacent timestamp,
    """,                                     #  e.g. "14:57:44 N27..." → lat=44)
    re.IGNORECASE | re.VERBOSE,
)
_OCR_LON_RE = re.compile(
    r"""
    \b
    (?P<hemi_pre>[EW])?\s*[:.]?\s*
    (?P<deg>\d{1,3}\.\d{3,8})
    \s*°?\s*
    (?P<hemi_post>[EW])?
    """,
    re.IGNORECASE | re.VERBOSE,
)
# Date-time formats: 2026-05-06 14:57:39 / 06-05-2026 14:57:39 / 2026/05/06 etc.
_OCR_TIME_RE = re.compile(
    r"""
    (\d{4}|\d{2})[-/.](\d{2})[-/.](\d{4}|\d{2})    # date
    [\sT]+
    (\d{2})[:.](\d{2})[:.](\d{2})                  # time
    """,
    re.VERBOSE,
)
# Speed: 015 KM/H / 15 km/h / 015KMH / 48K M/H (Tesseract often inserts a
# space between K and M when the digits are right next to the letters).
_OCR_SPEED_RE = re.compile(r"(\d+(?:\.\d+)?)\s*K\s*M\s*/?\s*H", re.IGNORECASE)

# Tesseract misreads on dashcam OSDs. The OSD font is white-on-dark, small,
# and butts up against scene pixels, so common substitutions creep in:
#   E → £   (the pound symbol — by far the most frequent on real frames)
#   E → ¬, ¢, €
#   N → H, M  (rare; usually only at edges)
#   0 → O, D  (handled by allowing letters in the digit run? no — kept strict)
# We normalise the few that are unambiguous before regex matching. Keeping
# the substitution list tight on purpose: anything broader risks turning
# scene text into false hemisphere markers.
_OCR_FIXUPS = [
    ("£", "E"),
    ("€", "E"),
    ("¢", "E"),
    ("¬", "E"),
]


def _parse_gps_overlay(text: str) -> dict | None:
    """
    Extract {lat, lon, time_str, speed_kmh} from OCR'd OSD text.
    Returns None when lat OR lon is missing — partial recoveries aren't
    useful for milestone walking.
    """
    text = text.replace("\n", " ").replace("|", " ").strip()
    for bad, good in _OCR_FIXUPS:
        text = text.replace(bad, good)
    if not text:
        return None

    # Find a hemisphere-tagged latitude. We prefer matches with explicit
    # N/S because raw decimal numbers also match speed/distance fields.
    lat = lon = None
    for m in _OCR_LAT_RE.finditer(text):
        hemi = (m.group("hemi_pre") or m.group("hemi_post") or "").upper()
        if not hemi: continue
        try:
            v = float(m.group("deg"))
        except ValueError:
            continue
        if 0 < v <= 90:
            lat = v if hemi == "N" else -v
            break
    for m in _OCR_LON_RE.finditer(text):
        hemi = (m.group("hemi_pre") or m.group("hemi_post") or "").upper()
        if not hemi: continue
        try:
            v = float(m.group("deg"))
        except ValueError:
            continue
        if 0 < v <= 180:
            lon = v if hemi == "E" else -v
            break
    if lat is None or lon is None:
        return None

    time_str = None
    tm = _OCR_TIME_RE.search(text)
    if tm:
        a, b, c, hh, mm, ss = tm.groups()
        # Normalise: figure out which token is the year (4 digits).
        if len(a) == 4:        year, month, day = a, b, c
        elif len(c) == 4:      day,  month, year = a, b, c
        else:                  year, month, day = "20" + a, b, c
        try:
            time_str = (f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
                        f"T{int(hh):02d}:{int(mm):02d}:{int(ss):02d}Z")
        except ValueError:
            time_str = None

    speed_kmh = None
    sm = _OCR_SPEED_RE.search(text)
    if sm:
        try: speed_kmh = float(sm.group(1))
        except ValueError: pass

    return {"lat": lat, "lon": lon, "time_str": time_str,
            "speed_kmh": speed_kmh}


def _extract_one_frame_at(mp4_path: str, t_sec: float,
                          gpu_device: int = 0) -> "np.ndarray | None":
    """Pull a single decoded frame at `t_sec` from the MP4. Uses NVDEC
    when available, plain CPU decode otherwise. Returns BGR numpy array
    or None on failure."""
    tmp = Path(tempfile.gettempdir()) / f"ocr_{os.getpid()}_{int(t_sec*1000)}.jpg"
    cmd_base = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
    ]
    if FFMPEG_HWACCEL == "cuda":
        cmd_base += ["-hwaccel", "cuda", "-hwaccel_device", str(gpu_device)]
    cmd = cmd_base + [
        "-ss", f"{t_sec:.3f}",
        "-i", str(mp4_path),
        "-frames:v", "1",
        "-vf", "format=yuvj420p",
        "-q:v", "2",
        str(tmp),
    ]
    try:
        r = subprocess.run(cmd, stdout=subprocess.DEVNULL,
                           stderr=subprocess.PIPE, timeout=30)
        if r.returncode != 0:
            return None
        img = cv2.imread(str(tmp))
        return img
    except Exception:
        return None
    finally:
        try: tmp.unlink()
        except Exception: pass


def _ocr_gps_strip(image_bgr) -> dict | None:
    """OCR the bottom OSD strip of one dashcam frame. Returns parsed
    GPS dict or None. Tesseract-only — falls back gracefully if the
    pytesseract / tesseract binaries aren't installed."""
    try:
        import pytesseract
    except ImportError:
        return None
    h, w = image_bgr.shape[:2]
    strip = image_bgr[int(h * (1 - OCR_STRIP_PCT)):, :]
    # Greyscale + adaptive threshold sharpens fixed-font OSD overlays
    # without blowing out the underlying scene.
    gray = cv2.cvtColor(strip, cv2.COLOR_BGR2GRAY)
    # Most dashcam OSDs are bright on dark background; threshold favours that.
    _, binar = cv2.threshold(gray, 180, 255, cv2.THRESH_BINARY)
    try:
        text = pytesseract.image_to_string(binar, config="--psm 6")
    except Exception:
        return None
    return _parse_gps_overlay(text)


def recover_corrupted_trackpoints(
    trackpoints: list[dict],
    mp4_path: str,
    *,
    gpu_device: int = 0,
    sec_per_trkpt: float = 1.0,
) -> tuple[int, int]:
    """
    Patch trackpoints with (lat==0 AND lon==0) by OCR'ing the dashcam OSD
    on the corresponding video frame. Mutates `trackpoints` in place.
    Returns (n_attempted, n_recovered). Caps total OCR attempts at
    OCR_MAX_PER_VIDEO so a hopelessly-bad GPX doesn't dominate wall time.

    `sec_per_trkpt` is the GPX sampling rate — most dashcams emit 1 Hz,
    so the i-th trackpoint corresponds to t = i seconds in the video.
    """
    # Quick scan: bail out early if there's nothing corrupted to recover.
    # Saves the tesseract binary check + tempfile setup on the common path.
    if not any(tp.get("lat") == 0 and tp.get("lon") == 0 for tp in trackpoints):
        return 0, 0
    n_attempt = n_recover = 0
    for i, tp in enumerate(trackpoints):
        if tp.get("lat") != 0 or tp.get("lon") != 0:
            continue
        if n_attempt >= OCR_MAX_PER_VIDEO:
            log.warning("    OCR cap (%d) hit — leaving remaining corrupted "
                        "trackpoints unrecovered", OCR_MAX_PER_VIDEO)
            break
        n_attempt += 1
        t_sec = i * sec_per_trkpt
        img = _extract_one_frame_at(mp4_path, t_sec, gpu_device=gpu_device)
        if img is None:
            continue
        gps = _ocr_gps_strip(img)
        if not gps:
            continue
        tp["lat"] = gps["lat"]
        tp["lon"] = gps["lon"]
        if gps.get("time_str"):
            tp["time_str"] = gps["time_str"]
        n_recover += 1
    if n_attempt:
        log.info("    OCR recovery: %d/%d corrupted trackpoints recovered "
                 "from frame OSD (bottom %.0f%% strip)",
                 n_recover, n_attempt, OCR_STRIP_PCT * 100)
    return n_attempt, n_recover


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
            text = gcs_download_text(gpx_blob, bucket=SOURCE_BUCKET)
        except Exception as e:
            log.warning("  download failed: %s — skipping pair", e); continue
        try:
            rewritten, stats = rewrite_gpx_with_kml_projection(
                text, poly, cumdist, max_perp_m=max_perp_m)
        except Exception as e:
            log.warning("  rewrite failed: %s — skipping pair", e); continue
        kml_gpx_blob = gpx_blob[:-4] + ".kml.gpx"
        gcs_upload_text(kml_gpx_blob, rewritten,
                        content_type="application/gpx+xml",
                        bucket=SOURCE_BUCKET)
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
    availability without per-road CLI flags. Reads from SOURCE_BUCKET.
    """
    cli = gcs_client()
    candidates = [
        b.name for b in cli.list_blobs(SOURCE_BUCKET, prefix=prefix)
        if b.name.lower().endswith((".kmz", ".kml"))
        and not any(skip in b.name for skip in _SKIPPED_SUBPATHS)
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda n: (0 if n.lower().endswith(".kmz") else 1, n))
    chosen = candidates[0]
    local = Path(tempfile.gettempdir()) / f"v2_kml_{Path(chosen).name}"
    cli.bucket(SOURCE_BUCKET).blob(chosen).download_to_filename(str(local))
    log.info("auto-detected KML in GCS prefix: gs://%s/%s → %s",
             SOURCE_BUCKET, chosen, local)
    return str(local)


def gcs_find_kml_for_uid(road_id:      str,
                         period_label: str | None,
                         uid_suffix:   str | None,
                         direction:    str | None,
                         subroad:      str | None = None) -> str | None:
    """
    Direction- and sub-road-aware KML lookup.

    v3 priority (most-specific → least-specific, first match wins):
      1. {road_id}/{period_label}/{day|night}/{LHS|RHS}/{subroad}/  ← per-subroad
      2. {road_id}/{period_label}/{day|night}/{LHS|RHS}/            ← per-direction
      3. {road_id}/{period_label}/{day|night}/                       ← per time
      4. {road_id}/{period_label}/                                   ← per period
      5. {road_id}/                                                  ← per road

    `period_label` should be the raw folder name (e.g. 'Week-1-Jan-2026'),
    not the compact code. Pass None for legacy single-period roads.
    """
    prefixes: list[str] = []
    if period_label and uid_suffix and direction and subroad:
        prefixes.append(f"{road_id}/{period_label}/{uid_suffix}/{direction}/{subroad}/")
    if period_label and uid_suffix and direction:
        prefixes.append(f"{road_id}/{period_label}/{uid_suffix}/{direction}/")
    if period_label and uid_suffix:
        prefixes.append(f"{road_id}/{period_label}/{uid_suffix}/")
    if period_label:
        prefixes.append(f"{road_id}/{period_label}/")
    prefixes.append(f"{road_id}/")

    cli = gcs_client()
    for pref in prefixes:
        # List only one level — don't pick up a KML from a sibling subfolder.
        cand: list[str] = []
        for b in cli.list_blobs(SOURCE_BUCKET, prefix=pref, delimiter="/"):
            if (b.name.lower().endswith((".kmz", ".kml"))
                    and not any(skip in b.name for skip in _SKIPPED_SUBPATHS)):
                cand.append(b.name)
        if not cand:
            continue
        cand.sort(key=lambda n: (0 if n.lower().endswith(".kmz") else 1, n))
        chosen = cand[0]
        local = Path(tempfile.gettempdir()) / f"v3_kml_{Path(chosen).name}"
        cli.bucket(SOURCE_BUCKET).blob(chosen).download_to_filename(str(local))
        log.info("auto-detected KML for uid=%s period=%s %s %s sub=%s: "
                 "gs://%s/%s",
                 road_id, period_label, uid_suffix, direction, subroad,
                 SOURCE_BUCKET, chosen)
        return str(local)
    return None


# NHAI sub-road taxonomy. Folder name (in GCS) → compact code used in uid.
# Kept short to stay grep-friendly:
#   MCW  = Main Carriage Way
#   SR   = Service Road
#   SL   = Slip Road
_SUBROAD_CODE = {
    "mcw":          "MCW",
    "service-road": "SR",
    "service_road": "SR",
    "slip-road":    "SL",
    "slip_road":    "SL",
}
_SUBROAD_LABEL = {v: k for k, v in {
    "MCW":          "MCW",
    "SR":           "service-road",
    "SL":           "slip-road",
}.items()}

# Human-readable road_type stored per-uid (inference_data + roads.surveys[]).
# Each survey is one sub-road, so road_type reflects which sub-road it is.
_SUBROAD_ROAD_TYPE = {
    "MCW": "Main Carriageway",
    "SR":  "Service Roads",
    "SL":  "Slip Road",
}

_MONTH_ABBR = {"jan","feb","mar","apr","may","jun",
               "jul","aug","sep","oct","nov","dec"}


def _compact_period(label: str | None) -> str | None:
    """'Week-1-Jan-2026' → 'W1Jan2026'. Returns None for unparseable labels."""
    if not label:
        return None
    m = re.match(r"^Week[-_]?(\d+)[-_]?([A-Za-z]+)[-_]?(\d{4})$", label.strip(), re.IGNORECASE)
    if not m:
        return None
    week, mon, yr = m.group(1), m.group(2).capitalize(), m.group(3)
    return f"W{week}{mon}{yr}"


def _compact_to_label(compact: str | None) -> str | None:
    """'W1Jan2026' → 'Week-1-Jan-2026'. Inverse of _compact_period.
    Returns None when the compact form isn't recognizable."""
    if not compact:
        return None
    m = re.match(r"^W(\d+)([A-Za-z]+)(\d{4})$", compact)
    if not m:
        return None
    return f"Week-{m.group(1)}-{m.group(2)}-{m.group(3)}"


def parse_source_path(blob_name: str, road_prefix: str) -> dict:
    """
    Extract per-video metadata from the blob path under `road_prefix`.
    `road_prefix` should point AT the road's own folder, e.g. 'R186871/'.
    road_id itself is NOT returned because every caller already has it.

    v3 layout (NHAI):
        <road>/Week-{N}-{Mon}-{YYYY}/{day|night}/{LHS|RHS}/{MCW|service-road|slip-road}/*.MP4
        → period_label='Week-1-Jan-2026', period='W1Jan2026',
          period_week=1, period_month='Jan', period_year=2026,
          uid_suffix='day'|'night', direction='LHS'|'RHS',
          subroad='MCW'|'service-road'|'slip-road',
          subroad_code='MCW'|'SR'|'SL'

    Any field is None when its folder isn't present.
    """
    rel = blob_name
    if road_prefix and rel.startswith(road_prefix):
        rel = rel[len(road_prefix):]
    parts = rel.strip("/").split("/")
    out = {
        "period_label": None, "period":       None,
        "period_week":  None, "period_month": None, "period_year":  None,
        "uid_suffix":   None, "direction":    None,
        "subroad":      None, "subroad_code": None,
        "filename":     parts[-1] if parts else "",
    }
    if not parts:
        return out
    seg0 = parts[0]
    m = re.match(r"^Week[-_]?(\d+)[-_]?([A-Za-z]+)[-_]?(\d{4})$", seg0, re.IGNORECASE)
    if not m or m.group(2).lower() not in _MONTH_ABBR:
        return out   # period folder missing/malformed — caller skips this file
    out["period_label"] = f"Week-{m.group(1)}-{m.group(2).capitalize()}-{m.group(3)}"
    out["period_week"]  = int(m.group(1))
    out["period_month"] = m.group(2).capitalize()
    out["period_year"]  = int(m.group(3))
    out["period"]       = f"W{m.group(1)}{m.group(2).capitalize()}{m.group(3)}"
    if len(parts) >= 2 and parts[1].lower() in ("day", "night"):
        out["uid_suffix"] = parts[1].lower()
    else:
        return out
    if len(parts) >= 3 and parts[2].upper() in ("LHS", "RHS"):
        out["direction"] = parts[2].upper()
    else:
        return out
    if len(parts) >= 4:
        sub = parts[3].lower()
        if sub in _SUBROAD_CODE:
            out["subroad"]      = parts[3]
            out["subroad_code"] = _SUBROAD_CODE[sub]
    return out


def build_uid(road_id: str, *,
              period:       str | None = None,
              uid_suffix:   str | None = None,
              direction:    str | None = None,
              subroad:      str | None = None,
              fallback_suffix: str = "day",
              # Back-compat: v2 callers passed `survey=<int>`. v3 callers
              # should pass `period="W1Jan2026"` directly; if only survey is
              # given we synthesize "S<n>" so old in-progress data still
              # composes a unique uid (lets reset/dashboards target v2 rows).
              survey:       int | None = None) -> str:
    """
    Construct the uid that keys inference_data per
        (road, period, time-of-day, direction, sub-road).

    v3 NHAI layout:  R186871_W1Jan2026_day_LHS_MCW
    Any None dim is omitted (uid is still globally unique within the
    populated dims).
    """
    suffix = uid_suffix or fallback_suffix
    period_token = period or (f"S{survey}" if survey is not None else None)
    out = [road_id]
    if period_token:
        out.append(period_token)
    out.append(suffix)
    if direction:
        out.append(direction)
    if subroad:
        out.append(subroad)
    return "_".join(out)


def merged_tag_for(uid_suffix: str,
                   direction:  str | None,
                   subroad:    str | None = None,
                   override:   str | None = None) -> str:
    """Compose the suffix for the per-road merged_frames_<tag>/ +
    merged_predict_<tag>/ folders.

    Default = uid_suffix + "_" + direction (+ "_" + subroad_code).
    Examples: 'day_LHS_MCW', 'day_RHS_SR'. Each (time × direction ×
    sub-road) tuple gets its own folder so frames from MCW vs service-
    road never clobber each other.

    `override` lets the caller force a custom tag via the --merged-tag
    CLI flag.
    """
    if override:
        return override
    suffix = uid_suffix or "day"
    tag = suffix
    if direction:
        tag = f"{tag}_{direction}"
    if subroad:
        tag = f"{tag}_{subroad}"
    return tag


_SKIPPED_SUBPATHS = ("/gps-test/",)


def gcs_list_pairs(prefix: str) -> list[dict]:
    """List MP4+GPX pairs under `prefix` in SOURCE_BUCKET, with the path
    metadata each pair carries.

    Returns a list of dicts (NOT tuples), each with:
      mp4, gpx     — full blob names
      road_id      — first path segment under prefix
      period       — compact uid token from 'Week-N-Mon-YYYY' folder
                     (e.g. 'W1Jan2026'), or None when missing
      period_label — raw folder name (e.g. 'Week-1-Jan-2026'), used for
                     KML lookup; None when missing
      uid_suffix   — 'day'|'night' or None
      direction    — 'LHS'|'RHS' or None
      subroad      — 'MCW'|'service-road'|'slip-road' or None (raw folder)
      subroad_code — 'MCW'|'SR'|'SL' or None (compact uid token)

    Skips:
      • Already-projected `.kml.gpx` files (so re-runs don't pair them
        as fresh inputs)
      • Anything under `<road>/gps-test/` (a sandbox directory the
        operator uses to verify GPS extraction without triggering the
        full pipeline)
    """
    cli = gcs_client()
    by_stem: dict[str, dict[str, str]] = defaultdict(dict)
    for blob in cli.list_blobs(SOURCE_BUCKET, prefix=prefix):
        name = blob.name
        # Skip the gps-test sandbox directory inside any road folder.
        if any(skip in name for skip in _SKIPPED_SUBPATHS):
            continue
        if name.lower().endswith(".mp4"):
            by_stem[name[:-4]]["mp4"] = name
        elif name.lower().endswith(".gpx"):
            stem = name[:-4]
            if stem.endswith(".kml"):
                continue
            by_stem[stem]["gpx"] = name
    # road_id is the last component of the prefix (e.g. 'R245345/' → 'R245345').
    road_id_from_prefix = prefix.rstrip("/").rsplit("/", 1)[-1] or None
    out: list[dict] = []
    n_orphan_mp4 = 0
    n_orphan_gpx = 0
    for stem, kinds in sorted(by_stem.items()):
        if "mp4" in kinds and "gpx" in kinds:
            meta = parse_source_path(kinds["mp4"], prefix)
            out.append({
                "mp4":          kinds["mp4"],
                "gpx":          kinds["gpx"],
                "road_id":      road_id_from_prefix,
                "period":       meta["period"],
                "period_label": meta["period_label"],
                "uid_suffix":   meta["uid_suffix"],
                "direction":    meta["direction"],
                "subroad":      meta["subroad"],
                "subroad_code": meta["subroad_code"],
                "needs_gpx_extract": False,
            })
        elif "mp4" in kinds and "gpx" not in kinds:
            # No sibling .gpx — pair the MP4 anyway and flag it for in-process
            # GPX extraction via exiftool from the dashcam's embedded metadata.
            # Many Novatek/RedTiger cameras carry a full GPS track in the MP4
            # itself; we extract it locally just before Phase B's parse step.
            meta = parse_source_path(kinds["mp4"], prefix)
            out.append({
                "mp4":          kinds["mp4"],
                "gpx":          None,
                "road_id":      road_id_from_prefix,
                "period":       meta["period"],
                "period_label": meta["period_label"],
                "uid_suffix":   meta["uid_suffix"],
                "direction":    meta["direction"],
                "subroad":      meta["subroad"],
                "subroad_code": meta["subroad_code"],
                "needs_gpx_extract": True,
            })
            n_orphan_mp4 += 1
        else:
            n_orphan_gpx += 1
    if n_orphan_mp4:
        log.info("gcs_list_pairs: %d MP4(s) without sibling .gpx — "
                 "will extract GPS from MP4 embedded metadata", n_orphan_mp4)
    if n_orphan_gpx:
        log.warning("gcs_list_pairs: %d .gpx file(s) without sibling MP4 — skipped",
                    n_orphan_gpx)
    return out


def gcs_download_text(blob_name: str, bucket: str | None = None) -> str:
    return gcs_client().bucket(bucket or GCS_BUCKET).blob(blob_name).download_as_text()


def gcs_download_bytes(blob_name: str) -> bytes:
    return gcs_client().bucket(GCS_BUCKET).blob(blob_name).download_as_bytes()


def gcs_upload_text(blob_name: str, body: str, content_type: str = "text/plain",
                    bucket: str | None = None) -> None:
    gcs_client().bucket(bucket or GCS_BUCKET).blob(blob_name).upload_from_string(
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


# ── PREVIOUS IBI Guideline severity table (kept for reference) ────────────
# # IBI Guideline severity table — maps each defect/asset name to one of
# # {high, medium, low, none}. Keys are lower-case + plain-hyphen; lookup
# # normalises the input before matching.
# SEVERITY_MAP: dict[str, str] = {
#     # Defect classes
#     "pavement defects": "high", "traffic behaviour": "high",
#     "encroachment": "medium", "illegal parking": "low",
#     "natural": "low", "infrastructure": "low",
#     # Assets — presence alone is not a defect
#     "road surface": "none", "road structure": "none",
#     "pavement marking": "none", "median": "none",
#     "refuge island": "none", "kerb ramp": "none",
#     "foot over bridge (fob)": "none", "pedestrian crossing": "none",
#     "pelican control signal": "none", "pedestrian signal": "none",
#     "speed limit signages": "none", "school zone signage": "none",
#     "pedestrian signage": "none", "metro signages": "none",
#     "speed hump": "none", "rumble strip": "none",
#     "street light": "none", "flood light (high mast)": "none",
#     # Pavement Defects sub-labels
#     "pothole": "high", "rutting": "high",
#     "faulting at joints": "high", "blow-up (buckling)": "high",
#     "cracking": "medium", "corner break (concrete)": "medium",
#     "delamination": "low", "patch": "none",
#     # State sub-labels
#     "damaged": "high", "non-functional": "high",
#     "poor": "high", "poor/faded": "high", "wrong side driving": "high",
#     "missing": "medium", "paint faded": "medium",
#     "damaged/faded": "medium", "not accessible": "medium",
#     "poor/worn out": "medium", "poor/damaged": "medium",
#     "barricading": "medium", "vendor": "medium", "shop spillover": "medium",
#     "visibly obstructed": "low", "obstructed": "low",
#     "overhanging branches": "low", "vegetation overgrowth": "low",
#     "electric pole/infrastructure": "low", "traffic kiosk": "low",
#     "good": "none", "bituminous": "none", "concrete": "none",
#     "paver blocks": "none", "unpaved": "none",
#     "flyover": "none", "underpass": "none",
#     "footpath": "none", "road": "none",
# }

# Defect catalogue — 57 labels split into two count categories:
#   • linear / interval — defects that span a continuous stretch of road
#     (markings, rutting, kerb damage). Future linear-count logic will
#     count these by length/extent rather than per-instance.
#   • spontaneous — point defects (potholes, damaged assets, signage).
#     Counted per instance as today.
# The category is consumed by downstream counters via label_category().
LABEL_CATEGORY: dict[str, str] = {
    # ─── Linear / Interval ──────────────────────────────────────────────
    "rutting":                                            "linear",
    "shoulder - rain cuts":                               "linear",
    "shoulder - edge drop":                               "linear",
    "shoulder - unevenness":                              "linear",
    "shoulder - vegetation growth":                       "linear",
    "damaged kerb":                                       "linear",
    "faded kerb painting":                                "linear",
    "reduced visibility due to plantation growth":        "linear",
    "damaged crash barriers":                             "linear",
    "faded painting concrete crash (cc) barrier":                        "linear",
    "damaged (mbcb) metal beam crash barrier":                                       "linear",
    "damaged (pgr) pedestrian guard rail":                      "linear",
    "barriers - faded painting guard rails":              "linear",
    "faded pavement marking":                             "linear",
    "pavement marking - poor visibility (day)":           "linear",
    "pavement marking - poor visibility (night)":         "linear",
    "bus bay - faded markings":                           "linear",
    "truck lay by - faded markings":                      "linear",
    "work zone - poor diversion arrangement / condition":             "linear",
    "unauthorized median openings":                       "linear",
    # ─── Spontaneous ────────────────────────────────────────────────────
    "potholes":                                           "spontaneous",
    "cracking":                                           "spontaneous",
    "missing plants / irregular gaps (median)":           "spontaneous",
    "deteriorated or damaged plants (median)":            "spontaneous",
    "damaged drain cover slabs":                          "spontaneous",
    "missing drain cover slabs":                          "spontaneous",
    "water stagnation":                                   "spontaneous",
    "damaged footpath tiles / paver blocks":              "spontaneous",
    "missing assets (guard rails)":                       "spontaneous",
    "damaged sign boards / sign structures":              "spontaneous",
    "signage - poor visibility (day)":                    "spontaneous",
    "signage - poor visibility (night)":                  "spontaneous",
    "missing assets (signages)":                          "spontaneous",
    "damaged blinkers":                                   "spontaneous",
    "damaged attenuators":                                "spontaneous",
    "damaged delineators":                                "spontaneous",
    "damaged anti-glare":                                 "spontaneous",
    "damaged road studs":                                 "spontaneous",
    "road studs - poor visibility (day)":                 "spontaneous",
    "road studs - poor visibility (night)":               "spontaneous",
    "damaged rumble strips":                              "spontaneous",
    "damaged hazard markers":                             "spontaneous",
    "bus bay - damaged shelters":                         "spontaneous",
    "bus bay - damaged signages":                         "spontaneous",
    "truck lay by - damaged shelters":                    "spontaneous",
    "truck lay by - damaged signages":                    "spontaneous",
    "damaged highway lights":                             "spontaneous",
    "non-functional highway lights":                      "spontaneous",
    "missing assets (street lights)":                     "spontaneous",
    "work zone - inadequate signboard visibility":        "spontaneous",
    "work zone - inadequate barricading":                 "spontaneous",
    "unauthorized signboards":                            "spontaneous",
    "unauthorized hoardings":                             "spontaneous",
    "illegal parking":                                    "spontaneous",
    "general encroachments":                              "spontaneous",
    "cleanliness - litter":                               "spontaneous",
    "cleanliness - debris":                               "spontaneous",
}

# Severity table for the 57 defect labels above. Keys mirror LABEL_CATEGORY
# (lower-case + plain hyphen); severity_for() normalises inputs before lookup.
SEVERITY_MAP: dict[str, str] = {
    # ─── Linear / Interval ──────────────────────────────────────────────
    "rutting":                                            "high",
    "shoulder - rain cuts":                               "medium",
    "shoulder - edge drop":                               "medium",
    "shoulder - unevenness":                              "medium",
    "shoulder - vegetation growth":                       "low",
    "damaged kerb":                                       "high",
    "faded kerb painting":                                "medium",
    "reduced visibility due to plantation growth":        "high",
    "damaged crash barriers":                             "high",
    "faded painting concrete crash (cc) barrier":                        "medium",
    "damaged (mbcb) metal beam crash barrier":                                       "high",
    "damaged (pgr) pedestrian guard rail":                      "high",
    "barriers - faded painting guard rails":              "medium",
    "faded pavement marking":                             "high",
    "pavement marking - poor visibility (day)":           "medium",
    "pavement marking - poor visibility (night)":         "high",
    "bus bay - faded markings":                           "low",
    "truck lay by - faded markings":                      "low",
    "work zone - poor diversion arrangement / condition":             "high",
    "unauthorized median openings":                       "high",
    # ─── Spontaneous ────────────────────────────────────────────────────
    "potholes":                                           "high",
    "cracking":                                           "medium",
    "missing plants / irregular gaps (median)":           "medium",
    "deteriorated or damaged plants (median)":            "medium",
    "damaged drain cover slabs":                          "high",
    "missing drain cover slabs":                          "high",
    "water stagnation":                                   "medium",
    "damaged footpath tiles / paver blocks":              "medium",
    "missing assets (guard rails)":                       "high",
    "damaged sign boards / sign structures":              "high",
    "signage - poor visibility (day)":                    "medium",
    "signage - poor visibility (night)":                  "high",
    "missing assets (signages)":                          "high",
    "damaged blinkers":                                   "high",
    "damaged attenuators":                                "high",
    "damaged delineators":                                "high",
    "damaged anti-glare":                                 "high",
    "damaged road studs":                                 "high",
    "road studs - poor visibility (day)":                 "medium",
    "road studs - poor visibility (night)":               "high",
    "damaged rumble strips":                              "high",
    "damaged hazard markers":                             "high",
    "bus bay - damaged shelters":                         "medium",
    "bus bay - damaged signages":                         "low",
    "truck lay by - damaged shelters":                    "medium",
    "truck lay by - damaged signages":                    "low",
    "damaged highway lights":                             "high",
    "non-functional highway lights":                      "high",
    "missing assets (street lights)":                     "high",
    "work zone - inadequate signboard visibility":        "medium",
    "work zone - inadequate barricading":                 "high",
    "unauthorized signboards":                            "low",
    "unauthorized hoardings":                             "medium",
    "illegal parking":                                    "low",
    "general encroachments":                              "medium",
    "cleanliness - litter":                               "low",
    "cleanliness - debris":                               "low",
}
_SEV_RANK  = {"high": 3, "medium": 2, "low": 1, "none": 0}
_RANK_NAME = {3: "high", 2: "medium", 1: "low", 0: "none"}


def _dominant_severity(severities) -> str:
    """Mode over a list of severity labels. Ties broken by higher rank
    (high > medium > low > none). Returns 'none' on empty input."""
    counts: dict[str, int] = {}
    for s in severities:
        counts[s] = counts.get(s, 0) + 1
    if not counts:
        return "none"
    return max(counts.items(),
               key=lambda kv: (kv[1], _SEV_RANK.get(kv[0], 0)))[0]


def _per_bucket_severity_and_rating(frames) -> tuple[str, float]:
    """Two-level per-100m aggregation using INSTANCE counts (not raw bboxes):
    Step 1 — bucket each frame by floor(chainage_km / 0.1).
    Step 2 — per bucket:
                spontaneous bbox  → 1 instance
                linear label/side → 1 instance (parallel sides count as 2)
              PCI = max(0, 100 - 10·H - 4·M - L) over instance counts.
    Step 3 — road_severity = mode of per-bucket dominant severities.
            road_rating   = mean of per-bucket PCI ratings (skip empty).
    Returns ('none', 100.0) when no detections exist on the road."""
    buckets = _per_bucket_instance_counts(frames)
    if not buckets:
        return "none", 100.0
    bucket_sevs: list[str] = []
    bucket_ratings: list[int] = []
    for data in buckets.values():
        if not data["severities"]:
            continue
        bucket_sevs.append(_dominant_severity(data["severities"]))
        c = data["sev_counts"]
        h, m, l = c["high"], c["medium"], c["low"]
        bucket_ratings.append(max(0, 100 - 10 * h - 4 * m - 1 * l))
    road_severity = _dominant_severity(bucket_sevs) if bucket_sevs else "none"
    road_rating = (round(sum(bucket_ratings) / len(bucket_ratings), 2)
                   if bucket_ratings else 100.0)
    return road_severity, road_rating


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


def label_category(label: str) -> str:
    """Resolve a defect's count category: 'linear' or 'spontaneous'.
    Returns '' when the label isn't in LABEL_CATEGORY (caller decides how
    to handle unknowns). Same normalisation as severity_for(); substring
    fallback handles slight label variants."""
    n = normalize_label(label).lower()
    if n in LABEL_CATEGORY:
        return LABEL_CATEGORY[n]
    for key, cat in LABEL_CATEGORY.items():
        if key in n or n in key:
            return cat
    return ""


# Strict allowlist: only labels in LABEL_CATEGORY (which has matching entries
# in SEVERITY_MAP) survive. Anything outside this set is dropped at inference
# time AND scrubbed by the Phase C / reprocess retag loops — keeping the bbox
# set drawn on frames, stored in Mongo, sent to LS, and counted in reports all
# matching exactly at 57 labels.
#
# Historical FILTERED_LABELS (kept for documentation only):
#   "excessive plantation growth", "manhole cover", "median separator paint faded",
#   "patching", "pavement damage (severe)", "pavement joint",
#   "stripping/delamination", "unsealed road"


def _is_filtered_label(label: str) -> bool:
    """True if a bbox carrying this label should be dropped — i.e. anything
    not in LABEL_CATEGORY. Uses normalize_label() so en-dash / case variants
    match the lowercase plain-hyphen keys in the dict."""
    return normalize_label(label).lower() not in LABEL_CATEGORY


# ─────────────────────────────────────────────────────────────────────────────
# Linear-defect instance counting
# ─────────────────────────────────────────────────────────────────────────────
# A linear defect (rutting, faded marking, kerb damage, …) typically spans
# many consecutive 10m frames. Counting per-bbox over-reports — a 300m rut
# becomes 30 "detections". The helpers below collapse contiguous frames of
# the same label on the same side of the frame into ONE run.
#
# Side detection: 2-way (left / right at 50% of estimated frame width).
# Frame width is estimated from the data: max(bbox.x + bbox.w) across all
# bboxes is a reliable proxy for the actual frame width on any road with
# more than a handful of detections.
LINEAR_GAP_TOLERANCE_FRAMES = 2   # missed frames allowed inside a run (~20m)


def _detect_frame_width(frames: list[dict]) -> float:
    """Estimate frame width as max(bbox.x + bbox.w) across all detections.
    Returns 1.0 if no bboxes (so divisions by frame width are safe)."""
    max_x = 0.0
    for f in frames:
        for b in (f.get("inference_info") or []):
            bbox = b.get("bbox") or [0, 0, 0, 0]
            x_right = float(bbox[0]) + float(bbox[2])
            if x_right > max_x:
                max_x = x_right
    return max_x or 1.0


def _bbox_side(bbox, frame_width: float) -> str:
    """'left' or 'right' based on bbox center X / frame width split at 50%."""
    x = float((bbox or [0])[0] or 0)
    w = float((bbox or [0, 0, 0])[2] if len(bbox or []) > 2 else 0)
    center_x = x + w / 2.0
    return "left" if center_x < frame_width * 0.5 else "right"


def _collect_linear_runs(
    frames: list[dict],
    *,
    gap_tolerance_frames: int = LINEAR_GAP_TOLERANCE_FRAMES,
) -> list[dict]:
    """Walk frames in chainage order; collapse each linear-category label's
    bboxes into per-side runs. Returns a list of run dicts:
        {label, label_lower, severity, side,
         start_frame_idx, end_frame_idx, mid_frame_idx,
         start_chainage_km, end_chainage_km,
         representative_inf}
    `severity` is the mode of bbox severities in the run.
    Spontaneous labels are NOT collapsed — they don't appear in this output."""
    if not frames:
        return []
    frame_width = _detect_frame_width(frames)
    state: dict[tuple, dict] = {}  # (label_lower, side) -> running state
    runs: list[dict] = []

    def _close(key):
        s = state.pop(key, None)
        if not s:
            return
        infs = s["infs"]; idxs = s["frame_idxs"]
        mid = len(infs) // 2
        runs.append({
            "label":             infs[mid].get("label", ""),
            "label_lower":       key[0],
            "severity":          _dominant_severity(s["severities"]),
            "side":              key[1],
            "start_frame_idx":   s["start_frame_idx"],
            "end_frame_idx":     idxs[-1],
            "mid_frame_idx":     idxs[mid],
            "start_chainage_km": float(frames[s["start_frame_idx"]].get("chainage_km") or 0),
            "end_chainage_km":   float(frames[idxs[-1]].get("chainage_km") or 0),
            "representative_inf": infs[mid],
        })

    for fi, f in enumerate(frames):
        present: dict[tuple, list] = {}  # (label_lower, side) -> [(sev, inf), …]
        for inf in (f.get("inference_info") or []):
            lbl_raw = inf.get("label", "")
            if label_category(lbl_raw) != "linear":
                continue
            lbl_lower = normalize_label(lbl_raw).lower()
            side = _bbox_side(inf.get("bbox") or [0, 0, 0, 0], frame_width)
            sev = (inf.get("severity") or "none").lower()
            present.setdefault((lbl_lower, side), []).append((sev, inf))
        # Extend existing runs or open new ones.
        for key, entries in present.items():
            if key not in state:
                state[key] = {"start_frame_idx": fi, "severities": [],
                              "infs": [], "frame_idxs": [], "gap": 0}
            for sev, inf in entries:
                state[key]["severities"].append(sev)
                state[key]["infs"].append(inf)
                state[key]["frame_idxs"].append(fi)
            state[key]["gap"] = 0
        # Bump gap counter for runs absent in this frame; close if over tolerance.
        for key in list(state.keys()):
            if key not in present:
                state[key]["gap"] += 1
                if state[key]["gap"] > gap_tolerance_frames:
                    _close(key)
    # Flush remaining open runs at EOF.
    for key in list(state.keys()):
        _close(key)
    return runs


def count_label_instances(
    label: str,
    frames: list[dict],
    *,
    gap_tolerance_frames: int = LINEAR_GAP_TOLERANCE_FRAMES,
) -> int:
    """Per-road instance count for `label`.
    - linear labels: number of distinct (side, run) pairs.
    - spontaneous labels: total bbox count.
    - unknown / filtered labels: 0."""
    cat = label_category(label)
    target = normalize_label(label).lower()
    if cat == "spontaneous":
        return sum(1 for f in frames for inf in (f.get("inference_info") or [])
                   if normalize_label(inf.get("label", "")).lower() == target)
    if cat == "linear":
        runs = _collect_linear_runs(frames, gap_tolerance_frames=gap_tolerance_frames)
        return sum(1 for r in runs if r["label_lower"] == target)
    return 0


def _per_bucket_instance_counts(
    frames: list[dict],
    *,
    gap_tolerance_frames: int = LINEAR_GAP_TOLERANCE_FRAMES,
) -> dict[int, dict]:
    """Per-100m bucket counts that respect linear-vs-spontaneous semantics.
    Returns: {bucket_idx: {"sev_counts": {high,medium,low,none: int},
                            "severities":  list[str],
                            "label_counts": {label_lower: int},
                            "label_sevs": {label_lower: list[str]}}}.

    `label_sevs` keeps every instance severity for a label in the bucket so
    callers report the MODE (most-frequent) severity — matching road_severity
    / defect_state — instead of the worst.

    Per bucket:
      • Spontaneous bbox → 1 instance in its severity tally + 1 to label_counts.
      • Linear label PRESENT on a side in the bucket → 1 instance per side
        (max 2). Severity for that side = worst rank of any bbox of that
        label+side in the bucket. label_counts[label] += sides present.
    """
    if not frames:
        return {}
    frame_width = _detect_frame_width(frames)
    out: dict[int, dict] = defaultdict(lambda: {
        "sev_counts":     {"high": 0, "medium": 0, "low": 0, "none": 0},
        "severities":     [],
        "label_counts":   defaultdict(int),
        "label_sevs":     defaultdict(list),
    })
    # First pass: spontaneous bboxes are tallied directly; linear bboxes are
    # bucketed by (label, side) within each chainage bucket so we can collapse
    # them after walking all frames.
    linear_buckets: dict[int, dict[tuple, str]] = defaultdict(dict)
    for f in frames:
        k = int(float(f.get("chainage_km") or 0) / 0.1)
        for inf in (f.get("inference_info") or []):
            lbl_raw = inf.get("label", "")
            cat = label_category(lbl_raw)
            sev = (inf.get("severity") or "none").lower()
            if cat == "spontaneous":
                lbl_lower = normalize_label(lbl_raw).lower()
                out[k]["sev_counts"][sev] += 1
                out[k]["severities"].append(sev)
                out[k]["label_counts"][lbl_lower] += 1
                out[k]["label_sevs"][lbl_lower].append(sev)
            elif cat == "linear":
                lbl_lower = normalize_label(lbl_raw).lower()
                side = _bbox_side(inf.get("bbox") or [0, 0, 0, 0], frame_width)
                key = (lbl_lower, side)
                prev = linear_buckets[k].get(key)
                if prev is None or _SEV_RANK.get(sev, 0) > _SEV_RANK.get(prev, 0):
                    linear_buckets[k][key] = sev
    # Collapse linear (label, side) entries → one instance per side per bucket.
    for k, sides in linear_buckets.items():
        for (lbl_lower, _side), sev in sides.items():
            out[k]["sev_counts"][sev] += 1
            out[k]["severities"].append(sev)
            out[k]["label_counts"][lbl_lower] += 1
            out[k]["label_sevs"][lbl_lower].append(sev)
    return dict(out)


# ─────────────────────────────────────────────────────────────────────────────
# Section 7 — 52-label colour palette + bbox drawer
# ─────────────────────────────────────────────────────────────────────────────
# Palette mirrors V1's annotation-pipeline/annotations.py (regular-hyphen
# keys, RGB tuples). Used both by the per-video Phase B drawer that produces
# annotated_frames/predict/* and by the consolidated merged_predict_<tag>/*
# stitch in Phase C. Labels not in the palette fall back to yellow so they
# remain visible.
# Palette tuned for visibility on light-theme dashboards / Label Studio.
# Stays below ~210 max-brightness so colours read on a white background
# while preserving family groupings — pavement defects RED, shoulder BROWN,
# drainage BLUE, kerbs PURPLE, signage GOLD/ORANGE, markings GREEN, bus bay
# PINK, truck lay-by MAGENTA, lights CYAN/TEAL, work zones bright RED,
# encroachments OLIVE, cleanliness DARK OLIVE, missing assets SLATE.
# Exactly 57 labels — matches LABEL_CATEGORY + SEVERITY_MAP.
LABEL_COLORS_RGB: dict[str, tuple[int, int, int]] = {
    # Pavement defects — RED family
    "Potholes":                                            (220,  20,  60),  # crimson
    "Cracking":                                            (255,  69,   0),  # orangered
    "Rutting":                                             (255, 140,   0),  # darkorange

    # Shoulder — BROWN family
    "Shoulder - Rain Cuts":                                (160,  82,  45),  # sienna
    "Shoulder - Edge Drop":                                (139,  69,  19),  # saddlebrown
    "Shoulder - Unevenness":                               (210, 105,  30),  # chocolate
    "Shoulder - Vegetation Growth":                        (107, 142,  35),  # olivedrab

    # Kerb / median / plantation — PURPLE family
    "Damaged Kerb":                                        (148,   0, 211),  # darkviolet
    "Faded Kerb Painting":                                 (186,  85, 211),  # mediumorchid
    "Reduced Visibility Due to Plantation Growth":         ( 75,   0, 130),  # indigo
    "Missing Plants / Irregular Gaps (Median)":            ( 34, 139,  34),  # forestgreen
    "Deteriorated or Damaged Plants (Median)":             (  0, 100,   0),  # darkgreen

    # Drains / water — BLUE family
    "Damaged Drain Cover Slabs":                           ( 30, 144, 255),  # dodgerblue
    "Missing Drain Cover Slabs":                           (  0,   0, 205),  # mediumblue
    "Water Stagnation":                                    (  0, 191, 255),  # deepskyblue

    # Footpath / barriers — GRAY family
    "Damaged Footpath Tiles / Paver Blocks":               (105, 105, 105),  # dimgray
    "Damaged Crash Barriers":                              ( 70,  70,  70),  # darkgray
    "Damaged (MBCB) Metal Beam Crash Barrier":             ( 50,  50,  50),  # near-black
    "Damaged (PGR) Pedestrian Guard Rail":                 ( 47,  79,  79),  # darkslategray
    "Faded Painting Concrete Crash (CC) Barrier":          (112, 128, 144),  # slategray
    "Barriers - Faded Painting Guard Rails":               ( 95, 158, 160),  # cadetblue

    # Signage — ORANGE / GOLD family
    "Damaged Sign Boards / Sign Structures":               (255, 165,   0),  # orange
    "Signage - Poor Visibility (Day)":                     (255, 191,   0),  # amber
    "Signage - Poor Visibility (Night)":                   (218, 165,  32),  # goldenrod

    # Blinkers / attenuators / delineators / anti-glare — CORAL family
    "Damaged Blinkers":                                    (255,  99,  71),  # tomato
    "Damaged Attenuators":                                 (205,  92,  92),  # indianred
    "Damaged Delineators":                                 (188, 143, 143),  # rosybrown
    "Damaged Anti-Glare":                                  (255, 127,  80),  # coral

    # Road studs / rumble strips / hazard — GREEN family
    "Damaged Road Studs":                                  ( 50, 205,  50),  # limegreen
    "Road Studs - Poor Visibility (Day)":                  ( 60, 179, 113),  # mediumseagreen
    "Road Studs - Poor Visibility (Night)":                ( 46, 139,  87),  # seagreen
    "Damaged Rumble Strips":                               ( 32, 178, 170),  # lightseagreen
    "Damaged Hazard Markers":                              (  0, 139, 139),  # darkcyan

    # Pavement marking — YELLOW-GREEN family
    "Faded Pavement Marking":                              (124, 200,   0),  # darker lawngreen
    "Pavement Marking - Poor Visibility (Day)":            (154, 205,  50),  # yellowgreen
    "Pavement Marking - Poor Visibility (Night)":          ( 85, 107,  47),  # darkolivegreen

    # Bus bay — DEEP PINK family
    "Bus Bay - Damaged Shelters":                          (255,  20, 147),  # deeppink
    "Bus Bay - Faded Markings":                            (255, 105, 180),  # hotpink
    "Bus Bay - Damaged Signages":                          (199,  21, 133),  # mediumvioletred

    # Truck lay-by — MAGENTA / ROSE family
    "Truck Lay By - Damaged Shelters":                     (219, 112, 147),  # palevioletred
    "Truck Lay By - Faded Markings":                       (255,   0, 255),  # magenta
    "Truck Lay By - Damaged Signages":                     (186,  85, 211),  # mediumorchid

    # Lights — TEAL / STEEL family
    "Damaged Highway Lights":                              (  0, 206, 209),  # darkturquoise
    "Non-Functional Highway Lights":                       ( 70, 130, 180),  # steelblue
    "Missing Assets (Street Lights)":                      ( 72,  61, 139),  # darkslateblue

    # Work zone — BRIGHT RED warning family
    "Work Zone - Inadequate Signboard Visibility":         (255,   0,   0),  # red
    "Work Zone - Inadequate Barricading":                  (178,  34,  34),  # firebrick
    "Work Zone - Poor Diversion Arrangement / Condition":  (165,  42,  42),  # brown

    # Unauthorized / encroachments — OLIVE / MUSTARD family
    "Unauthorized Median Openings":                        (184, 134,  11),  # darkgoldenrod
    "Unauthorized Signboards":                             (189, 183, 107),  # darkkhaki
    "Unauthorized Hoardings":                              (205, 133,  63),  # peru
    "Illegal Parking":                                     (210, 180,  30),  # mustard
    "General Encroachments":                               (139, 117,   0),  # dark mustard

    # Cleanliness — DARK OLIVE family
    "Cleanliness - Litter":                                (128, 128,   0),  # olive
    "Cleanliness - Debris":                                ( 85,  85,   0),  # dark olive

    # Missing assets — SLATE family
    "Missing Assets (Signages)":                           ( 47,  79,  79),  # darkslategray
    "Missing Assets (Guard Rails)":                        (119, 136, 153),  # lightslategray
}
_FALLBACK_RGB = (255, 200, 0)   # vivid amber — labels not in the palette


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
    cumulative     = 0.0
    next_milestone = 0.0

    p0 = out_pts[0]
    milestones.append({
        "video_offset_sec": p0["t"],
        "timeElapsed":      p0["t"],
        "latitude":         p0["lat"],
        "longitude":        p0["lon"],
        "orientation":      "landscapeLeft",
    })
    next_milestone = INTERVAL

    for i in range(1, len(out_pts)):
        prev = out_pts[i - 1]
        p    = out_pts[i]
        seg  = _haversine_m((prev["lat"], prev["lon"]), (p["lat"], p["lon"]))
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


def _extract_frames_nvdec(
    video_path: str,
    milestones: list[dict],
    out_dir: str,
    target_height: int,
    duration: float,
    fps: float,
    gpu_device: int = 0,
) -> int:
    """
    Single-pass NVDEC frame extractor — one ffmpeg per video, decode on GPU.

    Why one ffmpeg (not 8 segments like CPU mode): NVDEC decodes at ~25×
    realtime on a T4, so segment-splitting just creates more NVDEC streams
    and gains nothing while spending memory bandwidth. Total throughput
    comes from running many videos concurrently across the 4 NVDEC chips,
    not from splitting one video.

    Uses one big -vf select expression to keep only the milestone frames.
    `-hwaccel_device <gpu>` is what spreads decode load across the 4 T4s
    (caller passes worker_idx % NUM_GPUS).

    Returns the number of frames actually extracted. Returns 0 (without
    raising) on NVDEC failure so the caller can fall back to CPU mode —
    Novatek-chipset dashcam files sometimes have proprietary atom layouts
    that NVDEC refuses but CPU decoders handle.
    """
    eps = 0.5 / max(fps, 1.0)
    items = [(i, m) for i, m in enumerate(milestones)]
    items_sorted = sorted(items, key=lambda x: x[1]["video_offset_sec"])
    conditions = "+".join(
        f"lte(abs(t-{m['video_offset_sec']:.6f}),{eps:.6f})"
        for _, m in items_sorted)

    tmp_dir = tempfile.mkdtemp(prefix="nvdec_", dir=out_dir)
    out_pat = os.path.join(tmp_dir, "f_%05d.jpg")
    # IMPORTANT: don't pin `-c:v h264_cuvid` explicitly. Dashcam batches
    # mix H.264 and HEVC, and forcing the wrong decoder triggers
    # "Generic error in an external library" before we get a chance to
    # fall back. `-hwaccel cuda` lets ffmpeg auto-select h264_cuvid /
    # hevc_cuvid / av1_cuvid based on the actual input codec.
    #
    # Trailing `format=yuvj420p` is required: NVDEC delivers nv12 (or
    # p010 on 10-bit inputs), and the mjpeg/jpeg encoder only accepts
    # yuvj420p / yuv420p / rgb24. Without the format conversion ffmpeg
    # fails at output-init with the cryptic "Error opening output files:
    # Generic error in an external library".
    cmd = [
        "ffmpeg", "-y",
        "-hide_banner", "-loglevel", "error",
        "-hwaccel", "cuda",
        "-hwaccel_device", str(gpu_device),
        "-i", video_path,
        # Decode happens on GPU; select+scale runs on CPU after frames
        # come back. The select filter discards >99% of decoded frames
        # so we don't burn PCIe bandwidth on full-rate transfer.
        "-vf", (f"select='{conditions}',"
                f"scale=-1:{target_height},format=yuvj420p"),
        "-vsync", "0",
        "-q:v", "4",
        out_pat,
    ]
    t0 = time.time()
    result = subprocess.run(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        # Common: Novatek files with non-standard atoms → NVDEC bails.
        # Caller will fall back to CPU mode.
        log.warning("    NVDEC ffmpeg failed (gpu=%d): %s",
                    gpu_device, (result.stderr or "")[-200:])
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return 0

    produced = sorted(glob.glob(os.path.join(tmp_dir, "f_*.jpg")))
    count = 0
    for j, (orig_idx, _) in enumerate(items_sorted):
        if j >= len(produced):
            continue
        dst = os.path.join(out_dir, f"frame_{orig_idx:05d}.jpg")
        shutil.move(produced[j], dst)
        count += 1
    shutil.rmtree(tmp_dir, ignore_errors=True)
    log.info("    NVDEC extract: %d frames in %.1fs (gpu=%d)",
             count, time.time() - t0, gpu_device)
    return count


def extract_frames_parallel_ffmpeg(
    video_path: str,
    milestones: list[dict],
    out_dir: str,
    *,
    target_height: int = EXTRACT_TARGET_HEIGHT,
    num_workers: int = EXTRACT_FFMPEG_WORKERS,
    hwaccel: str | None = None,
    gpu_device: int = 0,
) -> int:
    """
    Frame extractor with two backends, picked by `hwaccel`:
      • "cuda" → single-pass NVDEC (recommended on T4+ hardware). Falls
                 back to "cpu" automatically on NVDEC errors.
      • "cpu"  → classic 8-segment ffmpeg split (default). Each process
                 decodes only 1/N of the video — speedup on multi-core
                 machines, no GPU needed.

    `hwaccel=None` reads the module-level FFMPEG_HWACCEL constant which
    main() sets from the --ffmpeg-hwaccel CLI flag. This way per-worker
    overrides aren't needed at every call site.

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

    # NVDEC path — single ffmpeg, decode on GPU. Falls through to CPU
    # mode on failure so a stubborn Novatek file still gets processed.
    # The "[NVDEC-FALLBACK]" tag in the warning is grep-able for fleet
    # monitoring (count fallback rate per batch).
    chosen = (hwaccel or FFMPEG_HWACCEL).lower()
    if chosen == "cuda":
        n = _extract_frames_nvdec(video_path, milestones, out_dir,
                                  target_height, duration, fps, gpu_device)
        if n > 0:
            return n
        log.warning("    [NVDEC-FALLBACK] %s — NVDEC produced 0 frames, "
                    "retrying on CPU (likely Novatek o4k1 atom or unsupported "
                    "codec). Total CPU-fallback events scrape:"
                    " grep '\\[NVDEC-FALLBACK\\]' workdir/*/pipeline.log | wc -l",
                    Path(video_path).name)

    # CPU path — distribute milestones across `num_workers` time-segments.
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
                    label  = _YOLO_CLASSES.get(cls_id, f"class_{cls_id}")
                    if _is_filtered_label(label):
                        continue
                    conf   = float(box.conf.item())
                    x1, y1, x2, y2 = box.xyxy[0].tolist()
                    local_bboxes.append({
                        "label":      label,
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
    period:          str | None = None,
    period_label:    str | None = None,
    uid_suffix:      str | None = None,
    direction:       str | None = None,
    subroad:         str | None = None,
    subroad_code:    str | None = None,
    gpu_device:      int = 0,
) -> dict | None:
    """
    Runs the full per-video Phase B in-process:
        download → GPX parse → 10 m milestones → ffmpeg extract → YOLO →
        bbox draw → upload {raw,annotated}_frames + result.json + per-UUID
        annotated_video.mp4 → annotation_segments doc.

    Per-video metadata (`period`, `period_label`, `uid_suffix`, `direction`,
    `subroad`, `subroad_code`) extracted from the source path is forwarded
    onto the annotation_segments doc so that Phase C can group segments
    into the right per-uid bucket. All are optional — when None, segments
    fall into the legacy "{road_id}_{uid_suffix}" uid space.

    Returns a dict with summary metadata (uuid, mp4_blob, frame count,
    defect total) or None on hard failure. Idempotent for re-runs against
    the same `run_id`: existing GCS blobs are overwritten, the Mongo doc
    is upserted on (uuid).
    """
    workdir.mkdir(parents=True, exist_ok=True)
    _log_section(f"video {Path(mp4_blob).name}  uuid={run_id}")

    timing_log.info("── process_one_video uuid=%s", run_id[:8])
    _video_t0 = time.perf_counter()

    # 1) Download MP4 + GPX into the per-UUID workdir.
    #    A None gpx_blob means gcs_list_pairs found no sibling .gpx — we
    #    extract one from the MP4's embedded GPS via exiftool after
    #    download.
    local_mp4 = workdir / "video.mp4"
    local_gpx = workdir / "data.gpx"
    log.info("    downloading MP4%s from GCS",
             " + GPX" if gpx_blob else " (no sibling GPX — will extract)")
    with timed("download_mp4_gpx"):
        gcs_client().bucket(SOURCE_BUCKET).blob(mp4_blob).download_to_filename(str(local_mp4))
        gpx_downloaded = False
        if gpx_blob:
            # Try the sibling .gpx first. The Cloud Run trigger can fire
            # the moment the .MP4 finalises — sometimes the .gpx isn't
            # uploaded yet, or the dashcam never produced a sibling. Fall
            # back to embedded-GPS extraction in either case instead of
            # raising.
            try:
                gcs_client().bucket(SOURCE_BUCKET).blob(gpx_blob).download_to_filename(str(local_gpx))
                gpx_downloaded = True
            except Exception as e:
                log.info("    sibling .gpx download failed (%s) — falling "
                         "back to embedded-GPS extraction", e.__class__.__name__)
        if not gpx_downloaded:
            ok = extract_gpx_from_mp4(str(local_mp4), str(local_gpx))
            if not ok:
                log.warning("    no usable GPX (sibling missing AND embedded "
                            "extraction failed) — skipping video")
                return None

    # 2) Parse GPX trackpoints
    with timed("parse_gpx_trackpoints"):
        trackpoints = parse_gpx_trackpoints(local_gpx.read_text(encoding="utf-8", errors="ignore"))
    log.info("    GPX trackpoints: %d", len(trackpoints))
    if not trackpoints:
        log.warning("    GPX has no trackpoints — skipping video")
        return None

    # 2b) OCR fallback for trackpoints whose lat/lon both came back 0.
    #     Some dashcams flush a junk record while the GPS module is still
    #     locking; the OSD overlay burned into the frame is usually still
    #     correct, so we recover by OCR'ing the bottom strip and patching
    #     in place. Silent no-op when there's nothing corrupted, when
    #     tesseract isn't installed, or when the OSD won't parse — the
    #     downstream perp-distance filter handles the unrecovered points.
    with timed("recover_corrupted_trackpoints"):
        recover_corrupted_trackpoints(
            trackpoints, str(local_mp4), gpu_device=gpu_device,
        )

    # 3) Compute 10 m milestones along the GPX walk. Returned empty when
    #    the surveyor was parked the whole clip — we treat that as a hard
    #    skip rather than falling back to time-based extraction (which
    #    produced 0-chainage frames that polluted the dashboard).
    with timed("compute_10m_milestones"):
        milestones = compute_10m_milestones(trackpoints)
    if not milestones or len(milestones) < 2:
        log.warning("    insufficient motion (only %d milestone%s) — "
                    "skipping video. Likely surveyor parked or GPS jitter.",
                    len(milestones), "" if len(milestones) == 1 else "s")
        return None

    # 4) Copy MP4 to RAM disk for fast parallel ffmpeg seeks. Pre-check
    #    /dev/shm headroom — copying when full just produces a partial
    #    file + cryptic ffmpeg errors. With 32 parallel workers each
    #    holding ~300 MB, /dev/shm fills fast on burst starts.
    raw_dir = workdir / "raw_frames"; raw_dir.mkdir(parents=True, exist_ok=True)
    ann_dir = workdir / "annotated_frames"; ann_dir.mkdir(parents=True, exist_ok=True)

    ram_mp4 = Path("/dev/shm") / f"v2_{run_id}.mp4"
    try:
        mp4_size_mb = local_mp4.stat().st_size / (1024 * 1024)
        shm_free_mb = shutil.disk_usage("/dev/shm").free / (1024 * 1024)
    except Exception:
        mp4_size_mb = shm_free_mb = 0
    # Headroom = need at least 1.2× the MP4 size so concurrent workers
    # don't all rush in and ENOSPC. If headroom is tight, decode from disk.
    if shm_free_mb >= mp4_size_mb * 1.2 and mp4_size_mb > 0:
        try:
            shutil.copy2(str(local_mp4), str(ram_mp4))
            ext_video = str(ram_mp4)
            log.info("    video copied to /dev/shm  (%.0f MB; %.0f MB free)",
                     mp4_size_mb, shm_free_mb)
        except Exception as e:
            log.warning("    /dev/shm copy failed (%s) — using disk video", e)
            ext_video = str(local_mp4)
    else:
        log.info("    /dev/shm low headroom (need %.0f MB, have %.0f MB) "
                 "— decoding from disk (no copy)",
                 mp4_size_mb * 1.2, shm_free_mb)
        ext_video = str(local_mp4)

    # 5) Extract frames in parallel (uses NVDEC when FFMPEG_HWACCEL=cuda,
    #    pinned to this worker's assigned GPU for round-robin load balance)
    with timed("extract_frames_parallel_ffmpeg"):
        extracted = extract_frames_parallel_ffmpeg(
            ext_video, milestones, str(raw_dir), gpu_device=gpu_device,
        )
    if extracted == 0:
        if ram_mp4.exists():
            ram_mp4.unlink()
        log.warning("    no frames extracted — skipping video"); return None

    # Truncated-MP4 detection: ffmpeg silently produces fewer frames than
    # milestones when the input is incomplete (interrupted upload, video
    # stream ends mid-file, audio-only tail). Loud warning so it shows up
    # in scrubbing later — partial Phase B output still gets used so the
    # rest of the road isn't blocked.
    expected = len(milestones)
    if extracted < expected:
        ratio = extracted / expected
        log.warning("    PARTIAL extraction: %d/%d frames (%.0f%%) — "
                    "MP4 likely truncated (interrupted upload, audio-only tail, "
                    "or codec corruption past timestamp %.1fs)",
                    extracted, expected, ratio * 100,
                    milestones[extracted].get("video_offset_sec", 0))

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
    with timed("yolo_infer_frames"):
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
    with timed("draw_overlays_on_jpeg (all frames)"):
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
    with timed("upload_frames_to_gcs"):
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

        def _stitch(encoder_args: list[str]) -> bool:
            cmd = [
                "ffmpeg", "-y",
                "-hide_banner", "-loglevel", "error",
                "-framerate", "1",
                "-i", str(ann_dir / "frame_%05d.jpg"),
                *encoder_args,
                "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
                "-r", "30",
                str(av_local),
            ]
            try:
                subprocess.run(cmd, check=True,
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE)
                return True
            except (subprocess.CalledProcessError, FileNotFoundError) as e:
                stderr = ""
                if isinstance(e, subprocess.CalledProcessError) and e.stderr:
                    stderr = e.stderr.decode(errors="ignore")[-160:].strip()
                log.warning("    annotated_video.mp4 stitch failed (%s)%s",
                            e, f" — {stderr}" if stderr else "")
                return False

        # NVENC first; fall back to libx264 on session-limit / driver
        # errors (T4 consumer driver caps NVENC sessions per GPU at ~2,
        # so under heavy --parallel some workers will collide on a busy
        # GPU and exit 187 / NV_ENC_ERR_NO_ENCODE_DEVICE).
        ok = _stitch(video_encoder_args(gpu_device=gpu_device))
        if not ok and USE_NVENC:
            log.info("    retrying annotated_video.mp4 with libx264")
            ok = _stitch([
                "-c:v", "libx264", "-pix_fmt", "yuv420p",
                "-preset", "ultrafast", "-crf", "28",
            ])
        if ok:
            gcs_upload_file(av_blob, str(av_local), content_type="video/mp4")
            log.info("    annotated_video.mp4 uploaded")

    # 13) Build frame_list_data + severity distribution + Mongo annotation_segments
    frame_list_data: list[dict] = []
    cum_km = 0.0
    prev_pt: tuple[float, float] | None = None
    for j, frame_idx in enumerate(raw_indices):
        m = milestones[frame_idx]
        cur = (m["latitude"], m["longitude"])
        if prev_pt is not None:
            cum_km += _haversine_m(prev_pt, cur) / 1000.0
        prev_pt = cur
        bboxes = detection_lookup.get(frame_idx) or []
        frame_sevs = [b.get("severity", "none") for b in bboxes]
        worst = _dominant_severity(frame_sevs)
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

    # Per-100m PCI (bucket by chainage / 0.1, average across non-empty buckets).
    road_length_km = round(cum_km, 3)
    _, road_rating = _per_bucket_severity_and_rating(frame_list_data)

    # Segment-level instance counts: each spontaneous bbox = 1 instance;
    # each linear (label, side) run = 1 instance. Same logic flows into
    # severity_distribution so road_rating + counts agree everywhere.
    severity_totals = {"high": 0, "medium": 0, "low": 0, "none": 0}
    total_defects = 0
    for f in frame_list_data:
        for inf in (f.get("inference_info") or []):
            if label_category(inf.get("label", "")) == "spontaneous":
                sev = (inf.get("severity") or "none").lower()
                severity_totals[sev] = severity_totals.get(sev, 0) + 1
                total_defects += 1
    for run in _collect_linear_runs(frame_list_data):
        sev = run["severity"]
        severity_totals[sev] = severity_totals.get(sev, 0) + 1
        total_defects += 1

    db = MongoClient(MONGO_URI)["roadvision"]
    db.annotation_segments.update_one(
        {"uuid": run_id},
        {"$set": {
            "uuid":               run_id,
            "road_id":            road_id,
            "source_mp4":         mp4_blob,         # used by Phase C sort fallback
            "period":             period,           # 'W1Jan2026' | None (compact uid token)
            "period_label":       period_label,     # 'Week-1-Jan-2026' | None (raw folder)
            "uid_suffix":         uid_suffix,       # 'day' | 'night' | None
            "direction":          direction,        # 'LHS' | 'RHS' | None
            "subroad":            subroad,          # 'MCW'|'service-road'|'slip-road'|None
            "subroad_code":       subroad_code,    # 'MCW'|'SR'|'SL'|None (compact uid token)
            "uid_key":            build_uid(road_id, period=period,
                                            uid_suffix=uid_suffix,
                                            direction=direction,
                                            subroad=subroad_code),
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
    timing_log.info("  %-42s %9.3fs    [TOTAL %s]",
                    "process_one_video TOTAL",
                    time.perf_counter() - _video_t0,
                    Path(mp4_blob).name)

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
    period:            str | None = None,
    period_label:      str | None = None,
    uid_suffix:        str | None = None,
    direction:         str | None = None,
    subroad:           str | None = None,
    subroad_code:      str | None = None,
    source_bucket:     str | None = None,
    gpu_device:        int = 0,
    ffmpeg_hwaccel:    str | None = None,
    use_nvenc:         bool | None = None,
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
    # Spawned workers also re-attach the timing handler so per-video
    # timings (download / extract / yolo / upload) land in
    # workdir/{road_id}/timelog.log alongside the parent's phase totals.
    file_handler:   logging.FileHandler | None = None
    timing_handler: logging.FileHandler | None = None
    resolved_log:        Path | None = None
    resolved_timing_log: Path | None = None
    if log_file_path:
        resolved_log = Path(log_file_path)
        # timelog sits next to pipeline.log; derive by sibling rename.
        resolved_timing_log = resolved_log.parent / "timelog.log"
    elif log_workdir_root:
        resolved_log        = road_log_path(Path(log_workdir_root), road_id)
        resolved_timing_log = timing_log_path(Path(log_workdir_root), road_id)
    if resolved_log is not None:
        file_handler = attach_road_log_handler(resolved_log)
    if resolved_timing_log is not None:
        timing_handler = attach_timing_handler(resolved_timing_log)
    # ProcessPoolExecutor workers spawn fresh Python processes that
    # re-import pipeline_v2 — at import time SOURCE_BUCKET defaults
    # back to GCS_BUCKET ("datanh11"). The parent's --source-bucket
    # override didn't ride along across the spawn, so the worker would
    # try to download from datanh11. Restore the override here.
    if source_bucket:
        global SOURCE_BUCKET
        SOURCE_BUCKET = source_bucket

    # Pin THIS worker to one specific GPU so the parallel pool actually
    # uses all 4 T4s instead of all stacking on cuda:0. CUDA_VISIBLE_DEVICES
    # MUST be set before any CUDA library import (YOLO/TRT, ffmpeg cuvid)
    # otherwise it's silently ignored. With CUDA_VISIBLE_DEVICES="N" set,
    # cuda:0 inside this process maps to the physical GPU N — so YOLO's
    # default device="cuda" still works without code changes.
    # ffmpeg's -hwaccel_device sees the *original* device numbering (it
    # talks to the driver directly, not the CUDA runtime), so we keep
    # passing the absolute index for NVDEC/NVENC.
    os.environ["CUDA_VISIBLE_DEVICES"] = str(gpu_device)

    # Propagate parent's GPU-feature flags (these are module-level globals
    # that don't survive the spawn boundary).
    if ffmpeg_hwaccel is not None:
        global FFMPEG_HWACCEL
        FFMPEG_HWACCEL = ffmpeg_hwaccel
    if use_nvenc is not None:
        global USE_NVENC
        USE_NVENC = use_nvenc

    workdir = Path(workdir_str)
    try:
        load_yolo_model(model_weights)
        return process_one_video(
            mp4_blob=mp4_blob, gpx_blob=gpx_blob,
            road_id=road_id, run_id=run_id, workdir=workdir,
            severity_override=severity_override,
            fast=fast,
            period=period, period_label=period_label,
            uid_suffix=uid_suffix, direction=direction,
            subroad=subroad, subroad_code=subroad_code,
            gpu_device=gpu_device,
        )
    except Exception:
        log.exception("[Phase B worker] %s failed", mp4_blob)
        return None
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
        detach_log_handler(file_handler)
        detach_timing_handler(timing_handler)


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
    source_bucket:     str | None = None,
    num_gpus:          int | None = None,
    ffmpeg_hwaccel:    str | None = None,
    use_nvenc:         bool | None = None,
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

    # Resolve GPU + hwaccel choices (caller's > module defaults).
    n_gpus = num_gpus if num_gpus is not None else max(1, NUM_GPUS or 1)
    hwaccel = ffmpeg_hwaccel if ffmpeg_hwaccel is not None else FFMPEG_HWACCEL
    nvenc   = use_nvenc      if use_nvenc      is not None else USE_NVENC

    log.info("[Phase B] dispatching %d videos (parallel=%d  gpus=%d  "
             "ffmpeg=%s  nvenc=%s)",
             len(jobs), parallel, n_gpus, hwaccel, "on" if nvenc else "off")

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
                    period=j.get("period"),
                    period_label=j.get("period_label"),
                    uid_suffix=j.get("uid_suffix"),
                    direction=j.get("direction"),
                    subroad=j.get("subroad"),
                    subroad_code=j.get("subroad_code"),
                    gpu_device=0,         # serial mode → single GPU
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
        # Round-robin assign each job to a GPU. With 4 T4s and parallel=32,
        # each GPU runs ~8 workers — at the compute saturation sweet spot.
        import multiprocessing as _mp
        from concurrent.futures import ProcessPoolExecutor as _PPE
        ctx = _mp.get_context("spawn")
        with _PPE(max_workers=parallel, mp_context=ctx) as pool:
            futs = {}
            for i, j in enumerate(jobs):
                gpu_idx = i % n_gpus
                fut = pool.submit(
                    _phase_b_worker,
                    j["mp4_blob"], j["gpx_blob"], j["road_id"],
                    j["run_id"], str(j["workdir"]),
                    severity_override, model_weights, fast,
                    log_file_path,        # explicit per-uid path (overrides road_id derivation)
                    log_workdir_root,     # else derive {root}/{road_id}/pipeline.log per worker
                    j.get("period"), j.get("period_label"),
                    j.get("uid_suffix"), j.get("direction"),
                    j.get("subroad"), j.get("subroad_code"),
                    source_bucket,        # restored in worker — survives spawn boundary
                    gpu_idx,              # pin worker to this GPU via CUDA_VISIBLE_DEVICES
                    hwaccel,              # ffmpeg backend — cuda or cpu
                    nvenc,                # NVENC for video stitch
                )
                futs[fut] = j
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


def _build_ls_lookup_from_tasks(tasks: list[dict]) -> dict[str, list[dict]]:
    """
    Convert a list of LS tasks (or annotations.json contents) into the
    {trailing_two_path_segments → [bbox dicts]} lookup that
    apply_ls_to_frames consumes. Each task's bboxes are converted from
    LS percentage coords to absolute pixel coords; original_width /
    original_height come from the LS result element. Severity is left
    as 'none' — Phase C's retag fills it in.

    IMPORTANT: every task lands in the lookup, INCLUDING tasks where the
    annotator explicitly drew zero bboxes (i.e. "I reviewed this frame
    and confirmed it has no defects"). The lookup value will be `[]` in
    that case, and apply_ls_to_frames will OVERWRITE the frame's
    inference_info with the empty list — clearing any stale YOLO bboxes
    from the original result.json. Without this, an empty-task frame
    would silently fall back to the old YOLO output and the dashboard
    would still show wrong overlays.
    """
    lookup: dict[str, list[dict]] = {}
    n_bboxes = 0
    n_empty_tasks = 0
    label_hits: Counter[str] = Counter()
    for t in tasks:
        image = (t.get("data") or {}).get("image") or ""
        if not image:
            continue
        parts = image.strip("/").split("/")
        if len(parts) < 2:
            continue
        tail = "/".join(parts[-2:])
        # Always seed the entry — confirms this frame was reviewed.
        if tail not in lookup:
            lookup[tail] = []
        for ann in t.get("annotations", []):
            paired = _ls_pair_results(ann.get("result", []) or [])
            for p in paired:
                if p["ow"] <= 0 or p["oh"] <= 0:
                    continue
                lookup[tail].append({
                    "label":    p["label"],
                    "bbox":     [p["x_pct"] * p["ow"] / 100.0,
                                 p["y_pct"] * p["oh"] / 100.0,
                                 p["w_pct"] * p["ow"] / 100.0,
                                 p["h_pct"] * p["oh"] / 100.0],
                    "severity": "none",
                })
                n_bboxes += 1
                label_hits[p["label"]] += 1
        if not lookup[tail]:
            n_empty_tasks += 1
    log.info("[LS] %d tasks (%d with bboxes, %d explicitly empty), "
             "%d total bboxes, %d distinct labels",
             len(lookup), len(lookup) - n_empty_tasks, n_empty_tasks,
             n_bboxes, len(label_hits))
    return lookup


def load_ls_export_dir(ls_root: str) -> dict[str, list[dict]]:
    """
    Build a lookup keyed by trailing two segments of each task's image URL
    from a multi-folder Label Studio export root (the typical layout when
    multiple LS projects' annotations are aggregated). Each subfolder
    contains an annotations.json. Used by --ls-export-dir.
    """
    root = Path(ls_root)
    if not root.is_dir():
        raise SystemExit(f"--ls-export-dir not found: {ls_root}")
    all_tasks: list[dict] = []
    for folder in sorted(root.iterdir()):
        if not folder.is_dir():
            continue
        ann_path = folder / "annotations.json"
        if not ann_path.exists():
            continue
        all_tasks.extend(json.load(open(ann_path)))
    return _build_ls_lookup_from_tasks(all_tasks)


def load_ls_export_from_gcs(road_id: str, uid: str) -> dict[str, list[dict]] | None:
    """
    Try to fetch the user-uploaded annotations.json sibling next to the
    merged result.json (RV Studio writes here directly):

        gs://datanh11/processed-data/{road_id}/{uid}/annotations.json

    When present, this is the human-corrected source of truth — V2's
    --reprocess uses it (instead of result.json's annotations) so manual
    edits propagate end-to-end. Returns None when the file doesn't exist.
    """
    blob_path = f"{PROCESSED_PREFIX}/{road_id}/{uid}/annotations.json"
    blob = gcs_client().bucket(GCS_BUCKET).blob(blob_path)
    if not blob.exists():
        return None
    try:
        text = blob.download_as_text()
        tasks = json.loads(text)
    except Exception as e:
        log.warning("[reprocess] annotations.json unreadable at gs://%s/%s: %s",
                    GCS_BUCKET, blob_path, e)
        return None
    if not isinstance(tasks, list):
        log.warning("[reprocess] annotations.json not a task list — skipping")
        return None
    log.info("[reprocess] found annotations.json (%d tasks) at gs://%s/%s "
             "→ using as ground truth (overrides result.json bboxes)",
             len(tasks), GCS_BUCKET, blob_path)
    return _build_ls_lookup_from_tasks(tasks)


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


def _mode_severity_str(severities) -> str:
    """Display string for the MODE severity of one label within a 100m bucket.
    Mirrors road_severity / defect_state (mode via _dominant_severity, ties
    broken by higher rank). Empty list or all-'none' → 'Nill'."""
    return {"high": "High", "medium": "Medium",
            "low": "Low"}.get(_dominant_severity(severities), "Nill")


def build_report_1_key(
    frames: list[dict],
    road_length_km: float,
    road_rating: float,
    start_addr: str,
    end_addr: str,
) -> tuple[str, int]:
    """Per-defect-type summary CSV, IBI shape.
    Cols: name,level,value,start,end,roadLength,roadRating,defect,unique_value
    Returns (csv_text, total_defects_using_instance_counts).

    Instance counts:
      • spontaneous label → bbox per bbox (each detection = one defect).
      • linear label    → per-side run (parallel ruts on left+right = 2).
    `unique_value` = number of frames containing any bbox of the label."""
    per_bucket: Counter[tuple[str, str]] = Counter()
    label_frames: dict[str, set[int]] = defaultdict(set)
    all_labels: set[str] = set()
    total_defects = 0

    # Spontaneous: count each bbox individually.
    for i, f in enumerate(frames):
        for inf in (f.get("inference_info") or []):
            label = (inf.get("label") or "").strip()
            if not label:
                continue
            label_frames[label].add(i)
            all_labels.add(label)
            if label_category(label) != "spontaneous":
                continue
            level = _cap_level((inf.get("severity") or "none").lower())
            per_bucket[(label, level)] += 1
            total_defects += 1

    # Linear: collapse to per-side runs.
    for run in _collect_linear_runs(frames):
        label = run["label"].strip() or run["label_lower"]
        all_labels.add(label)
        level = _cap_level(run["severity"])
        per_bucket[(label, level)] += 1
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

    # Per-bucket instance tallies via the shared helper:
    #   spontaneous bbox     → 1 instance
    #   linear (label, side) → 1 instance per side present in the bucket
    bucket_data = _per_bucket_instance_counts(frames)

    # Helper: case-correct lookup from lowercase catalogue key to the
    # original-casing label seen in detections.
    lower_to_orig = {normalize_label(l).lower(): l for l in labels}

    for idx, bucket in enumerate(sorted_buckets):
        flist = bucket_frames[bucket]
        data  = bucket_data.get(bucket, {"sev_counts": {"high":0,"medium":0,"low":0,"none":0},
                                          "label_counts": {}, "label_sevs": {}})
        label_counts_lower   = data["label_counts"]
        label_sevs_lower     = data["label_sevs"]
        sev_counts = data["sev_counts"]

        # PCI from instance counts in this bucket.
        pci = max(0, 100 - 10 * sev_counts["high"]
                       -  4 * sev_counts["medium"]
                       -  1 * sev_counts["low"])

        # Range label like "1000-1100m" — matches chainage_report.csv. When
        # consecutive segments have a physical gap, the missing buckets are
        # absent from sorted_buckets, so the labels naturally jump
        # (…1000-1100m → 2900-3000m) instead of pretending the road is
        # contiguous. The dashboard reads this column verbatim.
        row = [idx + 1, f"{bucket * 100}-{(bucket + 1) * 100}m",
               flist[0].get("latitude", 0),  flist[0].get("longitude", 0),
               flist[-1].get("latitude", 0), flist[-1].get("longitude", 0),
               pci]
        for lbl in labels:
            key = normalize_label(lbl).lower()
            count = label_counts_lower.get(key, 0)
            row += [count, _mode_severity_str(label_sevs_lower.get(key, [])),
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

    # Per-bucket instance tallies (shared helper — same as build_report_2_key).
    bucket_data = _per_bucket_instance_counts(frames)

    for idx, bucket in enumerate(sorted_buckets):
        flist = bucket_frames[bucket]
        data  = bucket_data.get(bucket, {"sev_counts": {"high":0,"medium":0,"low":0,"none":0},
                                          "label_counts": {}, "label_sevs": {}})
        label_counts_lower   = data["label_counts"]
        label_sevs_lower     = data["label_sevs"]
        sev_counts = data["sev_counts"]

        pci = max(0, 100 - 10 * sev_counts["high"]
                       -  4 * sev_counts["medium"]
                       -  1 * sev_counts["low"])

        row = [idx + 1, f"{bucket * 100}-{(bucket + 1) * 100}m",
               flist[0].get("latitude", 0),  flist[0].get("longitude", 0),
               flist[-1].get("latitude", 0), flist[-1].get("longitude", 0),
               pci]
        for lbl in labels:
            key = normalize_label(lbl).lower()
            count = label_counts_lower.get(key, 0)
            row += [count, _mode_severity_str(label_sevs_lower.get(key, [])),
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


def _merged_predict_url(road_id: str, merged_tag: str, frame_idx: int) -> str:
    """Full https URL of the consolidated merged_predict_<tag>/<seq>.jpg in
    GCS for the frame at `frame_idx` in the merged_frames list (seq = idx*10).
    Returns '' when road_id or merged_tag is missing — callers should treat
    that as 'no URL available'."""
    if not road_id or not merged_tag:
        return ""
    return (f"https://storage.googleapis.com/{GCS_BUCKET}/"
            f"{PROCESSED_PREFIX}/{road_id}/merged_predict_{merged_tag}/"
            f"{frame_idx * 10:06d}.jpg")


def build_report_3_key(
    frames: list[dict],
    *,
    road_id: str = "",
    merged_tag: str = "",
) -> str:
    """
    Per-frame road-distress CSV with real-world bbox areas.
    Columns: Latitude, Longitude, Road Distress, Area1..AreaN, File_URL.
    One row per frame that has at least one detection. Areas[i] aligns
    with the i-th label in the comma-joined "Road Distress" cell.
    File_URL is the consolidated merged_predict URL in GCS.
    """
    rows: list[dict] = []
    max_areas = 0
    for fi, f in enumerate(frames):
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
        url = (_merged_predict_url(road_id, merged_tag, fi)
               or f.get("inference_image") or f.get("og_file") or "")
        rows.append({
            "Latitude":      f.get("latitude"),
            "Longitude":     f.get("longitude"),
            "Road Distress": ", ".join(labels),
            "_areas":        areas,
            "File_URL":      url,
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


# NHAI Report Format (RFP based) — defect → asset-type category mapping.
# Source: /home/shubham/NHAI Report Format (RFP based).xlsx, sheet REPORT,
# columns C (Asset Type/ Evalution Category) + D (Defect Description).
# Keys here are the Defect Descriptions as they appear in V2's label palette
# (plain hyphen, ASCII), not the en-dash variants in the original NHAI sheet
# — the lookup helper normalises before matching.
NHAI_DEFECT_TO_CATEGORY: dict[str, str] = {
    "Potholes":                                       "PAVEMENT CONDITION",
    "Cracking":                                       "PAVEMENT CONDITION",
    "Rutting":                                        "PAVEMENT CONDITION",
    "Shoulder - Rain Cuts":                           "SHOULDER CONDITION",
    "Shoulder - Edge Drop":                           "SHOULDER CONDITION",
    "Shoulder - Unevenness":                          "SHOULDER CONDITION",
    "Shoulder - Vegetation Growth":                   "SHOULDER CONDITION",
    "Damaged Kerb":                                   "KERB CONDITION",
    "Faded Kerb Painting":                            "KERB CONDITION",
    "Reduced Visibility Due to Plantation Growth":    "PLANTATION/ VEGETATION",
    "Missing Plants / Irregular Gaps (Median)":       "PLANTATION/ VEGETATION",
    "Deteriorated or Damaged Plants (Median)":        "PLANTATION/ VEGETATION",
    "Damaged Drain Cover Slabs":                      "DRAINAGE",
    "Missing Drain Cover Slabs":                      "DRAINAGE",
    "Water Stagnation":                               "DRAINAGE",
    "Damaged Footpath Tiles / Paver Blocks":          "FOOTPATH",
    "Damaged Crash Barriers":                         "CC BARRIER",
    "Faded Painting Concrete Crash (CC) Barrier":     "CC BARRIER",
    "Damaged (MBCB) Metal Beam Crash Barrier":        "METAL BEAM CRASH BARRIER",
    "Missing Assets (Guard Rails)":                   "METAL BEAM CRASH BARRIER",
    "Damaged (PGR) Pedestrian Guard Rail":            "PGR",
    "Barriers - Faded Painting Guard Rails":          "PGR",
    "Damaged Sign Boards / Sign Structures":          "SIGNAGES AND OVERHEAD STRUCTURE",
    "Signage - Poor Visibility (Day)":                "SIGNAGES AND OVERHEAD STRUCTURE",
    "Signage - Poor Visibility (Night)":              "SIGNAGES AND OVERHEAD STRUCTURE",
    "Missing Assets (Signages)":                      "SIGNAGES AND OVERHEAD STRUCTURE",
    "Damaged Blinkers":                               "SAFETY DEVICES",
    "Damaged Attenuators":                            "SAFETY DEVICES",
    "Damaged Delineators":                            "SAFETY DEVICES",
    "Damaged Anti-Glare":                             "SAFETY DEVICES",
    "Damaged Road Studs":                             "ROAD STUDS",
    "Road Studs - Poor Visibility (Day)":             "ROAD STUDS",
    "Road Studs - Poor Visibility (Night)":           "ROAD STUDS",
    "Damaged Rumble Strips":                          "RUMBLE STRIPS",
    "Damaged Hazard Markers":                         "HAZARD MARKER",
    "Faded Pavement Marking":                         "PAVEMENT MARKING",
    "Pavement Marking - Poor Visibility (Day)":       "PAVEMENT MARKING",
    "Pavement Marking - Poor Visibility (Night)":     "PAVEMENT MARKING",
    "Bus Bay - Damaged Shelters":                     "BUS BAY FACILITIES",
    "Bus Bay - Faded Markings":                       "BUS BAY FACILITIES",
    "Bus Bay - Damaged Signages":                     "BUS BAY FACILITIES",
    "Truck Lay By - Damaged Shelters":                "TRUCK LAY BY FACILITIES",
    "Truck Lay By - Faded Markings":                  "TRUCK LAY BY FACILITIES",
    "Truck Lay By - Damaged Signages":                "TRUCK LAY BY FACILITIES",
    "Damaged Highway Lights":                         "HIGHWAY LIGHTING",
    "Non-Functional Highway Lights":                  "HIGHWAY LIGHTING",
    "Missing Assets (Street Lights)":                 "HIGHWAY LIGHTING",
    "Work Zone - Inadequate Signboard Visibility":    "WORK ZONE SAFETY",
    "Work Zone - Inadequate Barricading":             "WORK ZONE SAFETY",
    "Work Zone - Poor Diversion Arrangement / Condition": "WORK ZONE SAFETY",
    "Unauthorized Median Openings":                   "UNAUTHORIZED ACTIVITY/ ENCROACHMENT",
    "Unauthorized Signboards":                        "UNAUTHORIZED ACTIVITY/ ENCROACHMENT",
    "Unauthorized Hoardings":                         "UNAUTHORIZED ACTIVITY/ ENCROACHMENT",
    "Illegal Parking":                                "UNAUTHORIZED ACTIVITY/ ENCROACHMENT",
    "General Encroachments":                          "UNAUTHORIZED ACTIVITY/ ENCROACHMENT",
    "Cleanliness - Litter":                           "CLEANLINESS",
    "Cleanliness - Debris":                           "CLEANLINESS",
}


def _nhai_norm(label: str) -> str:
    """Normalise a label for NHAI category lookup. Handles en-dash vs
    hyphen, collapsed/extra spaces, and case differences."""
    if not label:
        return ""
    return (label
            .replace("–", "-")     # en-dash
            .replace("—", "-")     # em-dash
            .strip()
            .lower())


_NHAI_LOOKUP = {_nhai_norm(k): v for k, v in NHAI_DEFECT_TO_CATEGORY.items()}


def nhai_category_for(label: str) -> str:
    """Map a V2 defect label to its NHAI Asset Type/Evaluation Category.
    Returns '' for labels not in the NHAI taxonomy (e.g., V2-only labels
    like 'Patching', 'Stripping/Delamination')."""
    return _NHAI_LOOKUP.get(_nhai_norm(label), "")


def _populate_nhai_report_sheet(
    ws,
    frames: list[dict],
    *,
    project_meta:  dict | None = None,
    road_id:       str = "",
    merged_tag:    str = "",
    period_label:  str | None = None,
) -> int:
    """Fill `ws` (an openpyxl Worksheet) with the NHAI report layout for
    `frames`. Returns the count of detection rows written (0 when no
    spontaneous detections + no linear runs were found, useful for
    callers that want to flag empty tabs).

    Layout = optional PROJECT DETAILS preamble, header row, then one row
    per detection instance. Asset/Defect columns merged vertically across
    consecutive same-value runs. Defect Image cell hyperlinked to the
    merged_predict frame in GCS.

    Header preserves the original NHAI typo "Evalution" so it lines up
    byte-for-byte with the template:
      Sr. No., Survey Date, Asset Type/ Evalution Category,
      Defect Description, Side, Chainage, Latitude, Longitude, Defect Image

    The Survey Date column carries the per-uid survey TIMELINE (e.g.
    "Week-3-May-2026") rather than a calendar date — NHAI reports group
    defects by the survey campaign week. period_label takes precedence;
    project_meta.survey_date is the legacy fallback for older callers.
    """
    from openpyxl.styles import Alignment

    project_meta = project_meta or {}
    survey_date = (period_label
                   or project_meta.get("survey_date")
                   or _dt.date.today().strftime("%d-%m-%Y"))

    # Section order = NHAI category insertion order; "OTHER" pinned at end.
    asset_order = list(dict.fromkeys(NHAI_DEFECT_TO_CATEGORY.values())) + ["OTHER"]
    asset_rank = {a: i for i, a in enumerate(asset_order)}

    # Survey direction (LHS/RHS) is encoded in merged_tag
    # ("{day|night}_{LHS|RHS}_{MCW|SR|SL}"), not on the per-frame dicts —
    # pull it out so the Side column reflects the carriageway surveyed.
    survey_direction = next(
        (t for t in merged_tag.upper().split("_") if t in ("LHS", "RHS")), "")

    # Collect one record per INSTANCE — spontaneous bbox or linear run.
    detections: list[dict] = []
    for fi, f in enumerate(frames):
        ch_m = int(round(float(f.get("chainage_km") or 0) * 1000))
        url  = _merged_predict_url(road_id, merged_tag, fi)
        for b in (f.get("inference_info") or []):
            label = (b.get("label") or "").strip()
            if not label or label_category(label) != "spontaneous":
                continue
            asset = nhai_category_for(label) or "OTHER"
            detections.append({
                "asset":  asset,
                "defect": normalize_label(label),
                "ch_m":   ch_m,
                "side":   survey_direction,
                "lat":    f.get("latitude", ""),
                "lng":    f.get("longitude", ""),
                "url":    url,
            })
    for run in _collect_linear_runs(frames):
        mid = run["mid_frame_idx"]
        f   = frames[mid]
        ch_m = int(round(float(run["start_chainage_km"]) * 1000))
        asset = nhai_category_for(run["label"]) or "OTHER"
        detections.append({
            "asset":  asset,
            "defect": normalize_label(run["label"]),
            "ch_m":   ch_m,
            "side":   survey_direction,
            "lat":    f.get("latitude", ""),
            "lng":    f.get("longitude", ""),
            "url":    _merged_predict_url(road_id, merged_tag, mid),
        })
    detections.sort(key=lambda d: (asset_rank.get(d["asset"], len(asset_order)),
                                   d["defect"], d["ch_m"]))

    preamble_fields = [
        ("nh_number",      "NH Number"),
        ("project_name",   "Name of the Project"),
        ("upc_code",       "UPC Code"),
        ("start_chainage", "Start Chainage"),
        ("end_chainage",   "End Chainage"),
        ("project_length", "Project Length"),
        ("state",          "Name of the State"),
        ("ro_name",        "RO Name"),
        ("piu_name",       "PIU Name"),
        ("survey_date",    "Survey Date"),
    ]
    cur = 1
    if any(project_meta.get(k) for k, _ in preamble_fields):
        ws.cell(row=cur, column=1, value="PROJECT DETAILS")
        cur += 1
        for k, label in preamble_fields:
            v = project_meta.get(k, "")
            if v:
                ws.cell(row=cur, column=1, value=label)
                ws.cell(row=cur, column=2, value=v)
                cur += 1
        cur += 1

    headers = ["Sr. No.", "Survey Date", "Asset Type/ Evalution Category",
               "Defect Description", "Side", "Chainage",
               "Latitude", "Longitude", "Defect Image"]
    for ci, h in enumerate(headers, start=1):
        ws.cell(row=cur, column=ci, value=h)
    cur += 1

    widths = [8, 14, 30, 30, 8, 12, 14, 14, 22]
    for ci, w in enumerate(widths, start=1):
        ws.column_dimensions[chr(64 + ci)].width = w

    # Write one detection per row. Asset / Defect columns are NOT merged
    # across consecutive same-value runs — operations team requested the
    # label repeated on every row so spreadsheet filters/sorts/exports
    # don't drop the label on rows under the merged header.
    centre = Alignment(vertical="center", horizontal="center", wrap_text=True)
    s_no = 0
    for d in detections:
        s_no += 1
        ws.cell(row=cur, column=1, value=s_no)
        ws.cell(row=cur, column=2, value=survey_date)
        ws.cell(row=cur, column=3, value=d["asset"]).alignment  = centre
        ws.cell(row=cur, column=4, value=d["defect"]).alignment = centre
        ws.cell(row=cur, column=5, value=d["side"])
        ws.cell(row=cur, column=6, value=f"{d['ch_m'] // 1000}+{d['ch_m'] % 1000:03d}")
        ws.cell(row=cur, column=7, value=d["lat"])
        ws.cell(row=cur, column=8, value=d["lng"])
        img_cell = ws.cell(row=cur, column=9,
                           value="View image" if d["url"] else "")
        if d["url"]:
            img_cell.hyperlink = d["url"]
            img_cell.style = "Hyperlink"
        cur += 1

    return len(detections)


def build_nhai_report_xlsx(
    frames: list[dict],
    *,
    project_meta: dict | None = None,
    road_id: str = "",
    merged_tag: str = "",
    period_label: str | None = None,
) -> bytes:
    """Per-detection NHAI-format .xlsx for one uid (single sheet 'REPORT').
    Thin wrapper around `_populate_nhai_report_sheet` — kept so existing
    Phase-C callers stay one-line."""
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "REPORT"
    _populate_nhai_report_sheet(ws, frames, project_meta=project_meta,
                                road_id=road_id, merged_tag=merged_tag,
                                period_label=period_label)
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


def build_report_4_key(
    frames: list[dict],
    reporting_date: str | None = None,
    *,
    road_id: str = "",
    merged_tag: str = "",
) -> str:
    """
    Per-detection summary CSV. One row per bbox.
    Columns: S_No, Reporting_Date, Asset_Type, Defect_Description, Side,
             Chainage, Latitude, Longitude, Defect_Image,
             NH_Number, Project_Name, UPC_Code, State, RO_Name, PIU_Name,
             Survey_Date  (the trailing 6 fields are placeholders V1
             leaves blank; downstream IBI / NHAI tooling fills them).
    Defect_Image is the consolidated merged_predict URL in GCS.
    Row shape:
      • spontaneous label → one row per bbox.
      • linear label    → one row per per-side run (start chainage from the
        run's representative frame; Side column populated 'LEFT' / 'RIGHT').
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

    # Collect one record per instance (spontaneous bbox or linear run), then
    # sort by chainage so the CSV stays in along-road order.
    records: list[dict] = []
    for fi, f in enumerate(frames):
        for b in (f.get("inference_info") or []):
            label = (b.get("label") or "").strip()
            if not label or label_category(label) != "spontaneous":
                continue
            records.append({
                "frame_idx": fi,
                "label":     label,
                "side":      (f.get("direction") or "").upper(),
                "chainage_km": float(f.get("chainage_km") or 0),
                "lat":       f.get("latitude"),
                "lng":       f.get("longitude"),
            })
    for run in _collect_linear_runs(frames):
        mid = run["mid_frame_idx"]
        records.append({
            "frame_idx": mid,
            "label":     run["label"],
            "side":      run["side"].upper(),
            "chainage_km": run["start_chainage_km"],
            "lat":       frames[mid].get("latitude"),
            "lng":       frames[mid].get("longitude"),
        })
    records.sort(key=lambda r: (r["chainage_km"], r["frame_idx"]))

    for s_no, r in enumerate(records, start=1):
        url = (_merged_predict_url(road_id, merged_tag, r["frame_idx"])
               or frames[r["frame_idx"]].get("inference_image")
               or frames[r["frame_idx"]].get("og_file") or "")
        ch_m = int(r["chainage_km"] * 1000)
        bucket = (ch_m // 100) * 100
        chainage_str = f"{bucket}-{bucket + 100}m"
        w.writerow([
            s_no, reporting_date, "Road Defect",
            normalize_label(r["label"]), r["side"],
            chainage_str, r["lat"], r["lng"], url,
            "", "", "", "", "", "",           # NH_Number etc.
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


# ─────────────────────────────────────────────────────────────────────────────
# Section 11b — Multi-tab xlsx workbook (v3 NHAI deliverable)
# ─────────────────────────────────────────────────────────────────────────────
# Per (road_id × period × time × direction), bundle the per-100m chainage
# reports from all three sub-roads (MCW / Service Roads / Slip Roads) into
# one workbook with 3 tabs. Output:
#   gs://datanh11/processed-data/<road_id>/<period_label>/<uid_suffix>_<direction>_report.xlsx
# Reads from inference_data.data.dashboard_df_csv (already CSV text) so this
# is a pure aggregation step — no recomputation. Missing sub-roads emit a
# placeholder tab so the downstream layout stays stable.

_SUBROAD_TAB_ORDER = [
    ("MCW", "MCW",          "MCW"),
    ("SR",  "service-road", "Service Roads"),
    ("SL",  "slip-road",    "Slip Roads"),
]


def _clone_worksheet(src_ws, dst_ws) -> int:
    """Copy values, hyperlinks, fonts, alignment, column widths, and
    merged-cell ranges from `src_ws` into the (empty) `dst_ws`. Used to
    fold a per-uid nhai_report.xlsx sheet into one tab of the bundled
    workbook without rebuilding from raw frames. Returns the number of
    non-empty cells copied (caller logs it for a per-tab progress line)."""
    from copy import copy as _copy

    n = 0
    for row in src_ws.iter_rows():
        for src_cell in row:
            if src_cell.value is None and src_cell.hyperlink is None:
                continue
            dst_cell = dst_ws.cell(row=src_cell.row, column=src_cell.column,
                                   value=src_cell.value)
            if src_cell.has_style:
                dst_cell.font          = _copy(src_cell.font)
                dst_cell.fill          = _copy(src_cell.fill)
                dst_cell.alignment     = _copy(src_cell.alignment)
                dst_cell.border        = _copy(src_cell.border)
                dst_cell.number_format = src_cell.number_format
            if src_cell.hyperlink is not None:
                dst_cell.hyperlink = _copy(src_cell.hyperlink)
                dst_cell.style     = "Hyperlink"
            n += 1
    # Column widths
    for col_letter, dim in src_ws.column_dimensions.items():
        if dim.width:
            dst_ws.column_dimensions[col_letter].width = dim.width
    # Merged cell ranges
    for mr in src_ws.merged_cells.ranges:
        dst_ws.merge_cells(str(mr))
    return n


def build_subroad_xlsx_workbook(
    *,
    road_id:      str,
    period:       str,
    period_label: str,
    uid_suffix:   str,
    direction:    str,
    project_meta: dict | None = None,   # accepted for API symmetry; not used in merge mode
) -> str | None:
    """
    Compose a 3-tab xlsx (MCW / Service Roads / Slip Roads) by merging
    the per-sub-road `nhai_report.xlsx` files that Phase C already
    uploads at:
        gs://datanh11/processed-data/<road_id>/<uid>/nhai_report.xlsx

    Each tab is a byte-faithful copy of one sub-road's existing report
    sheet (values, fonts, hyperlinks, column widths, merged ranges) —
    NOT rebuilt from frames. This guarantees the bundled workbook stays
    in lock-step with what Phase C published per uid.

    Returns the GCS URL of the uploaded workbook, or None when no
    sub-road's nhai_report.xlsx exists yet (Phase C hasn't run for any
    sub-road of this direction).
    """
    try:
        import openpyxl
        from openpyxl import load_workbook
    except ImportError:
        log.warning("[xlsx] openpyxl not installed — skipping workbook build")
        return None

    cli = gcs_client()
    bucket = cli.bucket(GCS_BUCKET)
    wb = openpyxl.Workbook()
    wb.remove(wb.active)   # drop the default empty sheet

    sub_tab_summary: list[str] = []
    any_present = False
    for sub_code, _sub_label, tab_name in _SUBROAD_TAB_ORDER:
        uid = build_uid(road_id, period=period, uid_suffix=uid_suffix,
                        direction=direction, subroad=sub_code)
        src_blob = f"{PROCESSED_PREFIX}/{road_id}/{uid}/nhai_report.xlsx"
        dst_ws   = wb.create_sheet(title=tab_name)
        blob = bucket.blob(src_blob)
        if not blob.exists():
            dst_ws.cell(row=1, column=1, value=f"No data for {uid}")
            sub_tab_summary.append(f"{tab_name}=missing")
            continue
        body = blob.download_as_bytes()
        try:
            src_wb = load_workbook(io.BytesIO(body), data_only=False)
        except Exception as e:
            dst_ws.cell(row=1, column=1, value=f"Failed to load {uid}: {e}")
            sub_tab_summary.append(f"{tab_name}=load_failed")
            log.warning("[xlsx] couldn't open %s: %s", src_blob, e)
            continue
        # Per-uid nhai_report.xlsx has a single sheet titled 'REPORT'.
        src_ws = src_wb["REPORT"] if "REPORT" in src_wb.sheetnames else src_wb.active
        ncells = _clone_worksheet(src_ws, dst_ws)
        sub_tab_summary.append(f"{tab_name}={ncells}cells")
        any_present = True

    if not any_present:
        log.info("[xlsx] no per-uid nhai_report.xlsx found for "
                 "%s/%s/%s/%s — workbook not uploaded",
                 road_id, period_label, uid_suffix, direction)
        return None

    buf = io.BytesIO()
    wb.save(buf)
    body = buf.getvalue()

    blob_path = (f"{PROCESSED_PREFIX}/{road_id}/{period_label}/"
                 f"{uid_suffix}_{direction}_report.xlsx")
    bucket.blob(blob_path).upload_from_string(
        body, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    url = f"https://storage.googleapis.com/{GCS_BUCKET}/{blob_path}"
    log.info("[xlsx] %s/%s/%s/%s merged NHAI tabs (%s) → %s",
             road_id, period_label, uid_suffix, direction,
             ", ".join(sub_tab_summary), url)
    return url


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
        {"$set": {"data.frame_list_data": _persistable_frames(frames)}},
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
            *video_encoder_args(gpu_device=0),    # always GPU 0 for the
            # consolidated stitch — runs once per road in the main
            # process, not in a worker pool.
            "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
            str(out_local),
        ]
        try:
            subprocess.run(cmd, check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            log.warning("[video][%s] ffmpeg failed: %s — retrying with libx264", label, e)
            # Fallback to libx264 on NVENC failure (device contention etc.)
            cmd_cpu = [
                "ffmpeg", "-y",
                "-framerate", str(fps),
                "-i", str(Path(td) / "%06d.jpg"),
                "-c:v", "libx264", "-pix_fmt", "yuv420p",
                "-preset", "ultrafast", "-crf", "28",
                "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
                str(out_local),
            ]
            try:
                subprocess.run(cmd_cpu, check=True,
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            except (subprocess.CalledProcessError, FileNotFoundError) as e2:
                log.warning("[video][%s] CPU ffmpeg also failed: %s", label, e2)
                return None
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
        {"$set": {"data.frame_list_data": _persistable_frames(frames)}},
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
    period:            str | None = None,
    period_label:      str | None = None,
    direction:         str | None = None,
    subroad:           str | None = None,
    subroad_code:      str | None = None,
    nhai_meta:         dict | None = None,
) -> str | None:
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
    timing_log.info("── consolidate_and_rebuild road=%s uid_suffix=%s",
                    road_id, uid_suffix)
    _phase_c_t0 = time.perf_counter()

    db = MongoClient(MONGO_URI)["roadvision"]
    # Filter segments by uid_key when period/direction/subroad are provided.
    # This is what splits the new 5-axis layout into one inference_data doc
    # per (road, period, day/night, LHS/RHS, sub-road) tuple. When all three
    # are None the function falls back to the legacy "all segments for this
    # road" behaviour, preserving datanh11 backward-compat.
    # `subroad` here is the compact code ('MCW'|'SR'|'SL') — what build_uid
    # appends to the uid_key. Callers may pass `subroad_code=` as the kwarg;
    # we accept both names for ergonomics.
    if subroad is None and subroad_code is not None:
        subroad = subroad_code
    seg_filter: dict = {"road_id": road_id}
    if period is not None or direction is not None or subroad is not None:
        target_uid_key = build_uid(road_id, period=period,
                                   uid_suffix=uid_suffix,
                                   direction=direction,
                                   subroad=subroad)
        seg_filter["uid_key"] = target_uid_key
        log.info("[Phase C] segment filter: uid_key=%s", target_uid_key)
    with timed("load_annotation_segments"):
        segments = list(db.annotation_segments.find(seg_filter))
    if not segments:
        # A sub-road with no segments (e.g. slip-road never uploaded for
        # this road) is NOT a fatal error — skip it so the rest of the run
        # continues. Returning None instead of raising SystemExit, which
        # would kill the whole process mid-batch.
        log.warning("[Phase C] no annotation_segments for %s — skipping this uid",
                    seg_filter.get("uid_key") or road_id)
        return None
    log.info("[Phase C] loaded %d annotation_segments for %s", len(segments), road_id)

    # ── 1+2. Merge frames; stamp UUID + MP4 source on every frame. ────────
    merged_frames: list[dict] = []
    total_frames = 0
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
    if not merged_frames:
        log.warning("[Phase C] all segments empty for %s — skipping this uid", road_id)
        return None

    # ── 3. Sort along-road. KML projection wins when available. ──────────
    def _frame_index(f):
        og = f.get("og_file") or ""
        m = re.search(r"frame[_-]?(\d+)", og)
        return int(m.group(1)) if m else 0

    with timed("sort_frames_along_road"):
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

    # ── 4. Re-stamp chainage_km. Prefer per-segment ground truth: each
    # segment's Phase B run already projected its frames onto the KML
    # polyline and stamped a correct intra-segment chainage_km. Offset
    # each segment's frames by the cumulative road_length of prior
    # segments. Avoids the cumulative-haversine-across-mis-sorted-segments
    # bug (R361058: 25.07 km vs ground-truth 13.27 km when 7 videos were
    # driven out of spatial order and we lack a KML at reprocess time).
    # Index seg_lengths by the SAME key that gets stamped on each frame's
    # `_seg_order` (set in step 1+2 to src_mp4 || seg.source_mp4 ||
    # seg.gcs_uri || str(seg._id)). Earlier this was keyed by str(seg._id)
    # only, so the lookup always missed and every segment got offset=0 —
    # all videos collapsed into the same 0..3km chainage range, leaving
    # the dashboard chainage CSV with only the first segment's rows.
    seg_lengths: dict[str, float] = {}
    for seg in segments:
        seg_uuid_for_match = seg.get("uuid") or ""
        src_mp4 = ""
        if uuid_to_mp4 and seg_uuid_for_match in uuid_to_mp4:
            src_mp4 = uuid_to_mp4[seg_uuid_for_match]
        elif seg.get("source_mp4"):
            src_mp4 = seg["source_mp4"]
        elif seg.get("gcs_uri"):
            src_mp4 = seg["gcs_uri"]
        seg_order_key = src_mp4 or str(seg.get("_id", ""))
        seg_lengths[seg_order_key] = float(seg.get("road_length_km") or 0)
    # Pre-compute per-segment first/last GPS positions (in encounter order
    # along the sorted merged_frames). Used to insert a chainage GAP when
    # two consecutive segments are physically apart on the road — e.g. the
    # surveyor stopped, drove to a different location, and started again.
    # Without this, segments stack contiguously (1100m → 1200m) even when
    # they're 2 km apart on the GIS map. Threshold of 50 m discriminates
    # genuine gaps from sub-sampling overlap at segment boundaries.
    GAP_THRESHOLD_M = 50.0
    # Gaps larger than this are treated as PHANTOM: a real surveyor doesn't
    # drive ten-plus km without filming. When the KML polyline crosses itself
    # (junctions, loops) or doesn't perfectly match the road, two physically-
    # nearby segments can project to chainages 50+ km apart. Capping these
    # prevents road_length from ballooning to 2-3× the actual road length
    # (R784622 batch-1: 180 km reported vs 119 km actual, dominated by a
    # 78.6 km and a 40.2 km phantom gap that never shrink as new batches
    # arrive because new videos land outside the gap zones). 10 km is a
    # safe ceiling — real field gaps from surveyor breaks rarely exceed 2-3
    # km, so anything beyond 10 km is almost always a projection artefact.
    PHANTOM_GAP_THRESHOLD_M = 10_000.0
    seg_first:      dict[str, tuple[float, float]] = {}
    seg_last:       dict[str, tuple[float, float]] = {}
    seg_first_ch_m: dict[str, float] = {}   # KML-projected chainage of seg's first frame in sort order
    seg_last_ch_m:  dict[str, float] = {}   # KML-projected chainage of seg's last frame in sort order
    for f in merged_frames:
        sid = f.get("_seg_order", "")
        lat, lon = f.get("latitude"), f.get("longitude")
        if lat is None or lon is None:
            continue
        if sid not in seg_first:
            seg_first[sid] = (lat, lon)
        seg_last[sid] = (lat, lon)
        ch_m = f.get("_kml_chainage_m")
        if ch_m is not None and ch_m != float("inf"):
            if sid not in seg_first_ch_m:
                seg_first_ch_m[sid] = ch_m
            seg_last_ch_m[sid] = ch_m

    # Prefer chainage-based gap detection when a polyline projection exists.
    # Raw-GPS haversine ("prev seg's last GPS → next seg's first GPS")
    # falsely fires when a segment physically OVERLAPS its neighbours on
    # the road but is recorded out of driving order — e.g. R074598 RHS,
    # where segment 000018F covers chainage ~14–17 km, sandwiched between
    # 000017F and 000019F in sort order; the haversine of GPS endpoints
    # was 11+ km even though there's no actual road gap. Chainage-based
    # gap = max(0, next_first_chainage − prev_last_chainage) clamps
    # overlapping segments to zero, leaving only TRUE along-road gaps.
    use_chainage_gaps = bool(seg_first_ch_m)
    # Service roads + slip roads are DISCONTINUOUS — they exist only as
    # scattered stretches near junctions/interchanges, not along the whole
    # route. The physical distance BETWEEN two such segments is "no road
    # here", NOT unfilmed road, so inserting it as chainage wrongly inflates
    # road_length (R195178 LHS SR: 15.5 km real pavement → 45 km with gaps).
    # For SR/SL we skip gap insertion entirely → chainage is pure cumulative
    # pavement. MCW stays gap-aware: its gaps are unfilmed stretches of ONE
    # continuous carriageway, so adding them gives the true road length.
    _skip_gaps = (subroad_code or subroad or "").upper() in ("SR", "SL")
    # Recomputed from scratch on every Phase C run — no state leaks
    # between batches. Each call rebuilds seg_offsets / seg_gaps purely
    # from the current set of annotation_segments.
    seen_segs: list[str] = []; seg_offsets: dict[str, float] = {}
    seg_gaps:  dict[str, float] = {}     # gap (km) inserted before each seg
    phantom_gaps: list[tuple[str, float]] = []  # (sid, gap_m) suppressed as projection artefacts
    running = 0.0
    prev_seg_end:      tuple[float, float] | None = None
    prev_seg_end_ch_m: float | None = None   # running MAX chainage covered so far
    for f in merged_frames:
        sid = f.get("_seg_order", "")
        if sid in seg_offsets:
            continue
        # Sub-50 m gaps are sampling noise (adjacent segments often
        # overlap by a few metres at the boundary) and don't get added.
        # Gaps over PHANTOM_GAP_THRESHOLD_M are projection artefacts —
        # logged so they're visible in the trace, but excluded from chainage.
        if not _skip_gaps:
            if use_chainage_gaps:
                if prev_seg_end_ch_m is not None and sid in seg_first_ch_m:
                    gap_m = max(0.0, seg_first_ch_m[sid] - prev_seg_end_ch_m)
                    if gap_m > PHANTOM_GAP_THRESHOLD_M:
                        phantom_gaps.append((sid, gap_m))
                    elif gap_m > GAP_THRESHOLD_M:
                        seg_gaps[sid] = gap_m / 1000.0
                        running += seg_gaps[sid]
            else:
                if prev_seg_end is not None and sid in seg_first:
                    gap_m = _haversine_m(prev_seg_end, seg_first[sid])
                    if gap_m > PHANTOM_GAP_THRESHOLD_M:
                        phantom_gaps.append((sid, gap_m))
                    elif gap_m > GAP_THRESHOLD_M:
                        seg_gaps[sid] = gap_m / 1000.0
                        running += seg_gaps[sid]
        seg_offsets[sid] = running
        running += seg_lengths.get(sid, 0.0)
        seen_segs.append(sid)
        if sid in seg_last:
            prev_seg_end = seg_last[sid]
        # Track the HIGHEST chainage we've covered so far across all
        # processed segments. When two segs overlap (A spans 10-200, B
        # spans 50-100 and is sorted after A on first-frame chainage),
        # naively assigning prev = B's last (=100) drops the bookmark
        # backwards, so the NEXT segment's gap is over-counted by 100m.
        # Worse, when a new batch adds a seg in the same overlap zone,
        # the OLD batch's reported gaps still match because we re-derive
        # the same wrong bookmark — that's the symptom of "existing gaps
        # don't change after a new batch". Tracking the running max
        # keeps the bookmark monotonic, so new segments that physically
        # fall inside an existing gap can actually shrink it.
        if sid in seg_last_ch_m:
            if prev_seg_end_ch_m is None:
                prev_seg_end_ch_m = seg_last_ch_m[sid]
            else:
                prev_seg_end_ch_m = max(prev_seg_end_ch_m, seg_last_ch_m[sid])
    if seg_gaps:
        log.info("[Phase C] recomputed %d chainage gap(s) between segments "
                 "(fresh, batch-independent): %s",
                 len(seg_gaps),
                 ", ".join(f"{round(g * 1000)}m" for g in seg_gaps.values()))
    if phantom_gaps:
        log.info("[Phase C] suppressed %d phantom chainage gap(s) > %dm "
                 "(projection artefact, excluded from road_length): %s",
                 len(phantom_gaps),
                 int(PHANTOM_GAP_THRESHOLD_M),
                 ", ".join(f"{round(g)}m" for _, g in phantom_gaps))
    have_seg_chainage = all(
        f.get("chainage_km") is not None for f in merged_frames[:50]
    )
    if have_seg_chainage and sum(seg_lengths.values()) > 0:
        for f in merged_frames:
            sid = f.get("_seg_order", "")
            f["chainage_km"] = round(
                seg_offsets.get(sid, 0.0) + float(f.get("chainage_km") or 0), 3
            )
        cum_km = running   # includes gaps; was sum(seg_lengths.values())
    else:
        cum_km = 0.0; prev = None
        for f in merged_frames:
            cur = (f.get("latitude"), f.get("longitude"))
            if prev is not None and None not in cur and None not in prev:
                cum_km += _haversine_m(prev, cur) / 1000.0
            f["chainage_km"] = round(cum_km, 3)
            prev = cur if None not in cur else prev

    road_length_km = round(cum_km if cum_km > 0 else total_road_length, 2)
    # road_rating + road_severity are computed below after the severity retag
    # (per-100m two-level aggregation over the merged frames).

    # ── 5. Optional LS override BEFORE severity retag. ────────────────────
    # apply_ls_to_frames matches by trailing-two of og_file. The LS export
    # (annotations.json) was authored against merged_frames_<tag>/<seq>.jpg
    # paths, but at this point each frame's og_file is still the per-segment
    # relative path (frame_data/frames/frame_XXXXX.jpg). Stamp the merged
    # path now so the lookup actually hits — stamp_merged_urls_on_frames
    # later overwrites og_file with the absolute URL anyway.
    if ls_lookup:
        for idx, f in enumerate(merged_frames):
            seq = idx * 10
            f["og_file"] = f"merged_frames_{merged_tag}/{seq:06d}.jpg"
        apply_ls_to_frames(merged_frames, ls_lookup)

    # ── 6. Re-tag severity per IBI Guideline (or override). ───────────────
    for f in merged_frames:
        # Drop FILTERED_LABELS first — scrubs legacy data on reprocess so
        # those bboxes never reach reports / dashboards / NHAI xlsx.
        f["inference_info"] = [
            inf for inf in (f.get("inference_info") or [])
            if not _is_filtered_label(inf.get("label", ""))
        ]
        frame_sevs = []
        for inf in f["inference_info"]:
            sev = severity_for(inf.get("label", ""), severity_override)
            inf["severity"] = sev
            frame_sevs.append(sev)
        f["defect_state"] = _dominant_severity(frame_sevs)

    # Severity distribution + total defects — INSTANCE counts (linear runs
    # collapsed per side, spontaneous per bbox). Same logic as Phase B / B-2.
    sev_totals = {"high": 0, "medium": 0, "low": 0, "none": 0}
    for f in merged_frames:
        for inf in (f.get("inference_info") or []):
            if label_category(inf.get("label", "")) == "spontaneous":
                sev_totals[(inf.get("severity") or "none").lower()] += 1
    for run in _collect_linear_runs(merged_frames):
        sev_totals[run["severity"]] += 1
    log.info("[Phase C] severity totals: %s", sev_totals)

    road_severity, road_rating = _per_bucket_severity_and_rating(merged_frames)
    log.info("[Phase C] merged frames=%d  road_length=%.2f km  "
             "road_rating=%.2f  road_severity=%s",
             len(merged_frames), road_length_km, road_rating, road_severity)

    # RHS is driven opposite to LHS, so its start point IS the road's end
    # (and vice-versa). Swap the per-direction start/end addresses for RHS
    # so the dashboard + per-uid reports read in the surveyor's actual
    # travel direction: LHS = point1→point2, RHS = point2→point1.
    # RoadData stays CANONICAL (road-level, point1→point2, shared by all
    # directions) — only the per-uid inference_data + reports get swapped.
    if (direction or "").upper() == "RHS":
        ui_start_addr, ui_end_addr = end_addr, start_addr
    else:
        ui_start_addr, ui_end_addr = start_addr, end_addr

    # road_type = the human-readable sub-road this uid represents
    # (Main Carriageway / Service Roads / Slip Road). Stored per-uid on
    # inference_data + roads.surveys[] since each survey is one sub-road.
    _road_type = _SUBROAD_ROAD_TYPE.get((subroad or subroad_code or "").upper(),
                                        "Main Carriageway")

    # ── 7. Reports + 8. chainage CSV ──────────────────────────────────────
    with timed("build_reports_1_through_4 + chainage"):
        report_1, total_defects = build_report_1_key(
            merged_frames, road_length_km, road_rating, ui_start_addr, ui_end_addr)
        report_2, labels = build_report_2_key(merged_frames)
        _mt = merged_tag or uid_suffix or ""
        report_3 = build_report_3_key(merged_frames, road_id=road_id, merged_tag=_mt)
        report_4 = build_report_4_key(merged_frames, road_id=road_id, merged_tag=_mt)
        pie_chart2 = build_pie_chart2(merged_frames)
        chainage_csv = build_chainage_report_csv(merged_frames, ui_start_addr, ui_end_addr)

    # NHAI Report Format (RFP based) — per-detection .xlsx with the exact
    # column headers / asset-type categories the NHAI template expects.
    # Project metadata (NH Number, UPC Code, etc.) flows in via the
    # optional `nhai` block in road_metadata.json. Defect Image column
    # holds the annotated-frame URL (images are NOT embedded).
    with timed("build_nhai_report_xlsx"):
        nhai_xlsx = build_nhai_report_xlsx(
            merged_frames,
            project_meta=nhai_meta,
            road_id=road_id,
            merged_tag=_mt,
            period_label=period_label,
        )

    uid = build_uid(road_id, period=period, uid_suffix=uid_suffix,
                    direction=direction, subroad=subroad)

    # ── 9. Combined reports → GCS, plus the merged result.json. ───────────
    upload_combined_reports(
        road_id=road_id, uid=uid, segments=segments,
        report_1=report_1, report_2=report_2,
        report_3=report_3, report_4=report_4,
        chainage_csv=chainage_csv,
    )
    # NHAI report — separate upload (not part of the IBI 5-file bundle).
    nhai_blob = f"{PROCESSED_PREFIX}/{road_id}/{uid}/nhai_report.xlsx"
    gcs_upload_bytes(
        nhai_blob, nhai_xlsx,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    nhai_xlsx_url = f"https://storage.googleapis.com/{GCS_BUCKET}/{nhai_blob}"
    log.info("[Phase C] NHAI report → gs://%s/%s  (%d bytes)",
             GCS_BUCKET, nhai_blob, len(nhai_xlsx))
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
        "period":                period,            # 'W1Jan2026' | None (compact uid token)
        "period_label":          period_label,      # 'Week-1-Jan-2026' | None (raw folder)
        "uid_suffix":            uid_suffix,        # 'day' | 'night' | None
        "direction":             direction,         # 'LHS' | 'RHS' | None
        "subroad":               subroad,           # 'MCW'|'SR'|'SL' (compact) | None
        "subroad_code":          subroad,           # alias — compact uid token
        "road_type":             _road_type,        # 'Main Carriageway'|'Service Roads'|'Slip Road'
        "project_title_display": project_title,
        "start_add":             {"add": ui_start_addr},
        "end_add":               {"add": ui_end_addr},
        "showInference":         True,
        "is_deleted":            False,
        "created":               _dt.datetime.utcnow(),
        "meta_data":             {},
        "plot_data":             {"plots": {"pie_chart2": pie_chart2}},
        "data": {
            "road_id":            road_id,
            "road_length":        road_length_km,
            "road_rating":        road_rating,
            "road_severity":      road_severity,
            "road_type":          _road_type,   # 'Main Carriageway'|'Service Roads'|'Slip Road' (website reads data.road_type)
            "data_submitted":     _dt.date.today().strftime("%d-%m-%Y"),
            "total_defects":      total_defects,
            "frame_list_data":    _persistable_frames(merged_frames, keep_uuid=True),
            "category_information": {},
            "report_1_key":       report_1,
            "report_2_key":       report_2,
            "dashboard_df_csv":   report_2,        # IBI keeps these in sync
            "report_3_key":       report_3,
            "report_4_key":       report_4,
            "nhai_report_xlsx_url": nhai_xlsx_url,
            "CODEBUILD_BUILD_ID":     uid,
            "NEW_CODEBUILD_BUILD_ID": uid,
            "project_title_display":  project_title,
        },
    }
    with timed("upsert_inference_data"):
        db.inference_data.update_one(
            {"uid": uid, "organization": organization},
            {"$set": inference_doc},
            upsert=True,
        )
    log.info("[Phase C] inference_data upserted  uid=%s", uid)

    with timed("upsert_video_upload"):
        db.video_upload.update_one(
            {"video_uid": uid},
            {"$set": {
                "video_uid":          uid,
                "road_id":            road_id,
                "organization":       organization,
                "city":               city,
                "period":             period,
                "period_label":       period_label,
                "uid_suffix":         uid_suffix,
                "direction":          direction,
                "subroad":            subroad,
                "subroad_code":       subroad,
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

    # roads — one doc per road (NOT per uid). The `surveys[]` array
    # carries one entry per (period, uid_suffix, direction, subroad) combo.
    # We filter-then-extend so re-running Phase C for one combo doesn't
    # wipe entries from sibling combos. uid_key dedupes within the array.
    new_survey_entry = {
        "uid_key":         uid,
        "period":          period,
        "period_label":    period_label,
        "uid_suffix":      uid_suffix,
        "direction":       direction,
        "subroad":         subroad,
        "subroad_code":    subroad,
        "road_type":       _road_type,
        "total_length_km": road_length_km,
        "total_frames":    len(merged_frames),
        "road_rating":     road_rating,
        "survey_date":     survey_date,
    }
    existing_road = db.roads.find_one({"road_id": road_id}) or {}
    existing_surveys = [s for s in (existing_road.get("surveys") or [])
                        if s.get("uid_key") != uid]
    existing_surveys.append(new_survey_entry)
    # Road-level road_type is single-valued per road, so it must NOT just be
    # the last sub-road that consolidated (which would flip a mainly-MCW road
    # to "Slip Road" merely because the slip-road ran last). Prefer the main
    # carriageway when any MCW survey exists; the true per-sub-road values
    # live in surveys[] + inference_data.
    _survey_codes = {(s.get("subroad_code") or s.get("subroad") or "").upper()
                     for s in existing_surveys}
    if "MCW" in _survey_codes:
        _road_level_type = "Main Carriageway"
    elif "SR" in _survey_codes:
        _road_level_type = "Service Roads"
    else:
        _road_level_type = _road_type
    db.roads.update_one(
        {"road_id": road_id},
        {"$set": {
            "road_id":   road_id,
            "road_name": project_title,
            "road_type": _road_level_type,   # road-level: prefers MCW (per-survey detail in surveys[])
            "full_route": {
                "type": "LineString",
                "coordinates": full_route_coords,
            },
            "surveys": existing_surveys,
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
            "road_type":         _road_level_type,   # road-level: prefers MCW
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
                "start_add": {"add": ui_start_addr},   # direction-swapped (RHS = reversed)
                "end_add":   {"add": ui_end_addr},
            }},
        )
    # v3 NHAI: bundle this sub-road's per-100m report with the other
    # two sub-roads of the same (road × period × time × direction) into
    # a single 3-tab xlsx. Idempotent — re-runs overwrite the workbook
    # in GCS. Missing sub-roads (e.g. Phase C only finished MCW so far)
    # become "No data for …" placeholder tabs so the layout stays
    # stable and gets refilled on the next sub-road's consolidate.
    if period and direction:
        try:
            build_subroad_xlsx_workbook(
                road_id=road_id,
                period=period,
                period_label=period_label or _compact_to_label(period) or period,
                uid_suffix=uid_suffix,
                direction=direction,
            )
        except Exception as e:
            log.warning("[Phase C] xlsx workbook build failed (non-fatal): %s", e)

    timing_log.info("  %-42s %9.3fs    [TOTAL uid=%s]",
                    "consolidate_and_rebuild TOTAL",
                    time.perf_counter() - _phase_c_t0, uid)
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


def _video_tracker_key(blob_name: str, road_id: str) -> str:
    """Return the unique-within-road tracker key for a video blob.

    Old flat layout (R361058/video.mp4)         → "video.mp4"
    New nested layout (R245345/2/day/LHS/v.mp4) → "2/day/LHS/v.mp4"

    Both stay unique within their road_id and the new keys can't collide
    with old ones (they always contain at least one '/').
    """
    parts = blob_name.split("/")
    if road_id in parts:
        ridx = parts.index(road_id)
        rel = "/".join(parts[ridx + 1:])
        return rel or parts[-1]
    return parts[-1]


def _list_new_pairs(prefix: str, road_id: str, tracker: dict
                    ) -> list[dict]:
    """List MP4+GPX pairs (with per-pair metadata) in the GCS prefix that
    aren't yet tracked. Each item is a dict from gcs_list_pairs with an
    extra `tracker_key` field."""
    pairs = gcs_list_pairs(prefix)
    out: list[dict] = []
    for p in pairs:
        key = _video_tracker_key(p["mp4"], road_id)
        if is_video_tracked(tracker, road_id, key):
            continue
        out.append({**p, "tracker_key": key})
    return out


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
    file_handler   = attach_road_log_handler(log_path)
    timing_handler = attach_timing_handler(timing_log_path(Path(args.workdir),
                                                           args.road_id))

    log.info("─── WATCH MODE  road_id=%s ───", args.road_id)
    log.info("  prefix       : gs://%s/%s", SOURCE_BUCKET, prefix)
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
            for p in batch:
                bits = [p.get("period"),
                        p.get("uid_suffix"), p.get("direction"),
                        p.get("subroad_code")]
                tag = "/".join(b for b in bits if b)
                log.info("    📋 %s%s", Path(p["mp4"]).name,
                         f" · {tag}" if tag else "")

            # 4) Phase A (per-batch). phase_a_project_all_gpx expects
            # (mp4, gpx) tuples and returns the same — round-trip the
            # metadata around the call by remapping by mp4 blob name.
            if poly and cumdist:
                tuples_in = [(p["mp4"], p["gpx"]) for p in batch]
                tuples_out = phase_a_project_all_gpx(tuples_in, poly, cumdist, args.max_perp_m)
                gpx_by_mp4 = {m: g for m, g in tuples_out}
                for p in batch:
                    if p["mp4"] in gpx_by_mp4:
                        p["gpx"] = gpx_by_mp4[p["mp4"]]

            # 5) Phase B — honour --parallel via dispatch_phase_b. The
            #    tracker is updated to "processing" BEFORE dispatch (so a
            #    crash mid-batch doesn't lose the queue), then bumped to
            #    done/failed via on_done_callback as each worker finishes.
            jobs = []
            for p in batch:
                run_id = str(uuid_mod.uuid4())
                track_video(tracker, args.road_id, p["tracker_key"], run_id,
                            status="processing")
                uuid_to_mp4[run_id] = p["mp4"]
                jobs.append({
                    "mp4_blob": p["mp4"], "gpx_blob": p["gpx"],
                    "road_id":  args.road_id, "run_id": run_id,
                    "workdir":  workdir_root / run_id,
                    "period":       p.get("period"),
                    "period_label": p.get("period_label"),
                    "uid_suffix":   p.get("uid_suffix"),
                    "direction":    p.get("direction"),
                    "subroad":      p.get("subroad"),
                    "subroad_code": p.get("subroad_code"),
                    "_tracker_key": p["tracker_key"],
                })

            def _on_done(j, res):
                update_tracker_status(
                    tracker, args.road_id, j["_tracker_key"],
                    "done" if res else "failed",
                )

            ok = dispatch_phase_b(
                jobs=jobs, parallel=args.parallel,
                model_weights=args.model_weights,
                severity_override=severity_override,
                on_done_callback=_on_done,
                fast=args.fast,
                log_file_path=str(log_path),
                source_bucket=SOURCE_BUCKET,
            )
            log.info("[watch] batch #%d Phase B: %d/%d succeeded",
                     batch_num, ok, len(batch))
            if ok == 0:
                log.warning("[watch] batch had 0 successes — skipping Phase C")
                continue

            # 6) Phase C — one invocation per unique uid_key in this batch.
            #    Single-road still has multiple uids (per period × time × dir
            #    × sub-road).
            from collections import defaultdict as _dd
            single_uid_groups: dict[tuple, list[dict]] = _dd(list)
            for j in jobs:
                k = (j.get("period"), j.get("uid_suffix"),
                     j.get("direction"), j.get("subroad_code"))
                single_uid_groups[k].append(j)

            _local_meta = load_local_road_metadata(args.metadata_json, args.road_id)
            _nhai_meta  = (_local_meta.get("nhai") if isinstance(_local_meta, dict) else None) or {}
            for (period, suffix_from_path, direction, subroad_code), _g in single_uid_groups.items():
                effective_suffix = suffix_from_path or args.uid_suffix
                # Per-iteration: include direction + sub-road so each
                # (time × LHS/RHS × MCW/SR/SL) tuple gets its own
                # merged_frames_<tag>/ folder. --merged-tag still wins.
                tag = merged_tag_for(effective_suffix, direction,
                                     subroad=subroad_code,
                                     override=args.merged_tag)
                # period_label needed for KML lookup; recover from the
                # first job in the group (all share the same period).
                _period_label = (_g[0].get("period_label") if _g else None) \
                                or _compact_to_label(period)
                uid = consolidate_and_rebuild(
                    road_id=args.road_id,
                    uid_suffix=effective_suffix,
                    organization=args.organization,
                    city=args.city,
                    project_title=args.project_title,
                    start_addr=args.start_address,
                    end_addr=args.end_address,
                    severity_override=severity_override,
                    ls_lookup=ls_lookup,
                    uuid_to_mp4=uuid_to_mp4,
                    merged_tag=tag,
                    polyline=poly,
                    cumdist=cumdist,
                    kml_path=kml_path,
                    period=period,
                    period_label=_period_label,
                    direction=direction,
                    subroad=subroad_code,
                    subroad_code=subroad_code,
                    nhai_meta=_nhai_meta,
                )
                if not uid:        # sub-road had no segments — skip, don't abort
                    continue

                # 7) Merged frames + consolidated videos (per-uid)
                if not args.skip_merged_frames:
                    if args.fast:
                        urls = fast_finalize_frames_and_videos(
                            args.road_id, uid, tag, fps=args.video_fps)
                        if urls:
                            stamp_video_urls(uid, urls)
                    else:
                        build_merged_frames_folder(args.road_id, uid, tag)
                        if not args.skip_videos:
                            urls = build_consolidated_videos(args.road_id, uid, tag,
                                                             fps=args.video_fps)
                            if urls:
                                stamp_video_urls(uid, urls)

                # 8) Phase D — Label Studio project + GCS source storage (per-uid)
                if args.create_ls_project and not args.skip_merged_frames:
                    creds = load_ls_credentials(args.ls_credentials)
                    if creds:
                        pid = ensure_ls_project_with_gcs_storage(
                            road_id=args.road_id, uid_suffix=effective_suffix,
                            merged_tag=tag, creds=creds,
                            period=period, direction=direction,
                            subroad=subroad_code)
                        upload_ls_predictions_from_result_json(
                            road_id=args.road_id, uid_suffix=effective_suffix,
                            creds=creds, project_id=pid,
                            period=period, direction=direction,
                            subroad=subroad_code)

            log.info("[watch] batch #%d done — resuming watch", batch_num)
    except KeyboardInterrupt:
        log.info("[watch] Ctrl+C — exiting cleanly. Tracker preserved at %s",
                 TRACKER_FILE)
    finally:
        detach_log_handler(file_handler)
        detach_timing_handler(timing_handler)


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
    iterator = cli.list_blobs(SOURCE_BUCKET, prefix=root_prefix, delimiter="/")
    for page in iterator.pages:
        for prefix in page.prefixes:
            road_id = prefix.rstrip("/").split("/")[-1]
            if re.match(r"^R\d{6}$", road_id):
                found[road_id] = prefix
    return found


def sync_metadata_from_mongo(metadata_path: str | None = None) -> dict:
    """Read every doc in the RoadData collection and rewrite road_metadata.json.

    Mongo wins for fields it carries (organization, city, road_name → project_title,
    starting/ending address). Existing manual fields not in RoadData (notably the
    `nhai.*` preamble block — NH Number / UPC Code / Survey Date) are PRESERVED,
    so this sync is safe to run repeatedly. Falls back gracefully on Mongo / I/O
    errors so a transient outage never wipes the local file.

    Triggered by --sync-metadata at the top of main() before any per-road work."""
    path = Path(metadata_path) if metadata_path else Path(__file__).parent / "road_metadata.json"
    existing: dict = {}
    if path.exists():
        try:
            with open(path) as fh:
                existing = json.load(fh)
        except Exception as e:
            log.warning("[metadata-sync] existing %s unreadable: %s — starting fresh", path, e)
            existing = {}

    try:
        db = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)["roadvision"]
        docs = list(db.RoadData.find({}, {
            "road_id": 1, "organization": 1, "city": 1,
            "road_name": 1, "starting_address": 1, "ending_address": 1,
            "road_length": 1,
            "start_chainage_label": 1, "end_chainage_label": 1,
        }))
    except Exception as e:
        log.warning("[metadata-sync] Mongo query failed: %s — keeping existing file", e)
        return existing

    # Keep top-level comments (any key starting with "_") untouched.
    merged: dict = {k: v for k, v in existing.items() if k.startswith("_")}

    written = 0
    for doc in docs:
        rid = doc.get("road_id")
        if not rid:
            continue
        prev = existing.get(rid, {}) if isinstance(existing.get(rid), dict) else {}
        merged[rid] = {
            "organization":  doc.get("organization")     or prev.get("organization", ""),
            "city":          doc.get("city")             or prev.get("city", ""),
            "project_title": doc.get("road_name")        or prev.get("project_title", rid),
            "start_address": doc.get("starting_address") or prev.get("start_address", "TBD"),
            "end_address":   doc.get("ending_address")   or prev.get("end_address",   "TBD"),
        }
        # Preserve any manually-curated NHAI preamble; optionally enrich from
        # RoadData fields that have an obvious match. We never OVERWRITE an
        # existing nhai.* value — manual edits survive every sync.
        nhai = dict(prev.get("nhai") or {})
        if doc.get("road_length") and "project_length" not in nhai:
            nhai["project_length"] = f"{doc['road_length']} km"
        if doc.get("start_chainage_label") and "start_chainage" not in nhai:
            nhai["start_chainage"] = doc["start_chainage_label"]
        if doc.get("end_chainage_label") and "end_chainage" not in nhai:
            nhai["end_chainage"] = doc["end_chainage_label"]
        if nhai:
            merged[rid]["nhai"] = nhai
        written += 1

    # Atomic write — avoid corrupting the file on Ctrl-C mid-write.
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as fh:
        json.dump(merged, fh, indent=2)
    tmp.replace(path)
    log.info("[metadata-sync] synced %d roads from RoadData → %s", written, path)
    return merged


def load_local_road_metadata(metadata_path: str | None, road_id: str) -> dict:
    """
    Read road_metadata.json (a local file mapping road_id → defaults for
    organization / city / project_title / start_address / end_address) and
    return the entry for road_id. Used to fill in CLI flags that weren't
    supplied. CLI flags always win — this only fills the gaps.

    Defaults to ./road_metadata.json next to this script. Missing file or
    missing road_id key → empty dict (caller will then fail validation
    if any required field is still unset).
    """
    if metadata_path:
        path = Path(metadata_path)
    else:
        path = Path(__file__).parent / "road_metadata.json"
    if not path.exists():
        return {}
    try:
        with open(path) as fh:
            data = json.load(fh)
    except Exception as e:
        log.warning("[metadata] %s unreadable: %s — ignoring", path, e)
        return {}
    entry = data.get(road_id)
    if not isinstance(entry, dict):
        return {}
    return entry


def load_road_meta_from_gcs(prefix: str, road_id: str) -> dict:
    """
    Pull the optional `_road_meta.json` setup_varanasi_roads.py uploads
    next to a road's MP4s. Used to fill --project-title / --start-address
    / --end-address per-road in --all-roads mode. Missing fields default
    to road_id / "TBD".
    """
    try:
        text = gcs_download_text(f"{prefix}_road_meta.json", bucket=SOURCE_BUCKET)
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
    # When root is empty (bucket-root scan, e.g. nhai-upload), pass an
    # empty prefix to GCS — `"/"` would never match anything because
    # blob names don't start with a leading slash.
    _root = args.gcs_prefix_root.strip().strip("/")
    root_prefix = (_root + "/") if _root else ""
    workdir_root = Path(args.workdir) / "all_roads"
    workdir_root.mkdir(parents=True, exist_ok=True)
    merged_tag = args.merged_tag or args.uid_suffix

    log.info("─── WATCH ALL ROADS  root=gs://%s/%s ───",
             SOURCE_BUCKET, root_prefix)
    log.info("  batch_size  : %d", args.batch_size)
    log.info("  parallel    : %d", args.parallel)
    log.info("  poll every  : %ds", args.watch_interval)
    log.info("  settle time : %ds", args.settle_time)
    log.info("  tracker     : %s", TRACKER_FILE)
    log.info("  Ctrl+C to stop. Tracker survives restarts.")

    # Eagerly create per-road log files for every road currently in the
    # source prefix, so users see workdir/<road_id>/pipeline.log appear
    # immediately at watch startup (instead of only after that road's
    # first batch dispatches into Phase B/C).
    initial_roads = list_road_folders(root_prefix)
    for rid in initial_roads:
        with road_log_session(Path(args.workdir), rid):
            log.info("[watch-all] discovered %s — log session opened", rid)
    log.info("[watch-all] %d road(s) under watch: %s",
             len(initial_roads), ", ".join(sorted(initial_roads)) or "(none)")

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

    def _scan_all() -> dict[str, list[dict]]:
        """Return {road_id: [pair_dict, ...]} of NEW pairs only.

        Each pair_dict carries:
          mp4, gpx, road_id, period, period_label, uid_suffix, direction,
          subroad, subroad_code, tracker_key, prefix (the road's folder
          prefix — used by Phase A KML lookup)
        """
        roads = list_road_folders(root_prefix)
        out: dict[str, list[dict]] = {}
        for rid, prefix in roads.items():
            new_pairs = _list_new_pairs(prefix, rid, tracker)
            if new_pairs:
                out[rid] = [{**p, "prefix": prefix} for p in new_pairs]
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
            flat: list[dict] = []
            roads_cycle = list(new_by_road.keys())
            road_idx = 0
            while len(flat) < args.batch_size and any(new_by_road.values()):
                rid = roads_cycle[road_idx % len(roads_cycle)]
                if new_by_road[rid]:
                    flat.append(new_by_road[rid].pop(0))
                road_idx += 1
                if road_idx > len(roads_cycle) * args.batch_size:
                    break  # safety
            roads_in_batch = sorted(set(j["road_id"] for j in flat))

            batch_num += 1
            _log_section(f"BATCH #{batch_num}  ({len(flat)} videos across "
                         f"{len(roads_in_batch)} road(s): {', '.join(roads_in_batch)})")
            for j in flat:
                bits = [j.get("period"),
                        j.get("uid_suffix"), j.get("direction"),
                        j.get("subroad_code")]
                tag = "/".join(b for b in bits if b)
                log.info("    📋 %s  (road %s%s)",
                         Path(j["mp4"]).name, j["road_id"],
                         f" · {tag}" if tag else "")

            # Tee a "batch dispatched" line to each affected road's log so
            # users tailing workdir/<road_id>/pipeline.log can see when
            # work for their road kicked off.
            for rid in roads_in_batch:
                videos_for_rid = [Path(j["mp4"]).name for j in flat
                                  if j["road_id"] == rid]
                with road_log_session(Path(args.workdir), rid):
                    log.info("[watch-all] batch #%d dispatched: %d video(s) → %s",
                             batch_num, len(videos_for_rid),
                             ", ".join(videos_for_rid))

            # 5) Phase A per pair (cache KML poly/cumdist per road)
            phase_b_jobs = []
            for pair in flat:
                rid       = pair["road_id"]
                mp4_blob  = pair["mp4"]
                gpx_blob  = pair["gpx"]
                prefix    = pair["prefix"]
                kml = _get_kml(rid, prefix)
                if kml is not None:
                    poly, cumdist, _kml_path = kml
                    try:
                        text = gcs_download_text(gpx_blob, bucket=SOURCE_BUCKET)
                        rewritten, _stats = rewrite_gpx_with_kml_projection(
                            text, poly, cumdist, args.max_perp_m)
                        new_gpx = gpx_blob[:-4] + ".kml.gpx"
                        gcs_upload_text(new_gpx, rewritten,
                                        content_type="application/gpx+xml",
                                        bucket=SOURCE_BUCKET)
                        gpx_blob = new_gpx
                    except Exception as e:
                        log.warning("[Phase A] %s skipped: %s", gpx_blob, e)
                run_id = str(uuid_mod.uuid4())
                track_video(tracker, rid, pair["tracker_key"], run_id,
                            status="processing")
                phase_b_jobs.append({
                    "mp4_blob": mp4_blob, "gpx_blob": gpx_blob,
                    "road_id":  rid,      "run_id":   run_id,
                    "workdir":  workdir_root / run_id,
                    "period":       pair.get("period"),
                    "period_label": pair.get("period_label"),
                    "uid_suffix":   pair.get("uid_suffix"),
                    "direction":    pair.get("direction"),
                    "subroad":      pair.get("subroad"),
                    "subroad_code": pair.get("subroad_code"),
                    "_tracker_key": pair["tracker_key"], "_road_id": rid,
                })

            def _on_done(j, res):
                update_tracker_status(tracker, j["_road_id"],
                                      j["_tracker_key"],
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
                source_bucket=SOURCE_BUCKET,
            )
            log.info("[watch-all] batch #%d Phase B: %d/%d succeeded",
                     batch_num, ok, len(phase_b_jobs))
            if ok == 0:
                log.warning("[watch-all] batch had 0 successes — skipping Phase C")
                continue

            # 6) Phase C — one invocation per UNIQUE (road, period,
            #    uid_suffix, direction, sub-road) tuple represented in the
            #    batch. Each tuple maps to a distinct uid (e.g.
            #    R186871_W1Jan2026_day_LHS_MCW) and gets its own
            #    consolidated inference_data row. With the v3 grouping,
            #    one road can have up to 24 sibling rows (per
            #    period × 2 times × 2 dirs × 3 sub-roads).
            from collections import defaultdict as _dd
            uid_groups: dict[tuple, list[dict]] = _dd(list)
            for j in phase_b_jobs:
                key = (j["road_id"], j.get("period"),
                       j.get("uid_suffix"), j.get("direction"),
                       j.get("subroad_code"))
                uid_groups[key].append(j)

            for (rid, period, suffix_from_path, direction, subroad_code), _jobs in uid_groups.items():
                # Path-derived uid_suffix wins over the CLI flag (since
                # the path is per-folder authoritative). Falls back to
                # CLI when the path didn't carry a day/night component.
                effective_suffix = suffix_from_path or args.uid_suffix
                meta = _get_meta(rid, list_road_folders(root_prefix).get(rid, ""))
                # Per-road org/city/title from road_metadata.json (the
                # local JSON next to this script). CLI flags still win if
                # the operator passed --organization/--city explicitly.
                local = load_local_road_metadata(args.metadata_json, rid)
                org_for_road  = args.organization or local.get("organization")
                city_for_road = args.city         or local.get("city")
                title_for_road = (meta["project_title"]
                                  if meta["project_title"] != rid
                                  else local.get("project_title") or rid)
                start_for_road = (meta["start_address"]
                                  if meta["start_address"] not in ("", "TBD")
                                  else local.get("start_address") or "TBD")
                end_for_road   = (meta["end_address"]
                                  if meta["end_address"] not in ("", "TBD")
                                  else local.get("end_address") or "TBD")
                if not org_for_road or not city_for_road:
                    log.warning("[watch-all] %s has no organization/city in "
                                "road_metadata.json or CLI — skipping Phase C",
                                rid)
                    continue
                kml = _get_kml(rid, list_road_folders(root_prefix).get(rid, ""))
                if kml:
                    poly, cumdist, kml_path_for_road = kml
                else:
                    poly, cumdist, kml_path_for_road = None, None, None
                try:
                    with road_log_session(Path(args.workdir), rid):
                        # Direction- + sub-road-aware merged_tag per iteration
                        # so each (time × LHS/RHS × MCW/SR/SL) tuple lands
                        # in its own merged_frames_<tag>/ folder.
                        tag = merged_tag_for(effective_suffix, direction,
                                             subroad=subroad_code,
                                             override=args.merged_tag)
                        _period_label = (_jobs[0].get("period_label")
                                         if _jobs else None) \
                                        or _compact_to_label(period)
                        uid = consolidate_and_rebuild(
                            road_id=rid,
                            uid_suffix=effective_suffix,
                            organization=org_for_road,
                            city=city_for_road,
                            project_title=title_for_road,
                            start_addr=start_for_road,
                            end_addr=end_for_road,
                            severity_override=severity_override,
                            ls_lookup=ls_lookup,
                            merged_tag=tag,
                            polyline=poly, cumdist=cumdist,
                            kml_path=kml_path_for_road,
                            period=period,
                            period_label=_period_label,
                            direction=direction,
                            subroad=subroad_code,
                            subroad_code=subroad_code,
                            nhai_meta=(local.get("nhai") if isinstance(local, dict) else None) or {},
                        )
                        if not uid:        # sub-road had no segments — skip, don't abort
                            continue
                        if not args.skip_merged_frames:
                            if args.fast:
                                urls = fast_finalize_frames_and_videos(
                                    rid, uid, tag, fps=args.video_fps)
                                if urls:
                                    stamp_video_urls(uid, urls)
                            else:
                                build_merged_frames_folder(rid, uid, tag)
                                if not args.skip_videos:
                                    urls = build_consolidated_videos(rid, uid, tag,
                                                                     fps=args.video_fps)
                                    if urls:
                                        stamp_video_urls(uid, urls)
                        # Phase D — Label Studio project + GCS source storage.
                        # Project name = uid (e.g. R186871_W1Jan2026_day_LHS_MCW),
                        # so each (period, direction, sub-road) gets its own
                        # LS project — matches how Phase C splits inference_data.
                        if args.create_ls_project and not args.skip_merged_frames:
                            creds = load_ls_credentials(args.ls_credentials)
                            if creds:
                                pid = ensure_ls_project_with_gcs_storage(
                                    road_id=rid, uid_suffix=effective_suffix,
                                    merged_tag=tag, creds=creds,
                                    period=period, direction=direction,
                                    subroad=subroad_code)
                                upload_ls_predictions_from_result_json(
                                    road_id=rid, uid_suffix=effective_suffix,
                                    creds=creds, project_id=pid,
                                    period=period, direction=direction,
                                    subroad=subroad_code)
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
    counts["RoadData"]            = db.RoadData.delete_many(
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


def _persistable_frames(frames: list[dict], *, keep_uuid: bool = False) -> list[dict]:
    """Strip internal provenance keys before writing frame_list_data into
    inference_data, to keep the doc under MongoDB's hard 16 MB
    (16,793,600 B) BSON cap. The dashboard reads latitude/longitude/
    location/chainage_km/og_file/inference_image/defect_state/
    inference_info — none of which are touched here.

    `_source_mp4` + `_seg_order` (~2.3 MB on a 13k-frame road) are used
    only during consolidation sort/gap-detection and are ALWAYS stripped.

    `_uuid` is ALSO used by build_merged_frames_folder() AFTER consolidate
    — it resolves each frame's per-UUID source folder
    (processed-data/<road>/<_uuid>/annotated_frames/...) to copy raw +
    bboxed frames into merged_frames_<tag>/. So the consolidate write
    must KEEP _uuid (keep_uuid=True); the LATER writes (after the copy /
    URL-rewrite is done) strip it too. Stripping _uuid at consolidate was
    a regression that left merged_frames_<tag>/ empty on fresh roads."""
    drop = {"_source_mp4", "_seg_order"}
    if not keep_uuid:
        drop.add("_uuid")
    return [{k: v for k, v in f.items() if k not in drop} for f in frames]


def stamp_merged_urls_on_frames(
    uid: str,
    road_id: str,
    merged_tag: str,
    frames: list[dict],
) -> None:
    """Rewrite each frame's og_file + inference_image to point at the
    consolidated merged_frames_<tag>/<seq:06d>.jpg + merged_predict_<tag>/
    URLs in GCS, then persist to inference_data. The dashboard reads these
    fields to render the Inspection Point images; without absolute URLs
    they 404.

    Reprocess skips the per-UUID copy (frames already merged on disk) so
    nothing else stamps these — call this after redraw_merged_predict_from_frames.
    """
    raw_prefix = (f"https://storage.googleapis.com/{GCS_BUCKET}/"
                  f"{PROCESSED_PREFIX}/{road_id}/merged_frames_{merged_tag}")
    ann_prefix = (f"https://storage.googleapis.com/{GCS_BUCKET}/"
                  f"{PROCESSED_PREFIX}/{road_id}/merged_predict_{merged_tag}")
    for idx, f in enumerate(frames):
        seq = idx * 10
        f["og_file"] = f"{raw_prefix}/{seq:06d}.jpg"
        f["inference_image"] = f"{ann_prefix}/{seq:06d}.jpg"
    db = MongoClient(MONGO_URI)["roadvision"]
    db.inference_data.update_one(
        {"uid": uid},
        {"$set": {"data.frame_list_data": _persistable_frames(frames)}},
    )
    log.info("[reprocess] stamped %d merged URLs on inference_data.uid=%s",
             len(frames), uid)


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
    polyline_override: list[tuple[float, float]] | None = None,
    cumdist_override:  list[float] | None = None,
) -> str:
    """
    Re-run Phase C using:
      • annotation_segments → authoritative GPS / chainage / per-UUID
        provenance (lat/lng survives even when result.json doesn't carry it,
        which happens when an external tool re-exports result.json from
        annotations alone)
      • annotations.json (GCS sibling of result.json) → authoritative bbox
        source of truth (RV Studio writes here on every save)
      • result.json → fallback when segments are absent (legacy V1 data
        with no annotation_segments rows)

    Then redraws merged_predict frames + re-stitches consolidated videos
    + upserts inference_data so the dashboard shows the user's corrected
    annotations end to end. Returns the consolidated uid.
    """
    uid = f"{road_id}_{uid_suffix}"
    merged_tag = merged_tag or uid_suffix

    db = MongoClient(MONGO_URI)["roadvision"]
    segments = list(db.annotation_segments.find({"road_id": road_id}))

    # ── Primary path: rebuild from segments + annotations.json ────────────
    if segments:
        log.info("[reprocess] %d annotation_segments present — using as GPS "
                 "source of truth (result.json lat/lng would be lossy)",
                 len(segments))
        ls_lookup = load_ls_export_from_gcs(road_id, uid)
        if ls_lookup is None:
            log.info("[reprocess] no annotations.json next to result.json — "
                     "falling back to segments' original YOLO bboxes")

        # Parse period + direction + sub-road out of uid_suffix
        # (e.g. "W1Jan2026_day_LHS_MCW" → period="W1Jan2026", core="day",
        # direction="LHS", subroad="MCW") so consolidate filters
        # annotation_segments by uid_key instead of road_id alone. Without
        # this, sibling combos get merged into one uid_suffix's
        # inference_data doc (same kind of bug we saw with v2 surveys).
        # Pass the CORE suffix ("day"), not the full one — consolidate's
        # build_uid will re-add the period/direction/sub-road tokens
        # itself; passing the full form would double them.
        _m = re.match(
            r"^(?:(W\d+[A-Za-z]+\d{4})_)?(.+?)(?:_(LHS|RHS))?(?:_(MCW|SR|SL))?$",
            uid_suffix or "")
        _period       = _m.group(1) if _m else None
        _core_suffix  = _m.group(2) if _m else (uid_suffix or "day")
        _direction    = _m.group(3) if _m else None
        _subroad      = _m.group(4) if _m else None
        _period_label = _compact_to_label(_period)

        # KML for the multi-video chainage sort. Resolution priority:
        # 1) explicit caller override (main() already resolves with the
        #    correct --source-bucket + --gcs-prefix scope, including
        #    per-direction subfolders so RHS gets lucknow_rhs.kmz);
        # 2) period- + direction- + sub-road-aware lookup against
        #    SOURCE_BUCKET — works when --source-bucket is set, falls
        #    through otherwise;
        # 3) broad road-root scan (legacy single-direction fallback).
        # Without ANY of these, consolidate_and_rebuild falls back to
        # MP4-basename sort which gets the order wrong for surveys
        # recorded out of spatial sequence — adds tens of km of bogus
        # jumps to road_length, and disables the chainage-based gap
        # detection (forces it onto raw-GPS haversine which over-counts
        # phantom gaps between physically-overlapping segments).
        poly: list[tuple[float, float]] | None = None
        cumdist: list[float] | None = None
        kml_path: str | None = None
        if polyline_override and cumdist_override:
            poly, cumdist = polyline_override, cumdist_override
            log.info("[reprocess] using caller-provided polyline (%d vertices)", len(poly))
        else:
            kml_path = gcs_find_kml_for_uid(road_id, _period_label,
                                            _core_suffix, _direction,
                                            subroad=_subroad) \
                       or gcs_find_kml(f"{road_id}/")
            if kml_path:
                try:
                    poly = parse_kml(kml_path)
                    cumdist = precompute_polyline_chainage(poly)
                except Exception as e:
                    log.warning("[reprocess] KML parse failed (%s) — sort will "
                                "use MP4 basename order", e)

        # consolidate_and_rebuild does the merge + ls override + severity
        # retag + report build + result.json + inference_data upsert in
        # one shot, with proper lat/lng/chainage_km on every frame.
        _local_meta = load_local_road_metadata(None, road_id)
        consolidate_and_rebuild(
            road_id=road_id,
            uid_suffix=_core_suffix,
            organization=organization,
            city=city,
            project_title=project_title,
            start_addr=start_addr,
            end_addr=end_addr,
            severity_override=severity_override,
            ls_lookup=ls_lookup,
            merged_tag=merged_tag,
            polyline=poly,
            cumdist=cumdist,
            kml_path=kml_path,
            period=_period,
            period_label=_period_label,
            direction=_direction,
            subroad=_subroad,
            subroad_code=_subroad,
            nhai_meta=(_local_meta.get("nhai") if isinstance(_local_meta, dict) else None) or {},
        )
        # Pull the freshly-merged frames back so the redraw + video steps
        # below have the latest inference_info (annotations.json applied).
        inf_doc = db.inference_data.find_one({"uid": uid})
        merged_frames = ((inf_doc or {}).get("data") or {}).get("frame_list_data") or []

        # The redraw + videos step at the bottom of this function uses
        # `merged_frames` directly. Skip the legacy result.json reconstruct
        # path and jump to the redraw block.
        if redraw_frames:
            redraw_merged_predict_from_frames(road_id, uid, merged_tag, merged_frames)
        stamp_merged_urls_on_frames(uid, road_id, merged_tag, merged_frames)
        if rebuild_videos:
            urls = build_consolidated_videos(road_id, uid, merged_tag, fps=video_fps)
            if urls:
                stamp_video_urls(uid, urls)
        log.info("✅ reprocess complete  uid=%s", uid)
        return uid

    # ── Legacy fallback: result.json only (lat/lng may be missing) ────────
    log.warning("[reprocess] no annotation_segments for %s — falling back "
                "to result.json-only path (lat/lng will be None if missing)",
                road_id)

    # 1) Download + parse + reconstruct frames
    coco = download_merged_result_json(road_id, uid, explicit_gs_url)
    merged_frames = reconstruct_frames_from_result(coco)
    if not merged_frames:
        raise SystemExit("merged result.json had no images — nothing to reprocess")

    # 1b) If a sibling annotations.json exists in the same uid folder
    #     (RV Studio writes the human-corrected bboxes there), it's the
    #     source of truth — override the inference_info we just rebuilt
    #     from result.json. Lookup matches by trailing-two-segment image
    #     path (e.g. merged_frames_day/000010.jpg), which is exactly the
    #     format both result.json's image.file_name and annotations.json's
    #     task.data.image use.
    ls_lookup = load_ls_export_from_gcs(road_id, uid)
    if ls_lookup:
        replaced, no_match = apply_ls_to_frames(merged_frames, ls_lookup)
        log.info("[reprocess] applied annotations.json to %d frames "
                 "(%d frames had no LS task)", replaced, no_match)

    # 2) Severity retag (in case mappings changed since the last run)
    for f in merged_frames:
        # Drop FILTERED_LABELS first — scrubs legacy data on reprocess.
        f["inference_info"] = [
            inf for inf in (f.get("inference_info") or [])
            if not _is_filtered_label(inf.get("label", ""))
        ]
        frame_sevs = []
        for inf in f["inference_info"]:
            sev = severity_for(inf.get("label", ""), severity_override)
            inf["severity"] = sev
            frame_sevs.append(sev)
        f["defect_state"] = _dominant_severity(frame_sevs)

    # Severity distribution — INSTANCE counts (same logic as Phase C).
    sev_totals = {"high": 0, "medium": 0, "low": 0, "none": 0}
    for f in merged_frames:
        for inf in (f.get("inference_info") or []):
            if label_category(inf.get("label", "")) == "spontaneous":
                sev_totals[(inf.get("severity") or "none").lower()] += 1
    for run in _collect_linear_runs(merged_frames):
        sev_totals[run["severity"]] += 1
    log.info("[reprocess] severity totals: %s", sev_totals)

    # 3) Re-build IBI reports
    cum_km = max((float(f.get("chainage_km") or 0) for f in merged_frames), default=0)
    road_length_km = round(cum_km, 2)
    # Per-100m two-level aggregation (same as Phase C).
    road_severity, road_rating = _per_bucket_severity_and_rating(merged_frames)
    log.info("[reprocess] road_length=%.2f km  road_rating=%.2f  road_severity=%s",
             road_length_km, road_rating, road_severity)

    # RHS reads opposite to LHS — swap per-direction start/end addresses
    # (RoadData stays canonical; this only affects the per-uid doc + reports).
    if (_direction or "").upper() == "RHS":
        ui_start_addr, ui_end_addr = end_addr, start_addr
    else:
        ui_start_addr, ui_end_addr = start_addr, end_addr

    report_1, total_defects = build_report_1_key(
        merged_frames, road_length_km, road_rating, ui_start_addr, ui_end_addr)
    report_2, labels = build_report_2_key(merged_frames)
    _mt = merged_tag or uid_suffix or ""
    report_3 = build_report_3_key(merged_frames, road_id=road_id, merged_tag=_mt)
    report_4 = build_report_4_key(merged_frames, road_id=road_id, merged_tag=_mt)
    pie_chart2 = build_pie_chart2(merged_frames)
    chainage_csv = build_chainage_report_csv(merged_frames, ui_start_addr, ui_end_addr)

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
        "start_add":             {"add": ui_start_addr},
        "end_add":               {"add": ui_end_addr},
        "showInference":         True,
        "is_deleted":            False,
        "meta_data":             {"reprocessed_at": _dt.datetime.utcnow().isoformat()},
        "plot_data":             {"plots": {"pie_chart2": pie_chart2}},
        "data": {
            "road_id":              road_id,
            "road_length":          road_length_km,
            "road_rating":          road_rating,
            "road_severity":        road_severity,
            "data_submitted":       _dt.date.today().strftime("%d-%m-%Y"),
            "total_defects":        total_defects,
            "frame_list_data":      _persistable_frames(merged_frames, keep_uuid=True),
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
    stamp_merged_urls_on_frames(uid, road_id, merged_tag, merged_frames)

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
# Section 17b — Label Studio project provisioning (Phase D)
# ─────────────────────────────────────────────────────────────────────────────
# After Phase C uploads merged_frames_<tag>/ + result.json, optionally create
# (or reuse) a Label Studio project named "{road_id}-{uid_suffix}" and attach
# a Google Cloud Storage source storage pointing at the merged_frames folder.
# Annotators in LS see every frame as a task; their saves come back to V2 as
# the annotations.json reprocess loop already consumes.
#
# Credentials live in label_studio_credentials.json next to this script
# (gitignored, chmod 600). Override with --ls-credentials /custom/path.json.
#
# Idempotent: if the project already exists, attach storage to it (not
# duplicating). If the storage already points at the same prefix, skip.

LS_CREDENTIALS_PATH = Path(__file__).parent / "label_studio_credentials.json"


def load_ls_credentials(path: str | None = None) -> dict | None:
    """Load LS URL + token + GCS service account JSON from secrets file.
    Returns None if the file is missing — caller should disable the LS
    integration in that case rather than crash."""
    p = Path(path) if path else LS_CREDENTIALS_PATH
    if not p.exists():
        log.warning("[LS] credentials file %s missing — skipping LS project", p)
        return None
    try:
        with open(p) as fh:
            creds = json.load(fh)
    except Exception as e:
        log.warning("[LS] credentials file %s unreadable: %s", p, e)
        return None
    if not creds.get("url") or not creds.get("api_token"):
        log.warning("[LS] credentials missing url/api_token — skipping")
        return None
    return creds


def build_label_config_xml() -> str:
    """Generate a Label Studio <View> config from the strict 57-label allowlist.

    Only labels in LABEL_CATEGORY appear as annotation buttons — labels with a
    colour but no category/severity entry are excluded so annotators don't
    spend effort on labels the reports silently drop. Mirrors what
    _is_filtered_label() enforces at inference time."""
    lines = [
        '<View>',
        '  <Image name="image" value="$image" zoom="true" zoomControl="true"/>',
        '  <RectangleLabels name="label" toName="image" canRotate="false">',
    ]
    n = 0
    for label, (r, g, b) in LABEL_COLORS_RGB.items():
        # Skip RGB entries that aren't in the report allowlist.
        if normalize_label(label).lower() not in LABEL_CATEGORY:
            continue
        # LS expects HTML escape on attribute values; & and < are the risky ones
        safe = label.replace("&", "&amp;").replace('"', "&quot;").replace("<", "&lt;")
        hexcol = f"#{r:02x}{g:02x}{b:02x}"
        lines.append(f'    <Label value="{safe}" background="{hexcol}"/>')
        n += 1
    lines.append('  </RectangleLabels>')
    lines.append('</View>')
    log.debug("[LS] label_config: %d labels (LABEL_CATEGORY allowlist)", n)
    return "\n".join(lines)


def upload_ls_predictions_from_result_json(
    *,
    road_id:    str,
    uid_suffix: str,
    creds:      dict,
    project_id: int | None = None,
    period:     str | None = None,
    direction:  str | None = None,
    subroad:    str | None = None,
) -> int:
    """
    After the GCS sync has populated tasks for merged_frames_<tag>/*.jpg,
    walk processed-data/<road>/<uid>/result.json and POST one prediction
    per (task, bbox-set) pair. Annotators see the YOLO bboxes pre-drawn
    in the LS UI; they can accept, edit, or delete each one.

    Matching: LS task.data.image trailing-two-segments == COCO image
    file_name trailing-two-segments (e.g. "merged_frames_day/000010.jpg").

    Returns the number of predictions uploaded. Logs but doesn't raise on
    LS errors — predictions are nice-to-have, the pipeline shouldn't fail
    just because LS was momentarily unavailable.
    """
    from collections import defaultdict
    try:
        from label_studio_sdk import Client as LSClient
    except ImportError:
        log.warning("[LS-pred] label-studio-sdk not installed — skipping")
        return 0

    uid = build_uid(road_id, period=period, uid_suffix=uid_suffix,
                    direction=direction, subroad=subroad)
    try:
        coco = download_merged_result_json(road_id, uid)
    except SystemExit as e:
        log.warning("[LS-pred] result.json missing for %s: %s — skipping", uid, e)
        return 0

    cat_by_id = {c["id"]: c.get("name", "")
                 for c in (coco.get("categories") or [])}
    img_by_id = {im["id"]: im for im in (coco.get("images") or [])}

    # Phase C currently writes width=0/height=0 in result.json. Without
    # real pixel dims we can't convert YOLO bboxes (pixel coords) to LS
    # percentages. Peek at one merged JPEG to grab the real dims; the
    # ffmpeg scale step in Phase B makes every merged frame the same size.
    fallback_w = fallback_h = 0
    peek_tag = merged_tag_for(uid_suffix, direction, subroad=subroad)
    try:
        peek_blob = (f"{PROCESSED_PREFIX}/{road_id}/"
                     f"merged_frames_{peek_tag}/000000.jpg")
        peek_bytes = (gcs_client().bucket(GCS_BUCKET).blob(peek_blob)
                      .download_as_bytes())
        arr = np.frombuffer(peek_bytes, dtype=np.uint8)
        peek_img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if peek_img is not None:
            fallback_h, fallback_w = peek_img.shape[:2]
            log.info("[LS-pred] peeked merged_frames_%s/000000.jpg → "
                     "%dx%d (used as fallback for result.json's 0/0)",
                     peek_tag, fallback_w, fallback_h)
    except Exception as e:
        log.warning("[LS-pred] couldn't peek a merged frame for dims: %s", e)

    # Build {trailing-two-of-image-path → list[ls-prediction-result-dict]}
    preds_by_tail: dict[str, list[dict]] = defaultdict(list)
    for ann in (coco.get("annotations") or []):
        img = img_by_id.get(ann.get("image_id"))
        if not img:
            continue
        ow = float(img.get("width") or 0) or float(fallback_w)
        oh = float(img.get("height") or 0) or float(fallback_h)
        if ow <= 0 or oh <= 0:
            continue
        bbox = ann.get("bbox") or []
        if len(bbox) != 4:
            continue
        x, y, w, h = bbox
        label = cat_by_id.get(ann.get("category_id"), "") \
                or f"unknown_{ann.get('category_id')}"
        fname = (img.get("file_name") or "").strip("/")
        parts = fname.split("/")
        if len(parts) < 2:
            continue
        tail = "/".join(parts[-2:])
        preds_by_tail[tail].append({
            "type": "rectanglelabels",
            "from_name": "label", "to_name": "image",
            "original_width":  int(ow),
            "original_height": int(oh),
            "image_rotation":  0,
            "value": {
                "x":      max(0.0, x / ow * 100.0),
                "y":      max(0.0, y / oh * 100.0),
                "width":  max(0.0, w / ow * 100.0),
                "height": max(0.0, h / oh * 100.0),
                "rectanglelabels": [label],
            },
        })

    if not preds_by_tail:
        log.info("[LS-pred] result.json has no annotations to upload for %s",
                 road_id)
        return 0

    log.info("[LS-pred] result.json: %d frames have predictions, "
             "%d total bboxes", len(preds_by_tail),
             sum(len(v) for v in preds_by_tail.values()))

    try:
        ls = LSClient(url=creds["url"].rstrip("/"), api_key=creds["api_token"])
    except Exception as e:
        log.warning("[LS-pred] connection failed: %s", e)
        return 0

    project = None
    title = uid.replace("_", "-")
    try:
        for p in ls.list_projects():
            if (project_id is not None and p.id == project_id) \
                    or getattr(p, "title", None) == title:
                project = p
                break
    except Exception as e:
        log.warning("[LS-pred] list_projects failed: %s", e)
        return 0
    if project is None:
        log.warning("[LS-pred] project '%s' not found in LS", title)
        return 0

    # GCS sync is async on the LS side. After we triggered it in
    # ensure_ls_project_with_gcs_storage, tasks may take 5–60s+ to appear,
    # depending on how many JPEGs need to be registered. Poll up to 5 min.
    expected = len(preds_by_tail)
    tasks: list[dict] = []
    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            tasks = project.get_tasks() or []
        except Exception as e:
            log.warning("[LS-pred] get_tasks failed: %s", e)
            return 0
        if len(tasks) >= expected:
            break
        log.info("[LS-pred] %d/%d tasks synced, waiting…", len(tasks), expected)
        time.sleep(10)
    if not tasks:
        log.warning("[LS-pred] project '%s' still has no tasks after 5 min — "
                    "skipping. Re-run --create-ls-project once they appear.",
                    title)
        return 0
    if len(tasks) < expected:
        log.warning("[LS-pred] only %d/%d tasks visible; uploading "
                    "predictions for what's there. Re-run later to fill "
                    "the gap.", len(tasks), expected)
    # Idempotency: tasks that already have a v2-pipeline prediction get
    # skipped (re-running won't double-up bboxes).
    # Tasks where YOLO found nothing get an EMPTY prediction (result=[])
    # so the LS Data Manager shows "1" in the predictions column for
    # every task, not 0 — makes "all frames have been scored by YOLO"
    # visible at a glance. Empty predictions don't auto-complete the
    # task; annotators still need to draw or skip.
    import base64 as _b64
    import requests as _rq
    from concurrent.futures import ThreadPoolExecutor as _Pool
    from requests.adapters import HTTPAdapter as _Adapter
    from urllib3.util.retry import Retry as _Retry

    # Direct REST + retry adapter beats the LS SDK's create_prediction in
    # two ways:
    #   1. ThreadPoolExecutor lets us run ~8 POSTs in flight at once
    #      (the sequential SDK loop was the dominant cost — 30+ min for
    #       projects with ~7k tasks).
    #   2. The Retry adapter survives LS's occasional `{"status":"UP"}`
    #      health-check leak into HTTP responses (BadStatusLine) and
    #      transient 5xx; previously a single hiccup aborted the whole
    #      import and the user had to re-run manually.
    _ls_url = creds["url"].rstrip("/")
    _session = _rq.Session()
    _session.headers.update({
        "Authorization": f"Token {creds['api_token']}",
        "Content-Type":  "application/json",
    })
    _retry = _Retry(total=5, backoff_factor=0.4,
                    status_forcelist=[500, 502, 503, 504],
                    allowed_methods=frozenset(["GET", "POST", "DELETE", "PUT"]),
                    raise_on_status=False)
    _adapter = _Adapter(pool_connections=16, pool_maxsize=16, max_retries=_retry)
    _session.mount("http://", _adapter); _session.mount("https://", _adapter)

    def _post_one(task):
        """Return one of: 'already', 'skip', 'pred', 'empty', 'fail'."""
        if "v2-pipeline" in (task.get("predictions_model_versions") or ""):
            return "already"
        raw_img = ((task.get("data") or {}).get("image") or "")
        # LS sometimes serves images via a presigned wrapper:
        #   /tasks/<id>/presign/?fileuri=<base64(gs://bucket/path)>
        # Decode fileuri first when present so we recover the real
        # gs:// path before splitting.
        img = raw_img
        if "fileuri=" in img:
            try:
                b64 = img.split("fileuri=", 1)[1].split("&", 1)[0]
                b64 += "=" * (-len(b64) % 4)
                img = _b64.urlsafe_b64decode(b64).decode("utf-8", errors="ignore")
            except Exception:
                pass
        img = img.split("?")[0]
        parts = img.strip("/").split("/")
        if len(parts) < 2:
            return "skip"
        tail = "/".join(parts[-2:])
        results = preds_by_tail.get(tail) or []
        # Hand-roll one extra retry on BadStatusLine / ConnectionError —
        # urllib3.Retry doesn't trigger on those.
        for attempt in range(3):
            try:
                r = _session.post(
                    f"{_ls_url}/api/predictions/",
                    data=json.dumps({"task": task["id"],
                                     "result": results,
                                     "model_version": "v2-pipeline"}),
                    timeout=30,
                )
                if r.status_code in (200, 201):
                    return "pred" if results else "empty"
                return "fail"
            except (_rq.exceptions.ConnectionError,
                    _rq.exceptions.ChunkedEncodingError):
                if attempt == 2: return "fail"
                time.sleep(0.3 * (attempt + 1))

    n_pred = n_empty = n_already = n_skip = n_fail = 0
    _first_fails: list[str] = []
    with _Pool(max_workers=8) as _pool:
        for outcome in _pool.map(_post_one, tasks):
            if   outcome == "pred":    n_pred    += 1
            elif outcome == "empty":   n_empty   += 1
            elif outcome == "already": n_already += 1
            elif outcome == "skip":    n_skip    += 1
            else:
                n_fail += 1
                if len(_first_fails) < 3: _first_fails.append(str(outcome))
    if _first_fails:
        log.warning("[LS-pred] first failed outcomes: %s", _first_fails)
    log.info("[LS-pred] %d predictions with bboxes, %d empty predictions "
             "(no YOLO match), %d already had v2-pipeline, %d skipped, "
             "%d failed",
             n_pred, n_empty, n_already, n_skip, n_fail)
    return n_pred + n_empty


def ensure_ls_project_with_gcs_storage(
    *,
    road_id:    str,
    uid_suffix: str,
    merged_tag: str,
    creds:      dict,
    period:     str | None = None,
    direction:  str | None = None,
    subroad:    str | None = None,
) -> int | None:
    """
    Idempotent: ensure a Label Studio project named after the consolidated
    uid exists, attach (or reuse) a GCS source storage pointing at the
    merged frames folder, and trigger an initial sync.

    Project name format:
      • New v3 layout (period + direction + sub-road known):
            R186871-W1Jan2026-day-LHS-MCW
      • Old flat layout:
            R361058-day

    Returns the project id, or None on failure (logs but doesn't raise — LS
    being unreachable shouldn't abort the rest of Phase C).
    """
    try:
        from label_studio_sdk import Client as LSClient
    except ImportError:
        log.warning("[LS] label-studio-sdk not installed — skipping project creation")
        return None

    uid           = build_uid(road_id, period=period, uid_suffix=uid_suffix,
                              direction=direction, subroad=subroad)
    project_title = uid.replace("_", "-")
    gcs_prefix    = f"{PROCESSED_PREFIX}/{road_id}/merged_frames_{merged_tag}/"
    sa_json_str   = json.dumps(creds["gcs_service_account"]) \
                    if isinstance(creds.get("gcs_service_account"), dict) \
                    else (creds.get("gcs_service_account") or "")

    try:
        ls = LSClient(url=creds["url"].rstrip("/"), api_key=creds["api_token"])
        ls.check_connection()
    except Exception as e:
        log.warning("[LS] connection failed (%s @ %s) — skipping",
                    e, creds.get("url"))
        return None

    # Find-or-create the project (idempotent on title).
    project = None
    try:
        for p in ls.list_projects():
            if getattr(p, "title", None) == project_title:
                project = p
                break
    except Exception as e:
        log.warning("[LS] list_projects failed: %s — attempting create anyway", e)

    current_xml = build_label_config_xml()
    if project is None:
        try:
            project = ls.start_project(
                title=project_title,
                description=(f"Auto-created by V2 pipeline. Source: "
                             f"gs://{GCS_BUCKET}/{gcs_prefix}"),
                label_config=current_xml,
            )
            log.info("[LS] created project '%s' (id=%s)",
                     project_title, project.id)
        except Exception as e:
            log.warning("[LS] start_project failed for '%s': %s",
                        project_title, e)
            return None
    else:
        log.info("[LS] reusing existing project '%s' (id=%s)",
                 project_title, project.id)
        # Keep the project's label_config in sync with the current
        # LABEL_CATEGORY / LABEL_COLORS_RGB. Without this, a project created
        # before a label was added (or its colour/spelling changed) keeps
        # its STALE schema forever — annotators report "label X is missing
        # from the toolbar" even though the model now predicts it. PATCH
        # via REST so older SDK versions that don't expose update_settings
        # still work.
        try:
            import requests as _rq
            url = creds["url"].rstrip("/")
            r = _rq.patch(
                f"{url}/api/projects/{project.id}/",
                headers={"Authorization": f"Token {creds['api_token']}",
                         "Content-Type":  "application/json"},
                data=json.dumps({"label_config": current_xml}),
                timeout=30,
            )
            if r.status_code in (200, 201):
                log.info("[LS] refreshed label_config on existing "
                         "project id=%s", project.id)
            else:
                log.warning("[LS] label_config refresh got HTTP %d on "
                            "project id=%s: %s — old schema will remain "
                            "until updated manually",
                            r.status_code, project.id, r.text[:200])
        except Exception as e:
            log.warning("[LS] could not refresh label_config on "
                        "project id=%s: %s — old schema will remain",
                        project.id, e)

    # Attach GCS source storage if it isn't already attached for this prefix.
    try:
        existing = project.get_import_storages() or []
    except Exception as e:
        log.warning("[LS] get_import_storages failed: %s", e)
        existing = []
    storage_id = None
    for s in existing:
        if (s.get("type") == "gcs"
            and s.get("bucket") == GCS_BUCKET
            and (s.get("prefix") or "").rstrip("/") == gcs_prefix.rstrip("/")):
            storage_id = s.get("id")
            break
    if storage_id is not None:
        log.info("[LS] GCS storage already attached (id=%s) for gs://%s/%s — reusing",
                 storage_id, GCS_BUCKET, gcs_prefix)
    else:
        try:
            storage = project.connect_google_import_storage(
                bucket=GCS_BUCKET,
                prefix=gcs_prefix,
                google_application_credentials=sa_json_str,
                title=f"merged_frames_{merged_tag}",
                regex_filter=r".*\.jpg$",
                use_blob_urls=True,
                presign=True,
                presign_ttl=60,
            )
            storage_id = storage.get("id") if isinstance(storage, dict) \
                         else getattr(storage, "id", None)
            log.info("[LS] attached GCS storage (id=%s) → gs://%s/%s",
                     storage_id, GCS_BUCKET, gcs_prefix)
        except Exception as e:
            log.warning("[LS] connect_google_import_storage failed: %s", e)

    # ALWAYS trigger a sync, not just on first attach.  Each new batch of
    # frames written to merged_frames_<tag>/ needs to be pulled into LS as
    # new tasks; without this, the second+ batches sit in GCS but never
    # appear in the LS project, breaking automation.
    if storage_id is not None:
        try:
            project.sync_import_storage("gcs", storage_id)
            log.info("[LS] sync triggered on storage id=%s", storage_id)
        except Exception as e:
            log.warning("[LS] sync_import_storage failed: %s "
                        "(tasks for new frames won't appear until "
                        "you click Sync in LS UI)", e)

    return getattr(project, "id", None)


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
    ap.add_argument("--organization", default=None,
                    help="Organization the inference_data doc is scoped to "
                         '(e.g. "BangaloreOrg", "kotaOrg", "VaranasiOrg"). '
                         "If omitted, looked up by road_id in --metadata-json.")
    ap.add_argument("--city", default=None,
                    help="City label. If omitted, looked up by road_id in "
                         "--metadata-json.")
    ap.add_argument("--project-title", default=None,
                    help="Display name shown on the dashboard. "
                         "If omitted, looked up by road_id in --metadata-json. "
                         "--all-roads: read from each road's _road_meta.json "
                         "(falls back to road_id).")
    ap.add_argument("--start-address", default=None,
                    help='Start address. If omitted, looked up by road_id in '
                         '--metadata-json. --all-roads: read from _road_meta.json '
                         '(falls back to "TBD").')
    ap.add_argument("--end-address",   default=None,
                    help='End address. If omitted, looked up by road_id in '
                         '--metadata-json. --all-roads: read from _road_meta.json '
                         '(falls back to "TBD").')
    ap.add_argument("--metadata-json", default=None,
                    help="Path to a JSON mapping road_id → "
                         "{organization, city, project_title, start_address, "
                         "end_address}. Defaults to road_metadata.json next to "
                         "this script. CLI flags above always win over JSON.")
    ap.add_argument("--sync-metadata", action="store_true",
                    help="At startup, query the Mongo RoadData collection and "
                         "rewrite road_metadata.json with every road found "
                         "(organization, city, project_title, start/end address). "
                         "Existing manual fields not in RoadData (notably nhai.*) "
                         "are preserved. Off by default — opt-in for predictability.")

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
    ap.add_argument("--source-bucket", default=None,
                    help="GCS bucket to read source MP4/GPX/KML from. "
                         "Defaults to the output bucket ('datanh11'). Use "
                         "this when field teams upload to a separate "
                         "ingest-only bucket (e.g. 'nhai-upload') while "
                         "processed outputs stay on 'datanh11'. "
                         "Output writes (processed-data/, merged frames, "
                         "videos, reports) ALWAYS go to 'datanh11' "
                         "regardless of this flag.")
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
    ap.add_argument("--num-gpus", type=int, default=None,
                    help="Number of GPUs to spread Phase B workers across "
                         "(round-robin via CUDA_VISIBLE_DEVICES). When unset, "
                         "auto-detects via `nvidia-smi -L`. With 4 T4s + "
                         "--parallel 32 each GPU gets 8 workers — at the "
                         "compute-saturation sweet spot. Workers don't share "
                         "CUDA contexts, so this only matters for --parallel >1.")
    ap.add_argument("--ffmpeg-hwaccel", choices=["cpu", "cuda"], default=None,
                    help="ffmpeg frame-extraction backend. 'cuda' uses NVDEC "
                         "for ~3× faster 4K H.264 decode + frees CPU; falls "
                         "back to 'cpu' automatically when NVDEC refuses a "
                         "file (Novatek dashcams). 'cpu' uses the classic "
                         "8-segment parallel-ffmpeg split. Defaults to 'cuda' "
                         "when --num-gpus > 0, else 'cpu'.")
    ap.add_argument("--no-nvenc", action="store_true",
                    help="Disable h264_nvenc for video stitches; use libx264 "
                         "even when GPUs are present. Useful for debugging or "
                         "when NVENC sessions are scarce on the GPU.")
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

    # ── Label Studio (Phase D) ────────────────────────────────────────────
    ap.add_argument("--create-ls-project", action="store_true",
                    help="After Phase C uploads merged_frames_<tag>/ + "
                         "result.json, create (or reuse) a Label Studio "
                         "project named '{road_id}-{uid_suffix}' and attach "
                         "a GCS source storage pointing at the merged_frames "
                         "folder. Requires label_studio_credentials.json "
                         "next to this script (chmod 600). Idempotent — safe "
                         "to re-run.")
    ap.add_argument("--ls-credentials", default=None,
                    help="Override path to LS credentials JSON. Defaults to "
                         "label_studio_credentials.json next to this script.")
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
    ap.add_argument("--watch-interval", type=int, default=10,
                    help="Seconds between GCS scans when no new videos are "
                         "present (default 30)")
    ap.add_argument("--settle-time", type=int, default=10,
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

    # ── Optional metadata sync (--sync-metadata) ───────────────────────────
    # Pull every RoadData doc and refresh road_metadata.json so any new
    # roads added via the dashboard are picked up by this run. Done before
    # the per-road metadata fill below uses load_local_road_metadata.
    if args.sync_metadata:
        try:
            sync_metadata_from_mongo(args.metadata_json)
        except Exception as e:
            log.warning("[metadata-sync] failed: %s — continuing with existing file", e)

    # ── Source bucket override ─────────────────────────────────────────────
    # Lets the watch loop read MP4/GPX/KML from a separate ingest bucket
    # (e.g. 'nhai-upload') while output keeps going to 'datanh11'. Applied
    # before any GCS scan happens so list_road_folders / gcs_list_pairs /
    # gcs_find_kml all see the override on first call.
    if args.source_bucket:
        global SOURCE_BUCKET
        SOURCE_BUCKET = args.source_bucket
        log.info("source bucket override → gs://%s/  (output stays on gs://%s/)",
                 SOURCE_BUCKET, GCS_BUCKET)

    # ── GPU acceleration resolution ────────────────────────────────────────
    # Resolve --num-gpus / --ffmpeg-hwaccel / --no-nvenc into the module
    # globals, with auto-detect for --num-gpus (defaults to whatever
    # nvidia-smi reports). Logged once so the run banner shows what's
    # actually active.
    global NUM_GPUS, FFMPEG_HWACCEL, USE_NVENC
    if args.num_gpus is not None:
        NUM_GPUS = max(0, args.num_gpus)
    else:
        NUM_GPUS = detect_num_gpus()
    # Without GPUs, hwaccel and NVENC must be off regardless of defaults.
    if NUM_GPUS == 0:
        FFMPEG_HWACCEL = "cpu"
        USE_NVENC = False
    else:
        if args.ffmpeg_hwaccel is not None:
            FFMPEG_HWACCEL = args.ffmpeg_hwaccel
        # else keeps the module default ("cuda")
        if args.no_nvenc:
            USE_NVENC = False
    log.info("GPU acceleration: num_gpus=%d  ffmpeg_hwaccel=%s  nvenc=%s",
             NUM_GPUS, FFMPEG_HWACCEL, "on" if USE_NVENC else "off")

    # ── Fill any unset per-road args from road_metadata.json ──────────────
    # Single-road mode only — --all-roads has its own per-road meta path
    # via load_road_meta_from_gcs (_road_meta.json sitting in GCS).
    # CLI flags always win; JSON only fills the gaps.
    if args.road_id and not args.all_roads:
        meta = load_local_road_metadata(args.metadata_json, args.road_id)
        if meta:
            filled = []
            for cli_attr, json_key in (
                ("organization",  "organization"),
                ("city",          "city"),
                ("project_title", "project_title"),
                ("start_address", "start_address"),
                ("end_address",   "end_address"),
            ):
                if getattr(args, cli_attr) is None and meta.get(json_key):
                    setattr(args, cli_attr, meta[json_key])
                    filled.append(f"{cli_attr}={meta[json_key]}")
            if filled:
                log.info("[metadata] %s filled from road_metadata.json: %s",
                         args.road_id, ", ".join(filled))

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
            _r = args.gcs_prefix_root.strip().strip("/")
            root = (_r + "/") if _r else ""
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
    missing = [name for name, val in (
        ("--organization",  args.organization),
        ("--city",          args.city),
        ("--project-title", args.project_title),
        ("--start-address", args.start_address),
        ("--end-address",   args.end_address),
    ) if not val]
    if missing:
        raise SystemExit(
            f"Missing required field(s) in single-road mode: {', '.join(missing)}. "
            f"Either pass them as CLI flags, or add an entry for '{args.road_id}' "
            f"to road_metadata.json (or pass --all-roads --watch which reads "
            f"each road's _road_meta.json from GCS).")

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
        # Wrap in road_log_session so reprocess output also lands in
        # workdir/<road_id>/pipeline.log alongside one-shot runs.
        with road_log_session(Path(args.workdir), args.road_id):
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
                    polyline_override=poly,
                    cumdist_override=cumdist,
                )
        return

    # ── WATCH MODE — long-running upload-poll loop, exits on Ctrl+C ──────
    if args.watch:
        with road_log_session(Path(args.workdir), args.road_id):
            watch_mode(args, severity_override, ls_lookup, poly, cumdist, kml_path)
        return

    # ── ONE-SHOT MODE: PHASE A — KML/GPX projection ──────────────────────
    _log_section("PHASE A — KML / GPX projection")
    pairs = gcs_list_pairs(prefix_with_slash)
    if not pairs:
        raise SystemExit(f"No (mp4, gpx) pairs at gs://{GCS_BUCKET}/{args.gcs_prefix}")
    log.info("Found %d video pairs", len(pairs))

    # `pairs` is a list of dicts with metadata. Phase A's projector still
    # works on (mp4, gpx) tuples — round-trip those while preserving the
    # per-pair metadata (period, uid_suffix, direction, sub-road).
    if kml_path is None:
        log.info("Phase A skipped — no KML/KMZ for %s; running on raw GPX",
                 args.road_id)
        kml_pairs = pairs
    elif args.skip_phase_a:
        kml_pairs = [{**p, "gpx": p["gpx"][:-4] + ".kml.gpx"} for p in pairs]
        log.info("Phase A skipped — reusing existing .kml.gpx files")
    else:
        tuples_in  = [(p["mp4"], p["gpx"]) for p in pairs]
        tuples_out = phase_a_project_all_gpx(tuples_in, poly, cumdist, args.max_perp_m)
        gpx_by_mp4 = {m: g for m, g in tuples_out}
        kml_pairs  = [{**p, "gpx": gpx_by_mp4.get(p["mp4"], p["gpx"])}
                      for p in pairs]
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
            for p in kml_pairs:
                run_id = str(uuid_mod.uuid4())
                uuid_to_mp4[run_id] = p["mp4"]
                jobs.append({
                    "mp4_blob": p["mp4"], "gpx_blob": p["gpx"],
                    "road_id":  args.road_id, "run_id": run_id,
                    "workdir":  workdir_root / run_id,
                    "period":       p.get("period"),
                    "period_label": p.get("period_label"),
                    "uid_suffix":   p.get("uid_suffix"),
                    "direction":    p.get("direction"),
                    "subroad":      p.get("subroad"),
                    "subroad_code": p.get("subroad_code"),
                })
            with timed("dispatch_phase_b TOTAL (all videos)"):
                ok = dispatch_phase_b(
                    jobs=jobs, parallel=args.parallel,
                    model_weights=args.model_weights,
                    severity_override=severity_override,
                    fast=args.fast,
                    log_file_path=str(log_path),
                    source_bucket=SOURCE_BUCKET,
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
                    sorted_mp4s = sorted(p["mp4"] for p in kml_pairs)
                    missing_uuids = [s["uuid"] for s in missing]
                    for u, mp4 in zip(missing_uuids, sorted_mp4s):
                        uuid_to_mp4.setdefault(u, mp4)
                log.info("Phase B skipped — recovered %d UUID→MP4 entries",
                         len(uuid_to_mp4))
            except Exception as e:
                log.warning("Phase B skipped — UUID→MP4 recovery failed: %s", e)

        # ── PHASE C — one consolidate per (period, time, direction,
        # sub-road) tuple in the input. With the v3 layout, a single
        # one-shot run can produce up to 24 separate inference_data rows
        # (per period × 2 times × 2 dirs × 3 sub-roads); the old flat
        # layout collapses everything into one (all dims = None).
        _log_section("PHASE C — consolidation")
        from collections import defaultdict as _dd
        oneshot_groups: dict[tuple, list] = _dd(list)
        if not args.skip_phase_b:
            for j in jobs:
                k = (j.get("period"), j.get("uid_suffix"),
                     j.get("direction"), j.get("subroad_code"))
                oneshot_groups[k].append(j)
        else:
            # No jobs from Phase B (--skip-phase-b). Read existing segments
            # to determine which uid_keys exist for this road.
            try:
                db = MongoClient(MONGO_URI)["roadvision"]
                seg_keys = set()
                for s in db.annotation_segments.find(
                        {"road_id": args.road_id},
                        {"period": 1, "uid_suffix": 1, "direction": 1,
                         "subroad_code": 1}):
                    seg_keys.add((s.get("period"), s.get("uid_suffix"),
                                  s.get("direction"), s.get("subroad_code")))
                for k in seg_keys:
                    oneshot_groups[k] = []
            except Exception as e:
                log.warning("[Phase C] segment-key recovery failed: %s — "
                            "falling back to single uid", e)
        if not oneshot_groups:
            oneshot_groups[(None, None, None, None)] = []   # legacy fallback

        _oneshot_local = load_local_road_metadata(args.metadata_json, args.road_id)
        _oneshot_nhai  = (_oneshot_local.get("nhai")
                          if isinstance(_oneshot_local, dict) else None) or {}
        last_uid = None
        for (period, suffix_from_path, direction, subroad_code), _g in oneshot_groups.items():
            effective_suffix = suffix_from_path or args.uid_suffix
            # Direction- + sub-road-aware folder suffix per iteration so
            # each (time × LHS/RHS × MCW/SR/SL) tuple lands in its own
            # merged_frames_<tag>/ folder. --merged-tag CLI flag still
            # wins as an override.
            merged_tag = merged_tag_for(effective_suffix, direction,
                                        subroad=subroad_code,
                                        override=args.merged_tag)
            # period_label needed by consolidate's downstream writes;
            # recover from the first job in the group, else from compact.
            _period_label = (_g[0].get("period_label") if _g else None) \
                            or _compact_to_label(period)
            uid = consolidate_and_rebuild(
                road_id=args.road_id,
                uid_suffix=effective_suffix,
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
                period=period,
                period_label=_period_label,
                direction=direction,
                subroad=subroad_code,
                subroad_code=subroad_code,
                nhai_meta=_oneshot_nhai,
            )
            if not uid:            # sub-road had no segments — skip, don't abort
                continue
            last_uid = uid

            # ── Merged frames + consolidated videos (per uid) ───────────
            if not args.skip_merged_frames:
                _log_section(f"merged frames + videos · uid={uid}")
                if args.fast:
                    with timed("fast_finalize_frames_and_videos"):
                        urls = fast_finalize_frames_and_videos(
                            args.road_id, uid, merged_tag, fps=args.video_fps)
                    if urls:
                        stamp_video_urls(uid, urls)
                else:
                    with timed("build_merged_frames_folder"):
                        build_merged_frames_folder(args.road_id, uid, merged_tag)
                    if not args.skip_videos:
                        with timed("build_consolidated_videos"):
                            urls = build_consolidated_videos(args.road_id, uid, merged_tag,
                                                             fps=args.video_fps)
                        if urls:
                            stamp_video_urls(uid, urls)

            # Phase D — Label Studio (per uid)
            if args.create_ls_project and not args.skip_merged_frames:
                _log_section(f"Label Studio project · uid={uid}")
                with timed("PHASE D — Label Studio (per uid)"):
                    creds = load_ls_credentials(args.ls_credentials)
                    if creds:
                        pid = ensure_ls_project_with_gcs_storage(
                            road_id=args.road_id, uid_suffix=effective_suffix,
                            merged_tag=merged_tag, creds=creds,
                            period=period, direction=direction,
                            subroad=subroad_code)
                        upload_ls_predictions_from_result_json(
                            road_id=args.road_id, uid_suffix=effective_suffix,
                            creds=creds, project_id=pid,
                            period=period, direction=direction,
                            subroad=subroad_code)

        log.info("✅ Pipeline V2 complete  road_id=%s  uids=%d  last=%s  log=%s",
                 args.road_id, len(oneshot_groups), last_uid, log_path)


if __name__ == "__main__":
    main()
