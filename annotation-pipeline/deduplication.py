"""
Defect Deduplication Module

This module provides GPS-based spatial deduplication for road defect detection.
Since video frames are captured per-second, the same physical defect often appears
in multiple consecutive frames. This module identifies and consolidates duplicate
detections to provide accurate unique defect counts.

Key Concept:
- If the same defect category appears within DEDUP_DISTANCE_THRESHOLD meters
  of a previous detection, it's considered the same physical defect.
"""

import geopy.distance
from collections import defaultdict
from typing import Dict, List, Tuple, Any, Optional
import pandas as pd


# Configuration: Distance threshold in meters for considering defects as duplicates
DEDUP_DISTANCE_THRESHOLD = 5.0  # meters
# Configuration: IoU threshold for bbox overlap (0.3 = 30% overlap required)
DEDUP_IOU_THRESHOLD = 0.3


def calculate_gps_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance in meters between two GPS coordinates."""
    try:
        return geopy.distance.geodesic((lat1, lon1), (lat2, lon2)).meters
    except Exception:
        return float('inf')


def calculate_iou(bbox1: List, bbox2: List) -> float:
    """
    Calculate Intersection over Union (IoU) for two bounding boxes.

    Args:
        bbox1: First bbox in COCO format [x, y, width, height]
        bbox2: Second bbox in COCO format [x, y, width, height]

    Returns:
        IoU value between 0 and 1
    """
    if bbox1 is None or bbox2 is None:
        return 0.0

    if len(bbox1) < 4 or len(bbox2) < 4:
        return 0.0

    # Convert COCO format [x, y, w, h] to corner format [x1, y1, x2, y2]
    x1_1, y1_1 = bbox1[0], bbox1[1]
    x2_1, y2_1 = bbox1[0] + bbox1[2], bbox1[1] + bbox1[3]

    x1_2, y1_2 = bbox2[0], bbox2[1]
    x2_2, y2_2 = bbox2[0] + bbox2[2], bbox2[1] + bbox2[3]

    # Calculate intersection coordinates
    xi1 = max(x1_1, x1_2)
    yi1 = max(y1_1, y1_2)
    xi2 = min(x2_1, x2_2)
    yi2 = min(y2_1, y2_2)

    # Calculate intersection area
    inter_width = max(0, xi2 - xi1)
    inter_height = max(0, yi2 - yi1)
    inter_area = inter_width * inter_height

    # Calculate union area
    area1 = bbox1[2] * bbox1[3]
    area2 = bbox2[2] * bbox2[3]
    union_area = area1 + area2 - inter_area

    # Return IoU
    if union_area <= 0:
        return 0.0

    return inter_area / union_area


class DefectTracker:
    """
    Tracks unique defects across frames using GPS + IoU based spatial deduplication.

    Matching logic:
    - If GPS distance < threshold AND (no bbox OR IoU > iou_threshold): DUPLICATE
    - Otherwise: UNIQUE (new defect)
    """

    def __init__(self, distance_threshold: float = DEDUP_DISTANCE_THRESHOLD,
                 iou_threshold: float = DEDUP_IOU_THRESHOLD):
        self.distance_threshold = distance_threshold
        self.iou_threshold = iou_threshold
        # Dict: category_id -> list of unique defect info
        self.unique_defects: Dict[str, List[Dict]] = defaultdict(list)
        self.raw_detection_count: Dict[str, int] = defaultdict(int)

    def _find_nearby_defect(self, category_id: str, lat: float, lon: float,
                            bbox: List = None) -> Optional[int]:
        """
        Find existing defect of same category within threshold distance and IoU.

        Args:
            category_id: The defect category ID
            lat: Latitude of new detection
            lon: Longitude of new detection
            bbox: Bounding box of new detection [x, y, w, h]

        Returns:
            Index of matching defect if found (DUPLICATE), None otherwise (UNIQUE)
        """
        for idx, defect in enumerate(self.unique_defects[category_id]):
            gps_distance = calculate_gps_distance(lat, lon, defect['lat'], defect['lon'])

            # First check GPS distance
            if gps_distance <= self.distance_threshold:
                # GPS is within threshold - now check IoU if both have bbox
                if bbox is not None and defect.get('best_bbox') is not None:
                    iou = calculate_iou(bbox, defect['best_bbox'])
                    if iou >= self.iou_threshold:
                        # High IoU + close GPS = same defect (DUPLICATE)
                        return idx
                    # Low IoU = different defects even if GPS is close
                    # Continue checking other defects
                    continue
                else:
                    # No bbox data available, use GPS only (backward compatible)
                    return idx

        return None  # No match found = UNIQUE defect

    def add_detection(self, category_id: str, lat: float, lon: float,
                      area: float = 0, severity: str = 'none',
                      severity_order: Dict[str, int] = None,
                      frame_index: int = -1, bbox: List = None,
                      chainage: int = None) -> Tuple[bool, int]:
        """
        Add a defect detection. Returns (is_new_unique, unique_defect_index).

        Args:
            chainage: The chainage segment number (0, 1, 2, ...) where this defect is located
        """
        self.raw_detection_count[category_id] += 1

        if severity_order is None:
            severity_order = {'none': 0, 'low': 1, 'medium': 2, 'high': 3}

        # Pass bbox for IoU-based matching
        existing_idx = self._find_nearby_defect(category_id, lat, lon, bbox)

        if existing_idx is not None:
            # Update existing defect - keep best detection
            existing = self.unique_defects[category_id][existing_idx]
            existing['detection_count'] += 1
            if area > existing['max_area']:
                existing['max_area'] = area
                existing['best_frame_index'] = frame_index
                existing['best_bbox'] = bbox
                # Update chainage when we find better detection
                if chainage is not None:
                    existing['chainage'] = chainage
            if severity_order.get(severity, 0) > severity_order.get(existing['max_severity'], 0):
                existing['max_severity'] = severity
            # Update centroid
            n = existing['detection_count']
            existing['lat'] = ((n - 1) * existing['lat'] + lat) / n
            existing['lon'] = ((n - 1) * existing['lon'] + lon) / n
            return False, existing_idx
        else:
            # New unique defect
            idx = len(self.unique_defects[category_id])
            self.unique_defects[category_id].append({
                'lat': lat,
                'lon': lon,
                'max_area': area,
                'max_severity': severity,
                'detection_count': 1,
                'best_frame_index': frame_index,
                'best_bbox': bbox,
                'chainage': chainage  # Store chainage directly in defect
            })
            return True, idx

    def get_unique_count(self, category_id: str) -> int:
        return len(self.unique_defects[category_id])

    def get_all_unique_counts(self) -> Dict[str, int]:
        return {cat: len(defects) for cat, defects in self.unique_defects.items()}

    def get_unique_defects(self, category_id: str) -> List[Dict]:
        return self.unique_defects[category_id]


class ChainageDefectTracker:
    """Tracks unique defects per chainage segment using GPS + IoU deduplication."""

    def __init__(self, distance_threshold: float = DEDUP_DISTANCE_THRESHOLD,
                 iou_threshold: float = DEDUP_IOU_THRESHOLD):
        self.distance_threshold = distance_threshold
        self.iou_threshold = iou_threshold
        self.chainage_trackers: Dict[str, DefectTracker] = defaultdict(
            lambda: DefectTracker(distance_threshold, iou_threshold)
        )
        self.global_tracker = DefectTracker(distance_threshold, iou_threshold)

    def add_detection(self, chainage: str, category_id: str, lat: float, lon: float,
                      area: float = 0, severity: str = 'none',
                      severity_order: Dict[str, int] = None,
                      frame_index: int = -1, bbox: List = None) -> Tuple[bool, bool]:
        # Convert chainage string to int for storage
        try:
            chainage_int = int(chainage)
        except (ValueError, TypeError):
            chainage_int = 0

        is_unique_chainage, _ = self.chainage_trackers[chainage].add_detection(
            category_id, lat, lon, area, severity, severity_order, frame_index, bbox, chainage_int
        )
        is_unique_global, _ = self.global_tracker.add_detection(
            category_id, lat, lon, area, severity, severity_order, frame_index, bbox, chainage_int
        )
        return is_unique_chainage, is_unique_global

    def get_chainage_unique_counts(self, chainage: str) -> Dict[str, int]:
        return self.chainage_trackers[chainage].get_all_unique_counts()

    def get_global_unique_counts(self) -> Dict[str, int]:
        return self.global_tracker.get_all_unique_counts()

    def get_global_unique_defects(self) -> Dict[str, List[Dict]]:
        """Get all unique defects globally with their best frame info."""
        return dict(self.global_tracker.unique_defects)


def _sanitize_label_to_key(label: str) -> str:
    """Sanitize label to column key format."""
    key = label.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
    key = key.replace('/', '_').replace('.', '_').replace(',', '_')
    return key.strip('_')


def process_annotations_with_deduplication(df: pd.DataFrame,
                                           categories_mapping: Dict[str, List[str]],
                                           category_information: Dict[int, str],
                                           severity_order: Dict[str, int],
                                           distance_threshold: float = DEDUP_DISTANCE_THRESHOLD,
                                           iou_threshold: float = DEDUP_IOU_THRESHOLD) -> Tuple[pd.DataFrame, ChainageDefectTracker, Dict]:
    """
    Process annotations with deduplication - replaces counts with unique counts.

    Uses combined GPS distance + IoU (bounding box overlap) to identify duplicates:
    - GPS distance < threshold AND IoU > threshold = DUPLICATE
    - Otherwise = UNIQUE

    This function processes the DataFrame and returns:
    1. Modified df with per-frame unique flags
    2. ChainageDefectTracker with all unique defect info
    3. Stats dictionary for reporting

    Returns:
        Tuple of (df, tracker, stats)
    """
    tracker = ChainageDefectTracker(distance_threshold, iou_threshold)

    # Initialize tracking columns
    for defect_type in categories_mapping.keys():
        if defect_type + '_UniqueInFrame' not in df.columns:
            df[defect_type + '_UniqueInFrame'] = 0

    # Process all detections
    for index, row in df.iterrows():
        annotations = row.get('annotation')
        if annotations is None:
            continue

        lat = row.get('latitude', 0)
        lon = row.get('longitude', 0)
        chainage = str(row.get('chainage', '0'))

        for annotation in annotations:
            category_id = str(annotation.get('category_id', ''))
            area = annotation.get('area', 0)
            bbox = annotation.get('bbox', None)

            # Determine severity
            # --- OLD category_id refs (commented out) ---
            # if category_id in ['0', '38', '41']:
            # --- NEW category_id refs (active) ---
            # In new mapping: 0=Potholes, 1=Cracking, 2=Rutting
            if category_id in ['0', '1', '2']:
                severity = 'medium'
            else:
                severity = 'low'

            is_unique_chainage, is_unique_global = tracker.add_detection(
                chainage, category_id, lat, lon, area, severity,
                severity_order, index, bbox
            )

    # Calculate stats
    stats = {
        'raw_total': sum(tracker.global_tracker.raw_detection_count.values()),
        'unique_total': sum(len(d) for d in tracker.global_tracker.unique_defects.values()),
        'per_category': {}
    }

    for cat_id in set(list(tracker.global_tracker.raw_detection_count.keys()) +
                      list(tracker.global_tracker.unique_defects.keys())):
        raw = tracker.global_tracker.raw_detection_count[cat_id]
        unique = len(tracker.global_tracker.unique_defects[cat_id])
        stats['per_category'][cat_id] = {
            'raw': raw,
            'unique': unique,
            'reduction': round((raw - unique) / raw * 100, 1) if raw > 0 else 0
        }

    return df, tracker, stats


def get_deduplicated_chainage_counts(tracker: ChainageDefectTracker,
                                     categories_mapping: Dict[str, List[str]],
                                     category_information: Dict[int, str]) -> Dict[str, Dict[str, int]]:
    """
    Get deduplicated counts per chainage for updating df_grouped.

    Returns:
        Dict: {chainage: {column_name: unique_count}}
    """
    result = {}

    for chainage, chainage_tracker in tracker.chainage_trackers.items():
        result[chainage] = {}

        # Grouped category counts
        for group_name, category_ids in categories_mapping.items():
            unique_count = sum(
                chainage_tracker.get_unique_count(cat_id)
                for cat_id in category_ids
            )
            result[chainage][group_name + '_Count'] = unique_count

        # Individual category counts
        for cat_id, cat_name in category_information.items():
            label_key = _sanitize_label_to_key(cat_name)
            unique_count = chainage_tracker.get_unique_count(str(cat_id))
            result[chainage][label_key + '_Count'] = unique_count

    return result


def apply_deduplicated_counts_to_grouped(df_grouped: pd.DataFrame,
                                         dedup_counts: Dict[str, Dict[str, int]]) -> pd.DataFrame:
    """
    Replace raw counts in df_grouped with deduplicated unique counts.
    This modifies the _Count columns in place (same structure, unique values).
    """
    for index, row in df_grouped.iterrows():
        # Get chainage value - handle both 'chainage' and 'Chainage' column names
        chainage_col = 'Chainage' if 'Chainage' in df_grouped.columns else 'chainage'
        chainage_val = str(row[chainage_col])

        # Extract numeric chainage for matching (e.g., "0-100m" -> "0")
        chainage_key = chainage_val.split('-')[0].replace('m', '').strip()

        # Try to find matching chainage in dedup_counts
        matched_chainage = None
        for key in dedup_counts.keys():
            key_numeric = str(key).split('-')[0].replace('m', '').strip()
            if key_numeric == chainage_key or str(key) == chainage_val:
                matched_chainage = key
                break

        if matched_chainage and matched_chainage in dedup_counts:
            for col_name, unique_count in dedup_counts[matched_chainage].items():
                if col_name in df_grouped.columns:
                    df_grouped.loc[index, col_name] = unique_count

    return df_grouped


def print_deduplication_summary(stats: Dict, category_information: Dict[int, str]) -> None:
    """Print deduplication summary."""
    print("\n" + "="*60)
    print("DEDUPLICATION SUMMARY")
    print("="*60)
    print(f"{'Category':<30} {'Raw':<10} {'Unique':<10} {'Reduction':<10}")
    print("-"*60)

    for cat_id, cat_stats in sorted(stats['per_category'].items(),
                                     key=lambda x: x[1]['raw'], reverse=True):
        if cat_stats['raw'] > 0:
            cat_name = category_information.get(int(cat_id), f"Cat {cat_id}")[:28]
            print(f"{cat_name:<30} {cat_stats['raw']:<10} {cat_stats['unique']:<10} {cat_stats['reduction']:.1f}%")

    print("-"*60)
    reduction = round((stats['raw_total'] - stats['unique_total']) / stats['raw_total'] * 100, 1) if stats['raw_total'] > 0 else 0
    print(f"{'TOTAL':<30} {stats['raw_total']:<10} {stats['unique_total']:<10} {reduction:.1f}%")
    print("="*60 + "\n")
