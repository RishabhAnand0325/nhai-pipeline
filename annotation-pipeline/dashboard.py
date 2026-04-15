import pandas as pd

# Function to convert CSV data to specified JSON format
def convert_to_json(df, road_length_value, road_rating_value, defect_count_value, category_labels=None):
    # Initialize the JSON structure
    json_data = {
        "start": df['start_address'][0],
        "end": df['end_address'].iloc[-1],
        "roadLength": road_length_value,  # Placeholder as road length is not provided in the data
        "roadRating": road_rating_value,  # Average of IRC ratings
        "defect": defect_count_value,  # Total number of defects (rows)
        "defectDetails": []
    }

    # Helper function to sanitize labels (matching chainage.py)
    def _sanitize_label_to_key(label):
        key = label.strip()
        for ch in [' ', '/', '\\', '(', ')', '-', '&', ':', ',', ';']:
            key = key.replace(ch, '_')
        while '__' in key:
            key = key.replace('__', '_')
        return key.strip('_')

    # --- OLD 54-category labels (commented out) ---
    # all_category_labels = [
    #     "Crack",
    #     "pothole",
    #     "rutting",
    #     "Patching",
    #     "rain cuts",
    #     "water stagnation",
    #     "pavement marking(faded)",
    #     "Pavement Marking - Day",
    #     "Pavement Marking - Night",
    #     "missing road studs",
    #     "damaged road studs",
    #     "studs - Day",
    #     "studs - Night",
    #     "missing rumble strips",
    #     "damaged rumble strips",
    #     "missing hazard markers",
    #     "damaged hazard markers",
    #     "unevenness",
    #     "edge drop of shoulders",
    #     "vegetation growth of shoulders",
    #     "damaged drain cover slab",
    #     "missing drain cover slab",
    #     "damaged foot path tiles",
    #     "damaged foot path pavers",
    #     "cleanliness(poor)",
    #     "Encroachment",
    #     "illegal parking",
    #     "missing plants",
    #     "irregular gaps",
    #     "Damaged/deteriorated plants",
    #     "unauthorized median opening",
    #     "reduced sight distance(due to overgrowth)",
    #     "damaged kerb",
    #     "faded kerb painting",
    #     "damaged CC Guard Rails (Concrete Crash Barriers/jersey barriers)",
    #     "damaged MBCB Guard Rails (Metal Beam Crash Barriers)",
    #     "faded painting on barriers",
    #     "damaged signboard/sign structure",
    #     "signages - Day",
    #     "signages - Night",
    #     "damaged attenuators(blinkers)",
    #     "damaged delineators(blinkers)",
    #     "damaged anti-glare(blinkers)",
    #     "highway light damaged",
    #     "non functional light",
    #     "unauthorized signboard/hoarding",
    #     "inappropriate division",
    #     "damaged bus shelters",
    #     "faded bus-by markers",
    #     "truck lay-by markers",
    #     "holding boards",
    #     "advertisement boards",
    #     "work zone safety(signboard visibility)",
    #     "work zone safety(barricading)"
    # ]

    # Use caller-supplied labels (actual model categories) when provided;
    # fall back to hardcoded NHAI list otherwise.
    if category_labels is not None:
        all_category_labels = list(category_labels)
    else:
        # --- Hardcoded 17-category NHAI/Khammam labels (fallback) ---
        all_category_labels = [
            "Potholes", "Cracking", "Rutting", "Stripping/Delamination",
            "Pavement Joint", "Pavement Damage (Severe)", "Unsealed Road", "Settlement",
            "Shoulder - Rain Cuts", "Shoulder - Edge Drop", "Shoulder - Unevenness",
            "Shoulder - Vegetation Growth", "Damaged Kerb", "Faded Kerb Painting",
            "Reduced Visibility Due to Plantation Growth", "Median Separator Damaged",
            "Median Separator Paint Faded", "Missing Plants / Irregular Gaps (Median)",
            "Deteriorated or Damaged Plants (Median)", "Excessive Plantation Growth",
            "Damaged Drain Cover Slabs", "Missing Drain Cover Slabs", "Manhole Cover",
            "Water Stagnation", "Damaged Footpath Tiles / Paver Blocks",
            "Damaged Crash Barriers", "Damaged (MBCB) Metal Beam Crash Barrier",
            "Damaged (PGR) Pedestrian Guard Rail", "Faded Painting Concrete Crash (CC) Barrier",
            "Barriers - Faded Painting Guard Rails", "Damaged Sign Boards / Sign Structures",
            "Signage - Poor Visibility (Day)", "Signage - Poor Visibility (Night)",
            "Damaged Blinkers", "Damaged Attenuators", "Damaged Delineators", "Damaged Anti-Glare",
            "Damaged Road Studs", "Road Studs - Poor Visibility (Day)",
            "Road Studs - Poor Visibility (Night)", "Damaged Rumble Strips", "Damaged Hazard Markers",
            "Faded Pavement Marking", "Pavement Marking - Poor Visibility (Day)",
            "Pavement Marking - Poor Visibility (Night)", "Bus Bay - Damaged Shelters",
            "Bus Bay - Faded Markings", "Bus Bay - Damaged Signages",
            "Truck Lay By - Damaged Shelters", "Truck Lay By - Faded Markings",
            "Truck Lay By - Damaged Signages", "Damaged Highway Lights", "Non-Functional Highway Lights",
            "Work Zone - Inadequate Signboard Visibility", "Work Zone - Inadequate Barricading",
            "Work Zone - Poor Diversion Arrangement / Condition", "Unauthorized Median Openings",
            "Unauthorized Signboards", "Unauthorized Hoardings", "Illegal Parking",
            "General Encroachments", "Cleanliness - Litter", "Cleanliness - Debris",
            "Missing Assets (Signages)", "Missing Assets (Guard Rails)", "Missing Assets (Street Lights)"
        ]

    # Build defect_categories dict with sanitized column names
    defect_categories = {}
    for label in all_category_labels:
        sanitized = _sanitize_label_to_key(label)
        defect_categories[label] = f"{sanitized}_Count"

    # DO NOT include grouped categories here to avoid duplication
    # since individual labels already have severity (e.g., "Pothole (high)")
    # defect_categories["Potholes"] = "Potholes_Count"
    # defect_categories["Cracking"] = "Cracking_Count"
    # defect_categories["Rutting"] = "Rutting_Count"

    # ONLY include severity-specific labels from category_information
    # Do NOT include aggregated categories (Potholes, Cracking, Rutting, etc.)

    # Levels of severity
    severity_levels = ["Low", "Medium", "High"]

    # Iterate through each defect category and severity level to calculate the values
    for defect_name, column_name in defect_categories.items():
        # Skip if column doesn't exist in dataframe
        if column_name not in df.columns:
            continue

        severity_column_name = column_name.replace('Count', 'Severity')

        # Skip if severity column doesn't exist
        if severity_column_name not in df.columns:
            continue

        # Check if defect_name already has severity in its name
        # Format 1: "Pothole (high)" or Format 2: "potholes_high", "cracks_low", etc.
        has_severity_in_name = ('(high)' in defect_name.lower() or
                                '(medium)' in defect_name.lower() or
                                '(low)' in defect_name.lower() or
                                defect_name.lower().endswith('_high') or
                                defect_name.lower().endswith('_medium') or
                                defect_name.lower().endswith('_low'))

        if has_severity_in_name:
            # For labels with severity already in name, map directly
            if '(high)' in defect_name.lower() or defect_name.lower().endswith('_high'):
                severity_level = 'High'
            elif '(medium)' in defect_name.lower() or defect_name.lower().endswith('_medium'):
                severity_level = 'Medium'
            elif '(low)' in defect_name.lower() or defect_name.lower().endswith('_low'):
                severity_level = 'Low'

            count = df[column_name].sum()
            json_data["defectDetails"].append({
                "name": defect_name,
                "level": severity_level,
                "value": count
            })
        else:
            # For labels without severity in name (e.g., "Rain Cuts", "Water Stagnation")
            # Check if all rows have severity 'none' - if so, just add total count once
            all_severities = df[severity_column_name].str.lower().unique()

            if len(all_severities) == 1 and all_severities[0] == 'none':
                # No meaningful severity - just add total count with "Low" as default level
                count = df[column_name].sum()
                if count > 0:  # Only add if there are defects
                    json_data["defectDetails"].append({
                        "name": defect_name,
                        "level": "Low",  # Default level for defects without severity classification
                        "value": count
                    })
            else:
                # Has meaningful severity distribution - iterate through levels
                for level in severity_levels:
                    count = df[df[severity_column_name].str.lower() == level.lower()][column_name].sum()
                    json_data["defectDetails"].append({
                        "name": defect_name,
                        "level": level,
                        "value": count
                    })

    return json_data
