"""Configuration Manager for Annotation Pipeline - GCP Version"""
import os
import json

class ConfigManager:
    """Manages configuration settings for the annotation pipeline on Google Cloud"""

    def __init__(self):
        # ----------------- GCP & API Configuration ----------------- #
        self.gmaps_api_key = 'AIzaSyDzNunxl1PAIeR8ZTgvTVFYuIpLEPVeWjw'

        # Road ID — set per-job via environment variable
        self.road_id = os.environ.get("ROAD_ID", "R000000")

        # All outputs go to a single bucket; paths are segregated by road_id/build_id
        self.bucket_name      = "datanh11"
        self.artifacts_bucket = "datanh11"
        self.upload_bucket    = "datanh11"
        self.new_bucket       = "datanh11"
        self.video_bucket     = "datanh11"
        
        # Backend API Settings
        self.base_url = "https://roadvision-backend-new-505717192876.asia-south1.run.app/"
        self.secret_key = "sleeksitescodepipeline"
        
        # Local paths inside the Cloud Build container
        self.working_directory = "./predict"
        self.og_image_directory = "./frames"

        # Font settings for image annotations
        self.font_scale = 0.5
        self.light_thickness = 1
        self.thick_thickness = 3
        self.text_color_black = (0, 0, 0)

        # Gemini AI settings
        self.enable_gemini_inference = False  
        self.gemini_draw_bbox = False 

        # Maps category_id (from nhai_best.pt YOLO model) to defect properties
        # 34 classes matching the local GPU model
        self.defect_config = {
            0:  {"name": "Bus Bay – Faded Markings",                        "color": (170, 120, 170), "severity": "low",    "category": "Markings"},
            1:  {"name": "Cleanliness – Debris",                            "color": (80, 80, 80),    "severity": "low",    "category": "Cleanliness"},
            2:  {"name": "Cleanliness – Litter",                            "color": (90, 90, 90),    "severity": "low",    "category": "Cleanliness"},
            3:  {"name": "Cracking",                                        "color": (0, 255, 22),    "severity": "high",   "category": "Crack"},
            4:  {"name": "Damaged (MBCB) Metal Beam Crash Barrier",         "color": (170, 170, 170), "severity": "high",   "category": "Barriers"},
            5:  {"name": "Damaged Footpath Tiles / Paver Blocks",           "color": (200, 180, 160), "severity": "low",    "category": "Footpath"},
            6:  {"name": "Damaged Kerb",                                    "color": (180, 150, 120), "severity": "medium", "category": "Kerb"},
            7:  {"name": "Deteriorated or Damaged Plants (Median)",         "color": (150, 200, 100), "severity": "low",    "category": "Plantation"},
            8:  {"name": "Excessive Plantation Growth",                     "color": (100, 150, 80),  "severity": "medium", "category": "Plantation"},
            9:  {"name": "Faded Kerb Painting",                             "color": (200, 170, 140), "severity": "low",    "category": "Kerb"},
            10: {"name": "Faded Painting Concrete Crash (CC) Barrier",      "color": (190, 190, 190), "severity": "low",    "category": "Barriers"},
            11: {"name": "Faded Pavement Marking",                          "color": (200, 200, 200), "severity": "low",    "category": "Pavement Markings"},
            12: {"name": "General Encroachments",                           "color": (200, 100, 50),  "severity": "medium", "category": "Encroachment"},
            13: {"name": "Illegal Parking",                                 "color": (255, 50, 50),   "severity": "medium", "category": "Parking"},
            14: {"name": "Manhole Cover",                                   "color": (120, 120, 120), "severity": "medium", "category": "Drainage"},
            15: {"name": "Median Separator Paint Faded",                    "color": (180, 180, 150), "severity": "low",    "category": "Median"},
            16: {"name": "Missing Plants / Irregular Gaps (Median)",        "color": (130, 180, 100), "severity": "low",    "category": "Plantation"},
            17: {"name": "Non-Functional Highway Lights",                   "color": (255, 255, 100), "severity": "medium", "category": "Lighting"},
            18: {"name": "Patching",                                        "color": (123, 225, 255), "severity": "medium", "category": "Patching"},
            19: {"name": "Pavement Damage (Severe)",                        "color": (0, 0, 255),     "severity": "high",   "category": "Pavement Damage"},
            20: {"name": "Pavement Joint",                                  "color": (150, 150, 200), "severity": "low",    "category": "Pavement Joint"},
            21: {"name": "Pavement Marking – Poor Visibility (Day)",        "color": (220, 220, 220), "severity": "low",    "category": "Pavement Markings"},
            22: {"name": "Pavement Marking – Poor Visibility (Night)",      "color": (180, 180, 180), "severity": "low",    "category": "Pavement Markings"},
            23: {"name": "Potholes",                                        "color": (237, 254, 151), "severity": "high",   "category": "Potholes"},
            24: {"name": "Road Studs – Poor Visibility (Day)",              "color": (255, 140, 140), "severity": "low",    "category": "Road Studs"},
            25: {"name": "Rutting",                                         "color": (244, 255, 151), "severity": "high",   "category": "Rutting"},
            26: {"name": "Shoulder – Edge Drop",                            "color": (150, 100, 100), "severity": "medium", "category": "Shoulders"},
            27: {"name": "Shoulder – Unevenness",                           "color": (100, 150, 200), "severity": "medium", "category": "Shoulders"},
            28: {"name": "Shoulder – Vegetation Growth",                    "color": (100, 200, 100), "severity": "low",    "category": "Shoulders"},
            29: {"name": "Signage – Poor Visibility (Day)",                 "color": (120, 120, 200), "severity": "low",    "category": "Signage"},
            30: {"name": "Stripping/Delamination",                          "color": (200, 100, 100), "severity": "high",   "category": "Pavement Damage"},
            31: {"name": "Unauthorized Hoardings",                          "color": (200, 50, 200),  "severity": "medium", "category": "Unauthorized"},
            32: {"name": "Unsealed Road",                                   "color": (180, 130, 80),  "severity": "high",   "category": "Pavement Damage"},
            33: {"name": "Water Stagnation",                                "color": (50, 100, 200),  "severity": "medium", "category": "Water Stagnation"},
        }

        self.category_information = {k: v["name"] for k, v in self.defect_config.items()}
        self.defect_colors = {v["name"]: v["color"] for v in self.defect_config.values()}

        self.categories_mapping = {}
        for cat_id, defect in self.defect_config.items():
            category = defect["category"]
            if category not in self.categories_mapping:
                self.categories_mapping[category] = []
            self.categories_mapping[category].append(str(cat_id))

        self.severity_order = { 'high': 3, 'medium': 2, 'low': 1, 'none': 0 }
        self.pipeline_config = self._load_pipeline_config()

    def _load_pipeline_config(self):
        try:
            if os.path.exists("video/data.json"):
                with open("video/data.json", "r") as config_file:
                    config_data = json.load(config_file)
                    pipeline_value = config_data.get("pipeline")
                    return {
                        "pipeline": "NHAI" if pipeline_value and pipeline_value.upper() == "NHAI" else "default"
                    }
        except Exception: pass
        return {"pipeline": "default"}

    def create_directory(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)
