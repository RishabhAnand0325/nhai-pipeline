import os
import cv2
import json
import base64
import requests
import time
from typing import Dict, List
from ConfigManager import ConfigManager
from difflib import get_close_matches


class GeminiInference:
    def __init__(self, api_key: str = "YOUR_API_KEY", config: ConfigManager = None):
        """Initialize Gemini inference with API key and config."""
        self.api_key = api_key
        self.base_url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"

        # Load config or create new one
        self.config = config if config else ConfigManager()

        # Build label list and mapping from config (only Gemini-enabled defects)
        self.gemini_labels = []
        self.label_to_category_id = {}

        for category_id, defect_info in self.config.defect_config.items():
            if defect_info.get("use_gemini", False):
                label = defect_info["name"]
                self.gemini_labels.append(label)
                self.label_to_category_id[label] = category_id

        # Build dynamic prompt with labels from config
        labels_str = ", ".join(self.gemini_labels)

        self.prompt = f"""The image is a road scene of resolution 640x640 pixels. Please detect all road defect types from the following list:
{labels_str}.
For each defect detected, output its label, and bounding box coordinates in pixel units in the format:
{{ "label": "<LABEL_NAME>", "x_min": <value>, "y_min": <value>, "x_max": <value>, "y_max": <value> }}.
Return the result as a JSON array of such objects. If no defect from the list is found, return an empty array.
Example output:

[
  {{ "label": "RAIN_CUTS", "x_min": 35, "y_min": 78, "x_max": 102, "y_max": 143 }},
  {{ "label": "WATER_STAGNATION", "x_min": 10, "y_min": 50, "x_max": 45, "y_max": 95 }}
]


Use the image's full resolution of 640×640 for the coordinates."""

    def encode_image_to_base64(self, image_path: str) -> str:
        """Convert image to base64 string for Gemini API."""
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')

    def call_gemini_api(self, image_path: str, max_retries: int = 2) -> List[Dict]:
        """Make API call to Gemini for defect detection with retry logic."""
        for attempt in range(max_retries):
            try:
                # Encode image to base64
                image_base64 = self.encode_image_to_base64(image_path)

                # Prepare the request payload
                payload = {
                    "contents": [
                        {
                            "parts": [
                                {"text": self.prompt},
                                {
                                    "inline_data": {
                                        "mime_type": "image/jpeg",
                                        "data": image_base64
                                    }
                                }
                            ]
                        }
                    ]
                }

                headers = {
                    "Content-Type": "application/json"
                }

                # Make the API request with increased timeout
                response = requests.post(
                    f"{self.base_url}?key={self.api_key}",
                    json=payload,
                    headers=headers,
                    timeout=120  # Increased from 60 to 120 seconds
                )

                if response.status_code == 200:
                    result = response.json()

                    # Extract the text response
                    if 'candidates' in result and len(result['candidates']) > 0:
                        content = result['candidates'][0]['content']['parts'][0]['text']

                        # Parse JSON from the response
                        try:
                            # Extract JSON from the response text
                            json_start = content.find('[')
                            json_end = content.rfind(']') + 1
                            if json_start >= 0 and json_end > json_start:
                                json_content = content[json_start:json_end]
                                return json.loads(json_content)
                            else:
                                return []
                        except json.JSONDecodeError:
                            return []
                    else:
                        return []
                else:
                    if attempt < max_retries - 1:
                        time.sleep(1)  # Brief pause before retry
                        continue
                    return []

            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    continue  # Retry on timeout
                return []
            except Exception:
                return []

        return []

    def detect_defects(self, image_path: str) -> List[Dict]:
        """Detect defects in the image using Gemini API."""
        print(f"🔍 Processing {image_path} for Gemini defect detection...")
        detections = self.call_gemini_api(image_path)

        # Enrich detections with category_id from config
        enriched_detections = []
        for detection in detections:
            label = detection.get("label")

            # Try exact match first
            if label in self.label_to_category_id:
                detection["category_id"] = self.label_to_category_id[label]
                detection["category_name"] = label
                enriched_detections.append(detection)
            else:
                # Try fuzzy matching for misspellings (remove spaces and compare)
                label_normalized = label.replace(" ", "").replace("_", "").upper()
                matched = False

                for valid_label in self.gemini_labels:
                    valid_normalized = valid_label.replace(" ", "").replace("_", "").upper()
                    # Use fuzzy string matching
                    close_matches = get_close_matches(label_normalized, [valid_normalized], n=1, cutoff=0.8)
                    if close_matches:
                        # Found a close match, use the valid label
                        detection["category_id"] = self.label_to_category_id[valid_label]
                        detection["category_name"] = valid_label
                        enriched_detections.append(detection)
                        matched = True
                        break

                if not matched:
                    # Silently skip unknown labels (reduce log spam)
                    pass

        print(f"✅ Found {len(enriched_detections)} Gemini defects")
        return enriched_detections

    def get_gemini_enabled_count(self) -> int:
        """Return the count of Gemini-enabled defect types."""
        return len(self.gemini_labels)


# Example usage and testing function
if __name__ == "__main__":
    # Initialize the Gemini inference
    gemini = GeminiInference()

    print(f"📋 Loaded {gemini.get_gemini_enabled_count()} Gemini-enabled defect types from config")
    print(f"🏷️ Labels: {', '.join(gemini.gemini_labels[:5])}... (showing first 5)")

    # Test with a sample image (replace with actual image path)
    test_image_path = "test_frame.jpg"

    if os.path.exists(test_image_path):
        print("\n🚀 Testing Gemini Inference Module")

        # Test defect detection
        print("\n--- Gemini Defect Detection ---")
        defect_results = gemini.detect_defects(test_image_path)
        print(json.dumps(defect_results, indent=2))

        print("\n✅ Gemini Inference Module Test Complete")
    else:
        print(f"\n⚠️ Test image not found: {test_image_path}")
        print("Place a test image and run this script to test the module")
