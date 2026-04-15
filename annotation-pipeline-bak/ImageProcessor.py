"""Image Processor for drawing annotations on images"""
import cv2
import numpy as np


class ImageProcessor:
    """Handles image processing and annotation drawing"""

    def __init__(self, config):
        self.config = config
        self.font = cv2.FONT_HERSHEY_TRIPLEX

    def overlay_transparent_rectangle(self, image, top_left, bottom_right, color, transparency):
        """Overlay a transparent rectangle on an image"""
        if image is None:
            print("Error: Image is None in overlay_transparent_rectangle.")
            return None

        overlay = image.copy()
        output = image.copy()
        cv2.rectangle(overlay, top_left, bottom_right, color, -1)
        cv2.addWeighted(overlay, transparency, output, 1 - transparency, 0, output)
        return output

    def draw_finalized_bounding_boxes(self, img, bounding_boxes, labels):
        """Draw finalized bounding boxes with labels on image"""
        for bbox, lbl in zip(bounding_boxes, labels):
            x1, y1, w, h = map(int, bbox)
            x2, y2 = x1 + w, y1 + h
            # Get color from defect_colors (automatically generated from defect_config)
            color = self.config.defect_colors.get(lbl, (255, 255, 255))

            cv2.rectangle(img, (x1, y1), (x2, y2), color, self.config.thick_thickness)

            (text_width, text_height), _ = cv2.getTextSize(
                lbl, self.font, self.config.font_scale, self.config.light_thickness
            )
            label_bg_y1 = y1 - text_height - 5
            cv2.rectangle(img, (x1, label_bg_y1), (x1 + text_width, y1), color, -1)

            text_start_x = x1
            text_start_y = y1 - 7
            cv2.putText(
                img, lbl, (text_start_x, text_start_y),
                self.font, self.config.font_scale,
                self.config.text_color_black, self.config.light_thickness
            )

        return img

    def process_defect_annotations(self, image_path, annotations, category_information):
        """Process and draw defect annotations on image"""
        if not cv2.imread(image_path) is not None:
            print(f"Error: Cannot read image {image_path}")
            return None

        image = cv2.imread(image_path)
        final_severity = None

        for annotation in annotations:
            category_id = annotation["category_id"]

            # Get defect configuration from unified config
            defect = self.config.defect_config.get(category_id)
            if not defect:
                print(f"Warning: Unknown category_id {category_id}, using fallback")
                label = category_information.get(category_id, f"unknown_{category_id}")
                severity = "high"
                is_gemini = False
            else:
                label = defect["name"]
                severity = defect["severity"]
                is_gemini = defect.get("use_gemini", False)

            # Update final_severity if current severity is higher
            severity_ranks = {"high": 3, "medium": 2, "low": 1, None: 0}
            if severity_ranks.get(severity, 0) > severity_ranks.get(final_severity, 0):
                final_severity = severity

            # Check if we should draw this bounding box
            should_draw = True
            if is_gemini and not self.config.gemini_draw_bbox:
                should_draw = False

            # Draw bounding box if enabled
            if should_draw:
                bbox = annotation["bbox"]
                image = self.draw_finalized_bounding_boxes(image, [bbox], [label])

        cv2.imwrite(image_path, image)
        return final_severity
