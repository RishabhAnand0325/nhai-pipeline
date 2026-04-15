"""Video Generator for creating videos from annotated frames"""
import os
import cv2
import glob
import re
import subprocess
import json


class VideoGenerator:
    """Handles video generation from annotated frames"""

    def __init__(self, config):
        self.config = config

    def create_video_from_frames(self, uid, result_consolidated):
        """Create a video from the annotated frames in the frame_data/predict/ directory"""
        # Get FPS from the result.json
        fps = 1.0  # Default to 1 FPS
        try:
            if os.path.exists(result_consolidated):
                with open(result_consolidated, 'r') as f:
                    result_data = json.loads(f.read())
                    if isinstance(result_data, list) and len(result_data) > 0:
                        fps = result_data[0].get('info', {}).get('fps', 1.0)
                        if not fps or fps <= 0:
                            fps = 1.0
                        print(f"[*] Video FPS from result.json: {fps}")
        except Exception as e:
            print(f"[WARNING] Could not read FPS from result.json: {e}")
            print("[*] Using default FPS: 1.0")

        # Get all frames from frame_data/predict directory (support both PNG and JPEG)
        frame_files = sorted(
            glob.glob('frame_data/predict/frame_*.[jp][pn]g'),
            key=lambda x: int(re.findall(r'\d+', os.path.basename(x))[0])
        )

        if not frame_files:
            print("[WARNING] No annotated frames found in frame_data/predict/ directory. Skipping video generation.")
            return None

        print(f"[*] Found {len(frame_files)} annotated frames")

        # Read the first frame to get dimensions
        first_frame = cv2.imread(frame_files[0])
        if first_frame is None:
            print("[ERROR] Could not read first frame. Skipping video generation.")
            return None

        height, width = first_frame.shape[:2]
        print(f"[*] Video dimensions: {width}x{height}")

        # Create video directly with ffmpeg (H.264 codec for browser compatibility)
        output_path = f'{uid}.mp4'

        try:
            print("[*] Creating H.264 video directly with ffmpeg...")

            # Detect file extension from first frame
            file_ext = 'jpg' if frame_files[0].endswith('.jpg') else 'png'
            input_pattern = f'frame_data/predict/frame_*.{file_ext}'

            # Create video from image sequence using ffmpeg with glob pattern
            # This handles non-sequential frame numbers correctly
            ffmpeg_cmd = [
                'ffmpeg', '-y',
                '-framerate', str(fps),
                '-pattern_type', 'glob',
                '-i', input_pattern,
                '-vcodec', 'libx264',
                '-preset', 'fast',
                '-crf', '23',
                '-pix_fmt', 'yuv420p',
                '-vf', f'scale={width}:{height}',
                output_path
            ]

            result = subprocess.run(ffmpeg_cmd, capture_output=True, text=True, timeout=300)

            if result.returncode == 0:
                print(f"[✓] Video created successfully with H.264 codec: {output_path}")
                return output_path
            else:
                print(f"[ERROR] ffmpeg failed: {result.stderr}")
                print("[*] Falling back to OpenCV VideoWriter...")
                raise Exception("ffmpeg failed")

        except (FileNotFoundError, Exception) as e:
            # Fallback to OpenCV if ffmpeg is not available or fails
            print(f"[WARNING] ffmpeg not available or failed: {e}")
            print("[*] Using OpenCV VideoWriter (mp4v codec - may not play in browsers)")

            fourcc = cv2.VideoWriter_fourcc(*'mp4v')
            video_writer = cv2.VideoWriter(output_path, fourcc, fps, (width, height))

            if not video_writer.isOpened():
                print("[ERROR] Could not initialize video writer.")
                return None

            # Write frames to video
            for i, frame_file in enumerate(frame_files):
                frame = cv2.imread(frame_file)
                if frame is not None:
                    video_writer.write(frame)
                    if (i + 1) % 100 == 0:
                        print(f"[*] Processed {i + 1}/{len(frame_files)} frames")

            video_writer.release()
            print(f"[✓] Video created with OpenCV (may not play in browsers): {output_path}")

        return output_path

    def extract_uid_from_result(self, result_consolidated):
        """Extract UID from result.json"""
        uid = None
        try:
            if os.path.exists(result_consolidated):
                with open(result_consolidated, 'r') as f:
                    result_data = json.loads(f.read())
                    if isinstance(result_data, list) and len(result_data) > 0:
                        uid = result_data[0].get('info', {}).get('uid')
                        if uid:
                            print(f"[*] Extracted UID from result.json: {uid}")
        except Exception as e:
            print(f"[WARNING] Could not extract UID from result.json: {e}")

        if not uid:
            print("[WARNING] UID not found in result.json. Trying S3_KEY environment variable...")
            if 'S3_KEY' in os.environ:
                s3_key_env = os.environ['S3_KEY']
                uid = s3_key_env.split('/')[-2]
                print(f"[*] Extracted UID from S3_KEY: {uid}")

        return uid
