"""S3 Manager refactored for Google Cloud Storage (GCS) operations"""
import os
import threading
import time
import concurrent.futures
from google.cloud import storage

class S3Manager:
    """Handles all GCS download and upload operations (replaces AWS S3)"""

    def __init__(self, config):
        self.config = config
        self.storage_client = storage.Client()
        # Expand connection pool to avoid "Connection pool is full" warnings
        # when uploading with 32 parallel threads
        import requests.adapters as _ra
        _adapter = _ra.HTTPAdapter(pool_connections=32, pool_maxsize=32)
        self.storage_client._http.mount("https://", _adapter)
        self.storage_client._http.mount("http://",  _adapter)

    def download_file(self, bucket_name, gcs_key, target_path):
        """Download a single file from GCS"""
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(gcs_key)
            blob.download_to_filename(target_path)
        except Exception as e:
            print(f"Error downloading {gcs_key} from {bucket_name}: {e}")

    def upload_file(self, local_path, bucket_name, gcs_key, content_type=None, content_disposition=None):
        """Upload a file to GCS"""
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(gcs_key)
            
            if content_type:
                blob.content_type = content_type
            if content_disposition:
                blob.content_disposition = content_disposition

            blob.upload_from_filename(local_path)
            return True
        except Exception as e:
            print(f"Failed to upload {local_path}: {e}")
            return False

    def download_all_images(self, frame_list_data, codebuild_id):
        """Download all images in parallel using GCP logic"""
        count = 0
        for image in frame_list_data:
            while threading.active_count() > 100:
                time.sleep(1)

            # GCP pathing (matching your migrated AWS structure)
            base_path = f'videoModelOut/{codebuild_id}/artifacts/'
            inference_gcs = base_path + image['inference_image']
            og_image_gcs = base_path + image['og_file']

            inference_local = f"{self.config.working_directory}/{image['inference_image'].split('/')[-1]}"
            og_local = f"{self.config.og_image_directory}/{image['og_file'].split('/')[-1]}"

            threading.Thread(
                target=self.download_file,
                args=[self.config.bucket_name, inference_gcs, inference_local]
            ).start()

            threading.Thread(
                target=self.download_file,
                args=[self.config.bucket_name, og_image_gcs, og_local]
            ).start()

        while threading.active_count() > 1:
            time.sleep(0.5)
        print(f"All images downloaded from GCS")

    def upload_defect_images(self, dataframe, codebuild_id):
        """Upload defect images to GCS in parallel and return DataFrame with GCP Public URLs"""
        folder_path = f"processed-data/{self.config.road_id}/{codebuild_id}/defects/"

        rows = list(dataframe.iterrows())
        url_map = {}

        def _upload_one(item):
            index, row = item
            gcs_file_path = row['File_name']
            local_file_path = '/'.join(gcs_file_path.split('/')[6:])
            destination_key = f"{folder_path}{gcs_file_path.split('/')[-1]}"
            success = self.upload_file(local_file_path, self.config.new_bucket, destination_key)
            if success:
                url_map[index] = f"https://storage.googleapis.com/{self.config.new_bucket}/{destination_key}"

        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
            list(pool.map(_upload_one, rows))

        for index, url in url_map.items():
            dataframe.at[index, 'File_URL'] = url

        return dataframe

    def upload_video(self, video_path, uid):
        """Upload generated video to GCS bucket"""
        try:
            gcs_key = f"processed-data/{self.config.road_id}/{uid}/annotated_video.mp4"
            print(f"[*] Uploading video to gs://{self.config.video_bucket}/{gcs_key}")

            success = self.upload_file(
                video_path,
                self.config.video_bucket,
                gcs_key,
                content_type='video/mp4',
                content_disposition='inline'
            )

            if success:
                video_url = f"https://storage.googleapis.com/{self.config.video_bucket}/{gcs_key}"
                print(f"[✓] Video uploaded to GCP!")
                print(f"[*] Video URL: {video_url}")
                return video_url
            return None

        except Exception as e:
            print(f"[ERROR] GCP Video upload failed: {e}")
            return None

    def upload_frames_and_predict_folders(self, codebuild_id):
        """Upload frames and predict folders to GCS in parallel"""
        frames_dir = "frame_data/frames"
        predict_dir = "frame_data/predict"
        base_gcs_path = f"processed-data/{self.config.road_id}/{codebuild_id}/annotated_frames"

        # Build full task list first
        tasks = []
        for local_dir, sub_path in [(frames_dir, "frames"), (predict_dir, "predict")]:
            if os.path.exists(local_dir):
                for filename in os.listdir(local_dir):
                    local_path = os.path.join(local_dir, filename)
                    if os.path.isfile(local_path):
                        gcs_key = f"{base_gcs_path}/{sub_path}/{filename}"
                        tasks.append((local_path, gcs_key))
            else:
                print(f"⚠️ Directory not found: {local_dir}")

        uploaded_count = 0
        lock = threading.Lock()

        def _upload_one(task):
            nonlocal uploaded_count
            local_path, gcs_key = task
            if self.upload_file(local_path, self.config.artifacts_bucket, gcs_key):
                with lock:
                    uploaded_count += 1

        print(f"[*] Uploading {len(tasks)} files to GCS GenerateAnotatedImages/ (32 threads)...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
            list(pool.map(_upload_one, tasks))

        print(f"[✓] Total {uploaded_count} files uploaded to GCP GenerateAnotatedImages/{codebuild_id}/")
        return uploaded_count
