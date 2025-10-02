"""Google Cloud Storage management for BigLake integration."""

import json
import pandas as pd
from google.cloud import storage
from google.cloud.exceptions import NotFound
from typing import List, Dict, Any
from config import Config


class GCSManager:
    """Manages Google Cloud Storage operations for BigLake."""

    def __init__(self, config: Config):
        self.config = config
        self.client = storage.Client(project=config.project_id)

    def create_bucket_if_not_exists(self) -> bool:
        """Create GCS bucket if it doesn't exist."""
        try:
            bucket = self.client.get_bucket(self.config.bucket_name)
            print(f"Bucket {self.config.bucket_name} already exists")
            return True
        except NotFound:
            try:
                bucket = self.client.create_bucket(
                    self.config.bucket_name,
                    location=self.config.region
                )
                print(f"Created bucket {self.config.bucket_name} in {self.config.region}")
                return True
            except Exception as e:
                print(f"Error creating bucket: {e}")
                return False
        except Exception as e:
            if "Unable to acquire impersonated credentials" in str(e):
                print("❌ Authentication Error: Unable to acquire impersonated credentials")
                print("This usually means the current authentication setup is incorrect.")
                print("\nTo fix this, try one of these solutions:")
                print("1. Use application default credentials:")
                print("   gcloud auth application-default login")
                print("2. Or set a service account key:")
                print("   export GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account-key.json")
                print("3. Or use gcloud auth:")
                print("   gcloud auth login")
                print("   gcloud config set project YOUR_PROJECT_ID")
            else:
                print(f"Error accessing GCS: {e}")
            return False

    def upload_json_data(self, json_file_path: str) -> bool:
        """Upload JSON data to GCS in preparation for Iceberg conversion."""
        try:
            bucket = self.client.bucket(self.config.bucket_name)

            # Upload the raw JSON file
            json_blob = bucket.blob(f"raw-data/{self.config.table_name}.json")
            json_blob.upload_from_filename(json_file_path)
            print(f"Uploaded {json_file_path} to gs://{self.config.bucket_name}/raw-data/{self.config.table_name}.json")

            return True
        except Exception as e:
            print(f"Error uploading JSON data: {e}")
            return False

    def convert_json_to_parquet(self, json_file_path: str) -> bool:
        """Convert JSON data to Parquet format and upload to GCS."""
        try:
            # Read JSON data
            with open(json_file_path, 'r') as f:
                data = json.load(f)

            # Convert to DataFrame
            df = pd.json_normalize(data)

            # Convert to Parquet format in memory
            parquet_filename = f"{self.config.table_name}.parquet"
            df.to_parquet(parquet_filename, index=False)

            # Upload to GCS
            bucket = self.client.bucket(self.config.bucket_name)
            parquet_blob = bucket.blob(f"iceberg-tables/{self.config.table_name}/data/{parquet_filename}")
            parquet_blob.upload_from_filename(parquet_filename)

            print(f"Converted and uploaded Parquet file to gs://{self.config.bucket_name}/iceberg-tables/{self.config.table_name}/data/{parquet_filename}")

            # Clean up local file
            import os
            os.remove(parquet_filename)

            return True
        except Exception as e:
            print(f"Error converting JSON to Parquet: {e}")
            return False

    def list_bucket_contents(self, prefix: str = "") -> List[str]:
        """List contents of the GCS bucket."""
        try:
            bucket = self.client.bucket(self.config.bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)
            return [blob.name for blob in blobs]
        except Exception as e:
            print(f"Error listing bucket contents: {e}")
            return []

    def delete_bucket_contents(self, prefix: str = "") -> bool:
        """Delete all objects in bucket with given prefix."""
        try:
            bucket = self.client.bucket(self.config.bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)

            deleted_count = 0
            for blob in blobs:
                blob.delete()
                deleted_count += 1

            print(f"Deleted {deleted_count} objects with prefix '{prefix}'")
            return True
        except Exception as e:
            print(f"Error deleting bucket contents: {e}")
            return False