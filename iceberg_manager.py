"""Iceberg table management for BigLake integration."""

import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List
from google.cloud import storage
from config import Config


class IcebergManager:
    """Manages Apache Iceberg table operations for BigLake."""

    def __init__(self, config: Config):
        self.config = config
        self.storage_client = storage.Client(project=config.project_id)

    def create_iceberg_table_structure(self, json_file_path: str) -> bool:
        """Create Iceberg table structure from JSON data."""
        try:
            # Read and prepare data
            with open(json_file_path, 'r') as f:
                data = json.load(f)

            # Convert to DataFrame and normalize nested fields
            df = pd.json_normalize(data)

            # Ensure proper data types
            df['hire_date'] = pd.to_datetime(df['hire_date']).dt.date
            df['id'] = df['id'].astype('int64')
            df['age'] = df['age'].astype('int64')
            df['salary'] = df['salary'].astype('int64')
            df['is_active'] = df['is_active'].astype('bool')

            # Convert skills array to string representation for BigQuery compatibility
            df['skills'] = df['skills'].apply(lambda x: x if isinstance(x, list) else [])

            # Create the data directory structure for Iceberg
            table_path = f"iceberg-tables/{self.config.table_name}"
            self._create_iceberg_metadata(df, table_path)
            self._upload_data_as_parquet(df, table_path)

            print(f"Created Iceberg table structure for {self.config.table_name}")
            return True

        except Exception as e:
            print(f"Error creating Iceberg table structure: {e}")
            return False

    def _create_iceberg_metadata(self, df: pd.DataFrame, table_path: str) -> None:
        """Create Iceberg metadata files."""
        bucket = self.storage_client.bucket(self.config.bucket_name)

        # Create a simple metadata structure
        # Note: This is a simplified version. Full Iceberg implementation would require more complex metadata
        metadata = {
            "format-version": 2,
            "table-uuid": "example-uuid-1234",
            "location": f"gs://{self.config.bucket_name}/{table_path}",
            "last-sequence-number": 0,
            "last-updated-ms": pd.Timestamp.now().value // 1000000,
            "last-column-id": len(df.columns),
            "current-schema-id": 0,
            "schemas": [
                {
                    "schema-id": 0,
                    "type": "struct",
                    "fields": self._generate_schema_fields(df)
                }
            ],
            "default-spec-id": 0,
            "partition-specs": [
                {
                    "spec-id": 0,
                    "fields": []
                }
            ],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [
                {
                    "order-id": 0,
                    "fields": []
                }
            ],
            "properties": {},
            "current-snapshot-id": None,
            "refs": {},
            "snapshots": [],
            "statistics": [],
            "snapshot-log": [],
            "metadata-log": []
        }

        # Upload metadata
        metadata_blob = bucket.blob(f"{table_path}/metadata/v1.metadata.json")
        metadata_blob.upload_from_string(json.dumps(metadata, indent=2))

        # Create version hint
        version_hint_blob = bucket.blob(f"{table_path}/metadata/version-hint.text")
        version_hint_blob.upload_from_string("1")

    def _generate_schema_fields(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Generate Iceberg schema fields from DataFrame."""
        fields = []
        field_id = 1

        type_mapping = {
            'int64': 'long',
            'float64': 'double',
            'object': 'string',
            'bool': 'boolean',
            'datetime64[ns]': 'timestamp',
            'date': 'date'
        }

        for col_name, dtype in df.dtypes.items():
            iceberg_type = type_mapping.get(str(dtype), 'string')

            # Handle special cases
            if col_name == 'skills':
                iceberg_type = {
                    "type": "list",
                    "element-id": field_id + 1000,
                    "element": "string",
                    "element-required": False
                }
                field_id += 1000

            fields.append({
                "id": field_id,
                "name": col_name,
                "required": col_name in ['id', 'name', 'email'],
                "type": iceberg_type
            })
            field_id += 1

        return fields

    def _upload_data_as_parquet(self, df: pd.DataFrame, table_path: str) -> None:
        """Upload data as Parquet files in Iceberg format."""
        bucket = self.storage_client.bucket(self.config.bucket_name)

        # Create a data file
        parquet_filename = f"data_001.parquet"
        local_parquet_path = Path(parquet_filename)

        # Write to local Parquet file
        df.to_parquet(local_parquet_path, index=False, engine='pyarrow')

        # Upload to GCS
        data_blob = bucket.blob(f"{table_path}/data/{parquet_filename}")
        data_blob.upload_from_filename(str(local_parquet_path))

        # Clean up local file
        local_parquet_path.unlink()

        print(f"Uploaded data file: gs://{self.config.bucket_name}/{table_path}/data/{parquet_filename}")

    def list_iceberg_files(self) -> List[str]:
        """List all files in the Iceberg table directory."""
        try:
            bucket = self.storage_client.bucket(self.config.bucket_name)
            prefix = f"iceberg-tables/{self.config.table_name}/"
            blobs = bucket.list_blobs(prefix=prefix)
            return [blob.name for blob in blobs]
        except Exception as e:
            print(f"Error listing Iceberg files: {e}")
            return []

    def verify_iceberg_structure(self) -> bool:
        """Verify that the Iceberg table structure is properly created."""
        try:
            files = self.list_iceberg_files()

            required_files = [
                f"iceberg-tables/{self.config.table_name}/metadata/v1.metadata.json",
                f"iceberg-tables/{self.config.table_name}/metadata/version-hint.text",
                f"iceberg-tables/{self.config.table_name}/data/data_001.parquet"
            ]

            missing_files = [f for f in required_files if f not in files]

            if missing_files:
                print(f"Missing required Iceberg files: {missing_files}")
                return False

            print("Iceberg table structure verified successfully")
            print(f"Found {len(files)} files in table directory")
            return True

        except Exception as e:
            print(f"Error verifying Iceberg structure: {e}")
            return False

    def delete_iceberg_table(self) -> bool:
        """Delete the entire Iceberg table directory."""
        try:
            bucket = self.storage_client.bucket(self.config.bucket_name)
            prefix = f"iceberg-tables/{self.config.table_name}/"
            blobs = bucket.list_blobs(prefix=prefix)

            deleted_count = 0
            for blob in blobs:
                blob.delete()
                deleted_count += 1

            print(f"Deleted Iceberg table: {deleted_count} files removed")
            return True

        except Exception as e:
            print(f"Error deleting Iceberg table: {e}")
            return False