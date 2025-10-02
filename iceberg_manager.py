"""Iceberg table management for BigLake integration."""

import json
import pandas as pd
import struct
import io
from pathlib import Path
from typing import Dict, Any, List
from google.cloud import storage
from config import Config
import fastavro


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

            # Replace dots with underscores in column names for BigQuery compatibility
            df.columns = df.columns.str.replace('.', '_')

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
            parquet_filename = self._upload_data_as_parquet(df, table_path)
            self._create_manifest_files(df, table_path, parquet_filename)
            self._create_iceberg_metadata(df, table_path)

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
        # Generate timestamps and IDs
        current_timestamp = pd.Timestamp.now().value // 1000000
        snapshot_id = 1

        metadata = {
            "format-version": 2,
            "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
            "location": f"gs://{self.config.bucket_name}/{table_path}",
            "last-sequence-number": 1,
            "last-updated-ms": current_timestamp,
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
            "current-snapshot-id": snapshot_id,
            "refs": {
                "main": {
                    "snapshot-id": snapshot_id,
                    "type": "branch"
                }
            },
            "snapshots": [
                {
                    "snapshot-id": snapshot_id,
                    "timestamp-ms": current_timestamp,
                    "sequence-number": 1,
                    "summary": {
                        "operation": "append",
                        "added-files": "1",
                        "added-records": str(len(df)),
                        "total-files": "1",
                        "total-records": str(len(df))
                    },
                    "manifest-list": f"gs://{self.config.bucket_name}/{table_path}/metadata/snap-{snapshot_id}-1-manifest-list.avro",
                    "schema-id": 0
                }
            ],
            "statistics": [],
            "snapshot-log": [
                {
                    "timestamp-ms": current_timestamp,
                    "snapshot-id": snapshot_id
                }
            ],
            "metadata-log": [
                {
                    "timestamp-ms": current_timestamp,
                    "metadata-file": f"gs://{self.config.bucket_name}/{table_path}/metadata/v1.metadata.json"
                }
            ]
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

        # Return the parquet filename for manifest creation
        return parquet_filename

    def _create_manifest_files(self, df: pd.DataFrame, table_path: str, parquet_filename: str) -> None:
        """Create Iceberg manifest and manifest list files in Avro format."""
        bucket = self.storage_client.bucket(self.config.bucket_name)

        # Get file stats for the parquet file
        parquet_blob = bucket.blob(f"{table_path}/data/{parquet_filename}")
        parquet_blob.reload()  # Get updated metadata
        file_size = parquet_blob.size

        # Define Avro schema for data file record
        data_file_schema = {
            "type": "record",
            "name": "data_file",
            "fields": [
                {"name": "content", "type": "int"},
                {"name": "file_path", "type": "string"},
                {"name": "file_format", "type": "string"},
                {"name": "partition", "type": {"type": "map", "values": "string"}},
                {"name": "record_count", "type": "long"},
                {"name": "file_size_in_bytes", "type": "long"},
                {"name": "column_sizes", "type": {"type": "map", "values": "long"}},
                {"name": "value_counts", "type": {"type": "map", "values": "long"}},
                {"name": "null_value_counts", "type": {"type": "map", "values": "long"}},
                {"name": "nan_value_counts", "type": {"type": "map", "values": "long"}},
                {"name": "lower_bounds", "type": {"type": "map", "values": "bytes"}},
                {"name": "upper_bounds", "type": {"type": "map", "values": "bytes"}},
                {"name": "key_metadata", "type": ["null", "bytes"]},
                {"name": "split_offsets", "type": {"type": "array", "items": "long"}},
                {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int"}]},
                {"name": "sort_order_id", "type": ["null", "int"]}
            ]
        }

        # Define Avro schema for manifest entry
        manifest_entry_schema = {
            "type": "record",
            "name": "manifest_entry",
            "fields": [
                {"name": "status", "type": "int"},
                {"name": "snapshot_id", "type": ["null", "long"]},
                {"name": "sequence_number", "type": ["null", "long"]},
                {"name": "file_sequence_number", "type": ["null", "long"]},
                {"name": "data_file", "type": data_file_schema}
            ]
        }

        # Create manifest entry data
        manifest_entry = {
            "status": 1,  # ADDED
            "snapshot_id": 1,
            "sequence_number": 1,
            "file_sequence_number": 1,
            "data_file": {
                "content": 0,  # DATA content type
                "file_path": f"gs://{self.config.bucket_name}/{table_path}/data/{parquet_filename}",
                "file_format": "PARQUET",
                "partition": {},
                "record_count": len(df),
                "file_size_in_bytes": file_size,
                "column_sizes": {},
                "value_counts": {},
                "null_value_counts": {},
                "nan_value_counts": {},
                "lower_bounds": {},
                "upper_bounds": {},
                "key_metadata": None,
                "split_offsets": [4],
                "equality_ids": None,
                "sort_order_id": None
            }
        }

        # Create and upload manifest file in Avro format
        manifest_filename = f"snap-1-1-{len(df)}-manifest.avro"
        manifest_buffer = io.BytesIO()

        fastavro.writer(manifest_buffer, manifest_entry_schema, [manifest_entry])
        manifest_blob = bucket.blob(f"{table_path}/metadata/{manifest_filename}")
        manifest_blob.upload_from_string(manifest_buffer.getvalue(), content_type='application/octet-stream')

        # Define Avro schema for manifest file record
        manifest_file_schema = {
            "type": "record",
            "name": "manifest_file",
            "fields": [
                {"name": "manifest_path", "type": "string"},
                {"name": "manifest_length", "type": "long"},
                {"name": "partition_spec_id", "type": "int"},
                {"name": "content", "type": "int"},
                {"name": "sequence_number", "type": "long"},
                {"name": "min_sequence_number", "type": "long"},
                {"name": "added_snapshot_id", "type": "long"},
                {"name": "added_files_count", "type": "int"},
                {"name": "existing_files_count", "type": "int"},
                {"name": "deleted_files_count", "type": "int"},
                {"name": "added_rows_count", "type": "long"},
                {"name": "existing_rows_count", "type": "long"},
                {"name": "deleted_rows_count", "type": "long"},
                {"name": "partitions", "type": {"type": "array", "items": {
                    "type": "record",
                    "name": "field_summary",
                    "fields": [
                        {"name": "contains_null", "type": "boolean"},
                        {"name": "contains_nan", "type": ["null", "boolean"]},
                        {"name": "lower_bound", "type": ["null", "bytes"]},
                        {"name": "upper_bound", "type": ["null", "bytes"]}
                    ]
                }}}
            ]
        }

        # Create manifest list entry
        manifest_list_entry = {
            "manifest_path": f"gs://{self.config.bucket_name}/{table_path}/metadata/{manifest_filename}",
            "manifest_length": len(manifest_buffer.getvalue()),
            "partition_spec_id": 0,
            "content": 0,  # DATA content type
            "sequence_number": 1,
            "min_sequence_number": 1,
            "added_snapshot_id": 1,
            "added_files_count": 1,
            "existing_files_count": 0,
            "deleted_files_count": 0,
            "added_rows_count": len(df),
            "existing_rows_count": 0,
            "deleted_rows_count": 0,
            "partitions": []
        }

        # Create and upload manifest list file in Avro format
        manifest_list_filename = "snap-1-1-manifest-list.avro"
        manifest_list_buffer = io.BytesIO()

        fastavro.writer(manifest_list_buffer, manifest_file_schema, [manifest_list_entry])
        manifest_list_blob = bucket.blob(f"{table_path}/metadata/{manifest_list_filename}")
        manifest_list_blob.upload_from_string(manifest_list_buffer.getvalue(), content_type='application/octet-stream')

        print(f"Created manifest files:")
        print(f"  - {manifest_filename}")
        print(f"  - {manifest_list_filename}")

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

            # Check for manifest files (they have dynamic names, so we'll check if any exist)
            manifest_files = [f for f in files if 'manifest' in f and f.endswith('.avro')]
            if len(manifest_files) < 2:  # Should have at least manifest and manifest-list
                print(f"Missing manifest files. Found: {manifest_files}")
                return False

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