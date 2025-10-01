"""Configuration settings for BigLake Metastore integration."""

import os
from typing import Optional

class Config:
    """Configuration class for GCP resources."""

    def __init__(self):
        # GCP Project settings
        self.project_id: str = os.getenv('GCP_PROJECT_ID', 'your-project-id')
        self.region: str = os.getenv('GCP_REGION', 'us-central1')

        # Storage settings
        self.bucket_name: str = os.getenv('GCS_BUCKET_NAME', f'{self.project_id}-biglake-data')
        self.data_location: str = f'gs://{self.bucket_name}/iceberg-tables'

        # BigQuery settings
        self.dataset_id: str = os.getenv('BQ_DATASET_ID', 'biglake_dataset')
        self.table_name: str = os.getenv('BQ_TABLE_NAME', 'employee_data')
        self.connection_id: str = os.getenv('BQ_CONNECTION_ID', 'biglake-connection')

        # Data file settings
        self.json_file_path: str = 'sample_data.json'

    def validate(self) -> bool:
        """Validate required configuration values."""
        required_fields = [
            ('project_id', self.project_id),
            ('bucket_name', self.bucket_name),
            ('dataset_id', self.dataset_id),
            ('table_name', self.table_name),
            ('connection_id', self.connection_id)
        ]

        for field_name, value in required_fields:
            if not value or value.startswith('your-'):
                print(f"Error: {field_name} must be configured")
                return False

        return True

    def display_settings(self) -> None:
        """Display current configuration settings."""
        print("Current Configuration:")
        print(f"  Project ID: {self.project_id}")
        print(f"  Region: {self.region}")
        print(f"  Bucket Name: {self.bucket_name}")
        print(f"  Dataset ID: {self.dataset_id}")
        print(f"  Table Name: {self.table_name}")
        print(f"  Connection ID: {self.connection_id}")
        print(f"  Data Location: {self.data_location}")