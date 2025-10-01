"""Main orchestrator for BigLake Metastore operations."""

import sys
from config import Config
from gcs_manager import GCSManager
from bigquery_manager import BigQueryManager
from iceberg_manager import IcebergManager


class BigLakeOrchestrator:
    """Orchestrates the complete BigLake Metastore workflow."""

    def __init__(self, config: Config):
        self.config = config
        self.gcs_manager = GCSManager(config)
        self.bq_manager = BigQueryManager(config)
        self.iceberg_manager = IcebergManager(config)

    def setup_infrastructure(self) -> bool:
        """Set up all required GCP infrastructure."""
        print("=== Setting up BigLake Infrastructure ===")

        # Create GCS bucket
        if not self.gcs_manager.create_bucket_if_not_exists():
            return False

        # Create BigQuery dataset
        if not self.bq_manager.create_dataset_if_not_exists():
            return False

        # Create BigQuery connection
        if not self.bq_manager.create_connection_if_not_exists():
            print("Warning: Connection creation may have failed. You might need to create it manually.")

        print("Infrastructure setup completed")
        return True

    def upload_and_convert_data(self) -> bool:
        """Upload JSON data and convert to Iceberg format."""
        print("=== Uploading and Converting Data ===")

        # Upload raw JSON data
        if not self.gcs_manager.upload_json_data(self.config.json_file_path):
            return False

        # Create Iceberg table structure
        if not self.iceberg_manager.create_iceberg_table_structure(self.config.json_file_path):
            return False

        # Verify Iceberg structure
        if not self.iceberg_manager.verify_iceberg_structure():
            return False

        print("Data upload and conversion completed")
        return True

    def create_biglake_table(self) -> bool:
        """Create the BigLake external table."""
        print("=== Creating BigLake Table ===")

        if not self.bq_manager.create_biglake_table():
            return False

        print("BigLake table creation completed")
        return True

    def verify_setup(self) -> bool:
        """Verify the complete setup by querying the table."""
        print("=== Verifying Setup ===")

        # Get table information
        if not self.bq_manager.get_table_info():
            return False

        # Query the table
        if not self.bq_manager.query_table(limit=5):
            return False

        print("Setup verification completed successfully")
        return True

    def run_complete_workflow(self) -> bool:
        """Run the complete BigLake setup workflow."""
        print("Starting complete BigLake Metastore workflow...")
        print(f"Project: {self.config.project_id}")
        print(f"Dataset: {self.config.dataset_id}")
        print(f"Table: {self.config.table_name}")
        print()

        steps = [
            ("Setup Infrastructure", self.setup_infrastructure),
            ("Upload and Convert Data", self.upload_and_convert_data),
            ("Create BigLake Table", self.create_biglake_table),
            ("Verify Setup", self.verify_setup)
        ]

        for step_name, step_function in steps:
            print(f"Step: {step_name}")
            if not step_function():
                print(f"❌ Failed at step: {step_name}")
                return False
            print(f"✅ Completed: {step_name}")
            print()

        print("🎉 BigLake Metastore workflow completed successfully!")
        return True

    def cleanup_resources(self) -> bool:
        """Clean up all created resources."""
        print("=== Cleaning Up Resources ===")

        # Delete BigQuery table
        self.bq_manager.delete_table()

        # Delete Iceberg table files
        self.iceberg_manager.delete_iceberg_table()

        # Delete bucket contents
        self.gcs_manager.delete_bucket_contents()

        print("Cleanup completed")
        return True

    def display_status(self) -> None:
        """Display current status of resources."""
        print("=== Current Status ===")

        # Check bucket contents
        print("GCS Bucket Contents:")
        contents = self.gcs_manager.list_bucket_contents()
        for item in contents:
            print(f"  - {item}")

        print()

        # Check table info
        print("BigQuery Table:")
        self.bq_manager.get_table_info()

        print()

        # Check Iceberg files
        print("Iceberg Files:")
        iceberg_files = self.iceberg_manager.list_iceberg_files()
        for file in iceberg_files:
            print(f"  - {file}")


def main():
    """Main entry point."""
    # Load configuration
    config = Config()

    # Display current settings
    config.display_settings()
    print()

    # Validate configuration
    if not config.validate():
        print("❌ Configuration validation failed")
        print("Please update config.py or set environment variables:")
        print("  - GCP_PROJECT_ID")
        print("  - GCS_BUCKET_NAME (optional)")
        print("  - BQ_DATASET_ID (optional)")
        print("  - BQ_TABLE_NAME (optional)")
        print("  - BQ_CONNECTION_ID (optional)")
        sys.exit(1)

    # Create orchestrator
    orchestrator = BigLakeOrchestrator(config)

    # Parse command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()

        if command == "setup":
            orchestrator.run_complete_workflow()
        elif command == "cleanup":
            orchestrator.cleanup_resources()
        elif command == "status":
            orchestrator.display_status()
        elif command == "verify":
            orchestrator.verify_setup()
        else:
            print(f"Unknown command: {command}")
            print("Available commands: setup, cleanup, status, verify")
            sys.exit(1)
    else:
        print("Usage: python biglake_orchestrator.py [setup|cleanup|status|verify]")
        print()
        print("Commands:")
        print("  setup   - Run complete BigLake setup workflow")
        print("  cleanup - Clean up all created resources")
        print("  status  - Display current status")
        print("  verify  - Verify existing setup")


if __name__ == "__main__":
    main()