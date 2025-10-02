#!/usr/bin/env python3
"""Debug setup to test individual components."""

from config import Config
from gcs_manager import GCSManager
from iceberg_manager import IcebergManager

def debug_gcs_and_iceberg():
    """Test GCS and Iceberg components only."""
    print("🔍 DEBUG MODE - Testing GCS and Iceberg components")
    print("=" * 55)

    # Load configuration
    config = Config()
    config.display_settings()
    print()

    if not config.validate():
        print("❌ Configuration validation failed")
        return False

    # Test GCS operations
    print("Step 1: Testing GCS Operations")
    print("-" * 35)
    gcs_manager = GCSManager(config)

    if not gcs_manager.create_bucket_if_not_exists():
        print("❌ GCS bucket creation failed")
        return False

    if not gcs_manager.upload_json_data(config.json_file_path):
        print("❌ JSON upload failed")
        return False

    print("✅ GCS operations completed successfully")
    print()

    # Test Iceberg operations
    print("Step 2: Testing Iceberg Operations")
    print("-" * 38)
    iceberg_manager = IcebergManager(config)

    if not iceberg_manager.create_iceberg_table_structure(config.json_file_path):
        print("❌ Iceberg table creation failed")
        return False

    if not iceberg_manager.verify_iceberg_structure():
        print("❌ Iceberg verification failed")
        return False

    print("✅ Iceberg operations completed successfully")
    print()

    # List all created files
    print("Step 3: Listing Created Files")
    print("-" * 33)

    print("GCS Bucket Contents:")
    contents = gcs_manager.list_bucket_contents()
    for item in contents[:10]:  # Show first 10 items
        print(f"  - {item}")
    if len(contents) > 10:
        print(f"  ... and {len(contents) - 10} more files")

    print()
    print("Iceberg Files:")
    iceberg_files = iceberg_manager.list_iceberg_files()
    for file in iceberg_files:
        print(f"  - {file}")

    print()
    print("🎉 GCS and Iceberg setup completed successfully!")
    print("\n📋 Next Steps:")
    print("1. The BigQuery API issue needs to be resolved")
    print("2. Once resolved, run: python3 biglake_orchestrator.py setup")
    print("3. Or manually create the BigQuery external table")

    return True

if __name__ == "__main__":
    debug_gcs_and_iceberg()