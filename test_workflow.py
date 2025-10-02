#!/usr/bin/env python3
"""Test workflow to demonstrate BigLake Metastore setup without requiring GCP authentication."""

import json
import pandas as pd
from pathlib import Path
from config import Config


def simulate_workflow():
    """Simulate the BigLake workflow without actual GCP calls."""
    print("🧪 SIMULATION MODE - BigLake Metastore Workflow")
    print("=" * 50)

    # Load configuration
    config = Config()
    config.display_settings()
    print()

    if not config.validate():
        print("❌ Configuration validation failed")
        return False

    # Step 1: Validate JSON data
    print("Step 1: Validating JSON Data")
    print("-" * 30)
    try:
        with open(config.json_file_path, 'r') as f:
            data = json.load(f)

        print(f"✅ JSON file loaded: {len(data)} records")
        print(f"Sample record keys: {list(data[0].keys())}")

        # Convert to DataFrame to validate structure
        df = pd.json_normalize(data)
        print(f"✅ DataFrame created: {df.shape[0]} rows, {df.shape[1]} columns")
        print(f"Columns: {list(df.columns)}")

    except Exception as e:
        print(f"❌ Error processing JSON: {e}")
        return False

    print()

    # Step 2: Simulate GCS operations
    print("Step 2: Simulating GCS Operations")
    print("-" * 35)
    print(f"✅ Would create bucket: {config.bucket_name}")
    print(f"✅ Would upload JSON to: gs://{config.bucket_name}/raw-data/{config.table_name}.json")
    print(f"✅ Would create Iceberg structure at: {config.data_location}")
    print()

    # Step 3: Simulate Iceberg conversion
    print("Step 3: Simulating Iceberg Conversion")
    print("-" * 38)

    # Demonstrate data type conversion
    df_converted = df.copy()
    df_converted['hire_date'] = pd.to_datetime(df_converted['hire_date']).dt.date
    df_converted['id'] = df_converted['id'].astype('int64')
    df_converted['age'] = df_converted['age'].astype('int64')
    df_converted['salary'] = df_converted['salary'].astype('int64')
    df_converted['is_active'] = df_converted['is_active'].astype('bool')

    print("✅ Data types converted for Iceberg compatibility:")
    print(f"   - hire_date: {df_converted['hire_date'].dtype}")
    print(f"   - id: {df_converted['id'].dtype}")
    print(f"   - age: {df_converted['age'].dtype}")
    print(f"   - salary: {df_converted['salary'].dtype}")
    print(f"   - is_active: {df_converted['is_active'].dtype}")

    # Save a sample Parquet file locally to demonstrate
    sample_parquet = "sample_output.parquet"
    df_converted.to_parquet(sample_parquet, index=False)
    print(f"✅ Sample Parquet file created: {sample_parquet}")

    # Check file size
    file_size = Path(sample_parquet).stat().st_size
    print(f"   File size: {file_size:,} bytes")
    print()

    # Step 4: Simulate BigQuery operations
    print("Step 4: Simulating BigQuery Operations")
    print("-" * 38)
    print(f"✅ Would create dataset: {config.project_id}.{config.dataset_id}")
    print(f"✅ Would create connection: {config.connection_id}")
    print(f"✅ Would create external table: {config.dataset_id}.{config.table_name}")
    print(f"   Table schema would include {len(df_converted.columns)} columns")
    print()

    # Step 5: Show sample queries
    print("Step 5: Sample Queries")
    print("-" * 22)
    sample_queries = [
        f"SELECT COUNT(*) FROM `{config.project_id}.{config.dataset_id}.{config.table_name}`;",
        f"SELECT name, department, salary FROM `{config.project_id}.{config.dataset_id}.{config.table_name}` WHERE is_active = true ORDER BY salary DESC LIMIT 5;",
        f"SELECT department, AVG(salary) as avg_salary FROM `{config.project_id}.{config.dataset_id}.{config.table_name}` GROUP BY department;"
    ]

    for i, query in enumerate(sample_queries, 1):
        print(f"Query {i}:")
        print(f"  {query}")
        print()

    # Cleanup
    try:
        Path(sample_parquet).unlink()
        print("✅ Cleaned up sample files")
    except:
        pass

    print("🎉 Simulation completed successfully!")
    print("\n📋 Next Steps:")
    print("1. Set up GCP authentication (see error message above)")
    print("2. Run: python3 biglake_orchestrator.py setup")
    print("3. Verify with: python3 biglake_orchestrator.py verify")

    return True


if __name__ == "__main__":
    simulate_workflow()