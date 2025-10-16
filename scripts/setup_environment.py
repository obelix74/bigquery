#!/usr/bin/env python3
"""
Environment Setup Script for BigLake Metastore

This script helps set up the development environment and validates
the BigLake Metastore configuration.
"""

import os
import sys
import subprocess
import json
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_python_version():
    """Check if Python version is compatible."""
    min_version = (3, 8)
    current_version = sys.version_info[:2]
    
    if current_version < min_version:
        print(f"❌ Python {min_version[0]}.{min_version[1]}+ required, found {current_version[0]}.{current_version[1]}")
        return False
    
    print(f"✅ Python version {current_version[0]}.{current_version[1]} is compatible")
    return True


def check_gcloud_cli():
    """Check if gcloud CLI is installed and authenticated."""
    try:
        # Check if gcloud is installed
        result = subprocess.run(['gcloud', 'version'], capture_output=True, text=True)
        if result.returncode != 0:
            print("❌ gcloud CLI not found. Please install Google Cloud SDK.")
            return False
        
        print("✅ gcloud CLI is installed")
        
        # Check authentication
        result = subprocess.run(['gcloud', 'auth', 'list'], capture_output=True, text=True)
        if 'ACTIVE' not in result.stdout:
            print("⚠️ gcloud not authenticated. Run 'gcloud auth login' to authenticate.")
            return False
        
        print("✅ gcloud is authenticated")
        return True
        
    except FileNotFoundError:
        print("❌ gcloud CLI not found. Please install Google Cloud SDK.")
        return False


def install_dependencies():
    """Install Python dependencies."""
    print("📦 Installing Python dependencies...")
    
    requirements_file = Path(__file__).parent.parent / "requirements.txt"
    
    if not requirements_file.exists():
        print("❌ requirements.txt not found")
        return False
    
    try:
        subprocess.run([
            sys.executable, '-m', 'pip', 'install', '-r', str(requirements_file)
        ], check=True)
        print("✅ Dependencies installed successfully")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False


def check_environment_variables():
    """Check if required environment variables are set."""
    required_vars = [
        'PROJECT_ID',
        'REGION',
        'BUCKET_NAME',
        'DATASET_ID',
        'CONNECTION_ID'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        print("💡 Please set these variables in your .env file or environment")
        return False
    
    print("✅ All required environment variables are set")
    return True


def validate_gcp_resources():
    """Validate that GCP resources exist and are accessible."""
    project_id = os.getenv('PROJECT_ID')
    bucket_name = os.getenv('BUCKET_NAME')
    dataset_id = os.getenv('DATASET_ID')
    
    if not all([project_id, bucket_name, dataset_id]):
        print("❌ Missing required environment variables for validation")
        return False
    
    print("🔍 Validating GCP resources...")
    
    # Check project access
    try:
        result = subprocess.run([
            'gcloud', 'projects', 'describe', project_id
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Cannot access project '{project_id}': {result.stderr}")
            return False
        
        print(f"✅ Project '{project_id}' is accessible")
        
    except Exception as e:
        print(f"❌ Error checking project: {e}")
        return False
    
    # Check bucket access
    try:
        result = subprocess.run([
            'gcloud', 'storage', 'buckets', 'describe', f'gs://{bucket_name}'
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Cannot access bucket 'gs://{bucket_name}': {result.stderr}")
            return False
        
        print(f"✅ Bucket 'gs://{bucket_name}' is accessible")
        
    except Exception as e:
        print(f"❌ Error checking bucket: {e}")
        return False
    
    # Check BigQuery dataset
    try:
        result = subprocess.run([
            'bq', 'show', f'{project_id}:{dataset_id}'
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Cannot access dataset '{dataset_id}': {result.stderr}")
            return False
        
        print(f"✅ Dataset '{dataset_id}' is accessible")
        
    except Exception as e:
        print(f"❌ Error checking dataset: {e}")
        return False
    
    return True


def check_apis_enabled():
    """Check if required APIs are enabled."""
    project_id = os.getenv('PROJECT_ID')
    if not project_id:
        print("❌ PROJECT_ID not set")
        return False
    
    required_apis = [
        'bigquery.googleapis.com',
        'biglake.googleapis.com',
        'storage.googleapis.com',
        'bigqueryconnection.googleapis.com'
    ]
    
    print("🔍 Checking required APIs...")
    
    try:
        result = subprocess.run([
            'gcloud', 'services', 'list', '--enabled', '--format=value(name)'
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Failed to list enabled APIs: {result.stderr}")
            return False
        
        enabled_apis = result.stdout.strip().split('\n')
        missing_apis = []
        
        for api in required_apis:
            if api not in enabled_apis:
                missing_apis.append(api)
        
        if missing_apis:
            print(f"❌ Missing APIs: {', '.join(missing_apis)}")
            print("💡 Enable them with: gcloud services enable " + " ".join(missing_apis))
            return False
        
        print("✅ All required APIs are enabled")
        return True
        
    except Exception as e:
        print(f"❌ Error checking APIs: {e}")
        return False


def create_sample_config():
    """Create a sample configuration file."""
    config_dir = Path(__file__).parent.parent / "config"
    config_dir.mkdir(exist_ok=True)
    
    sample_config = {
        "project_id": os.getenv('PROJECT_ID', 'your-project-id'),
        "region": os.getenv('REGION', 'us-central1'),
        "bucket_name": os.getenv('BUCKET_NAME', 'your-biglake-bucket'),
        "dataset_id": os.getenv('DATASET_ID', 'biglake_dataset'),
        "connection_id": os.getenv('CONNECTION_ID', 'biglake-connection'),
        "catalog_name": os.getenv('CATALOG_NAME', 'biglake_catalog'),
        "app_name": os.getenv('APP_NAME', 'BigLakeMetastoreClient'),
        "credential_vending": os.getenv('CREDENTIAL_VENDING', 'true').lower() == 'true'
    }
    
    config_file = config_dir / "current_config.json"
    
    with open(config_file, 'w') as f:
        json.dump(sample_config, f, indent=2)
    
    print(f"✅ Configuration saved to {config_file}")
    return True


def run_basic_tests():
    """Run basic validation tests."""
    print("🧪 Running basic validation tests...")
    
    # Add src to path
    src_path = Path(__file__).parent.parent / "src"
    sys.path.insert(0, str(src_path))
    
    try:
        # Test imports
        from biglake_metastore_client import BigLakeConfig
        from iceberg_operations import TableSchema
        
        print("✅ Core modules import successfully")
        
        # Test configuration creation
        config = BigLakeConfig(
            project_id='test-project',
            region='us-central1',
            bucket_name='test-bucket',
            dataset_id='test-dataset',
            connection_id='test-connection'
        )
        
        print("✅ Configuration creation works")
        
        # Test schema creation
        schema = TableSchema(
            name='test_table',
            columns=[
                {'name': 'id', 'type': 'int', 'nullable': False},
                {'name': 'name', 'type': 'string', 'nullable': True}
            ]
        )
        
        print("✅ Schema creation works")
        
        return True
        
    except Exception as e:
        print(f"❌ Basic tests failed: {e}")
        return False


def main():
    """Main setup function."""
    print("🚀 BigLake Metastore Environment Setup")
    print("=" * 50)
    
    checks = [
        ("Python Version", check_python_version),
        ("gcloud CLI", check_gcloud_cli),
        ("Environment Variables", check_environment_variables),
        ("Required APIs", check_apis_enabled),
        ("GCP Resources", validate_gcp_resources),
        ("Python Dependencies", install_dependencies),
        ("Basic Tests", run_basic_tests),
        ("Sample Configuration", create_sample_config)
    ]
    
    results = {}
    
    for check_name, check_func in checks:
        print(f"\n🔍 {check_name}:")
        try:
            results[check_name] = check_func()
        except Exception as e:
            print(f"❌ {check_name} failed with error: {e}")
            results[check_name] = False
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 Setup Summary:")
    
    passed = sum(results.values())
    total = len(results)
    
    for check_name, result in results.items():
        status = "✅" if result else "❌"
        print(f"  {status} {check_name}")
    
    print(f"\n📈 {passed}/{total} checks passed")
    
    if passed == total:
        print("🎉 Environment setup completed successfully!")
        print("\n🚀 Next steps:")
        print("  1. Run: python examples/complete_workflow.py")
        print("  2. Or run: python tests/test_biglake_integration.py")
    else:
        print("⚠️ Some checks failed. Please address the issues above.")
        return False
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
