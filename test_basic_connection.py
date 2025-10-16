#!/usr/bin/env python3
"""
Basic Connection Test for BigLake Metastore

This script tests the basic connection without requiring full Spark/Iceberg setup.
"""

import os
import sys
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from biglake_metastore_client import BigLakeConfig, BigLakeMetastoreClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_basic_connection():
    """Test basic connection without Spark."""
    
    print("🧪 Testing Basic BigLake Metastore Connection")
    print("=" * 50)
    
    try:
        # Create configuration from environment
        config = BigLakeConfig.from_env()
        print(f"✅ Configuration loaded:")
        print(f"   Project ID: {config.project_id}")
        print(f"   Region: {config.region}")
        print(f"   Bucket: {config.bucket_name}")
        print(f"   Dataset: {config.dataset_id}")
        print(f"   Connection: {config.connection_id}")
        
        # Create client (without Spark session)
        client = BigLakeMetastoreClient(config)
        print("✅ Client initialized")
        
        # Test BigQuery connection
        print("\n🔍 Testing BigQuery connection...")
        bq_client = client.get_bigquery_client()
        datasets = list(bq_client.list_datasets())
        print(f"✅ BigQuery connected - found {len(datasets)} datasets")
        
        # Test REST API connection
        print("\n🔍 Testing BigLake REST API connection...")
        catalog_info = client.initialize_catalog()
        print("✅ BigLake REST API connected")
        print(f"   Catalog info: {catalog_info}")
        
        # Test authentication
        print("\n🔍 Testing authentication...")
        auth_headers = client.auth_manager.get_auth_headers()
        print("✅ Authentication working")
        
        print("\n🎉 All basic connection tests passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Connection test failed: {e}")
        logger.exception("Detailed error:")
        return False


def test_spark_minimal():
    """Test minimal Spark session creation."""
    
    print("\n🧪 Testing Minimal Spark Session")
    print("=" * 50)
    
    try:
        from pyspark.sql import SparkSession
        
        # Create minimal Spark session with network fixes
        spark = SparkSession.builder \
            .appName("BigLakeTest") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        print("✅ Minimal Spark session created")
        
        # Test basic SQL
        df = spark.sql("SELECT 1 as test_column")
        result = df.collect()
        print(f"✅ Basic SQL test passed: {result}")
        
        # Clean up
        spark.stop()
        print("✅ Spark session stopped cleanly")
        
        return True
        
    except Exception as e:
        print(f"❌ Spark test failed: {e}")
        logger.exception("Detailed error:")
        return False


def main():
    """Main test function."""
    
    # Test 1: Basic connection without Spark
    basic_success = test_basic_connection()
    
    # Test 2: Minimal Spark session
    spark_success = test_spark_minimal()
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 Test Summary:")
    print(f"  {'✅' if basic_success else '❌'} Basic Connection Test")
    print(f"  {'✅' if spark_success else '❌'} Spark Session Test")
    
    if basic_success and spark_success:
        print("\n🎉 All tests passed! Ready to run full workflow.")
        return True
    else:
        print("\n⚠️ Some tests failed. Check the errors above.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
