#!/usr/bin/env python3
"""
BigLake Metastore Client with Apache Iceberg REST Catalog Support

This module provides a comprehensive client for interacting with BigLake Metastore
using Apache Spark and the Iceberg REST catalog. It includes authentication,
connection management, and table operations.
"""

import os
import logging
import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from google.cloud import bigquery
from google.auth import default
from google.auth.transport.requests import Request
import requests


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class BigLakeConfig:
    """Configuration class for BigLake Metastore connection."""
    project_id: str
    region: str
    bucket_name: str
    dataset_id: str
    connection_id: str
    catalog_name: str = "biglake_catalog"
    app_name: str = "BigLakeMetastoreClient"
    credential_vending: bool = True
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        required_fields = ['project_id', 'region', 'bucket_name', 'dataset_id', 'connection_id']
        for field in required_fields:
            if not getattr(self, field):
                raise ValueError(f"Required configuration field '{field}' is missing")
    
    @classmethod
    def from_env(cls) -> 'BigLakeConfig':
        """Create configuration from environment variables."""
        return cls(
            project_id=os.getenv('PROJECT_ID', ''),
            region=os.getenv('REGION', 'us-central1'),
            bucket_name=os.getenv('BUCKET_NAME', ''),
            dataset_id=os.getenv('DATASET_ID', 'biglake_dataset'),
            connection_id=os.getenv('CONNECTION_ID', 'biglake-connection'),
            catalog_name=os.getenv('CATALOG_NAME', 'biglake_catalog'),
            app_name=os.getenv('APP_NAME', 'BigLakeMetastoreClient'),
            credential_vending=os.getenv('CREDENTIAL_VENDING', 'true').lower() == 'true'
        )
    
    @classmethod
    def from_file(cls, config_path: str) -> 'BigLakeConfig':
        """Create configuration from JSON file."""
        with open(config_path, 'r') as f:
            config_data = json.load(f)
        return cls(**config_data)


class AuthenticationManager:
    """Manages Google Cloud authentication for BigLake Metastore."""
    
    def __init__(self):
        self.credentials = None
        self.project_id = None
        self._initialize_credentials()
    
    def _initialize_credentials(self):
        """Initialize Google Cloud credentials."""
        try:
            self.credentials, self.project_id = default()
            logger.info(f"Authenticated with project: {self.project_id}")
        except Exception as e:
            logger.error(f"Failed to initialize credentials: {e}")
            raise
    
    def get_access_token(self) -> str:
        """Get a fresh access token."""
        try:
            if self.credentials.expired:
                self.credentials.refresh(Request())
            return self.credentials.token
        except Exception as e:
            logger.error(f"Failed to get access token: {e}")
            raise
    
    def get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for API requests."""
        return {
            'Authorization': f'Bearer {self.get_access_token()}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }


class BigLakeMetastoreClient:
    """Main client for BigLake Metastore operations."""
    
    def __init__(self, config: BigLakeConfig):
        self.config = config
        self.auth_manager = AuthenticationManager()
        self.spark_session: Optional[SparkSession] = None
        self.bigquery_client: Optional[bigquery.Client] = None
        self.catalog_prefix = f"projects/{config.project_id}/catalogs/{config.bucket_name}"
        
        # Initialize clients
        self._initialize_bigquery_client()
        logger.info(f"BigLake Metastore Client initialized for project: {config.project_id}")
    
    def _initialize_bigquery_client(self):
        """Initialize BigQuery client."""
        try:
            self.bigquery_client = bigquery.Client(project=self.config.project_id)
            logger.info("BigQuery client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            raise
    
    def initialize_spark_session(self) -> SparkSession:
        """Initialize and configure Spark session for BigLake Metastore."""
        try:
            # Build Spark configuration
            spark_config = self._build_spark_config()

            # Create Spark session with Iceberg packages compatible with Spark 4.0
            # Using the latest Iceberg version that supports Spark 4.0
            self.spark_session = (SparkSession.builder
                                .appName(self.config.app_name)
                                .config("spark.jars.packages",
                                       "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
                                       "org.apache.iceberg:iceberg-gcp-bundle:1.6.1"))

            # Apply all configurations
            for key, value in spark_config.items():
                self.spark_session = self.spark_session.config(key, value)

            self.spark_session = self.spark_session.getOrCreate()

            logger.info(f"Spark session initialized with catalog: {self.config.catalog_name}")
            return self.spark_session

        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {e}")
            raise
    
    def _build_spark_config(self) -> Dict[str, str]:
        """Build Spark configuration for BigLake Metastore."""
        import subprocess

        catalog_name = self.config.catalog_name

        # Get access token for authentication
        try:
            result = subprocess.run(['gcloud', 'auth', 'application-default', 'print-access-token'],
                                  capture_output=True, text=True, check=True)
            access_token = result.stdout.strip()
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to get access token: {e}")
            raise

        base_config = {
            f'spark.sql.catalog.{catalog_name}': 'org.apache.iceberg.spark.SparkCatalog',
            f'spark.sql.catalog.{catalog_name}.type': 'rest',
            f'spark.sql.catalog.{catalog_name}.uri': 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
            f'spark.sql.catalog.{catalog_name}.warehouse': f'gs://{self.config.bucket_name}',
            f'spark.sql.catalog.{catalog_name}.header.x-goog-user-project': self.config.project_id,
            f'spark.sql.catalog.{catalog_name}.token': access_token,
            f'spark.sql.catalog.{catalog_name}.oauth2-server-uri': 'https://oauth2.googleapis.com/token',
            f'spark.sql.catalog.{catalog_name}.io-impl': 'org.apache.iceberg.gcp.gcs.GCSFileIO',
            f'spark.sql.catalog.{catalog_name}.rest-metrics-reporting-enabled': 'false',
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            'spark.sql.defaultCatalog': catalog_name,
            # Network binding fixes for local development
            'spark.driver.bindAddress': '127.0.0.1',
            'spark.driver.host': '127.0.0.1',
            'spark.ui.enabled': 'false',  # Disable UI to avoid port conflicts
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true'
        }

        # Add credential vending configuration - always enabled since the catalog is configured for it
        base_config[f'spark.sql.catalog.{catalog_name}.header.X-Iceberg-Access-Delegation'] = 'vended-credentials'

        return base_config
    
    def initialize_catalog(self) -> Dict[str, Any]:
        """Initialize the BigLake Metastore REST catalog."""
        try:
            url = f"https://biglake.googleapis.com/iceberg/v1/restcatalog/v1/config"
            params = {'warehouse': f'gs://{self.config.bucket_name}'}
            
            headers = self.auth_manager.get_auth_headers()
            headers['x-goog-user-project'] = self.config.project_id
            
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            
            catalog_info = response.json()
            logger.info("Catalog initialized successfully")
            return catalog_info
            
        except Exception as e:
            logger.error(f"Failed to initialize catalog: {e}")
            raise
    
    def enable_credential_vending(self) -> Dict[str, Any]:
        """Enable credential vending mode for the catalog."""
        try:
            url = f"https://biglake.googleapis.com/iceberg/v1/restcatalog/extensions/{self.catalog_prefix}"
            params = {'update_mask': 'credential_mode'}
            
            headers = self.auth_manager.get_auth_headers()
            headers['x-goog-user-project'] = self.config.project_id
            
            data = {'credential_mode': 'CREDENTIAL_MODE_VENDED_CREDENTIALS'}
            
            response = requests.patch(url, params=params, headers=headers, json=data)
            response.raise_for_status()
            
            result = response.json()
            logger.info("Credential vending enabled successfully")
            return result
            
        except Exception as e:
            logger.error(f"Failed to enable credential vending: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test the connection to BigLake Metastore."""
        try:
            # Test REST API connection
            catalog_info = self.initialize_catalog()
            
            # Test Spark session if initialized
            if self.spark_session:
                # Try to list namespaces
                namespaces = self.spark_session.sql("SHOW NAMESPACES").collect()
                logger.info(f"Found {len(namespaces)} namespaces")
            
            # Test BigQuery connection
            if self.bigquery_client:
                datasets = list(self.bigquery_client.list_datasets())
                logger.info(f"Found {len(datasets)} BigQuery datasets")
            
            logger.info("All connection tests passed")
            return True
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def get_spark_session(self) -> SparkSession:
        """Get the Spark session, initializing if necessary."""
        if self.spark_session is None:
            self.initialize_spark_session()
        return self.spark_session
    
    def get_bigquery_client(self) -> bigquery.Client:
        """Get the BigQuery client."""
        return self.bigquery_client
    
    def close(self):
        """Clean up resources."""
        if self.spark_session and hasattr(self.spark_session, 'stop'):
            try:
                self.spark_session.stop()
            except Exception as e:
                logger.warning(f"Error stopping Spark session: {e}")
            self.spark_session = None
        logger.info("BigLake Metastore Client closed")


def create_client_from_env() -> BigLakeMetastoreClient:
    """Create a BigLake Metastore client from environment variables."""
    config = BigLakeConfig.from_env()
    return BigLakeMetastoreClient(config)


def create_client_from_config(config_path: str) -> BigLakeMetastoreClient:
    """Create a BigLake Metastore client from configuration file."""
    config = BigLakeConfig.from_file(config_path)
    return BigLakeMetastoreClient(config)


if __name__ == "__main__":
    # Example usage
    try:
        # Create client from environment variables
        client = create_client_from_env()
        
        # Initialize Spark session
        spark = client.initialize_spark_session()
        
        # Test connection
        if client.test_connection():
            print("✅ BigLake Metastore connection successful!")
        else:
            print("❌ BigLake Metastore connection failed!")
        
        # Clean up
        client.close()
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise
