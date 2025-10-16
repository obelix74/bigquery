#!/usr/bin/env python3
"""
Test Suite for BigLake Metastore Integration

This module contains comprehensive tests for the BigLake Metastore implementation,
including unit tests, integration tests, and validation scenarios.
"""

import unittest
import os
import sys
import tempfile
import json
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from biglake_metastore_client import BigLakeConfig, BigLakeMetastoreClient, AuthenticationManager
from iceberg_operations import IcebergTableManager, BigQueryIntegration, TableSchema


class TestBigLakeConfig(unittest.TestCase):
    """Test BigLake configuration management."""
    
    def test_config_from_dict(self):
        """Test creating config from dictionary."""
        config_data = {
            'project_id': 'test-project',
            'region': 'us-central1',
            'bucket_name': 'test-bucket',
            'dataset_id': 'test-dataset',
            'connection_id': 'test-connection'
        }
        
        config = BigLakeConfig(**config_data)
        self.assertEqual(config.project_id, 'test-project')
        self.assertEqual(config.region, 'us-central1')
        self.assertEqual(config.bucket_name, 'test-bucket')
    
    def test_config_validation(self):
        """Test configuration validation."""
        with self.assertRaises(ValueError):
            BigLakeConfig(
                project_id='',  # Empty project_id should fail
                region='us-central1',
                bucket_name='test-bucket',
                dataset_id='test-dataset',
                connection_id='test-connection'
            )
    
    def test_config_from_env(self):
        """Test creating config from environment variables."""
        env_vars = {
            'PROJECT_ID': 'env-project',
            'REGION': 'us-west1',
            'BUCKET_NAME': 'env-bucket',
            'DATASET_ID': 'env-dataset',
            'CONNECTION_ID': 'env-connection'
        }
        
        with patch.dict(os.environ, env_vars):
            config = BigLakeConfig.from_env()
            self.assertEqual(config.project_id, 'env-project')
            self.assertEqual(config.region, 'us-west1')
    
    def test_config_from_file(self):
        """Test creating config from JSON file."""
        config_data = {
            'project_id': 'file-project',
            'region': 'us-east1',
            'bucket_name': 'file-bucket',
            'dataset_id': 'file-dataset',
            'connection_id': 'file-connection'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            temp_file = f.name
        
        try:
            config = BigLakeConfig.from_file(temp_file)
            self.assertEqual(config.project_id, 'file-project')
            self.assertEqual(config.region, 'us-east1')
        finally:
            os.unlink(temp_file)


class TestTableSchema(unittest.TestCase):
    """Test table schema operations."""
    
    def test_schema_creation(self):
        """Test creating a table schema."""
        schema = TableSchema(
            name='test_table',
            columns=[
                {'name': 'id', 'type': 'int', 'nullable': False},
                {'name': 'name', 'type': 'string', 'nullable': True}
            ],
            partition_columns=['id']
        )
        
        self.assertEqual(schema.name, 'test_table')
        self.assertEqual(len(schema.columns), 2)
        self.assertEqual(schema.partition_columns, ['id'])
    
    def test_spark_schema_conversion(self):
        """Test converting to Spark schema."""
        schema = TableSchema(
            name='test_table',
            columns=[
                {'name': 'id', 'type': 'int', 'nullable': False},
                {'name': 'name', 'type': 'string', 'nullable': True}
            ]
        )
        
        spark_schema = schema.to_spark_schema()
        self.assertEqual(len(spark_schema.fields), 2)
        self.assertEqual(spark_schema.fields[0].name, 'id')
        self.assertFalse(spark_schema.fields[0].nullable)


class TestAuthenticationManager(unittest.TestCase):
    """Test authentication management."""
    
    @patch('google.auth.default')
    def test_credential_initialization(self, mock_default):
        """Test credential initialization."""
        mock_credentials = Mock()
        mock_credentials.token = 'test-token'
        mock_credentials.expired = False
        mock_default.return_value = (mock_credentials, 'test-project')
        
        auth_manager = AuthenticationManager()
        self.assertEqual(auth_manager.project_id, 'test-project')
        self.assertIsNotNone(auth_manager.credentials)
    
    @patch('google.auth.default')
    def test_access_token_retrieval(self, mock_default):
        """Test access token retrieval."""
        mock_credentials = Mock()
        mock_credentials.token = 'test-token'
        mock_credentials.expired = False
        mock_default.return_value = (mock_credentials, 'test-project')
        
        auth_manager = AuthenticationManager()
        token = auth_manager.get_access_token()
        self.assertEqual(token, 'test-token')
    
    @patch('google.auth.default')
    def test_auth_headers(self, mock_default):
        """Test authentication headers generation."""
        mock_credentials = Mock()
        mock_credentials.token = 'test-token'
        mock_credentials.expired = False
        mock_default.return_value = (mock_credentials, 'test-project')
        
        auth_manager = AuthenticationManager()
        headers = auth_manager.get_auth_headers()
        
        self.assertIn('Authorization', headers)
        self.assertEqual(headers['Authorization'], 'Bearer test-token')
        self.assertEqual(headers['Content-Type'], 'application/json')


class TestBigLakeMetastoreClient(unittest.TestCase):
    """Test BigLake Metastore client functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config = BigLakeConfig(
            project_id='test-project',
            region='us-central1',
            bucket_name='test-bucket',
            dataset_id='test-dataset',
            connection_id='test-connection'
        )
    
    @patch('biglake_metastore_client.bigquery.Client')
    @patch('biglake_metastore_client.AuthenticationManager')
    def test_client_initialization(self, mock_auth, mock_bq_client):
        """Test client initialization."""
        mock_auth_instance = Mock()
        mock_auth.return_value = mock_auth_instance
        
        client = BigLakeMetastoreClient(self.config)
        
        self.assertEqual(client.config, self.config)
        self.assertIsNotNone(client.auth_manager)
        self.assertIsNotNone(client.bigquery_client)
    
    @patch('biglake_metastore_client.bigquery.Client')
    @patch('biglake_metastore_client.AuthenticationManager')
    def test_spark_config_generation(self, mock_auth, mock_bq_client):
        """Test Spark configuration generation."""
        mock_auth_instance = Mock()
        mock_auth.return_value = mock_auth_instance
        
        client = BigLakeMetastoreClient(self.config)
        spark_config = client._build_spark_config()
        
        # Check essential configuration keys
        catalog_name = self.config.catalog_name
        self.assertIn(f'spark.sql.catalog.{catalog_name}', spark_config)
        self.assertIn(f'spark.sql.catalog.{catalog_name}.type', spark_config)
        self.assertIn(f'spark.sql.catalog.{catalog_name}.uri', spark_config)
        
        # Check credential vending configuration
        if self.config.credential_vending:
            self.assertIn(f'spark.sql.catalog.{catalog_name}.header.X-Iceberg-Access-Delegation', spark_config)


class TestIntegrationScenarios(unittest.TestCase):
    """Integration test scenarios."""
    
    def setUp(self):
        """Set up integration test fixtures."""
        self.config = BigLakeConfig(
            project_id='test-project',
            region='us-central1',
            bucket_name='test-bucket',
            dataset_id='test-dataset',
            connection_id='test-connection'
        )
    
    @patch('requests.get')
    @patch('biglake_metastore_client.bigquery.Client')
    @patch('biglake_metastore_client.AuthenticationManager')
    def test_catalog_initialization(self, mock_auth, mock_bq_client, mock_requests):
        """Test catalog initialization workflow."""
        # Mock authentication
        mock_auth_instance = Mock()
        mock_auth_instance.get_auth_headers.return_value = {
            'Authorization': 'Bearer test-token',
            'Content-Type': 'application/json'
        }
        mock_auth.return_value = mock_auth_instance
        
        # Mock API response
        mock_response = Mock()
        mock_response.json.return_value = {
            'overrides': {
                'catalog_credential_mode': 'CREDENTIAL_MODE_END_USER',
                'prefix': f'projects/{self.config.project_id}/catalogs/{self.config.bucket_name}'
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_requests.return_value = mock_response
        
        client = BigLakeMetastoreClient(self.config)
        result = client.initialize_catalog()
        
        self.assertIn('overrides', result)
        self.assertIn('prefix', result['overrides'])
    
    def test_sample_data_generation(self):
        """Test sample data generation for testing."""
        from examples.complete_workflow import generate_sample_data
        
        employee_data, sales_data = generate_sample_data()
        
        # Validate employee data
        self.assertIsInstance(employee_data, list)
        self.assertGreater(len(employee_data), 0)
        
        for employee in employee_data:
            self.assertIn('id', employee)
            self.assertIn('name', employee)
            self.assertIn('department', employee)
            self.assertIsInstance(employee['hire_date'], datetime)
        
        # Validate sales data
        self.assertIsInstance(sales_data, list)
        self.assertGreater(len(sales_data), 0)
        
        for sale in sales_data:
            self.assertIn('transaction_id', sale)
            self.assertIn('amount', sale)
            self.assertIsInstance(sale['transaction_date'], datetime)


class TestValidationScenarios(unittest.TestCase):
    """Validation test scenarios."""
    
    def test_configuration_validation(self):
        """Test configuration validation scenarios."""
        # Test missing required fields
        with self.assertRaises(ValueError):
            BigLakeConfig(
                project_id='',
                region='us-central1',
                bucket_name='test-bucket',
                dataset_id='test-dataset',
                connection_id='test-connection'
            )
        
        # Test valid configuration
        config = BigLakeConfig(
            project_id='valid-project',
            region='us-central1',
            bucket_name='valid-bucket',
            dataset_id='valid-dataset',
            connection_id='valid-connection'
        )
        self.assertIsNotNone(config)
    
    def test_schema_validation(self):
        """Test table schema validation."""
        # Test valid schema
        schema = TableSchema(
            name='valid_table',
            columns=[
                {'name': 'id', 'type': 'int', 'nullable': False},
                {'name': 'data', 'type': 'string', 'nullable': True}
            ]
        )
        self.assertEqual(schema.name, 'valid_table')
        
        # Test schema with partitioning
        partitioned_schema = TableSchema(
            name='partitioned_table',
            columns=[
                {'name': 'id', 'type': 'int', 'nullable': False},
                {'name': 'partition_col', 'type': 'string', 'nullable': False}
            ],
            partition_columns=['partition_col']
        )
        self.assertEqual(partitioned_schema.partition_columns, ['partition_col'])


def run_validation_tests():
    """Run validation tests with detailed output."""
    print("🧪 Running BigLake Metastore Validation Tests")
    print("=" * 60)
    
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases
    test_classes = [
        TestBigLakeConfig,
        TestTableSchema,
        TestAuthenticationManager,
        TestBigLakeMetastoreClient,
        TestIntegrationScenarios,
        TestValidationScenarios
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print("\n" + "=" * 60)
    if result.wasSuccessful():
        print("✅ All validation tests passed!")
    else:
        print(f"❌ {len(result.failures)} test(s) failed, {len(result.errors)} error(s)")
        
        if result.failures:
            print("\nFailures:")
            for test, traceback in result.failures:
                print(f"  - {test}: {traceback}")
        
        if result.errors:
            print("\nErrors:")
            for test, traceback in result.errors:
                print(f"  - {test}: {traceback}")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    run_validation_tests()
