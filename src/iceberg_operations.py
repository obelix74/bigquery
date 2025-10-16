#!/usr/bin/env python3
"""
Iceberg Operations Module for BigLake Metastore

This module provides comprehensive operations for managing Apache Iceberg tables
through BigLake Metastore, including table creation, data manipulation, and
BigQuery integration.
"""

import logging
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from google.cloud import bigquery

from biglake_metastore_client import BigLakeMetastoreClient

logger = logging.getLogger(__name__)


@dataclass
class TableSchema:
    """Represents an Iceberg table schema."""
    name: str
    columns: List[Dict[str, str]]
    partition_columns: Optional[List[str]] = None
    clustering_columns: Optional[List[str]] = None
    
    def to_spark_schema(self) -> StructType:
        """Convert to Spark StructType."""
        fields = []
        for col in self.columns:
            spark_type = self._get_spark_type(col['type'])
            fields.append(StructField(col['name'], spark_type, col.get('nullable', True)))
        return StructType(fields)
    
    def _get_spark_type(self, type_str: str):
        """Map string type to Spark type."""
        type_mapping = {
            'string': StringType(),
            'int': IntegerType(),
            'integer': IntegerType(),
            'double': DoubleType(),
            'float': DoubleType(),
            'timestamp': TimestampType()
        }
        return type_mapping.get(type_str.lower(), StringType())


class IcebergTableManager:
    """Manages Iceberg table operations through BigLake Metastore."""
    
    def __init__(self, client: BigLakeMetastoreClient):
        self.client = client
        self.spark = client.get_spark_session()
        self.catalog_name = client.config.catalog_name
        
    def create_namespace(self, namespace: str, properties: Optional[Dict[str, str]] = None) -> bool:
        """Create a namespace (schema) in the Iceberg catalog."""
        try:
            sql = f"CREATE NAMESPACE IF NOT EXISTS {self.catalog_name}.{namespace}"
            if properties:
                props = ", ".join([f"'{k}' = '{v}'" for k, v in properties.items()])
                sql += f" WITH PROPERTIES ({props})"
            
            self.spark.sql(sql)
            logger.info(f"Namespace '{namespace}' created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create namespace '{namespace}': {e}")
            return False
    
    def list_namespaces(self) -> List[str]:
        """List all namespaces in the catalog."""
        try:
            result = self.spark.sql(f"SHOW NAMESPACES IN {self.catalog_name}")
            namespaces = [row['namespace'] for row in result.collect()]
            logger.info(f"Found {len(namespaces)} namespaces")
            return namespaces
            
        except Exception as e:
            logger.error(f"Failed to list namespaces: {e}")
            return []
    
    def create_table(self, namespace: str, table_name: str, schema: TableSchema, 
                    location: Optional[str] = None) -> bool:
        """Create an Iceberg table."""
        try:
            full_table_name = f"{self.catalog_name}.{namespace}.{table_name}"
            
            # Build column definitions
            columns = []
            for col in schema.columns:
                col_def = f"{col['name']} {col['type']}"
                if not col.get('nullable', True):
                    col_def += " NOT NULL"
                columns.append(col_def)
            
            columns_str = ", ".join(columns)
            
            # Build CREATE TABLE SQL
            sql = f"CREATE TABLE IF NOT EXISTS {full_table_name} ({columns_str}) USING ICEBERG"
            
            # Add partitioning if specified
            if schema.partition_columns:
                partition_str = ", ".join(schema.partition_columns)
                sql += f" PARTITIONED BY ({partition_str})"
            
            # Add clustering if specified
            if schema.clustering_columns:
                cluster_str = ", ".join(schema.clustering_columns)
                sql += f" CLUSTERED BY ({cluster_str})"
            
            # Add location if specified
            if location:
                sql += f" LOCATION '{location}'"
            
            self.spark.sql(sql)
            logger.info(f"Table '{full_table_name}' created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table '{namespace}.{table_name}': {e}")
            return False
    
    def list_tables(self, namespace: str) -> List[str]:
        """List all tables in a namespace."""
        try:
            result = self.spark.sql(f"SHOW TABLES IN {self.catalog_name}.{namespace}")
            tables = [row['tableName'] for row in result.collect()]
            logger.info(f"Found {len(tables)} tables in namespace '{namespace}'")
            return tables
            
        except Exception as e:
            logger.error(f"Failed to list tables in namespace '{namespace}': {e}")
            return []
    
    def describe_table(self, namespace: str, table_name: str) -> Optional[Dict[str, Any]]:
        """Get table description and metadata."""
        try:
            full_table_name = f"{self.catalog_name}.{namespace}.{table_name}"
            
            # Get table schema
            schema_result = self.spark.sql(f"DESCRIBE TABLE {full_table_name}")
            schema_info = schema_result.collect()
            
            # Get table properties
            props_result = self.spark.sql(f"SHOW TBLPROPERTIES {full_table_name}")
            properties = {row['key']: row['value'] for row in props_result.collect()}
            
            return {
                'schema': schema_info,
                'properties': properties,
                'full_name': full_table_name
            }
            
        except Exception as e:
            logger.error(f"Failed to describe table '{namespace}.{table_name}': {e}")
            return None
    
    def insert_data(self, namespace: str, table_name: str, data: Union[DataFrame, pd.DataFrame, List[Dict]]) -> bool:
        """Insert data into an Iceberg table."""
        try:
            full_table_name = f"{self.catalog_name}.{namespace}.{table_name}"
            
            # Convert data to Spark DataFrame if needed
            if isinstance(data, pd.DataFrame):
                spark_df = self.spark.createDataFrame(data)
            elif isinstance(data, list):
                spark_df = self.spark.createDataFrame(data)
            elif isinstance(data, DataFrame):
                spark_df = data
            else:
                raise ValueError(f"Unsupported data type: {type(data)}")
            
            # Insert data
            spark_df.writeTo(full_table_name).append()
            
            logger.info(f"Data inserted successfully into '{full_table_name}'")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert data into '{namespace}.{table_name}': {e}")
            return False
    
    def query_table(self, namespace: str, table_name: str, limit: Optional[int] = None) -> Optional[DataFrame]:
        """Query data from an Iceberg table."""
        try:
            full_table_name = f"{self.catalog_name}.{namespace}.{table_name}"
            
            sql = f"SELECT * FROM {full_table_name}"
            if limit:
                sql += f" LIMIT {limit}"
            
            result = self.spark.sql(sql)
            logger.info(f"Query executed successfully on '{full_table_name}'")
            return result
            
        except Exception as e:
            logger.error(f"Failed to query table '{namespace}.{table_name}': {e}")
            return None
    
    def update_table_schema(self, namespace: str, table_name: str, new_columns: List[Dict[str, str]]) -> bool:
        """Add new columns to an existing table (schema evolution)."""
        try:
            full_table_name = f"{self.catalog_name}.{namespace}.{table_name}"
            
            for col in new_columns:
                sql = f"ALTER TABLE {full_table_name} ADD COLUMN {col['name']} {col['type']}"
                if not col.get('nullable', True):
                    sql += " NOT NULL"
                
                self.spark.sql(sql)
                logger.info(f"Added column '{col['name']}' to table '{full_table_name}'")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update schema for table '{namespace}.{table_name}': {e}")
            return False
    
    def drop_table(self, namespace: str, table_name: str) -> bool:
        """Drop an Iceberg table."""
        try:
            full_table_name = f"{self.catalog_name}.{namespace}.{table_name}"
            self.spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            logger.info(f"Table '{full_table_name}' dropped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to drop table '{namespace}.{table_name}': {e}")
            return False


class BigQueryIntegration:
    """Handles BigQuery integration with BigLake Iceberg tables."""
    
    def __init__(self, client: BigLakeMetastoreClient):
        self.client = client
        self.bq_client = client.get_bigquery_client()
        self.project_id = client.config.project_id
        self.dataset_id = client.config.dataset_id
        self.bucket_name = client.config.bucket_name
        self.connection_id = client.config.connection_id
    
    def create_biglake_table(self, table_name: str, schema: TableSchema, 
                           namespace: str = "default") -> bool:
        """Create a BigLake Iceberg table in BigQuery."""
        try:
            # Build column definitions for BigQuery
            columns = []
            for col in schema.columns:
                bq_type = self._map_to_bigquery_type(col['type'])
                mode = "NULLABLE" if col.get('nullable', True) else "REQUIRED"
                columns.append(bigquery.SchemaField(col['name'], bq_type, mode=mode))
            
            # Create table reference
            table_ref = bigquery.TableReference.from_string(
                f"{self.project_id}.{self.dataset_id}.{table_name}"
            )
            
            # Configure BigLake table
            table = bigquery.Table(table_ref, schema=columns)
            
            # Set BigLake configuration
            table.external_data_configuration = bigquery.ExternalConfig("ICEBERG")
            table.external_data_configuration.source_uris = [
                f"gs://{self.bucket_name}/{namespace}/{table_name}/*"
            ]
            table.external_data_configuration.connection_id = (
                f"{self.project_id}.{self.client.config.region}.{self.connection_id}"
            )
            
            # Create the table
            table = self.bq_client.create_table(table, exists_ok=True)
            logger.info(f"BigLake table '{table_name}' created in BigQuery")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create BigLake table '{table_name}': {e}")
            return False
    
    def query_biglake_table(self, table_name: str, query: Optional[str] = None) -> Optional[pd.DataFrame]:
        """Query a BigLake table from BigQuery."""
        try:
            if query is None:
                query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{table_name}` LIMIT 1000"
            
            # Execute query
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            # Convert to pandas DataFrame
            df = results.to_dataframe()
            logger.info(f"Query executed successfully on BigLake table '{table_name}'")
            return df
            
        except Exception as e:
            logger.error(f"Failed to query BigLake table '{table_name}': {e}")
            return None
    
    def _map_to_bigquery_type(self, spark_type: str) -> str:
        """Map Spark/Iceberg types to BigQuery types."""
        type_mapping = {
            'string': 'STRING',
            'int': 'INTEGER',
            'integer': 'INTEGER',
            'long': 'INTEGER',
            'double': 'FLOAT',
            'float': 'FLOAT',
            'boolean': 'BOOLEAN',
            'timestamp': 'TIMESTAMP',
            'date': 'DATE'
        }
        return type_mapping.get(spark_type.lower(), 'STRING')


# Example schemas for testing
SAMPLE_SCHEMAS = {
    'employee': TableSchema(
        name='employee',
        columns=[
            {'name': 'id', 'type': 'int', 'nullable': False},
            {'name': 'name', 'type': 'string', 'nullable': False},
            {'name': 'department', 'type': 'string', 'nullable': True},
            {'name': 'salary', 'type': 'double', 'nullable': True},
            {'name': 'hire_date', 'type': 'timestamp', 'nullable': True}
        ],
        partition_columns=['department'],
        clustering_columns=['hire_date']
    ),
    'sales': TableSchema(
        name='sales',
        columns=[
            {'name': 'transaction_id', 'type': 'string', 'nullable': False},
            {'name': 'product_id', 'type': 'string', 'nullable': False},
            {'name': 'customer_id', 'type': 'string', 'nullable': False},
            {'name': 'amount', 'type': 'double', 'nullable': False},
            {'name': 'transaction_date', 'type': 'timestamp', 'nullable': False}
        ],
        partition_columns=['transaction_date'],
        clustering_columns=['customer_id']
    )
}
