#!/usr/bin/env python3
"""
Complete BigLake Metastore Workflow Example

This script demonstrates a complete end-to-end workflow for using BigLake Metastore
with Apache Iceberg tables, including:
1. Setting up the connection
2. Creating namespaces and tables
3. Inserting and querying data
4. BigQuery integration
5. Schema evolution
"""

import os
import sys
import logging
from datetime import datetime, timedelta
import pandas as pd

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from biglake_metastore_client import create_client_from_env, BigLakeConfig
from iceberg_operations import IcebergTableManager, BigQueryIntegration, TableSchema, SAMPLE_SCHEMAS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_sample_data():
    """Generate sample data for testing."""
    
    # Employee data
    employee_data = [
        {
            'id': 1,
            'name': 'John Doe',
            'department': 'Engineering',
            'salary': 75000.0,
            'hire_date': datetime(2022, 1, 15)
        },
        {
            'id': 2,
            'name': 'Jane Smith',
            'department': 'Marketing',
            'salary': 65000.0,
            'hire_date': datetime(2022, 3, 20)
        },
        {
            'id': 3,
            'name': 'Bob Johnson',
            'department': 'Engineering',
            'salary': 80000.0,
            'hire_date': datetime(2021, 11, 10)
        },
        {
            'id': 4,
            'name': 'Alice Brown',
            'department': 'Sales',
            'salary': 70000.0,
            'hire_date': datetime(2023, 2, 5)
        }
    ]
    
    # Sales data
    sales_data = [
        {
            'transaction_id': 'TXN001',
            'product_id': 'PROD001',
            'customer_id': 'CUST001',
            'amount': 299.99,
            'transaction_date': datetime.now() - timedelta(days=1)
        },
        {
            'transaction_id': 'TXN002',
            'product_id': 'PROD002',
            'customer_id': 'CUST002',
            'amount': 149.99,
            'transaction_date': datetime.now() - timedelta(days=2)
        },
        {
            'transaction_id': 'TXN003',
            'product_id': 'PROD001',
            'customer_id': 'CUST003',
            'amount': 299.99,
            'transaction_date': datetime.now() - timedelta(days=3)
        }
    ]
    
    return employee_data, sales_data


def main():
    """Main workflow execution."""
    
    print("🚀 Starting BigLake Metastore Complete Workflow")
    print("=" * 60)
    
    try:
        # Step 1: Initialize client
        print("\n📡 Step 1: Initializing BigLake Metastore Client")
        client = create_client_from_env()
        
        # Initialize Spark session
        spark = client.initialize_spark_session()
        print("✅ Spark session initialized")
        
        # Test connection
        if not client.test_connection():
            print("❌ Connection test failed. Please check your configuration.")
            return
        print("✅ Connection test passed")
        
        # Step 2: Initialize managers
        print("\n🔧 Step 2: Initializing Table and BigQuery Managers")
        table_manager = IcebergTableManager(client)
        bq_integration = BigQueryIntegration(client)
        print("✅ Managers initialized")
        
        # Step 3: Create namespace
        print("\n📁 Step 3: Creating Namespace")
        namespace = "demo_namespace"
        if table_manager.create_namespace(namespace, {"description": "Demo namespace for testing"}):
            print(f"✅ Namespace '{namespace}' created")
        else:
            print(f"❌ Failed to create namespace '{namespace}'")
            return
        
        # List namespaces
        namespaces = table_manager.list_namespaces()
        print(f"📋 Available namespaces: {namespaces}")
        
        # Step 4: Create tables
        print("\n📊 Step 4: Creating Iceberg Tables")
        
        # Create employee table
        employee_schema = SAMPLE_SCHEMAS['employee']
        if table_manager.create_table(namespace, 'employees', employee_schema):
            print("✅ Employee table created")
        else:
            print("❌ Failed to create employee table")
            return
        
        # Create sales table
        sales_schema = SAMPLE_SCHEMAS['sales']
        if table_manager.create_table(namespace, 'sales', sales_schema):
            print("✅ Sales table created")
        else:
            print("❌ Failed to create sales table")
            return
        
        # List tables
        tables = table_manager.list_tables(namespace)
        print(f"📋 Tables in '{namespace}': {tables}")
        
        # Step 5: Insert sample data
        print("\n💾 Step 5: Inserting Sample Data")
        employee_data, sales_data = generate_sample_data()
        
        # Insert employee data
        if table_manager.insert_data(namespace, 'employees', employee_data):
            print("✅ Employee data inserted")
        else:
            print("❌ Failed to insert employee data")
        
        # Insert sales data
        if table_manager.insert_data(namespace, 'sales', sales_data):
            print("✅ Sales data inserted")
        else:
            print("❌ Failed to insert sales data")
        
        # Step 6: Query data with Spark
        print("\n🔍 Step 6: Querying Data with Spark")
        
        # Query employee table
        employee_df = table_manager.query_table(namespace, 'employees', limit=10)
        if employee_df:
            print("📊 Employee data (first 10 rows):")
            employee_df.show()
        
        # Query sales table
        sales_df = table_manager.query_table(namespace, 'sales', limit=10)
        if sales_df:
            print("📊 Sales data (first 10 rows):")
            sales_df.show()
        
        # Step 7: Demonstrate schema evolution
        print("\n🔄 Step 7: Demonstrating Schema Evolution")
        new_columns = [
            {'name': 'email', 'type': 'string', 'nullable': True},
            {'name': 'phone', 'type': 'string', 'nullable': True}
        ]
        
        if table_manager.update_table_schema(namespace, 'employees', new_columns):
            print("✅ Schema evolution completed - added email and phone columns")
            
            # Describe updated table
            table_info = table_manager.describe_table(namespace, 'employees')
            if table_info:
                print("📋 Updated table schema:")
                for row in table_info['schema'][:10]:  # Show first 10 schema rows
                    print(f"  - {row}")
        
        # Step 8: BigQuery integration
        print("\n🔗 Step 8: BigQuery Integration")
        
        # Create BigLake tables in BigQuery
        if bq_integration.create_biglake_table('employees_bq', employee_schema, namespace):
            print("✅ Employee BigLake table created in BigQuery")
        
        if bq_integration.create_biglake_table('sales_bq', sales_schema, namespace):
            print("✅ Sales BigLake table created in BigQuery")
        
        # Query from BigQuery
        print("\n📊 Querying BigLake tables from BigQuery:")
        
        # Query employee data from BigQuery
        employee_bq_df = bq_integration.query_biglake_table('employees_bq')
        if employee_bq_df is not None:
            print("📊 Employee data from BigQuery:")
            print(employee_bq_df.head())
        
        # Custom BigQuery query
        custom_query = f"""
        SELECT 
            department,
            COUNT(*) as employee_count,
            AVG(salary) as avg_salary
        FROM `{client.config.project_id}.{client.config.dataset_id}.employees_bq`
        GROUP BY department
        ORDER BY avg_salary DESC
        """
        
        dept_stats = bq_integration.query_biglake_table('employees_bq', custom_query)
        if dept_stats is not None:
            print("\n📊 Department statistics from BigQuery:")
            print(dept_stats)
        
        # Step 9: Advanced Spark SQL queries
        print("\n🔍 Step 9: Advanced Spark SQL Queries")
        
        # Register tables as temporary views for easier querying
        employee_df.createOrReplaceTempView("employees_view")
        sales_df.createOrReplaceTempView("sales_view")
        
        # Complex query example
        complex_query = """
        SELECT 
            e.department,
            COUNT(DISTINCT e.id) as employee_count,
            AVG(e.salary) as avg_salary,
            COUNT(s.transaction_id) as total_transactions,
            SUM(s.amount) as total_sales
        FROM employees_view e
        LEFT JOIN sales_view s ON e.id = CAST(SUBSTRING(s.customer_id, 5) AS INT)
        GROUP BY e.department
        ORDER BY total_sales DESC NULLS LAST
        """
        
        result_df = spark.sql(complex_query)
        print("📊 Department performance analysis:")
        result_df.show()
        
        print("\n🎉 Workflow completed successfully!")
        print("=" * 60)
        
        # Step 10: Cleanup (optional)
        print("\n🧹 Step 10: Cleanup (optional)")
        cleanup = input("Do you want to clean up the created tables? (y/N): ").lower().strip()
        
        if cleanup == 'y':
            print("🗑️ Cleaning up tables...")
            table_manager.drop_table(namespace, 'employees')
            table_manager.drop_table(namespace, 'sales')
            print("✅ Tables cleaned up")
        else:
            print("ℹ️ Tables preserved for further exploration")
        
    except Exception as e:
        logger.error(f"Workflow failed: {e}")
        print(f"❌ Workflow failed: {e}")
        raise
    
    finally:
        # Clean up resources
        if 'client' in locals():
            client.close()
            print("🔒 Resources cleaned up")


if __name__ == "__main__":
    main()
