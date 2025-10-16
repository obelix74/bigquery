# BigLake Metastore Usage Examples

This document provides comprehensive examples for using the BigLake Metastore implementation with various scenarios and use cases.

## Table of Contents

1. [Basic Operations](#basic-operations)
2. [Advanced Queries](#advanced-queries)
3. [Schema Evolution](#schema-evolution)
4. [BigQuery Integration](#bigquery-integration)
5. [Data Pipeline Examples](#data-pipeline-examples)
6. [Performance Optimization](#performance-optimization)

## Basic Operations

### 1. Client Initialization

```python
from src.biglake_metastore_client import create_client_from_env, BigLakeConfig
from src.iceberg_operations import IcebergTableManager, BigQueryIntegration

# Method 1: From environment variables
client = create_client_from_env()

# Method 2: From configuration file
client = create_client_from_config('config/biglake_config.json')

# Method 3: Direct configuration
config = BigLakeConfig(
    project_id='my-project',
    region='us-central1',
    bucket_name='my-biglake-bucket',
    dataset_id='my_dataset',
    connection_id='my-connection'
)
client = BigLakeMetastoreClient(config)

# Initialize Spark session
spark = client.initialize_spark_session()
```

### 2. Namespace Management

```python
table_manager = IcebergTableManager(client)

# Create namespace with properties
table_manager.create_namespace(
    "analytics", 
    properties={
        "description": "Analytics data namespace",
        "owner": "data-team@company.com"
    }
)

# List all namespaces
namespaces = table_manager.list_namespaces()
print(f"Available namespaces: {namespaces}")

# Use namespace in Spark SQL
spark.sql("USE analytics")
```

### 3. Table Creation and Management

```python
from src.iceberg_operations import TableSchema

# Define table schema
user_schema = TableSchema(
    name='users',
    columns=[
        {'name': 'user_id', 'type': 'string', 'nullable': False},
        {'name': 'email', 'type': 'string', 'nullable': False},
        {'name': 'first_name', 'type': 'string', 'nullable': True},
        {'name': 'last_name', 'type': 'string', 'nullable': True},
        {'name': 'created_at', 'type': 'timestamp', 'nullable': False},
        {'name': 'country', 'type': 'string', 'nullable': True}
    ],
    partition_columns=['country'],
    clustering_columns=['created_at']
)

# Create table
table_manager.create_table("analytics", "users", user_schema)

# List tables in namespace
tables = table_manager.list_tables("analytics")
print(f"Tables in analytics: {tables}")

# Describe table
table_info = table_manager.describe_table("analytics", "users")
print(f"Table schema: {table_info['schema']}")
```

## Advanced Queries

### 1. Complex Spark SQL Queries

```python
# Time-based analysis
time_analysis = spark.sql("""
    SELECT 
        DATE_TRUNC('month', created_at) as month,
        country,
        COUNT(*) as user_count,
        COUNT(DISTINCT email) as unique_emails
    FROM analytics.users
    WHERE created_at >= '2023-01-01'
    GROUP BY DATE_TRUNC('month', created_at), country
    ORDER BY month DESC, user_count DESC
""")

time_analysis.show()

# Window functions for user ranking
user_ranking = spark.sql("""
    SELECT 
        user_id,
        email,
        country,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY country 
            ORDER BY created_at
        ) as user_rank_in_country
    FROM analytics.users
    WHERE country IS NOT NULL
""")

user_ranking.show()

# Cohort analysis
cohort_analysis = spark.sql("""
    WITH user_cohorts AS (
        SELECT 
            user_id,
            DATE_TRUNC('month', created_at) as cohort_month,
            created_at
        FROM analytics.users
    )
    SELECT 
        cohort_month,
        COUNT(*) as cohort_size,
        COUNT(CASE WHEN created_at >= cohort_month + INTERVAL 1 MONTH THEN 1 END) as retained_month_1
    FROM user_cohorts
    GROUP BY cohort_month
    ORDER BY cohort_month
""")

cohort_analysis.show()
```

### 2. Data Aggregation and Analytics

```python
# Create aggregated views
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW user_summary AS
    SELECT 
        country,
        COUNT(*) as total_users,
        MIN(created_at) as first_user_date,
        MAX(created_at) as latest_user_date,
        COUNT(DISTINCT DATE(created_at)) as active_days
    FROM analytics.users
    GROUP BY country
""")

# Query the view
summary_df = spark.sql("SELECT * FROM user_summary ORDER BY total_users DESC")
summary_df.show()

# Export to pandas for further analysis
summary_pandas = summary_df.toPandas()
print(summary_pandas.describe())
```

## Schema Evolution

### 1. Adding Columns

```python
# Add new columns to existing table
new_columns = [
    {'name': 'phone_number', 'type': 'string', 'nullable': True},
    {'name': 'subscription_tier', 'type': 'string', 'nullable': True},
    {'name': 'last_login', 'type': 'timestamp', 'nullable': True}
]

table_manager.update_table_schema("analytics", "users", new_columns)

# Verify schema update
updated_info = table_manager.describe_table("analytics", "users")
print("Updated schema:")
for field in updated_info['schema']:
    print(f"  {field}")
```

### 2. Handling Schema Changes in Queries

```python
# Query with new columns (handles missing data gracefully)
enhanced_query = spark.sql("""
    SELECT 
        user_id,
        email,
        COALESCE(subscription_tier, 'free') as tier,
        COALESCE(last_login, created_at) as last_activity,
        CASE 
            WHEN last_login IS NULL THEN 'never_logged_in'
            WHEN last_login > current_timestamp() - INTERVAL 7 DAYS THEN 'active'
            WHEN last_login > current_timestamp() - INTERVAL 30 DAYS THEN 'inactive'
            ELSE 'dormant'
        END as user_status
    FROM analytics.users
""")

enhanced_query.show()
```

## BigQuery Integration

### 1. Creating BigLake Tables

```python
bq_integration = BigQueryIntegration(client)

# Create BigLake table in BigQuery
bq_integration.create_biglake_table('users_bq', user_schema, 'analytics')

# Query from BigQuery using SQL
users_bq_df = bq_integration.query_biglake_table('users_bq', """
    SELECT 
        country,
        COUNT(*) as user_count,
        AVG(EXTRACT(YEAR FROM created_at)) as avg_signup_year
    FROM `{project}.{dataset}.users_bq`
    WHERE country IS NOT NULL
    GROUP BY country
    ORDER BY user_count DESC
    LIMIT 10
""".format(
    project=client.config.project_id,
    dataset=client.config.dataset_id
))

print(users_bq_df)
```

### 2. Cross-Platform Queries

```python
# Query Iceberg table from Spark and BigQuery table from BigQuery
spark_result = spark.sql("""
    SELECT country, COUNT(*) as spark_count
    FROM analytics.users
    GROUP BY country
""").toPandas()

bq_result = bq_integration.query_biglake_table('users_bq', """
    SELECT country, COUNT(*) as bq_count
    FROM `{}.{}.users_bq`
    GROUP BY country
""".format(client.config.project_id, client.config.dataset_id))

# Compare results
import pandas as pd
comparison = pd.merge(spark_result, bq_result, on='country', how='outer')
comparison['difference'] = comparison['spark_count'] - comparison['bq_count']
print(comparison)
```

## Data Pipeline Examples

### 1. ETL Pipeline

```python
import pandas as pd
from datetime import datetime, timedelta

def daily_user_etl():
    """Daily ETL pipeline for user data."""
    
    # Extract: Generate sample data (replace with your data source)
    new_users = [
        {
            'user_id': f'user_{i}',
            'email': f'user{i}@example.com',
            'first_name': f'User{i}',
            'last_name': 'Test',
            'created_at': datetime.now() - timedelta(days=i),
            'country': ['US', 'UK', 'CA', 'DE'][i % 4]
        }
        for i in range(100, 200)
    ]
    
    # Transform: Clean and validate data
    df = pd.DataFrame(new_users)
    df['email'] = df['email'].str.lower()
    df['full_name'] = df['first_name'] + ' ' + df['last_name']
    
    # Load: Insert into Iceberg table
    table_manager.insert_data("analytics", "users", df.to_dict('records'))
    
    print(f"Loaded {len(new_users)} new users")

# Run ETL
daily_user_etl()
```

### 2. Streaming Data Simulation

```python
import time
import random

def simulate_streaming_data():
    """Simulate streaming data ingestion."""
    
    countries = ['US', 'UK', 'CA', 'DE', 'FR', 'JP', 'AU']
    
    for batch in range(5):  # 5 batches
        batch_data = []
        
        for i in range(20):  # 20 records per batch
            user_data = {
                'user_id': f'stream_user_{batch}_{i}',
                'email': f'stream{batch}{i}@example.com',
                'first_name': f'Stream{i}',
                'last_name': f'User{batch}',
                'created_at': datetime.now(),
                'country': random.choice(countries)
            }
            batch_data.append(user_data)
        
        # Insert batch
        table_manager.insert_data("analytics", "users", batch_data)
        print(f"Inserted batch {batch + 1} with {len(batch_data)} records")
        
        # Wait before next batch
        time.sleep(2)

# Run streaming simulation
simulate_streaming_data()
```

### 3. Data Quality Checks

```python
def run_data_quality_checks():
    """Run data quality checks on the users table."""
    
    checks = []
    
    # Check 1: No duplicate emails
    duplicate_emails = spark.sql("""
        SELECT email, COUNT(*) as count
        FROM analytics.users
        GROUP BY email
        HAVING COUNT(*) > 1
    """)
    
    duplicate_count = duplicate_emails.count()
    checks.append(("No duplicate emails", duplicate_count == 0, f"Found {duplicate_count} duplicates"))
    
    # Check 2: Valid email format
    invalid_emails = spark.sql("""
        SELECT COUNT(*) as count
        FROM analytics.users
        WHERE email NOT RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
    """)
    
    invalid_count = invalid_emails.collect()[0]['count']
    checks.append(("Valid email format", invalid_count == 0, f"Found {invalid_count} invalid emails"))
    
    # Check 3: Recent data
    recent_data = spark.sql("""
        SELECT COUNT(*) as count
        FROM analytics.users
        WHERE created_at > current_timestamp() - INTERVAL 7 DAYS
    """)
    
    recent_count = recent_data.collect()[0]['count']
    checks.append(("Recent data exists", recent_count > 0, f"Found {recent_count} recent records"))
    
    # Print results
    print("Data Quality Check Results:")
    print("-" * 50)
    for check_name, passed, message in checks:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} {check_name}: {message}")

# Run quality checks
run_data_quality_checks()
```

## Performance Optimization

### 1. Query Optimization

```python
# Use partitioning for better performance
partitioned_query = spark.sql("""
    SELECT *
    FROM analytics.users
    WHERE country = 'US'  -- Partition pruning
    AND created_at >= '2023-01-01'  -- Additional filtering
""")

# Cache frequently used DataFrames
users_df = spark.table("analytics.users")
users_df.cache()

# Use broadcast joins for small tables
country_mapping = spark.createDataFrame([
    ('US', 'United States'),
    ('UK', 'United Kingdom'),
    ('CA', 'Canada'),
    ('DE', 'Germany')
], ['code', 'name'])

country_mapping.createOrReplaceTempView("country_mapping")

enriched_users = spark.sql("""
    SELECT u.*, c.name as country_name
    FROM analytics.users u
    LEFT JOIN country_mapping c ON u.country = c.code
""")

enriched_users.show()
```

### 2. Monitoring and Metrics

```python
def get_table_metrics():
    """Get table metrics and statistics."""
    
    # Table size and record count
    record_count = spark.sql("SELECT COUNT(*) as count FROM analytics.users").collect()[0]['count']
    
    # Partition distribution
    partition_stats = spark.sql("""
        SELECT 
            country,
            COUNT(*) as record_count,
            MIN(created_at) as earliest_date,
            MAX(created_at) as latest_date
        FROM analytics.users
        GROUP BY country
        ORDER BY record_count DESC
    """)
    
    print(f"Total records: {record_count}")
    print("\nPartition distribution:")
    partition_stats.show()
    
    # Query performance metrics
    start_time = time.time()
    test_query = spark.sql("""
        SELECT country, COUNT(*) 
        FROM analytics.users 
        WHERE created_at >= '2023-01-01'
        GROUP BY country
    """)
    test_query.collect()  # Trigger execution
    end_time = time.time()
    
    print(f"\nQuery execution time: {end_time - start_time:.2f} seconds")

# Get metrics
get_table_metrics()
```

### 3. Resource Management

```python
# Configure Spark for optimal performance
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Monitor Spark UI
print(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")

# Clean up resources when done
def cleanup():
    """Clean up Spark resources."""
    spark.catalog.clearCache()
    client.close()

# Register cleanup function
import atexit
atexit.register(cleanup)
```

This comprehensive set of examples demonstrates the full capabilities of the BigLake Metastore implementation, from basic operations to advanced analytics and performance optimization.
