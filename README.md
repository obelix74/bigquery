# BigLake Metastore with Apache Iceberg REST Catalog

A comprehensive Python implementation for working with Google Cloud BigLake Metastore using Apache Iceberg REST catalog. This solution provides seamless integration between BigQuery and Iceberg tables stored in Cloud Storage.

## 🚀 Features

- **Complete BigLake Metastore Integration**: Full support for Apache Iceberg REST catalog
- **Spark Client Integration**: Native PySpark integration with proper authentication
- **BigQuery Integration**: Seamless querying of Iceberg tables from BigQuery
- **Credential Vending**: Support for secure credential delegation
- **Schema Evolution**: Dynamic table schema updates and management
- **Comprehensive Testing**: Full test suite with validation scenarios
- **Production Ready**: Error handling, logging, and configuration management

## 📋 Prerequisites

- Python 3.8+
- Google Cloud SDK (gcloud CLI)
- Active Google Cloud project with billing enabled
- Required APIs enabled:
  - BigQuery API
  - BigLake API
  - Cloud Storage API
  - BigQuery Connection API

## 🛠️ Installation

### 1. Clone and Setup

```bash
git clone <repository-url>
cd bigquery
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment

Copy the example environment file and update with your values:

```bash
cp .env.example .env
# Edit .env with your project details
```

### 4. Run Setup Script

```bash
python scripts/setup_environment.py
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file with the following variables:

```bash
# Google Cloud Project Configuration
PROJECT_ID=your-project-id
REGION=us-central1

# Storage Configuration
BUCKET_NAME=your-biglake-bucket

# BigQuery Configuration
DATASET_ID=biglake_dataset
CONNECTION_ID=biglake-connection

# Catalog Configuration
CATALOG_NAME=biglake_catalog
APP_NAME=BigLakeMetastoreClient

# Authentication and Security
CREDENTIAL_VENDING=true
```

### Configuration File

Alternatively, use a JSON configuration file:

```json
{
  "project_id": "your-project-id",
  "region": "us-central1",
  "bucket_name": "your-biglake-bucket",
  "dataset_id": "biglake_dataset",
  "connection_id": "biglake-connection",
  "catalog_name": "biglake_catalog",
  "app_name": "BigLakeMetastoreClient",
  "credential_vending": true
}
```

## 🏗️ Infrastructure Setup

Follow the detailed infrastructure setup guide:

```bash
# See docs/infrastructure-setup.md for complete instructions

# Quick setup commands:
export PROJECT_ID="your-project-id"
export BUCKET_NAME="your-biglake-bucket"
export REGION="us-central1"

# Enable APIs
gcloud services enable bigquery.googleapis.com biglake.googleapis.com storage.googleapis.com bigqueryconnection.googleapis.com

# Create storage bucket
gcloud storage buckets create gs://${BUCKET_NAME} --project=${PROJECT_ID} --location=${REGION} --enable-autoclass --public-access-prevention --uniform-bucket-level-access

# Create connection
bq mk --connection --location=${REGION} --project_id=${PROJECT_ID} --connection_type=CLOUD_RESOURCE biglake-connection
```

## 🚀 Quick Start

### Basic Usage

```python
from src.biglake_metastore_client import create_client_from_env
from src.iceberg_operations import IcebergTableManager, TableSchema

# Initialize client
client = create_client_from_env()
spark = client.initialize_spark_session()

# Create table manager
table_manager = IcebergTableManager(client)

# Create namespace
table_manager.create_namespace("demo_namespace")

# Define table schema
schema = TableSchema(
    name='employees',
    columns=[
        {'name': 'id', 'type': 'int', 'nullable': False},
        {'name': 'name', 'type': 'string', 'nullable': False},
        {'name': 'department', 'type': 'string', 'nullable': True},
        {'name': 'salary', 'type': 'double', 'nullable': True}
    ]
)

# Create table
table_manager.create_table("demo_namespace", "employees", schema)

# Insert data
data = [
    {'id': 1, 'name': 'John Doe', 'department': 'Engineering', 'salary': 75000.0},
    {'id': 2, 'name': 'Jane Smith', 'department': 'Marketing', 'salary': 65000.0}
]
table_manager.insert_data("demo_namespace", "employees", data)

# Query data
df = table_manager.query_table("demo_namespace", "employees")
df.show()
```

### Complete Workflow

Run the complete workflow example:

```bash
python examples/complete_workflow.py
```

This example demonstrates:
- Client initialization and connection testing
- Namespace and table creation
- Data insertion and querying
- Schema evolution
- BigQuery integration
- Advanced Spark SQL queries

## 🧪 Testing

### Run All Tests

```bash
python tests/test_biglake_integration.py
```

### Validation Tests

```bash
python scripts/setup_environment.py
```

### Manual Testing

```python
# Test connection
from src.biglake_metastore_client import create_client_from_env

client = create_client_from_env()
if client.test_connection():
    print("✅ Connection successful!")
else:
    print("❌ Connection failed!")
```

## 📊 BigQuery Integration

### Query Iceberg Tables from BigQuery

```sql
-- Query Iceberg table directly from BigQuery
SELECT 
    department,
    COUNT(*) as employee_count,
    AVG(salary) as avg_salary
FROM `your-project.biglake_dataset.employees`
GROUP BY department
ORDER BY avg_salary DESC;
```

### Create BigLake Tables

```python
from src.iceberg_operations import BigQueryIntegration

bq_integration = BigQueryIntegration(client)
bq_integration.create_biglake_table('employees_bq', schema, 'demo_namespace')

# Query from BigQuery
df = bq_integration.query_biglake_table('employees_bq')
print(df.head())
```

## 🔧 Advanced Features

### Schema Evolution

```python
# Add new columns to existing table
new_columns = [
    {'name': 'email', 'type': 'string', 'nullable': True},
    {'name': 'phone', 'type': 'string', 'nullable': True}
]
table_manager.update_table_schema("demo_namespace", "employees", new_columns)
```

### Credential Vending

Enable secure credential delegation:

```python
# Enable credential vending mode
client.enable_credential_vending()

# Configure Spark with credential vending
# (automatically handled when credential_vending=true in config)
```

### Custom Spark Configuration

```python
# Override Spark configuration
custom_config = {
    'spark.driver.memory': '4g',
    'spark.executor.memory': '4g',
    'spark.executor.cores': '2'
}

# Apply custom configuration
for key, value in custom_config.items():
    spark.conf.set(key, value)
```

## 🐛 Troubleshooting

### Common Issues

1. **Authentication Errors**
   ```bash
   gcloud auth application-default login
   gcloud config set project YOUR_PROJECT_ID
   ```

2. **Permission Errors**
   - Ensure service accounts have proper IAM roles
   - Check bucket permissions and uniform bucket-level access

3. **API Not Enabled**
   ```bash
   gcloud services enable bigquery.googleapis.com biglake.googleapis.com
   ```

4. **Connection Issues**
   - Verify all resources are in the same region
   - Check network connectivity and firewall rules

### Debug Mode

Enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Validation Script

Run the validation script to check your setup:

```bash
python scripts/setup_environment.py
```

## 📁 Project Structure

```
bigquery/
├── src/
│   ├── biglake_metastore_client.py    # Main client implementation
│   └── iceberg_operations.py          # Iceberg table operations
├── examples/
│   └── complete_workflow.py           # Complete workflow example
├── tests/
│   └── test_biglake_integration.py    # Test suite
├── scripts/
│   └── setup_environment.py           # Environment setup
├── docs/
│   └── infrastructure-setup.md        # Infrastructure guide
├── config/
│   └── biglake_config.json           # Configuration template
├── requirements.txt                   # Python dependencies
├── .env.example                      # Environment template
└── README.md                         # This file
```

## 🔒 Security Considerations

- Use credential vending mode for production environments
- Restrict bucket write permissions to prevent data corruption
- Enable audit logging for operational transparency
- Use customer-managed encryption keys for sensitive data
- Implement proper IAM roles and least privilege access

## 📈 Performance Optimization

- Use single-region buckets co-located with BigQuery
- Enable Autoclass for automatic storage optimization
- Configure appropriate Spark memory and executor settings
- Use clustering and partitioning for large tables
- Monitor query performance and optimize as needed

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:

1. Check the troubleshooting section
2. Run the validation script
3. Review the logs for error details
4. Contact biglake-help@google.com for BigLake-specific issues

## 🔗 Additional Resources

- [BigLake Metastore Documentation](https://cloud.google.com/bigquery/docs/blms-rest-catalog)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Google Cloud Storage Documentation](https://cloud.google.com/storage/docs)
