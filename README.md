# BigLake Metastore with Iceberg Integration

This project demonstrates how to write JSON data to BigLake Metastore in Google Cloud Platform using the Apache Iceberg format.

## Overview

The solution creates a complete pipeline that:
1. Takes sample JSON data
2. Converts it to Apache Iceberg format
3. Stores it in Google Cloud Storage
4. Creates a BigLake external table in BigQuery
5. Enables querying through BigQuery SQL

## Prerequisites

1. **Google Cloud Project** with the following APIs enabled:
   - BigQuery API
   - Cloud Storage API
   - BigQuery Connection API

2. **Authentication**: Set up authentication using one of:
   - Service Account key file
   - Application Default Credentials (`gcloud auth application-default login`)
   - Service Account attached to compute instance

3. **Permissions**: Your account/service account needs:
   - BigQuery Data Editor
   - BigQuery Job User
   - Storage Admin
   - BigQuery Connection Admin

## Installation

1. Clone or download this repository
2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

1. **Set Environment Variables** (recommended):
   ```bash
   export GCP_PROJECT_ID="your-project-id"
   export GCP_REGION="us-central1"
   export GCS_BUCKET_NAME="your-project-id-biglake-data"
   export BQ_DATASET_ID="biglake_dataset"
   export BQ_TABLE_NAME="employee_data"
   export BQ_CONNECTION_ID="biglake-connection"
   ```

2. **Or edit config.py** directly:
   ```python
   self.project_id = "your-actual-project-id"
   ```

## Usage

### Quick Start - Complete Workflow

Run the complete setup process:
```bash
python biglake_orchestrator.py setup
```

This will:
- Create GCS bucket
- Create BigQuery dataset
- Create BigQuery connection (may require manual setup)
- Upload and convert JSON data to Iceberg format
- Create BigLake external table
- Verify the setup by querying the table

### Individual Commands

Check current status:
```bash
python biglake_orchestrator.py status
```

Verify existing setup:
```bash
python biglake_orchestrator.py verify
```

Clean up all resources:
```bash
python biglake_orchestrator.py cleanup
```

### Manual BigQuery Connection Setup

If the automatic connection creation fails, create it manually:

1. Go to BigQuery in Google Cloud Console
2. Click "External connections" in the left sidebar
3. Click "Create connection"
4. Choose "Cloud resource" as connection type
5. Set Connection ID to the value in your config (default: `biglake-connection`)
6. Select the same region as your bucket
7. Note the service account created and grant it Storage Object Viewer permission on your bucket

## Project Structure

```
├── biglake_orchestrator.py    # Main orchestration script
├── config.py                  # Configuration management
├── gcs_manager.py             # Google Cloud Storage operations
├── bigquery_manager.py        # BigQuery operations
├── iceberg_manager.py         # Iceberg table management
├── sample_data.json           # Sample JSON data
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## Sample Data

The project includes `sample_data.json` with employee records containing:
- Basic info (id, name, email, age)
- Employment data (department, salary, hire_date, is_active)
- Skills array
- Nested address object

## Querying Your Data

Once set up, you can query your data in BigQuery:

```sql
SELECT
    name,
    department,
    salary,
    ARRAY_TO_STRING(skills, ', ') as skills_list,
    address.city
FROM `your-project.biglake_dataset.employee_data`
WHERE is_active = true
ORDER BY salary DESC;
```

## Architecture

```
JSON Data → Iceberg Format → Google Cloud Storage → BigLake Table → BigQuery Queries
```

1. **JSON Data**: Source data in JSON format
2. **Iceberg Format**: Data converted to Apache Iceberg with metadata
3. **Cloud Storage**: Data stored in GCS bucket with Iceberg structure
4. **BigLake Table**: External table pointing to Iceberg data
5. **BigQuery**: Query interface for the data

## Troubleshooting

### Common Issues

1. **Authentication Error**:
   ```
   Error: Could not automatically determine credentials
   ```
   Solution: Run `gcloud auth application-default login`

2. **Permission Denied**:
   ```
   Error: 403 Forbidden
   ```
   Solution: Ensure your account has the required IAM roles

3. **Connection Creation Failed**:
   ```
   Error managing connection: ...
   ```
   Solution: Create the BigQuery connection manually (see instructions above)

4. **Bucket Already Exists Error**:
   ```
   Error: The bucket you tried to create already exists
   ```
   Solution: Choose a different bucket name in the configuration

### Debugging

Enable detailed logging by modifying the scripts to include more print statements or by checking:
- BigQuery Job History in the console
- Cloud Storage bucket contents
- Error messages in Cloud Logging

## Cost Considerations

- **Cloud Storage**: Storage costs for data and metadata files
- **BigQuery**: Query costs based on data scanned
- **BigQuery Slots**: Slot usage for queries (on-demand or flat-rate)

## Next Steps

1. **Add More Data**: Modify `sample_data.json` or create new JSON files
2. **Schema Evolution**: Update the Iceberg schema for new fields
3. **Partitioning**: Add partitioning strategy for larger datasets
4. **Automation**: Set up Cloud Functions or Cloud Run for automated processing
5. **Monitoring**: Add Cloud Monitoring and alerting

## License

This project is provided as an example. Modify as needed for your use case.