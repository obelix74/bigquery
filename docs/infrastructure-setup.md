# BigLake Metastore Infrastructure Setup Guide

This guide provides step-by-step instructions for setting up BigLake Metastore on Google Cloud Platform with Apache Iceberg REST catalog support.

## Prerequisites

### 1. Google Cloud Project Setup
- Active Google Cloud project with billing enabled
- Google Cloud SDK installed and configured
- Required APIs enabled

### 2. Required APIs
Enable the following APIs in your Google Cloud project:

```bash
# Enable required APIs
gcloud services enable bigquery.googleapis.com
gcloud services enable biglake.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable bigqueryconnection.googleapis.com
```

### 3. Required IAM Roles

#### For BigLake Metastore Administration:
- `roles/biglake.admin` - BigLake Admin
- `roles/storage.admin` - Storage Admin (on Cloud Storage bucket)

#### For Credential Vending Mode:
- `roles/biglake.viewer` - BigLake Viewer (read access)
- `roles/biglake.editor` - BigLake Editor (write access)

#### For Non-Credential Vending Mode:
- `roles/biglake.viewer` + `roles/storage.objectViewer`
- `roles/biglake.editor` + `roles/storage.objectUser`

#### For BigQuery Integration:
- `roles/bigquery.dataOwner` - Create BigLake Iceberg tables
- `roles/bigquery.connectionAdmin` - Manage connections
- `roles/bigquery.dataViewer` - Query tables
- `roles/bigquery.user` - Execute queries

## Step 1: Create Cloud Storage Bucket

### 1.1 Create Storage Bucket
```bash
# Set variables
export PROJECT_ID="your-project-id"
export BUCKET_NAME="your-biglake-bucket"
export REGION="us-central1"  # Choose your preferred region

# Create bucket with recommended settings
gcloud storage buckets create gs://${BUCKET_NAME} \
    --project=${PROJECT_ID} \
    --location=${REGION} \
    --enable-autoclass \
    --public-access-prevention \
    --uniform-bucket-level-access
```

### 1.2 Bucket Configuration Best Practices
- Use single-region buckets co-located with BigQuery dataset
- Enable uniform bucket-level access
- Enable public access prevention
- Use Autoclass for automatic storage optimization
- Keep default soft delete policy (7 days)

### 1.3 Security Recommendations
```bash
# Apply bucket-level IAM policy to restrict write access
# Create a policy file: bucket-policy.json
cat > bucket-policy.json << EOF
{
  "bindings": [
    {
      "role": "roles/storage.objectViewer",
      "members": [
        "serviceAccount:your-service-account@project.iam.gserviceaccount.com"
      ]
    }
  ]
}
EOF

# Apply the policy
gcloud storage buckets set-iam-policy gs://${BUCKET_NAME} bucket-policy.json
```

## Step 2: Create Cloud Resource Connection

### 2.1 Create Connection
```bash
# Create Cloud Resource connection
export CONNECTION_ID="biglake-connection"

bq mk --connection \
    --location=${REGION} \
    --project_id=${PROJECT_ID} \
    --connection_type=CLOUD_RESOURCE \
    ${CONNECTION_ID}
```

### 2.2 Get Service Account
```bash
# Retrieve connection service account
bq show --connection ${PROJECT_ID}.${REGION}.${CONNECTION_ID}

# Extract service account ID (save this for next step)
export SERVICE_ACCOUNT=$(bq show --connection ${PROJECT_ID}.${REGION}.${CONNECTION_ID} \
    --format="value(cloudResource.serviceAccountId)")

echo "Service Account: ${SERVICE_ACCOUNT}"
```

### 2.3 Grant Storage Permissions
```bash
# Grant required storage permissions to connection service account
gcloud storage buckets add-iam-policy-binding gs://${BUCKET_NAME} \
    --member=serviceAccount:${SERVICE_ACCOUNT} \
    --role=roles/storage.objectUser

gcloud storage buckets add-iam-policy-binding gs://${BUCKET_NAME} \
    --member=serviceAccount:${SERVICE_ACCOUNT} \
    --role=roles/storage.legacyBucketReader
```

## Step 3: Initialize BigLake Metastore REST Catalog

### 3.1 Initialize Catalog
```bash
# Initialize the catalog
curl -H "x-goog-user-project: ${PROJECT_ID}" \
     -H "Accept: application/json" \
     -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
     "https://biglake.googleapis.com/iceberg/v1/restcatalog/v1/config?warehouse=gs://${BUCKET_NAME}"
```

### 3.2 Extract Catalog Prefix
Save the `prefix` value from the response for use in credential vending setup.

```bash
# Example response - save the prefix value
# {
#   "overrides": {
#     "catalog_credential_mode": "CREDENTIAL_MODE_END_USER",
#     "prefix": "projects/PROJECT_ID/catalogs/BUCKET_NAME"
#   },
#   ...
# }
export CATALOG_PREFIX="projects/${PROJECT_ID}/catalogs/${BUCKET_NAME}"
```

## Step 4: Enable Credential Vending (Optional but Recommended)

### 4.1 Enable Credential Vending Mode
```bash
# Enable credential vending
curl -X PATCH \
     -H "Content-Type: application/json" \
     -H "x-goog-user-project: ${PROJECT_ID}" \
     -H "Accept: application/json" \
     -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
     "https://biglake.googleapis.com/iceberg/v1/restcatalog/extensions/${CATALOG_PREFIX}?update_mask=credential_mode" \
     -d '{"credential_mode":"CREDENTIAL_MODE_VENDED_CREDENTIALS"}'
```

### 4.2 Grant Permissions to BigLake Service Account
```bash
# Extract BigLake service account from response
# Save the "biglake-service-account" value from the response
export BIGLAKE_SERVICE_ACCOUNT="extracted-service-account@gcp-sa-biglake.iam.gserviceaccount.com"

# Grant Storage Object User role to BigLake service account
gcloud storage buckets add-iam-policy-binding gs://${BUCKET_NAME} \
    --member=serviceAccount:${BIGLAKE_SERVICE_ACCOUNT} \
    --role=roles/storage.objectUser
```

## Step 5: Create BigQuery Dataset

### 5.1 Create Dataset
```bash
# Create BigQuery dataset
export DATASET_ID="biglake_dataset"

bq mk --dataset \
    --location=${REGION} \
    ${PROJECT_ID}:${DATASET_ID}
```

## Step 6: Verify Setup

### 6.1 Test Connection
```bash
# Test the connection by listing namespaces (should return empty initially)
curl -H "x-goog-user-project: ${PROJECT_ID}" \
     -H "Accept: application/json" \
     -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
     "https://biglake.googleapis.com/iceberg/v1/restcatalog/v1/${CATALOG_PREFIX}/namespaces"
```

### 6.2 Verify Permissions
```bash
# Check if service account has proper permissions
gcloud storage buckets get-iam-policy gs://${BUCKET_NAME}
```

## Step 7: Configure Dataproc (Optional)

If you plan to use Dataproc for Spark workloads:

### 7.1 Create Dataproc Cluster
```bash
export CLUSTER_NAME="biglake-cluster"
export DATAPROC_VERSION="2.2"

gcloud dataproc clusters create ${CLUSTER_NAME} \
    --enable-component-gateway \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --optional-components=ICEBERG \
    --image-version=${DATAPROC_VERSION}
```

## Environment Variables Summary

Create a `.env` file with all the configuration:

```bash
# Project Configuration
export PROJECT_ID="your-project-id"
export REGION="us-central1"

# Storage Configuration
export BUCKET_NAME="your-biglake-bucket"

# BigQuery Configuration
export DATASET_ID="biglake_dataset"
export CONNECTION_ID="biglake-connection"

# Service Accounts (populated during setup)
export SERVICE_ACCOUNT="connection-service-account@project.iam.gserviceaccount.com"
export BIGLAKE_SERVICE_ACCOUNT="biglake-service-account@gcp-sa-biglake.iam.gserviceaccount.com"

# Catalog Configuration
export CATALOG_PREFIX="projects/${PROJECT_ID}/catalogs/${BUCKET_NAME}"
export CATALOG_NAME="biglake_catalog"
```

## Next Steps

After completing this infrastructure setup:

1. **Test the Python implementation** - Use the provided Python scripts to test connectivity
2. **Create Iceberg tables** - Start creating and managing Iceberg tables
3. **Set up monitoring** - Configure logging and monitoring for your BigLake setup
4. **Implement data governance** - Set up row-level and column-level security as needed

## Troubleshooting

### Common Issues:

#### 1. Permission Errors
**Symptoms**: Access denied errors, 403 Forbidden responses
**Solutions**:
```bash
# Check current authentication
gcloud auth list

# Re-authenticate if needed
gcloud auth application-default login

# Verify project access
gcloud projects describe ${PROJECT_ID}

# Check IAM roles for your account
gcloud projects get-iam-policy ${PROJECT_ID} --flatten="bindings[].members" --filter="bindings.members:user:$(gcloud config get-value account)"
```

#### 2. API Not Enabled
**Symptoms**: API not enabled errors, service unavailable
**Solutions**:
```bash
# Check enabled APIs
gcloud services list --enabled --filter="name:(bigquery OR biglake OR storage OR bigqueryconnection)"

# Enable missing APIs
gcloud services enable bigquery.googleapis.com biglake.googleapis.com storage.googleapis.com bigqueryconnection.googleapis.com

# Verify API enablement
gcloud services list --enabled | grep -E "(bigquery|biglake|storage)"
```

#### 3. Region Mismatch
**Symptoms**: Cross-region errors, performance issues
**Solutions**:
```bash
# Check bucket location
gcloud storage buckets describe gs://${BUCKET_NAME} --format="value(location)"

# Check BigQuery dataset location
bq show --format=prettyjson ${PROJECT_ID}:${DATASET_ID} | grep location

# Ensure all resources are in the same region
```

#### 4. Bucket Access Issues
**Symptoms**: Storage access denied, bucket not found
**Solutions**:
```bash
# Verify bucket exists and is accessible
gcloud storage buckets describe gs://${BUCKET_NAME}

# Check bucket IAM policy
gcloud storage buckets get-iam-policy gs://${BUCKET_NAME}

# Test bucket access
gcloud storage ls gs://${BUCKET_NAME}

# Verify uniform bucket-level access
gcloud storage buckets describe gs://${BUCKET_NAME} --format="value(iamConfiguration.uniformBucketLevelAccess.enabled)"
```

#### 5. Connection Issues
**Symptoms**: Connection creation fails, service account not found
**Solutions**:
```bash
# List existing connections
bq ls --connection --location=${REGION}

# Show connection details
bq show --connection ${PROJECT_ID}.${REGION}.${CONNECTION_ID}

# Recreate connection if needed
bq rm --connection ${PROJECT_ID}.${REGION}.${CONNECTION_ID}
bq mk --connection --location=${REGION} --connection_type=CLOUD_RESOURCE ${CONNECTION_ID}
```

#### 6. Credential Vending Issues
**Symptoms**: Vended credentials not working, access delegation errors
**Solutions**:
```bash
# Check catalog configuration
curl -H "x-goog-user-project: ${PROJECT_ID}" \
     -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
     "https://biglake.googleapis.com/iceberg/v1/restcatalog/v1/config?warehouse=gs://${BUCKET_NAME}"

# Verify BigLake service account permissions
gcloud storage buckets get-iam-policy gs://${BUCKET_NAME} | grep biglake
```

### Diagnostic Commands:

```bash
# Complete environment check
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Bucket: ${BUCKET_NAME}"
echo "Dataset: ${DATASET_ID}"
echo "Connection: ${CONNECTION_ID}"

# Check API status
gcloud services list --enabled --filter="name:(bigquery OR biglake OR storage OR bigqueryconnection)"

# Verify IAM roles
gcloud projects get-iam-policy ${PROJECT_ID}

# Test authentication
gcloud auth application-default print-access-token

# Check bucket configuration
gcloud storage buckets describe gs://${BUCKET_NAME}

# Verify BigQuery dataset
bq show ${PROJECT_ID}:${DATASET_ID}

# Test connection
bq show --connection ${PROJECT_ID}.${REGION}.${CONNECTION_ID}
```

### Log Analysis:

```bash
# Check Cloud Logging for BigLake errors
gcloud logging read "resource.type=biglake_metastore" --limit=50

# Check BigQuery job logs
gcloud logging read "resource.type=bigquery_resource" --limit=50

# Check storage access logs
gcloud logging read "resource.type=gcs_bucket resource.labels.bucket_name=${BUCKET_NAME}" --limit=50
```
