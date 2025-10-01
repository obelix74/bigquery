# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository is focused on implementing solutions for writing data to BigLake Metastore in Google Cloud Platform using the Apache Iceberg format. The main objective is to create Python code that can write JSON data to BigLake Metastore tables.

## Key Requirements

Based on Requirements.md, the project aims to:
- Create sample JSON data files
- Write data to BigLake Metastore in Iceberg format
- Use Python for GCP integration
- Provide step-by-step implementation guidance

## Expected Architecture

The solution will likely involve:
- Google Cloud Storage bucket for data storage
- BigQuery connection resources for Cloud Storage access
- BigLake external tables with Iceberg format
- Python scripts using GCP client libraries (google-cloud-bigquery, google-cloud-storage)

## Development Setup

This is a new project with minimal setup. When developing:
- Python will be the primary language
- GCP client libraries will be required
- Consider using virtual environments for dependency management
- Authentication to GCP will be needed (service accounts or gcloud auth)

## Commands

### Setup and Run
```bash
# Install dependencies
pip install -r requirements.txt

# Run complete BigLake setup workflow
python biglake_orchestrator.py setup

# Check status of resources
python biglake_orchestrator.py status

# Verify existing setup
python biglake_orchestrator.py verify

# Clean up all resources
python biglake_orchestrator.py cleanup
```

### Configuration
- Copy `.env.example` to `.env` and update with your GCP project details
- Or set environment variables directly:
  - `GCP_PROJECT_ID`: Your Google Cloud project ID
  - `GCS_BUCKET_NAME`: Cloud Storage bucket name (optional)
  - `BQ_DATASET_ID`: BigQuery dataset name (optional)

## Architecture

The implementation consists of several key components:

1. **Configuration Management** (`config.py`): Centralizes all GCP settings and validates configuration
2. **GCS Manager** (`gcs_manager.py`): Handles Cloud Storage bucket creation and file uploads
3. **BigQuery Manager** (`bigquery_manager.py`): Manages BigQuery datasets, connections, and external tables
4. **Iceberg Manager** (`iceberg_manager.py`): Creates Apache Iceberg table structure and metadata
5. **Orchestrator** (`biglake_orchestrator.py`): Main workflow coordinator

### Data Flow
JSON Data → Iceberg Format → Cloud Storage → BigLake External Table → BigQuery Queries

## Key Implementation Details

- Uses Apache Iceberg format for open table format compatibility
- Creates BigLake external tables for querying data in BigQuery
- Handles nested JSON structures (flattens for BigQuery compatibility)
- Implements proper Iceberg metadata structure for table versioning
- Provides comprehensive error handling and status reporting

## Authentication Requirements

The code expects GCP authentication via:
- Service account key file
- Application Default Credentials (`gcloud auth application-default login`)
- Compute Engine service account

Required IAM permissions:
- BigQuery Data Editor, Job User, Connection Admin
- Storage Admin