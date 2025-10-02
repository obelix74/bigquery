"""BigQuery management for BigLake Metastore integration."""

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from config import Config


class BigQueryManager:
    """Manages BigQuery operations for BigLake integration."""

    def __init__(self, config: Config):
        self.config = config
        self.client = bigquery.Client(project=config.project_id)

    def create_dataset_if_not_exists(self) -> bool:
        """Create BigQuery dataset if it doesn't exist."""
        try:
            dataset = self.client.get_dataset(self.config.dataset_id)
            print(f"Dataset {self.config.dataset_id} already exists")
            return True
        except NotFound:
            try:
                dataset = bigquery.Dataset(f"{self.config.project_id}.{self.config.dataset_id}")
                dataset.location = self.config.region
                dataset = self.client.create_dataset(dataset)
                print(f"Created dataset {self.config.dataset_id}")
                return True
            except Exception as e:
                print(f"Error creating dataset: {e}")
                return False
        except Exception as e:
            if "has not enabled BigQuery" in str(e):
                print("❌ BigQuery API Error: BigQuery API is not enabled")
                print("\nTo fix this, enable the required APIs:")
                print(f"1. Enable BigQuery API:")
                print(f"   gcloud services enable bigquery.googleapis.com --project={self.config.project_id}")
                print(f"2. Enable BigQuery Connection API:")
                print(f"   gcloud services enable bigqueryconnection.googleapis.com --project={self.config.project_id}")
                print(f"3. Enable Cloud Storage API:")
                print(f"   gcloud services enable storage.googleapis.com --project={self.config.project_id}")
                print("\nOr enable them in the Google Cloud Console:")
                print(f"https://console.cloud.google.com/apis/library?project={self.config.project_id}")
            else:
                print(f"Error accessing BigQuery: {e}")
            return False

    def create_connection_if_not_exists(self) -> bool:
        """Create BigQuery connection for Cloud Storage access."""
        from google.cloud import bigquery_connection_v1

        try:
            connection_client = bigquery_connection_v1.ConnectionServiceClient()
            parent = f"projects/{self.config.project_id}/locations/{self.config.region}"

            # Check if connection already exists
            try:
                connection_path = f"{parent}/connections/{self.config.connection_id}"
                connection = connection_client.get_connection(name=connection_path)
                print(f"Connection {self.config.connection_id} already exists")
                return True
            except NotFound:
                pass

            # Create new connection
            connection = bigquery_connection_v1.Connection()
            connection.cloud_resource = bigquery_connection_v1.CloudResourceProperties()

            request = bigquery_connection_v1.CreateConnectionRequest(
                parent=parent,
                connection_id=self.config.connection_id,
                connection=connection
            )

            operation = connection_client.create_connection(request=request)
            print(f"Created connection {self.config.connection_id}")
            return True

        except Exception as e:
            print(f"Error managing connection: {e}")
            print("Note: You may need to create the connection manually in the Google Cloud Console")
            print(f"Go to BigQuery > External connections > Create connection")
            print(f"Connection ID: {self.config.connection_id}")
            print(f"Connection type: Cloud resource")
            return False

    def create_biglake_table(self) -> bool:
        """Create BigLake external table with Iceberg format."""
        try:
            # Table reference
            table_ref = self.client.dataset(self.config.dataset_id).table(self.config.table_name)

            # Check if table already exists
            try:
                table = self.client.get_table(table_ref)
                print(f"Table {self.config.table_name} already exists")
                return True
            except NotFound:
                pass

            # Create external table configuration
            external_config = bigquery.ExternalConfig("ICEBERG")
            external_config.source_uris = [f"{self.config.data_location}/{self.config.table_name}/metadata/v1.metadata.json"]
            external_config.connection_id = f"projects/{self.config.project_id}/locations/{self.config.region}/connections/{self.config.connection_id}"

            # Create table (no schema needed for Iceberg - auto-detected from metadata)
            table = bigquery.Table(table_ref)
            table.external_data_configuration = external_config

            table = self.client.create_table(table)
            print(f"Created BigLake table {self.config.table_name}")
            return True

        except Exception as e:
            print(f"Error creating BigLake table: {e}")
            return False

    def query_table(self, limit: int = 10) -> bool:
        """Query the BigLake table to verify data."""
        try:
            query = f"""
            SELECT *
            FROM `{self.config.project_id}.{self.config.dataset_id}.{self.config.table_name}`
            LIMIT {limit}
            """

            query_job = self.client.query(query)
            results = query_job.result()

            print(f"Query results from {self.config.table_name}:")
            for row in results:
                print(row)

            return True

        except Exception as e:
            print(f"Error querying table: {e}")
            return False

    def delete_table(self) -> bool:
        """Delete the BigLake table."""
        try:
            table_ref = self.client.dataset(self.config.dataset_id).table(self.config.table_name)
            self.client.delete_table(table_ref, not_found_ok=True)
            print(f"Deleted table {self.config.table_name}")
            return True
        except Exception as e:
            print(f"Error deleting table: {e}")
            return False

    def get_table_info(self) -> bool:
        """Get information about the BigLake table."""
        try:
            table_ref = self.client.dataset(self.config.dataset_id).table(self.config.table_name)
            table = self.client.get_table(table_ref)

            print(f"Table Information:")
            print(f"  Table ID: {table.table_id}")
            print(f"  Schema: {[field.name + ':' + field.field_type for field in table.schema]}")
            print(f"  External Data Configuration: {table.external_data_configuration}")
            print(f"  Created: {table.created}")
            print(f"  Modified: {table.modified}")

            return True

        except NotFound:
            print(f"Table {self.config.table_name} not found")
            return False
        except Exception as e:
            print(f"Error getting table info: {e}")
            return False