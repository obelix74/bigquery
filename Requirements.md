Writing data to BigLake Metastore in Google Cloud Platform primarily involves creating and managing external tables, often leveraging open table formats like Apache Iceberg, and using query engines like Spark or BigQuery to interact with them.

Here's a general workflow for writing data to BigLake Metastore:
        Set up Cloud Storage:Create a Cloud Storage bucket to store your data files (e.g., Parquet, ORC, Avro) and the metadata files for your chosen open table format (e.g., Iceberg metadata).
        Create a BigQuery Connection:Create a BigQuery connection resource in the Google Cloud Console to establish a link between BigQuery and your Cloud Storage bucket. This connection allows BigQuery to access the data.
        Create BigLake External Tables:Using BigQuery: Create an external table in BigQuery that points to your data in Cloud Storage and specifies the open table format (e.g., Iceberg). You can define the schema or enable schema auto-detection.

Create a sample data JSON file and write it to the biglake metastore in the google cloud platform in the iceberg format.

Give me detailed instructions on how to do this step by step.

Write code in a language like Python that works well for GCP
