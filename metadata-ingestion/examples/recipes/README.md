# Metastore DataHub Metadata Ingestion
Clone this DataHub repo and cd into the root directory of the cloned repository.
# Start DataHub in Docker
Run the following command to download and run all Docker containers locally:
./docker/quickstart.sh

validate that all required  containers is running:
datahub check local-docker

# Checkout branch ...
cd into metadata-ingestion

# Set up Python environment for Metadata Ingestion from source using doc:
https://datahubproject.io/docs/metadata-ingestion/developing/

# Generate fresh kitchensink_json_dump using next command:
cd into kitchensink-etl project
install fresh airflow tools: https://scribdjira.atlassian.net/wiki/spaces/DE/pages/1379565601/airflow-tools+-+User+guide
run: airflow-tools dump --dag kitchensink-etl > kitchensink-graph.json

# Ajust metastore_yml_path and kitchensink_json_dump path in metadata-ingestion/examples/recipes/metastore_to_datahub.yml

# Run import from metastore:
datahub ingest -c ./examples/recipes/metastore_to_datahub.yml

# Import Looker data
# Export Looker creds as system variables
export LOOKERSDK_CLIENT_ID=<YOUR_CLIENT_ID>
export LOOKERSDK_CLIENT_SECRET=<YOUR_CLIENT_SECRET>
export LOOKERSDK_BASE_URL=https://scribd.cloud.looker.com

# To the same venv as above install additional requirement:
pip3 install looker-sdk==0.1.3b20

# Run import
cd scripts
python3 looker_dashboard_ingestion.py