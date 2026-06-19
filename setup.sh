#!/bin/bash

set -e

echo "======================================"
echo " Terraform Configuration Generator"
echo "======================================"
echo ""

read -p "Enter GCP Project ID: " PROJECT_ID

echo ""
echo "Select region:"
echo "1) europe-west1 (Belgium)"
echo "2) europe-west4 (Netherlands)"
echo "3) europe-west6 (Zurich)"
echo "4) europe-central2 (Warsaw)"
echo "5) us-central1 (Iowa)"
echo "6) us-east1 (South Carolina)"
echo "7) us-west1 (Oregon)"
echo "8) asia-southeast1 (Singapore)"
echo "9) Other (manual entry)"

read -p "Choice: " REGION_CHOICE

case $REGION_CHOICE in
    1) REGION="europe-west1" ;;
    2) REGION="europe-west4" ;;
    3) REGION="europe-west6" ;;
    4) REGION="europe-central2" ;;
    5) REGION="us-central1" ;;
    6) REGION="us-east1" ;;
    7) REGION="us-west1" ;;
    8) REGION="asia-southeast1" ;;
    9) read -p "Enter region: " REGION ;;
    *)
        echo "Invalid selection."
        exit 1
        ;;
esac


# Google cloud auth

# Delete previous login data
rm -rf ~/.config/gcloud

# Login to google caccount
gcloud auth application-default login

# Set quota
gcloud auth application-default set-quota-project "$PROJECT_ID"

# Terraform

# Delete old file
rm -f terraform/terraform.tfvars

HASH=$(echo -n "$PROJECT_ID" | md5sum | cut -c1-8)

# Create new
cat > terraform/terraform.tfvars <<EOF
project_id = "$PROJECT_ID"
region     = "$REGION"
hash       = "$HASH"
EOF

# Set permissions
chmod 644 terraform/terraform.tfvars


echo ""
echo "Generated terraform.tfvars:"
echo "-------------------------"
cat terraform/terraform.tfvars




echo "Starting terraform:"
echo "-------------------------"

docker compose run --rm terraform init
docker compose run --rm terraform plan

read -p "Apply terrafom changes? (yes/no): " CONFIRM
CONFIRM=$(echo "$CONFIRM" | tr '[:upper:]' '[:lower:]')

if [[ "$CONFIRM" =~ ^(y|yes)$ ]]; then
    docker compose run --rm terraform apply -auto-approve
fi


# Ensure credentials directory exists
mkdir -p credentials
chmod 755 credentials

# Remove old config
rm -f credentials/config.json

cat > credentials/config.json <<EOF
{
  "gcs": {
    "bucket": "${PROJECT_ID}-weather-data-${HASH}",
    "bigquery_project": "$PROJECT_ID",
    "bigquery_dataset": "weather_data",
    "region": "$REGION"
  }
}
EOF

# Set read permisions
chmod 644 credentials/config.json


# Ensure output directory exists
mkdir -p output
chmod 755 output

# Remove previous report
rm -f output/report.pdf


echo "Running ingestion..."
docker compose run --rm ingest

echo "Waiting for cloud resources..."
sleep 3

echo "Running Spark processing..."
docker compose run --rm spark

echo "Waiting for BigQuery..."
sleep 3

echo "Running dbt models..."
docker compose run --rm dbt

echo "Pipeline completed successfully."












