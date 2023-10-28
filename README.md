# Metavoice Audio Pipeline
This project processes audio files, performs transcription, tokenization, and generates summary statistics.

## Prerequisites
- Python 3.7 or higher
- AWS Account with appropriate permissions
- Access to an Apache Airflow environment (optional)

## Setup
1. Install dependencies:

pip install -r requirements.txt

2. Configure AWS credentials [Profile r2]:

aws configure

3. Update `config.json` with your specific details.

## Lambda Function

1. Package required files using provided audiopipeline.py, lambda_function.py and config.json.

2. Deploy on AWS Lambda with necessary permissions.

## Apache Airflow

1. Copy `metavoice_dag.py` to your Airflow DAGs directory.

2. Start the Airflow scheduler:

airflow scheduler

3. Access Airflow UI at `http://localhost:8080`.

## Monitoring and Troubleshooting

1. Check logs at `log/metavoice_audio_pipeline.log` for errors or warnings.

## Infrastructure as Code (IaC)

The project is designed to be provisioned using Terraform.

1. Navigate to `infra` directory.

2. Update `terraform.tfvars` with your specific details.

3. Initialize Terraform:

terraform init

4. Apply the configuration:

terraform apply

## Clean Up (Terraform)

terraform destroy

## Provisioning Infrastructure via Terraform

### Folder Structure

├── infra
│   ├── main.tf
│   ├── variables.tf
│   ├── terraform.tfvars
│   └── outputs.tf

### main.tf

provider "aws" {
  region = var.region
}

resource "aws_s3_bucket" "data_bucket" {
  bucket = var.bucket_name
  acl    = "private"
}

output "bucket_name" {
  value = aws_s3_bucket.data_bucket.id
}

### variables.tf

variable "region" {
  description = "AWS region"
  type        = string
}

variable "bucket_name" {
  description = "Name of the S3 bucket"
  type        = string
}

### terraform.tfvars

region      = "us-east-1"
bucket_name = "your-data-bucket-name"

### outputs.tf

output "bucket_name" {
  value = aws_s3_bucket.data_bucket.id
}

### Provisioning

1. Navigate to `infra` directory.

2. Update `terraform.tfvars` with your specific details.

3. Initialize Terraform:

terraform init

4. Apply the configuration:

terraform apply

### Clean Up (Terraform)

terraform destroy

By following these steps, you can easily set up and scale your Metavoice Audio Pipeline with optimized speed and scalability.
