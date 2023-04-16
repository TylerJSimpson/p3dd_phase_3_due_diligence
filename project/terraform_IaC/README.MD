# Terraform GCP Cloud Infrastructure Details
Terraform IaC for the GCP infrastructure necessary for this project. This includes a project, GCS, and BigQuery.    

## Setup

Ensure you are in the python environment:
```bash
conda activate zoom
```

Authorize gcloud:
```bash
gcloud auth application-default login
```

Install Terraform at the official [website](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli).  

Initialize Terraform:
```bash
terraform init
```

Terraform file [main.tf](https://github.com/TylerJSimpson/data_engineering_zoomcamp/blob/main/week_1/terraform/main.tf) created. See below in the **infrastructure** section for more details.  

Cleanup the Terraform file automatically:  
```bash
terraform fmt
```

Check current state and compare with desired state:  
```bash
terraform plan
```

Apply:  
```bash
terraform apply
```

If you need to destroy the Terraform build:  
```bash
terraform destroy
```

## Infrastructure

### main.tf
[main.tf](https://github.com/TylerJSimpson/data_engineering_zoomcamp/blob/main/week_1/terraform/main.tf) includes the Terraform code for a GCP project, cloud storage (GCS), and cloud datawarehouse (BigQuery).


