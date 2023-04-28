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

### Main
[main.tf](https://github.com/TylerJSimpson/p3dd_phase_3_due_diligence/blob/main/project/terraform_IaC/main.tf) is the main configuration file used by Terraform to define and manage infrastructure resources.  

### Variables
[variables.tf](https://github.com/TylerJSimpson/p3dd_phase_3_due_diligence/blob/main/project/terraform_IaC/variables.tf) defines the variables used in the main.tf file.  
[terraform.tfvars](https://github.com/TylerJSimpson/p3dd_phase_3_due_diligence/blob/main/project/terraform_IaC/terraform.tfvars) assigns values to the variables in the variables.tf file.  

### Sub Infrastructure
It is possible to separate out infrastructure from the main.tf and call it from the main.tf.  
Here this is done for the separate BigQuery tables as the schemas can be quite large.  
[bronze_nct.tf](https://github.com/TylerJSimpson/p3dd_phase_3_due_diligence/blob/main/project/terraform_IaC/bigquery/bronze_nct.tf)  
[bronze_figi.tf](https://github.com/TylerJSimpson/p3dd_phase_3_due_diligence/blob/main/project/terraform_IaC/bigquery/bronze_figi.tf)  
[bronze_jobs.tf](https://github.com/TylerJSimpson/p3dd_phase_3_due_diligence/blob/main/project/terraform_IaC/bigquery/bronze_jobs.tf)  