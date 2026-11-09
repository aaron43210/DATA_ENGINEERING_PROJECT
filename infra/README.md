# AWS Production Deployment

## Prerequisites

- AWS CLI configured (`aws configure`)
- Terraform ≥ 1.7
- `psql` available locally (for PostGIS extension bootstrap)
- Docker + AWS ECR login

## Deploy Steps

### 1. Create Terraform state bucket (once)

```bash
aws s3 mb s3://geoplatform-tf-state --region us-east-1
```

### 2. Create `prod.tfvars`

```hcl
postgres_password   = "..."
airflow_fernet_key  = "..."
airflow_secret_key  = "..."
jwt_secret          = "..."
api_key             = "..."
google_client_id    = "..."
openweather_api_key = "..."
```

### 3. Apply Terraform

```bash
cd infra/terraform
terraform init
terraform plan -var-file=prod.tfvars
terraform apply -var-file=prod.tfvars
```

### 4. Push images to ECR

```bash
ECR_API=$(terraform output -raw ecr_api_url)
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin "$ECR_API"
docker build -t geoplatform-api ./api
docker tag geoplatform-api:latest "$ECR_API:latest"
docker push "$ECR_API:latest"
```

### 5. Bootstrap PostGIS (one-time, handled by null_resource)

Applied automatically by `terraform apply` via the `null_resource.enable_postgis` provisioner.
To run manually:

```bash
psql "$(terraform output -raw rds_endpoint)" -U geoplatform -d geoplatform \
  -c "CREATE EXTENSION IF NOT EXISTS postgis;" \
  -c "CREATE EXTENSION IF NOT EXISTS postgis_topology;"
psql "$(terraform output -raw rds_endpoint)" -U geoplatform -d geoplatform \
  < ../../scripts/init_postgis.sql
```

### 6. Deploy SPA

```bash
cd frontend && npm run build
aws s3 sync dist/ s3://geoplatform-spa/ --delete
aws cloudfront create-invalidation --distribution-id <id> --paths "/*"
```

## Outputs

| Output | Description |
|---|---|
| `api_url` | ALB URL for the FastAPI (also proxied via CloudFront `/api/*`) |
| `spa_url` | CloudFront URL — this is the public app URL |
| `rds_endpoint` | RDS host:port |
| `ecr_api_url` | Push FastAPI image here |
| `ecr_airflow_url` | Push Airflow image here |
