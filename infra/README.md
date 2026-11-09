# AWS Production Deployment

## Prerequisites

- AWS CLI configured (`aws configure`)
- Terraform ≥ 1.7
- Docker + AWS ECR login

## Deploy Steps

### 1. Create Terraform state bucket (once)

```bash
B=geoplatform-tf-state-<ACCOUNT_ID>   # match backend.tf
aws s3 mb s3://$B --region us-east-1
# State holds every secret in plaintext: keep history and refuse plain HTTP.
aws s3api put-bucket-versioning --bucket $B --versioning-configuration Status=Enabled
aws s3api put-public-access-block --bucket $B --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-policy --bucket $B --policy "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"DenyInsecureTransport\",\"Effect\":\"Deny\",\"Principal\":\"*\",\"Action\":\"s3:*\",\"Resource\":[\"arn:aws:s3:::$B\",\"arn:aws:s3:::$B/*\"],\"Condition\":{\"Bool\":{\"aws:SecureTransport\":\"false\"}}}]}"
```

### 2. Create `prod.tfvars`

```hcl
postgres_password   = "..."
airflow_fernet_key  = "..."
airflow_secret_key  = "..."
jwt_secret          = "..."
google_client_id    = "..."
openweather_api_key = "..."
origin_verify_secret = "..."   # python3 -c 'import secrets;print(secrets.token_urlsafe(32))'
budget_alert_email   = "you@example.com"
monthly_budget_usd   = 50
```

`prod.tfvars` is gitignored. Never commit it.

### 3. Apply Terraform

```bash
cd infra/terraform
terraform init
terraform plan -var-file=prod.tfvars
# First time only: create CloudFront (it sends X-Origin-Verify) before the ALB starts requiring it.
terraform apply -var-file=prod.tfvars -target=aws_cloudfront_distribution.spa
terraform apply -var-file=prod.tfvars
```

The ALB answers 403 to anything without CloudFront's `X-Origin-Verify` header, so the WAF cannot be bypassed.
The API database connection uses `sslmode=verify-full` with the RDS CA bundle baked into the API image.

### 4. Push images to ECR

```bash
ECR_API=$(terraform output -raw ecr_api_url)
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin "$ECR_API"
docker build -t geoplatform-api ./api
docker tag geoplatform-api:latest "${ECR_API}:latest"
docker push "${ECR_API}:latest"
```

### 5. Bootstrap the database (one-time)

RDS is private, so the schema is loaded by a one-off Fargate task inside the VPC.
It runs `scripts/init_postgis.sql` from the branch set in `bootstrap_sql_ref`:

```bash
aws ecs run-task --cluster geoplatform-cluster --launch-type FARGATE \
  --task-definition geoplatform-db-bootstrap \
  --network-configuration "awsvpcConfiguration={subnets=[$(terraform output -json public_subnet_ids | jq -r 'join(",")')],securityGroups=[$(terraform output -raw ecs_api_security_group_id)],assignPublicIp=ENABLED}"
aws logs tail /ecs/geoplatform-api --log-stream-name-prefix bootstrap --follow
```

### 5b. Load data (one-time, then automatic)

`api/ingest.py` pulls OSM (Overpass), Sentinel-2 (Earth Search STAC) and OpenWeather into RDS.
EventBridge Scheduler runs it automatically: weather every hour, and everything once a day.
To fill the DB right away, start the daily task once:

```bash
aws ecs run-task --cluster geoplatform-cluster --launch-type FARGATE \
  --task-definition geoplatform-ingest-daily \
  --network-configuration "awsvpcConfiguration={subnets=[$(terraform output -json public_subnet_ids | jq -r 'join(",")')],securityGroups=[$(terraform output -raw ecs_api_security_group_id)],assignPublicIp=ENABLED}"
aws logs tail /ecs/geoplatform-api --log-stream-name-prefix ingest --follow
```

The `catalog` step publishes only the products that AWS has a live source for. These are satellite imagery, OSM vectors and the live weather stream.

### Tear down

`deletion_protection` is on for RDS. To destroy everything, set it to `false`, then run `terraform apply` and `terraform destroy`.
A final snapshot named `geoplatform-final` is kept. Delete it in the RDS console if you don't need it.

### 6. Deploy SPA

```bash
cd frontend && VITE_GOOGLE_CLIENT_ID=<client id> npm run build
aws s3 sync dist/ s3://geoplatform-spa/ --delete --exclude "*.mjs"
# MapLibre's module worker is refused unless served as JavaScript
aws s3 cp dist/assets/ s3://geoplatform-spa/assets/ --recursive --exclude "*" --include "*.mjs" --content-type text/javascript
aws cloudfront create-invalidation --distribution-id <id> --paths "/*"
```

## Outputs

| Output | Description |
|---|---|
| `api_url` | Public API base URL (CloudFront `/api`) |
| `spa_url` | CloudFront URL — this is the public app URL |
| `rds_endpoint` | RDS host:port |
| `ecr_api_url` | Push FastAPI image here |
| `ecr_airflow_url` | Push Airflow image here |

## CI/CD

- `.github/workflows/ci.yml` runs on every PR and push to `main`/`development`: API, frontend, streaming, dbt, and Terraform checks.
- `.github/workflows/deploy.yml` runs only after CI passes on a push to `main`. It authenticates through GitHub OIDC, so the repo holds no AWS keys.

One-time setup after `terraform apply`:

1. In GitHub, go to Settings → Environments and create `production`. Add required reviewers to gate releases.
2. Add these environment **variables**, using the Terraform outputs:

| Variable | Source |
|---|---|
| `AWS_REGION` | e.g. `us-east-1` |
| `AWS_DEPLOY_ROLE_ARN` | `terraform output -raw github_deploy_role_arn` |
| `ECR_API_URL` | `terraform output -raw ecr_api_url` |
| `ECS_CLUSTER` | `terraform output -raw ecs_cluster_name` |
| `ECS_API_SERVICE` | `terraform output -raw ecs_api_service_name` |
| `SPA_BUCKET` | `terraform output -raw spa_bucket` |
| `CLOUDFRONT_DISTRIBUTION_ID` | `terraform output -raw cloudfront_distribution_id` |
| `GOOGLE_CLIENT_ID` | Google OAuth client ID |
