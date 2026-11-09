variable "aws_region" {
  description = "AWS region for all resources (except WAF, which is always us-east-1)"
  type        = string
  default     = "us-east-1"
}

variable "app_name" {
  description = "Short name prefix for all resources"
  type        = string
  default     = "geoplatform"
}

variable "acm_cert_arn" {
  description = "ACM certificate ARN for the ALB HTTPS listener (must be in var.aws_region)"
  type        = string
  default     = ""
}

# ── Sensitive vars ────────────────────────────────────────────────────────────

variable "postgres_password" {
  description = "RDS master password"
  type        = string
  sensitive   = true
}

variable "airflow_fernet_key" {
  description = "Airflow Fernet encryption key"
  type        = string
  sensitive   = true
}

variable "airflow_secret_key" {
  description = "Airflow webserver secret key"
  type        = string
  sensitive   = true
}

variable "jwt_secret" {
  description = "FastAPI JWT signing secret"
  type        = string
  sensitive   = true
}

variable "origin_verify_secret" {
  description = "Shared secret CloudFront sends to the ALB (X-Origin-Verify); requests without it get 403"
  type        = string
  sensitive   = true
}

variable "google_client_id" {
  description = "Google OAuth client ID"
  type        = string
  default     = ""
}

variable "openweather_api_key" {
  description = "OpenWeatherMap API key"
  type        = string
  sensitive   = true
  default     = ""
}

variable "gf_admin_password" {
  description = "Grafana admin password"
  type        = string
  sensitive   = true
  default     = ""
}

variable "budget_alert_email" {
  description = "Email that receives AWS budget alerts"
  type        = string
}

variable "monthly_budget_usd" {
  description = "Monthly cost budget; alerts at 80% actual and 100% forecast"
  type        = number
  default     = 50
}

variable "bootstrap_sql_ref" {
  description = "Git branch or commit whose scripts/init_postgis.sql the db-bootstrap task runs"
  type        = string
  default     = "main"
}

variable "pipeline_ssh_cidr" {
  description = "Your public IP as a /32, the only address allowed to SSH to the pipeline host"
  type        = string
}

variable "pipeline_public_key" {
  description = "SSH public key for the pipeline host, e.g. the contents of ~/.ssh/id_ed25519.pub"
  type        = string
}

variable "pipeline_instance_type" {
  description = "Pipeline host size (8 GB). m7i-flex.large is allowed on the AWS Free plan; t3.large is ~$9/month cheaper once the account is upgraded"
  type        = string
  default     = "m7i-flex.large"
}

variable "pipeline_repo_url" {
  description = "Git repository the pipeline host clones on first boot"
  type        = string
  default     = "https://github.com/aaron43210/DATA_ENGINEERING_PROJECT.git"
}

variable "pipeline_start_cron" {
  description = "When the pipeline host starts, India time (Asia/Kolkata)"
  type        = string
  default     = "cron(0 7 * * ? *)"
}

variable "pipeline_stop_cron" {
  description = "When the pipeline host stops, India time (Asia/Kolkata)"
  type        = string
  default     = "cron(0 1 * * ? *)"
}
