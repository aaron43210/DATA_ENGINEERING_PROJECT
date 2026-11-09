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
