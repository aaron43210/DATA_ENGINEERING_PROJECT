output "api_url" {
  description = "ALB DNS name (API)"
  value       = "https://${aws_lb.api.dns_name}"
}

output "spa_url" {
  description = "CloudFront URL (SPA + API)"
  value       = "https://${aws_cloudfront_distribution.spa.domain_name}"
}

output "rds_endpoint" {
  description = "RDS endpoint (host:port)"
  value       = aws_db_instance.postgres.endpoint
}

output "ecr_api_url" {
  description = "ECR repository URL for the FastAPI image"
  value       = aws_ecr_repository.api.repository_url
}

output "ecr_airflow_url" {
  description = "ECR repository URL for the Airflow image"
  value       = aws_ecr_repository.airflow.repository_url
}
