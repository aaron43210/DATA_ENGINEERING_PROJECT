output "api_url" {
  description = "Public API base URL (CloudFront; the ALB only accepts CloudFront traffic)"
  value       = "https://${aws_cloudfront_distribution.spa.domain_name}/api"
}

output "public_subnet_ids" {
  value = aws_subnet.public[*].id
}

output "ecs_api_security_group_id" {
  value = aws_security_group.ecs_api.id
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

output "github_deploy_role_arn" {
  description = "Set as AWS_DEPLOY_ROLE_ARN in the GitHub 'production' environment"
  value       = aws_iam_role.github_deploy.arn
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.main.name
}

output "ecs_api_service_name" {
  value = aws_ecs_service.api.name
}

output "spa_bucket" {
  value = aws_s3_bucket.spa.bucket
}

output "cloudfront_distribution_id" {
  value = aws_cloudfront_distribution.spa.id
}
