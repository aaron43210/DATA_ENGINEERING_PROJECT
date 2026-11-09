# Keyless CI/CD: GitHub Actions assumes this role via OIDC, no long-lived AWS keys.
variable "github_repo" {
  description = "GitHub repo allowed to deploy, as owner/name"
  type        = string
  default     = "aaron43210/DATA_ENGINEERING_PROJECT"
}

variable "github_oidc_sub_prefix" {
  description = "Subject prefix GitHub sends when the repo uses immutable OIDC subjects (gh api repos/OWNER/REPO/actions/oidc/customization/sub)"
  type        = string
  default     = "repo:aaron43210@225593939/DATA_ENGINEERING_PROJECT@1398503048"
}

resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

resource "aws_iam_role" "github_deploy" {
  name = "${var.app_name}-github-deploy"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.github.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          # Both subject formats: classic owner/name and GitHub's immutable owner@id/name@id.
          "token.actions.githubusercontent.com:sub" = [
            "repo:${var.github_repo}:environment:production",
            "${var.github_oidc_sub_prefix}:environment:production",
          ]
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "github_deploy" {
  name = "${var.app_name}-github-deploy"
  role = aws_iam_role.github_deploy.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload", "ecr:PutImage", "ecr:BatchGetImage",
          "ecr:DescribeImages", "ecr:GetDownloadUrlForLayer",
        ]
        Resource = [aws_ecr_repository.api.arn, aws_ecr_repository.airflow.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["ecs:UpdateService", "ecs:DescribeServices"]
        Resource = aws_ecs_service.api.id
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.spa.arn
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:DeleteObject"]
        Resource = "${aws_s3_bucket.spa.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["cloudfront:CreateInvalidation"]
        Resource = aws_cloudfront_distribution.spa.arn
      },
    ]
  })
}
