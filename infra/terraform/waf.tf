resource "aws_wafv2_web_acl" "api_rate_limit" {
  provider = aws.us_east_1
  name     = "${var.app_name}-rate-limit"
  scope    = "CLOUDFRONT"

  default_action {
    allow {}
  }

  rule {
    name     = "rate-limit-per-ip"
    priority = 1
    action {
      block {}
    }
    statement {
      rate_based_statement {
        limit              = 1000
        aggregate_key_type = "IP"
      }
    }
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${var.app_name}RateLimitPerIP"
      sampled_requests_enabled   = true
    }
  }

  visibility_config {
    cloudwatch_metrics_enabled = true
    metric_name                = "${var.app_name}WAF"
    sampled_requests_enabled   = true
  }

  tags = { Name = "${var.app_name}-waf" }
}
