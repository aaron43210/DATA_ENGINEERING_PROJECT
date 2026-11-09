resource "aws_cloudfront_distribution" "spa" {
  # S3 origin for the SPA static files
  origin {
    domain_name              = aws_s3_bucket.spa.bucket_regional_domain_name
    origin_id                = "spa-s3"
    origin_access_control_id = aws_cloudfront_origin_access_control.spa.id
  }

  # ALB origin for the API
  origin {
    domain_name = aws_lb.api.dns_name
    origin_id   = "api-alb"
    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "http-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }
  }

  # Default: serve SPA from S3
  default_cache_behavior {
    target_origin_id       = "spa-s3"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    forwarded_values {
      query_string = false
      cookies { forward = "none" }
    }
    min_ttl     = 0
    default_ttl = 86400
    max_ttl     = 31536000
    function_association {
      event_type   = "viewer-request"
      function_arn = aws_cloudfront_function.spa_rewrite.arn
    }
  }

  # /api/* routes to the ALB — never cached
  ordered_cache_behavior {
    path_pattern           = "/api/*"
    target_origin_id       = "api-alb"
    viewer_protocol_policy = "https-only"
    allowed_methods        = ["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"]
    cached_methods         = ["GET", "HEAD"]
    forwarded_values {
      query_string = true
      headers      = ["Authorization", "X-API-Key", "Content-Type", "Host"]
      cookies { forward = "none" }
    }
    min_ttl     = 0
    default_ttl = 0
    max_ttl     = 0
    function_association {
      event_type   = "viewer-request"
      function_arn = aws_cloudfront_function.strip_api_prefix.arn
    }
  }

  web_acl_id          = aws_wafv2_web_acl.api_rate_limit.arn
  enabled             = true
  is_ipv6_enabled     = true
  default_root_object = "index.html"

  restrictions {
    geo_restriction { restriction_type = "none" }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }

  tags = { Name = "${var.app_name}-cloudfront" }
}

# FastAPI routes have no /api prefix; nginx strips it locally, this does it on AWS.
resource "aws_cloudfront_function" "strip_api_prefix" {
  name    = "${var.app_name}-strip-api-prefix"
  runtime = "cloudfront-js-2.0"
  publish = true
  code    = <<-JS
    function handler(event) {
      var req = event.request;
      req.uri = req.uri.replace(/^\/api/, "") || "/";
      return req;
    }
  JS
}

# Client-side routes (no file extension) get index.html. Done per-behavior instead of
# custom_error_response so real API 401/403/404 responses reach the browser intact.
resource "aws_cloudfront_function" "spa_rewrite" {
  name    = "${var.app_name}-spa-rewrite"
  runtime = "cloudfront-js-2.0"
  publish = true
  code    = <<-JS
    function handler(event) {
      var req = event.request;
      if (req.uri.indexOf(".") === -1) {
        req.uri = "/index.html";
      }
      return req;
    }
  JS
}
