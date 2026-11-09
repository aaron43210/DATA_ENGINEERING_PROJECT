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
    custom_header {
      name  = "X-Origin-Verify"
      value = var.origin_verify_secret
    }
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
    min_ttl                    = 0
    default_ttl                = 86400
    max_ttl                    = 31536000
    response_headers_policy_id = aws_cloudfront_response_headers_policy.spa.id
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
      cookies {
        forward           = "whitelist"
        whitelisted_names = ["gp_session"]
      }
    }
    min_ttl                    = 0
    default_ttl                = 0
    max_ttl                    = 0
    response_headers_policy_id = aws_cloudfront_response_headers_policy.api.id
    function_association {
      event_type   = "viewer-request"
      function_arn = aws_cloudfront_function.strip_api_prefix.arn
    }
  }

  web_acl_id          = aws_wafv2_web_acl.api_rate_limit.arn
  enabled             = true
  is_ipv6_enabled     = true
  aliases             = var.acm_cert_arn != "" ? ["geoplatformkerala.tech"] : []
  default_root_object = "index.html"

  restrictions {
    geo_restriction { restriction_type = "none" }
  }

  viewer_certificate {
    cloudfront_default_certificate = var.acm_cert_arn == ""
    acm_certificate_arn            = var.acm_cert_arn != "" ? var.acm_cert_arn : null
    ssl_support_method             = var.acm_cert_arn != "" ? "sni-only" : null
    minimum_protocol_version       = var.acm_cert_arn != "" ? "TLSv1.2_2021" : null
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
      // Prometheus metrics are for internal scraping only.
      if (req.uri === "/metrics" || req.uri.indexOf("/metrics/") === 0) {
        return { statusCode: 404, statusDescription: "Not Found" };
      }
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

# Browser hardening for the SPA. CSP lists exactly what the app loads: Google sign-in,
# OSM tiles, Earth Search STAC metadata and Sentinel-2 thumbnails on S3.
resource "aws_cloudfront_response_headers_policy" "spa" {
  name = "${var.app_name}-spa-security"
  security_headers_config {
    strict_transport_security {
      access_control_max_age_sec = 31536000
      include_subdomains         = true
      override                   = true
    }
    content_type_options { override = true }
    frame_options {
      frame_option = "DENY"
      override     = true
    }
    referrer_policy {
      referrer_policy = "strict-origin-when-cross-origin"
      override        = true
    }
    content_security_policy {
      override = true
      content_security_policy = join("; ", [
        "default-src 'self'",
        "script-src 'self' https://accounts.google.com/gsi/client",
        "style-src 'self' 'unsafe-inline' https://accounts.google.com/gsi/style",
        "img-src 'self' data: blob: https://tile.openstreetmap.org https://*.amazonaws.com https://*.googleusercontent.com",
        "connect-src 'self' https://accounts.google.com/gsi/ https://tile.openstreetmap.org https://earth-search.aws.element84.com",
        "frame-src https://accounts.google.com/gsi/",
        "worker-src 'self' blob:",
        "object-src 'none'",
        "base-uri 'self'",
        "form-action 'self'",
        "frame-ancestors 'none'",
      ])
    }
  }
}

# API responses: same transport headers, no CSP (Swagger UI at /api/docs loads from a CDN).
resource "aws_cloudfront_response_headers_policy" "api" {
  name = "${var.app_name}-api-security"
  security_headers_config {
    strict_transport_security {
      access_control_max_age_sec = 31536000
      include_subdomains         = true
      override                   = true
    }
    content_type_options { override = true }
    frame_options {
      frame_option = "DENY"
      override     = true
    }
    referrer_policy {
      referrer_policy = "no-referrer"
      override        = true
    }
  }
}
