locals {
  data_buckets = ["bronze", "silver", "gold", "warehouse"]
}

resource "aws_s3_bucket" "data" {
  for_each = toset(local.data_buckets)
  bucket   = "${var.app_name}-${each.key}"
  tags     = { Name = "${var.app_name}-${each.key}" }
}

resource "aws_s3_bucket_versioning" "data" {
  for_each = toset(["bronze", "warehouse"])
  bucket   = aws_s3_bucket.data[each.key].id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.data["bronze"].id
  rule {
    id     = "bronze-retention"
    status = "Enabled"
    expiration { days = 365 }
    noncurrent_version_expiration { noncurrent_days = 30 }
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  for_each                = toset(local.data_buckets)
  bucket                  = aws_s3_bucket.data[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# SPA bucket — served exclusively via CloudFront OAC
resource "aws_s3_bucket" "spa" {
  bucket = "${var.app_name}-spa"
  tags   = { Name = "${var.app_name}-spa" }
}

resource "aws_s3_bucket_public_access_block" "spa" {
  bucket                  = aws_s3_bucket.spa.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_cloudfront_origin_access_control" "spa" {
  name                              = "${var.app_name}-spa-oac"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_s3_bucket_policy" "spa" {
  bucket = aws_s3_bucket.spa.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowCloudFrontOAC"
      Effect    = "Allow"
      Principal = { Service = "cloudfront.amazonaws.com" }
      Action    = "s3:GetObject"
      Resource  = "${aws_s3_bucket.spa.arn}/*"
      Condition = {
        StringEquals = {
          "AWS:SourceArn" = aws_cloudfront_distribution.spa.arn
        }
      }
    }]
  })
}
