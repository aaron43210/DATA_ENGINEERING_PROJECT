terraform {
  required_version = ">= 1.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.40"
    }
  }
  backend "s3" {
    # Create this bucket manually before `terraform init`:
    # aws s3 mb s3://geoplatform-tf-state --region us-east-1
    bucket  = "geoplatform-tf-state"
    key     = "platform/terraform.tfstate"
    region  = "us-east-1"
    encrypt = true
  }
}

provider "aws" {
  region = var.aws_region
}

# WAF for CloudFront must be provisioned in us-east-1 regardless of main region
provider "aws" {
  alias  = "us_east_1"
  region = "us-east-1"
}
