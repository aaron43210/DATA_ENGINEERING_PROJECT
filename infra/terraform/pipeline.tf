# One EC2 host for the batch and streaming pipeline: Airflow + dbt, Redpanda, Flink,
# the Iceberg REST catalog, Prometheus and Grafana, run with docker compose
# (docker-compose.yml + docker-compose.aws.yml). RDS and S3 replace the local
# postgres and MinIO containers. No UI port is open: reach them over an SSH tunnel.

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
}

resource "aws_key_pair" "pipeline" {
  key_name   = "${var.app_name}-pipeline"
  public_key = var.pipeline_public_key
}

resource "aws_security_group" "pipeline" {
  name   = "${var.app_name}-pipeline-sg"
  vpc_id = aws_vpc.main.id
  ingress {
    description = "SSH, also used to tunnel the Airflow/Flink/Redpanda/Grafana UIs"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.pipeline_ssh_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = { Name = "${var.app_name}-pipeline-sg" }
}

resource "aws_iam_role" "pipeline" {
  name = "${var.app_name}-pipeline"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "pipeline_s3" {
  name = "data-buckets"
  role = aws_iam_role.pipeline.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [for b in aws_s3_bucket.data : b.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = [for b in aws_s3_bucket.data : "${b.arn}/*"]
      },
    ]
  })
}

resource "aws_iam_instance_profile" "pipeline" {
  name = "${var.app_name}-pipeline"
  role = aws_iam_role.pipeline.name
}

resource "aws_instance" "pipeline" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.pipeline_instance_type
  subnet_id              = aws_subnet.public[0].id
  vpc_security_group_ids = [aws_security_group.pipeline.id]
  key_name               = aws_key_pair.pipeline.key_name
  iam_instance_profile   = aws_iam_instance_profile.pipeline.name

  # Hop limit 2 lets containers on the docker bridge reach the instance role.
  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  # Standard credits: a busy CPU slows down instead of adding surplus-credit charges.
  credit_specification {
    cpu_credits = "standard"
  }

  root_block_device {
    volume_size = 50
    volume_type = "gp3"
    encrypted   = true
  }

  user_data = <<-EOF
    #!/bin/bash
    set -eux
    curl -fsSL https://get.docker.com | sh
    usermod -aG docker ubuntu
    fallocate -l 4G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
    echo '/swapfile none swap sw 0 0' >> /etc/fstab
    sudo -u ubuntu git clone ${var.pipeline_repo_url} /home/ubuntu/DE_PROJECT
  EOF

  # Re-running user_data on an existing host would not help; changes need a new host.
  lifecycle { ignore_changes = [ami, user_data] }

  tags = { Name = "${var.app_name}-pipeline" }
}

# Private DNS name for the ECS API task, so Prometheus on the pipeline host can
# scrape /metrics directly (CloudFront blocks /metrics on the public path).
resource "aws_service_discovery_private_dns_namespace" "internal" {
  name = "${var.app_name}.internal"
  vpc  = aws_vpc.main.id
}

resource "aws_service_discovery_service" "api" {
  name = "api"
  dns_config {
    namespace_id   = aws_service_discovery_private_dns_namespace.internal.id
    routing_policy = "MULTIVALUE"
    dns_records {
      ttl  = 10
      type = "A"
    }
  }
}

# Fixed address, so the SSH command keeps working after a stop/start.
resource "aws_eip" "pipeline" {
  instance = aws_instance.pipeline.id
  domain   = "vpc"
  tags     = { Name = "${var.app_name}-pipeline" }
}
