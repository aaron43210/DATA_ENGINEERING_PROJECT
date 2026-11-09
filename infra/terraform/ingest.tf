# Scheduled data loads (api/ingest.py). Airflow/Flink run only locally, so AWS
# refreshes the showcased products with short Fargate tasks instead.

locals {
  ingest_jobs = {
    # Weather and air quality are hourly; OSM and Sentinel-2 change slowly, so once a day.
    hourly = { schedule = "rate(1 hour)", command = ["python", "ingest.py", "weather", "air_quality"] }
    daily  = { schedule = "rate(1 day)", command = ["python", "ingest.py", "catalog", "satellite", "osm", "weather", "air_quality"] }
  }
}

resource "aws_ecs_task_definition" "ingest" {
  for_each                 = local.ingest_jobs
  family                   = "${var.app_name}-ingest-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 1024 # Overpass road geometry for Kerala needs more than 512 MB
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn

  container_definitions = jsonencode([{
    name    = "ingest"
    image   = "${aws_ecr_repository.api.repository_url}:latest"
    command = each.value.command
    environment = [
      { name = "POSTGRES_HOST", value = aws_db_instance.postgres.address },
      { name = "POSTGRES_DB", value = "geoplatform" },
      { name = "POSTGRES_USER", value = "geoplatform" },
      { name = "POSTGRES_SSLMODE", value = "verify-full" },
      { name = "POSTGRES_SSLROOTCERT", value = "/etc/ssl/rds-global-bundle.pem" },
    ]
    secrets = [
      { name = "POSTGRES_PASSWORD", valueFrom = aws_secretsmanager_secret.platform["postgres_password"].arn },
      { name = "OPENWEATHER_API_KEY", valueFrom = aws_secretsmanager_secret.platform["openweather_api_key"].arn },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.api.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "ingest"
      }
    }
  }])
}

resource "aws_iam_role" "scheduler" {
  name = "${var.app_name}-ingest-scheduler"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "scheduler_run_ingest" {
  name = "${var.app_name}-run-ingest"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "ecs:RunTask"
        Resource = [for t in aws_ecs_task_definition.ingest : "${t.arn_without_revision}:*"]
      },
      {
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = aws_iam_role.ecs_task_execution.arn
      },
    ]
  })
}

resource "aws_scheduler_schedule" "ingest" {
  for_each            = local.ingest_jobs
  name                = "${var.app_name}-ingest-${each.key}"
  schedule_expression = each.value.schedule

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_ecs_cluster.main.arn
    role_arn = aws_iam_role.scheduler.arn

    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.ingest[each.key].arn
      launch_type         = "FARGATE"

      network_configuration {
        subnets          = aws_subnet.public[*].id
        security_groups  = [aws_security_group.ecs_api.id]
        assign_public_ip = true
      }
    }
  }
}
