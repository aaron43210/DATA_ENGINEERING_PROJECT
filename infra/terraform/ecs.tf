resource "aws_ecs_cluster" "main" {
  name = "${var.app_name}-cluster"
  tags = { Name = "${var.app_name}-cluster" }
}

# ── IAM: ECS task execution role ─────────────────────────────────────────────

resource "aws_iam_role" "ecs_task_execution" {
  name = "${var.app_name}-ecs-task-execution"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "secrets_read" {
  name = "${var.app_name}-secrets-read"
  role = aws_iam_role.ecs_task_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = [for s in aws_secretsmanager_secret.platform : s.arn]
    }]
  })
}

# ── ALB ───────────────────────────────────────────────────────────────────────

resource "aws_lb" "api" {
  name               = "${var.app_name}-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = aws_subnet.public[*].id
  tags               = { Name = "${var.app_name}-alb" }
}

resource "aws_lb_target_group" "api" {
  name        = "${var.app_name}-api-tg"
  port        = 8000
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = aws_vpc.main.id
  health_check {
    path                = "/health"
    healthy_threshold   = 2
    unhealthy_threshold = 3
    interval            = 30
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.api.arn
  port              = 80
  protocol          = "HTTP"
  # Viewers get TLS at CloudFront. The security group admits any CloudFront edge, so
  # the listener also requires the secret header only our distribution sends.
  default_action {
    type = "fixed-response"
    fixed_response {
      content_type = "text/plain"
      message_body = "Forbidden"
      status_code  = "403"
    }
  }
}

resource "aws_lb_listener_rule" "from_cloudfront" {
  listener_arn = aws_lb_listener.http.arn
  priority     = 10
  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }
  condition {
    http_header {
      http_header_name = "X-Origin-Verify"
      values           = [var.origin_verify_secret]
    }
  }
}

resource "aws_lb_listener" "https" {
  count             = var.acm_cert_arn != "" ? 1 : 0
  load_balancer_arn = aws_lb.api.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = var.acm_cert_arn
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }
}

# ── CloudWatch log group ──────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "api" {
  name              = "/ecs/${var.app_name}-api"
  retention_in_days = 30
}

# ── API task definition ───────────────────────────────────────────────────────

resource "aws_ecs_task_definition" "api" {
  family                   = "${var.app_name}-api"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn

  container_definitions = jsonencode([{
    name         = "api"
    image        = "${aws_ecr_repository.api.repository_url}:latest"
    portMappings = [{ containerPort = 8000, protocol = "tcp" }]
    environment = [
      { name = "POSTGRES_HOST", value = aws_db_instance.postgres.address },
      { name = "POSTGRES_DB", value = "geoplatform" },
      { name = "POSTGRES_USER", value = "geoplatform" },
      { name = "POSTGRES_SSLMODE", value = "verify-full" },
      { name = "POSTGRES_SSLROOTCERT", value = "/etc/ssl/rds-global-bundle.pem" },
      { name = "AWS_DEFAULT_REGION", value = var.aws_region },
      { name = "GOOGLE_CLIENT_ID", value = var.google_client_id },
      { name = "ROOT_PATH", value = "/api" },
    ]
    secrets = [
      { name = "POSTGRES_PASSWORD", valueFrom = aws_secretsmanager_secret.platform["postgres_password"].arn },
      { name = "JWT_SECRET", valueFrom = aws_secretsmanager_secret.platform["jwt_secret"].arn },
      { name = "OPENWEATHER_API_KEY", valueFrom = aws_secretsmanager_secret.platform["openweather_api_key"].arn },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.api.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "api"
      }
    }
  }])
}

resource "aws_ecs_service" "api" {
  name            = "${var.app_name}-api"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.api.arn
  desired_count   = 1
  launch_type     = "FARGATE"
  # Public subnets + public IP instead of a NAT gateway (~$35/mo saved).
  # The security group still only admits traffic from the ALB.
  network_configuration {
    subnets          = aws_subnet.public[*].id
    security_groups  = [aws_security_group.ecs_api.id]
    assign_public_ip = true
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.api.arn
    container_name   = "api"
    container_port   = 8000
  }
  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }
  depends_on = [aws_lb_listener_rule.from_cloudfront]
}

# One-off schema bootstrap (RDS is private, so it must run inside the VPC):
#   see infra/README.md "Bootstrap the database"
resource "aws_ecs_task_definition" "db_bootstrap" {
  family                   = "${var.app_name}-db-bootstrap"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn

  container_definitions = jsonencode([{
    name    = "bootstrap"
    image   = "postgres:16-alpine"
    command = ["sh", "-c", "wget -qO \"$PGSSLROOTCERT\" https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem && wget -qO- \"$SQL_URL\" | psql -v ON_ERROR_STOP=1"]
    environment = [
      { name = "PGHOST", value = aws_db_instance.postgres.address },
      { name = "PGDATABASE", value = "geoplatform" },
      { name = "PGUSER", value = "geoplatform" },
      { name = "PGSSLMODE", value = "verify-full" },
      { name = "PGSSLROOTCERT", value = "/tmp/rds-global-bundle.pem" },
      { name = "SQL_URL", value = "https://raw.githubusercontent.com/${var.github_repo}/${var.bootstrap_sql_ref}/scripts/init_postgis.sql" },
    ]
    secrets = [
      { name = "PGPASSWORD", valueFrom = aws_secretsmanager_secret.platform["postgres_password"].arn },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.api.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "bootstrap"
      }
    }
  }])
}
