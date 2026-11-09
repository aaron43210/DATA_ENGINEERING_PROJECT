resource "aws_db_parameter_group" "postgres16" {
  name   = "${var.app_name}-postgres16"
  family = "postgres16"
  tags   = { Name = "${var.app_name}-postgres16" }
}

resource "aws_db_instance" "postgres" {
  identifier             = "${var.app_name}-postgres"
  engine                 = "postgres"
  engine_version         = "16.3"
  instance_class         = "db.t3.medium"
  allocated_storage      = 20
  db_name                = "geoplatform"
  username               = "geoplatform"
  password               = var.postgres_password
  publicly_accessible    = false
  skip_final_snapshot    = false
  deletion_protection    = true
  multi_az               = false
  storage_encrypted      = true
  parameter_group_name   = aws_db_parameter_group.postgres16.name
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  tags                   = { Name = "${var.app_name}-postgres" }
}

# PostGIS is not auto-enabled on RDS; run this once after the instance is ready.
resource "null_resource" "enable_postgis" {
  depends_on = [aws_db_instance.postgres]
  provisioner "local-exec" {
    command = <<-EOT
      psql "postgresql://geoplatform:${var.postgres_password}@${aws_db_instance.postgres.endpoint}/geoplatform?sslmode=require" \
        -c "CREATE EXTENSION IF NOT EXISTS postgis;" \
        -c "CREATE EXTENSION IF NOT EXISTS postgis_topology;"
    EOT
  }
}
