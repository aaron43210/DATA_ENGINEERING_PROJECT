resource "aws_db_parameter_group" "postgres16" {
  name   = "${var.app_name}-postgres16"
  family = "postgres16"
  tags   = { Name = "${var.app_name}-postgres16" }
}

resource "aws_db_instance" "postgres" {
  identifier                 = "${var.app_name}-postgres"
  engine                     = "postgres"
  engine_version             = "16.3"
  instance_class             = "db.t3.micro"
  allocated_storage          = 20
  db_name                    = "geoplatform"
  username                   = "geoplatform"
  password                   = var.postgres_password
  publicly_accessible        = false
  skip_final_snapshot        = false
  final_snapshot_identifier  = "${var.app_name}-final"
  deletion_protection        = true
  multi_az                   = false
  backup_retention_period    = 1 # AWS free plan maximum; raise to 7 after upgrading the account
  auto_minor_version_upgrade = true
  copy_tags_to_snapshot      = true
  apply_immediately          = true # do not wait for the weekly maintenance window
  storage_encrypted          = true
  parameter_group_name       = aws_db_parameter_group.postgres16.name
  db_subnet_group_name       = aws_db_subnet_group.main.name
  vpc_security_group_ids     = [aws_security_group.rds.id]
  tags                       = { Name = "${var.app_name}-postgres" }
}
