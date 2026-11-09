locals {
  secrets = {
    postgres_password   = var.postgres_password
    airflow_fernet_key  = var.airflow_fernet_key
    airflow_secret_key  = var.airflow_secret_key
    jwt_secret          = var.jwt_secret
    openweather_api_key = var.openweather_api_key
  }
}

resource "aws_secretsmanager_secret" "platform" {
  for_each                = local.secrets
  name                    = "${var.app_name}/${each.key}"
  recovery_window_in_days = 7
  tags                    = { Name = "${var.app_name}/${each.key}" }
}

resource "aws_secretsmanager_secret_version" "platform" {
  for_each      = local.secrets
  secret_id     = aws_secretsmanager_secret.platform[each.key].id
  secret_string = each.value
}
