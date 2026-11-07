#!/bin/sh
# One-shot, safe to re-run: buckets, private access, versioning and lifecycle.
set -eu

BRONZE="${BUCKET_BRONZE:-bronze}"
RETENTION_DAYS="${BRONZE_RETENTION_DAYS:-365}"
NONCURRENT_DAYS="${BRONZE_NONCURRENT_DAYS:-30}"

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

for bucket in "$BRONZE" "${BUCKET_SILVER:-silver}" "${BUCKET_GOLD:-gold}" "${BUCKET_WAREHOUSE:-warehouse}"; do
  mc mb --ignore-existing "local/$bucket"
  mc anonymous set none "local/$bucket"
done

# Versioning keeps Bronze recoverable if an object is ever overwritten.
mc version enable "local/$BRONZE"

# GDPR retention: raw data expires, and old versions do not linger after that.
mc ilm rule rm --all --force "local/$BRONZE" 2>/dev/null || true
mc ilm rule add --expire-days "$RETENTION_DAYS" --noncurrent-expire-days "$NONCURRENT_DAYS" "local/$BRONZE"

mc ilm rule ls "local/$BRONZE"
echo "MinIO buckets initialized."
