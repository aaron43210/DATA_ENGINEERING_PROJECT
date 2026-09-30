#!/bin/bash
# Run after LakeFS container is healthy
set -e

LAKEFS_ENDPOINT="http://localhost:8000"
ACCESS_KEY="accesskey"
SECRET_KEY="secretkey"

# Wait for LakeFS to be ready
echo "Waiting for LakeFS to be ready..."
until curl -s -f -o /dev/null "$LAKEFS_ENDPOINT/_health"; do
    sleep 2
done

echo "LakeFS is healthy. Creating repository: geoplatform..."
curl -s -X POST "$LAKEFS_ENDPOINT/api/v1/repositories" \
  -u "$ACCESS_KEY:$SECRET_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "geoplatform",
    "storage_namespace": "s3://lakefs-data/geoplatform",
    "default_branch": "main"
  }'

# Create branches for each data layer (like Git branching for data)
for BRANCH in bronze silver gold staging; do
  echo "Creating branch: $BRANCH"
  curl -s -X POST "$LAKEFS_ENDPOINT/api/v1/repositories/geoplatform/branches" \
    -u "$ACCESS_KEY:$SECRET_KEY" \
    -H "Content-Type: application/json" \
    -d "{\"name\": \"$BRANCH\", \"source\": \"main\"}"
done

echo "✅ LakeFS setup complete"
echo "   UI: http://localhost:8000  (admin/accesskey/secretkey)"
