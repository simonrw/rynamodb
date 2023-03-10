#!/usr/bin/env bash

set -xeuo pipefail

ENDPOINT_URL=http://localhost:3050

aws --endpoint-url $ENDPOINT_URL dynamodb put-item \
    --table-name foo \
    --item '{"pk": {"S": "abc"}}'
