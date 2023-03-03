#!/usr/bin/env bash

set -euo pipefail

ENDPOINT_URL=http://localhost:3050

aws --endpoint-url $ENDPOINT_URL dynamodb create-table \
    --table-name foo \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pks,KeyType=HASH
