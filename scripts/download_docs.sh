#!/usr/bin/env bash

# Taken from https://adamj.eu/tech/2023/03/07/download-documentation-website-with-wget/?utm_source=pocket_saves

set -euo pipefail

DOCS_URL=https://docs.aws.amazon.com/amazondynamodb/latest/developerguide

(
    cd docs
    wget \
        --mirror \
        --convert-links \
        --adjust-extension \
        --page-requisites \
        --no-parent \
        "$DOCS_URL"
)
