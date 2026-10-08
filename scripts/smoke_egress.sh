#!/usr/bin/env bash
set -euo pipefail

egress_image="soveren-sandbox-egress:route-test"
docker build -f deploy/sandbox/Egress.Dockerfile -t "$egress_image" .
SOVEREN_TEST_EGRESS_IMAGE="$egress_image" uv run pytest tests/test_squid_egress_integration.py
