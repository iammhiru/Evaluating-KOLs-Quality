#!/bin/bash
set -e

echo "⏳ Waiting for Spark master to be ready..."
sleep 5

exec /opt/bitnami/scripts/spark/run.sh "$@"
