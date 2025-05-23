#!/usr/bin/env bash
set -e

superset db upgrade

superset fab create-admin \
  --username admin \
  --firstname Superset \
  --lastname Admin \
  --email admin@superset.io \
  --password Admin123
  
superset init

exec superset run \
     --host 0.0.0.0 \
     --port 8088 \
     --with-threads \
     --reload
