#!/bin/bash

# 1. Nâng cấp DB
superset db upgrade

# 2. Tạo user admin
superset fab create-admin \
  --username admin \
  --firstname Superset \
  --lastname Admin \
  --email admin@superset.com \
  --password admin

# 3. Init Superset
superset init

