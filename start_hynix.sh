#!/bin/bash
export MINIO_ROOT_USER="your-access-key"
export MINIO_ROOT_PASSWORD="your-secret-key"
export MINIO_ENDPOINT="127.0.0.1:9000"
cd /root/hynix
exec ./hynix-service
