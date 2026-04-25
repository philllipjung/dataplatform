#!/bin/bash
cd /root/hynix

export MINIO_ENDPOINT=localhost:9000
export MINIO_ROOT_USER=your-access-key
export MINIO_ROOT_PASSWORD=your-secret-key
export MINIO_BUCKET=1234

echo "Stopping existing services..."
pkill -f './hynix' 2>/dev/null
pkill -f './proxy' 2>/dev/null
sleep 2

echo "Starting services..."
mkdir -p /root/hynix
nohup ./hynix >> /root/hynix/hynix.log 2>&1 &
nohup ./proxy >> /root/hynix/proxy.log 2>&1 &
sleep 3

echo ""
echo "======================================"
echo "Services started!"
echo "======================================"
echo ""
echo "Service URLs:"
echo "  API Server:     http://localhost:8080"
echo "  Proxy Server:   http://localhost:8082"
echo ""
echo "Log files:"
echo "  API Server:  /root/hynix/hynix.log"
echo "  Proxy Server:  /root/hynix/proxy.log"
echo ""
