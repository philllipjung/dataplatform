# Hynix

Spark Application Orchestrator for Kubernetes with intelligent resource allocation and gang scheduling capabilities.

## Overview

Hynix is a microservice that automates the creation and management of Spark applications on Kubernetes. It provides dynamic resource allocation based on data size, integrates with YuniKorn for gang scheduling, and supports multi-tier resource management.

## Features

- **Dynamic Spark Application Creation**: Automatically creates Kubernetes SparkApplication CRDs based on configurable templates
- **Intelligent Resource Allocation**: Automatically calculates and assigns resources based on:
  - MinIO file/folder sizes
  - YuniKorn queue utilization
  - Resource quotas and thresholds
- **Gang Scheduling**: Ensures all required resources are allocated before starting Spark applications via YuniKorn
- **Multi-Tier Resource Management**: Supports different resource tiers (small, medium, large) with automatic selection
- **RESTful API**: Simple HTTP API for Spark application management
- **Prometheus Metrics**: Built-in metrics collection and monitoring
- **Build Number Management**: Semantic versioning support for Spark applications

## Architecture

```
hynix/
├── main.go                 # Main entry point (port 8080)
├── handlers/              # HTTP request handlers
│   ├── create.go         # Spark application creation endpoint
│   ├── reference.go      # Template reference endpoint
│   ├── types.go          # Request/response types
│   └── health.go         # Health check endpoint
├── services/             # Business logic layer
│   ├── config.go         # Configuration management
│   ├── template.go       # Template processing
│   ├── kubernetes.go     # Kubernetes/YuniKorn integration
│   └── utils.go          # Utility functions
├── metrics/              # Prometheus metrics
├── template/             # Spark application templates
├── config/               # Configuration files
├── logger/               # Logging infrastructure
└── middleware/           # HTTP middleware
```

## API Endpoints

### Health Check
```
GET /health
```

### Prometheus Metrics
```
GET /metrics
```

### Create Spark Application
```
POST /api/v1/spark/create
Content-Type: application/json

{
  "provision_id": "0002_wfbm",
  "service_id": "1234-wfbm",
  "category": "tttm",
  "region": "ic",
  "uid": "optional-uid",
  "arguments": "1000000"  // Optional: Spark application arguments
}
```

**Arguments Field**:
- Optional field for passing arguments to the Spark application
- Space-separated values (e.g., `"111 222 333"`)
- If provided, adds `arguments:` section under `spec:` in the YAML
- Example: `"arguments": "1000000"` → `spec.arguments: - "1000000"`

### Get Application Template
```
GET /api/v1/spark/reference?provision_id=0002_wfbm
```

## API Response Codes

### Response Structure

All API responses follow a standard structure:

**Success Response** (2xx):
```json
{
  "success": true,
  "message": "SparkApplication CR 생성 성공",
  "data": {
    "provision_id": "0002_wfbm",
    "service_id": "1234-wfbm",
    "category": "tttm",
    "region": "ic",
    "result": {
      "name": "app-name",
      "namespace": "default"
    }
  }
}
```

**Error Response** (4xx/5xx):
```json
{
  "success": false,
  "message": "프로비저닝 설정을 찾을 수 없습니다",
  "error": {
    "code": "NOT_FOUND",
    "message": "지정된 프로비저닝 ID가 존재하지 않습니다",
    "details": "999_invalid",
    "retryable": false
  }
}
```

### HTTP Status Codes

| Code | Type | Retryable | Description |
|------|------|-----------|-------------|
| **200** | Success | No | SparkApplication template YAML returned (reference endpoint) |
| **201** | Success | No | SparkApplication CR created successfully (create endpoint) |
| **400** | Business Error | **No** | Bad Request or Validation Failed |
| **404** | Business Error | **No** | Resource not found (provision ID, template) |
| **500** | Technical Error | **Yes** | Internal Server Error (config load, Kubernetes API, etc.) |

### Error Codes

**Business Errors (4xx) - Do Not Retry**:
- `BAD_REQUEST`: Invalid JSON format or request structure
- `VALIDATION_FAILED`: Missing required fields (provision_id, service_id, category, region)
- `NOT_FOUND`: Provision ID or template not found
- `UNPROCESSABLE`: Request cannot be processed as-is

**Technical Errors (5xx) - Safe to Retry**:
- `CONFIG_LOAD_FAILED`: Failed to load config.json
- `TEMPLATE_LOAD_FAILED`: Failed to load template YAML
- `KUBERNETES_ERROR`: Kubernetes API request failed
- `INTERNAL_ERROR`: Unexpected server error
- `SERVICE_UNAVAILABLE`: Service temporarily unavailable
- `YUNIKORN_ERROR`: YuniKorn API request failed
- `MINIO_ERROR`: MinIO connection error

### Examples

**1. Success Response**:
```bash
curl -X POST http://localhost:8080/api/v1/spark/create \
  -H "Content-Type: application/json" \
  -d '{
    "provision_id": "0002_wfbm",
    "service_id": "test-app",
    "category": "batch",
    "region": "ic",
    "uid": "001"
  }'
```

**Response** (201):
```json
{
  "success": true,
  "message": "SparkApplication CR 생성 성공",
  "data": {
    "result": {
      "name": "test-app-batch-001",
      "namespace": "default"
    }
  }
}
```

**2. Validation Error** (400 - No Retry):
```bash
curl -X POST http://localhost:8080/api/v1/spark/create \
  -H "Content-Type: application/json" \
  -d '{
    "provision_id": "0002_wfbm",
    "service_id": "test",
    "category": "",
    "region": "ic",
    "uid": "001"
  }'
```

**Response** (400):
```json
{
  "success": false,
  "message": "요청 검증에 실패했습니다",
  "error": {
    "code": "VALIDATION_FAILED",
    "message": "필수 파라미터가 누락되었습니다",
    "details": "provision_id, service_id, category, region 필드가 모두 필요합니다",
    "retryable": false
  }
}
```

**3. Not Found Error** (404 - No Retry):
```bash
curl -X POST http://localhost:8080/api/v1/spark/create \
  -H "Content-Type: application/json" \
  -d '{
    "provision_id": "999_invalid",
    "service_id": "test",
    "category": "test",
    "region": "ic",
    "uid": "001"
  }'
```

**Response** (404):
```json
{
  "success": false,
  "message": "프로비저닝 설정을 찾을 수 없습니다",
  "error": {
    "code": "NOT_FOUND",
    "message": "지정된 프로비저닝 ID가 존재하지 않습니다",
    "details": "999_invalid",
    "retryable": false
  }
}
```

**4. Technical Error** (500 - Retry Safe):
```bash
curl -X POST http://localhost:8080/api/v1/spark/create \
  -H "Content-Type: application/json" \
  -d '{
    "provision_id": "0002_wfbm",
    "service_id": "test",
    "category": "test",
    "region": "ic",
    "uid": "001"
  }'
```

**Response** (500):
```json
{
  "success": false,
  "message": "서버 설정 로드에 실패했습니다",
  "error": {
    "code": "CONFIG_LOAD_FAILED",
    "message": "설정 파일을 로드할 수 없습니다",
    "details": "failed to read config.json: no such file or directory",
    "retryable": true
  }
}
```

## Configuration

### Configuration File (`config/config.json`)

```json
{
  "config_specs": [
    {
      "provision_id": "0002_wfbm",
      "resource_calculation": {
        "enabled": "true",
        "minio": "1234/5678/<<service_id>>",
        "tiers": [
          {
            "name": "small",
            "max_size": 10000000,
            "queue": "ias.small",
            "executor": 1,
            "cpu": 1
          },
          {
            "name": "medium",
            "min_size": 10000000,
            "queue": "ias.medium",
            "executor": 2,
            "cpu": 2
          },
          {
            "name": "large",
            "min_size": 53687091200,
            "queue": "ias.large",
            "executor": 3,
            "cpu": 4
          }
        ]
      },
      "resource_allocation": {
        "enabled": true,
        "name": "ias allocation",
        "source": {
          "cpu": 50,
          "memory": 50,
          "queue": "root.ias"
        },
        "target": {
          "cpu": 5,
          "memory": 5,
          "queue": "temp"
        },
        "namespace": "common"
      },
```

**Note**: Queue names in config should not include the `root.` prefix, as it's automatically added by the service. For example:
- Use `"queue": "temp"` instead of `"queue": "root.temp"`
- Use `"queue": "ias"` instead of `"queue": "root.ias"`
      "build_number": {
        "major": "4",
        "minor": "0",
        "patch": "1"
      }
    }
  ]
}
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | Server port | `8080` |
| `MINIO_ROOT_USER` | MinIO access key | - |
| `MINIO_ROOT_PASSWORD` | MinIO secret key | - |
| `MINIO_ENDPOINT` | MinIO endpoint | `localhost:9000` |
| `YUNIKORN_SERVICE_URL` | YuniKorn REST API URL | `http://yunikorn-service:9080` |
| `KUBECONFIG` | Kubernetes config path | `~/.kube/config` |

## Resource Allocation Logic

### Priority 1: Resource Allocation Thresholds

When `resource_allocation.enabled` is `true`, the service checks if:
- Source queue usage >= threshold (e.g., CPU >= 50%, Memory >= 50%)
- Target namespace usage <= threshold (e.g., CPU <= 5%, Memory <= 5%)

If conditions are met, the application is scheduled in the target namespace/queue.

### Priority 2: Resource Calculation (Tier-based)

If resource allocation conditions are not met, the service falls back to tier-based selection based on MinIO file/folder size:

| Tier | File Size | Queue | Executors | CPU |
|------|-----------|-------|-----------|-----|
| small | < 10 MB | ias.small | 1 | 1 |
| medium | >= 10 MB | ias.medium | 2 | 2 |
| large | >= 50 GB | ias.large | 3 | 4 |

**CPU Configuration**:
The `cpu` field in config.json automatically sets 4 executor CPU parameters:

| Field | Calculation | Example (CPU=2) |
|-------|-------------|-----------------|
| `spec.executor.cores` | CPU | `2` |
| `spec.driver.annotations.yunikorn.apache.org/task-groups[1].minResource.cpu` | CPU × 100m | `"200m"` |
| `spec.executor.template.spec.containers[0].resources.limits.cpu` | CPU | `"2"` |
| `spec.executor.template.spec.containers[0].resources.requests.cpu` | CPU × 500m | `"1000m"` |

**MinIO Configuration**:
- Set `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_ENDPOINT` environment variables
- Configure bucket and path in config.json: `"minio": "bucket/path/<<service_id>>"`
- File size is automatically retrieved from MinIO object metadata
- If file doesn't exist, defaults to `ias.small` queue

### Gang Scheduling

All Spark applications use YuniKorn gang scheduling with configurable:
- CPU per task
- Memory per task
- Executor minimum members

## Templates

Spark application templates are stored in `template/` directory with naming convention: `{provision_id}.yaml` (hyphens converted to underscores).

Example: `provision_id: "0002-wfbm"` → `template/0002_wfbm.yaml`

### Template Placeholders

Templates support the following placeholders:
- `SERVICE_ID_PLACEHOLDER`: Replaced with actual service ID
- `BUILD_NUMBER`: Replaced with semantic version from config.json (e.g., `4.0.1`)
  - Format: `{major}.{minor}.{patch}` from `build_number` field in config.json
  - Applied to: image, sparkVersion, and build-number labels
- `QUEUE_PLACEHOLDER`: Replaced with selected queue path

### Arguments Injection

When the `arguments` field is provided in the request, the service dynamically adds an `arguments` section under `spec:` in the YAML:

**Single argument** (`"arguments": "1000000"`):
```yaml
spec:
  arguments:
    - "1000000"
  type: Scala
```

**Multiple arguments** (`"arguments": "111 222 333"`):
```yaml
spec:
  arguments:
    - "111"
    - "222"
    - "333"
  type: Scala
```

**No arguments** (field omitted or empty):
The `arguments` section is not added to the YAML, preserving the template's default configuration.

## Deployment

### Prerequisites

- Kubernetes cluster with Spark Operator installed
- YuniKorn scheduler configured
- MinIO instance for object storage
- Network policies allowing access to Kubernetes API

### Build

```bash
go build -o hynix main.go
```

### Run

```bash
# Using default port (8080)
./hynix

# Using custom port
PORT=9090 ./hynix

# With MinIO configuration
MINIO_ROOT_USER=minioadmin \
MINIO_ROOT_PASSWORD=minioadmin \
MINIO_ENDPOINT=minio:9000 \
./hynix
```

### Namespace Setup

#### Create common Namespace

```bash
kubectl create namespace common
```

#### Configure RBAC for common Namespace

```bash
# Apply ServiceAccount
kubectl apply -f deploy/spark-operator-spark-common.yaml

# Apply RBAC
kubectl apply -f deploy/spark-operator-rbac-common.yaml
```

#### Configure YuniKorn Queues

```bash
# Apply YuniKorn queue configuration
kubectl apply -f deploy/yunikorn-queues-config.yaml

# Restart YuniKorn to load new configuration
kubectl rollout restart deployment yunikorn-scheduler -n default
```

**Note**: The YuniKorn configuration includes:
- `root.temp` queue for common namespace
- `root.ias` queue with small/medium/large sub-queues
- Placement rules to automatically assign applications to correct queues

### Docker

```dockerfile
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o hynix main.go

FROM alpine:latest
COPY --from=builder /app/hynix /hynix
COPY config/ /app/config/
COPY template/ /app/template/
EXPOSE 8080
CMD ["/hynix"]
```

## Monitoring

### Prometheus Metrics

The service exposes the following metrics:

- `hynix_requests_total`: Total request count (by provision_id, endpoint, status)
- `hynix_request_duration_seconds`: Request duration
- `hynix_k8s_creation_total`: Kubernetes CR creation count
- `hynix_provision_mode_total`: Provision mode usage
- `hynix_queue_selection_total`: Queue selection count
- `hynix_resource_allocation_decision_total`: Resource allocation decisions
- `hynix_resource_calculation_skipped_total`: Skipped calculations
- `hynix_executor_min_member`: Executor minimum member count
- `hynix_gang_scheduling_resources`: Gang scheduling resources

### Logging

Structured logging with Zap includes:
- Client input parameters
- Configuration values
- MinIO metadata
- Resource calculation results
- Final YAML manifests
- Kubernetes API responses

## Spark Application Templates

Templates are stored in `template/` directory and follow the SparkOperator CRD format:

- **0001_wfbm.yaml**: Basic WFBM provision
- **0002_wfbm.yaml**: WFBM with resource calculation
- **0003_wfbm.yaml**: Long-running WFBM jobs
- **0004_wfbm.yaml**: Extended WFBM configuration

Each template includes:
- YuniKorn gang scheduling annotations
- Prometheus metrics configuration
- Security hardening (seccomp, non-root)
- Resource limits and requests

## Development

### Project Structure

- **Go 1.23+**
- **Gin** - Web framework
- **client-go** - Kubernetes API client
- **minio-go** - MinIO client
- **Zap** - Structured logging

### Testing

```bash
# Run tests
go test ./...

# Run with coverage
go test -cover ./...
```

## Troubleshooting

### Common Issues

1. **MinIO Connection Failed**
   - Verify `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` are set
   - Check `MINIO_ENDPOINT` accessibility

2. **YuniKorn API Error**
   - Verify `YUNIKORN_SERVICE_URL` is correct
   - For local testing: `export YUNIKORN_SERVICE_URL=http://localhost:9080`
   - Ensure port-forward is running: `kubectl port-forward svc/yunikorn-service 9080:9080`

3. **Template Not Found**
   - Ensure template file exists: `template/{provision_id}.yaml`
   - Check filename uses underscores instead of hyphens

4. **Kubernetes API Error**
   - Verify RBAC permissions for SparkApplication CRD
   - Check service account has appropriate roles

## License

SK hynix

## Contributing

This is an internal SK hynix project. Please contact the maintainers for contribution guidelines.
