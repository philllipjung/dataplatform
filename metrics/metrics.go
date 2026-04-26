package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// RequestsTotal - 총 요청 수
	RequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_service_requests_total",
			Help: "Total number of requests to the spark service",
		},
		[]string{"provision_id", "endpoint", "status"},
	)

	// RequestDuration - 요청 처리 시간
	RequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "spark_service_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"provision_id", "endpoint"},
	)

	// QueueSelection - 큐 선택 수
	QueueSelection = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_service_queue_selection_total",
			Help: "Total number of queue selections (min/max)",
		},
		[]string{"provision_id", "queue"},
	)

	// ProvisionMode - 프로비저닝 모드 사용 현황
	ProvisionMode = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_service_provision_mode_total",
			Help: "Total number of requests by provision mode (enabled true/false)",
		},
		[]string{"provision_id", "enabled"},
	)

	// ResourceCalculationSkipped - 리소스 계산 스킵 횟수
	ResourceCalculationSkipped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_service_resource_calculation_skipped_total",
			Help: "Total number of times resource calculation was skipped",
		},
		[]string{"provision_id", "reason"},
	)

	// K8sCreation - Kubernetes 생성 성공/실패
	K8sCreation = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_service_k8s_creation_total",
			Help: "Total number of Kubernetes SparkApplication creations",
		},
		[]string{"provision_id", "namespace", "status"},
	)

	// K8sDeletion - 기존 리소스 삭제 횟수
	K8sDeletion = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_service_k8s_deletion_total",
			Help: "Total number of existing SparkApplication deletions",
		},
		[]string{"provision_id", "namespace"},
	)

	// FileSize - 파일 크기 (MB)
	FileSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "spark_service_resource_file_size_mb",
			Help: "File size in MB used for resource calculation",
		},
		[]string{"provision_id", "file_path"},
	)

	// ExecutorMinMember - Executor minMember 설정값
	ExecutorMinMember = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "spark_service_executor_min_member",
			Help: "Executor minMember value for Gang Scheduling",
		},
		[]string{"provision_id"},
	)

	// GangSchedulingResources - Gang Scheduling CPU/Memory 설정
	GangSchedulingResources = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "spark_service_gang_scheduling_resources",
			Help: "Gang Scheduling resource values (cpu/memory)",
		},
		[]string{"provision_id", "resource_type"},
	)

	// ResourceAllocationDecision - Resource Allocation 결정 수
	ResourceAllocationDecision = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_service_resource_allocation_decision_total",
			Help: "Total number of resource allocation decisions (allocated/skipped/disabled/error)",
		},
		[]string{"provision_id", "decision"},
	)

	// ResourceAllocationSourceUsage - Source 리소스 사용량
	ResourceAllocationSourceUsage = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "spark_service_resource_allocation_source_usage_percent",
			Help: "Source resource usage percentage for resource allocation",
		},
		[]string{"provision_id", "resource_type"},
	)

	// ResourceAllocationTargetUsage - Target 리소스 사용량
	ResourceAllocationTargetUsage = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "spark_service_resource_allocation_target_usage_percent",
			Help: "Target resource usage percentage for resource allocation",
		},
		[]string{"provision_id", "resource_type"},
	)
)
