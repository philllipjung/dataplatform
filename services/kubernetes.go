package services

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// YuniKornQueueInfo - YuniKorn 큐 정보 응답
type YuniKornQueueInfo struct {
	Queues []YuniKornQueue `json:"children,omitempty"`
}

// YuniKornQueue - YuniKorn 큐 정보
type YuniKornQueue struct {
	QueueName      string              `json:"queuename"`
	Parent         string              `json:"parent,omitempty"`
	Children       []YuniKornQueue     `json:"children,omitempty"`
	UsedResources  YuniKornResources   `json:"usedResources,omitempty"`
	MaxResources   YuniKornResources   `json:"maxResources,omitempty"`
	AllocatedResources YuniKornResources `json:"allocatedResource,omitempty"`
	Properties     map[string]string   `json:"properties,omitempty"`
}

// YuniKornResources - YuniKorn 리소스 정보
type YuniKornResources struct {
	VCore int64 `json:"vcore"`
	Memory int64 `json:"memory"`
}

// KubernetesClient - Kubernetes API 클라이언트 래퍼
type KubernetesClient struct {
	clientset *kubernetes.Clientset
}

// NewKubernetesClient - Kubernetes 클라이언트 생성
func NewKubernetesClient() (*KubernetesClient, error) {
	// kubeconfig 경로 가져오기
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("HOME") + "/.kube/config"
	}

	// kubeconfig 로드 (항상 kubeconfig 사용)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("kubeconfig 로드 실패: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("kubernetes clientset 생성 실패: %w", err)
	}

	return &KubernetesClient{clientset: clientset}, nil
}

// GetNamespaceResourceQuotaUsage - 네임스페이스 리소스쿼터 사용량 조회
func (k *KubernetesClient) GetNamespaceResourceQuotaUsage(namespace string) (*ResourceUsage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// ResourceQuota 목록 조회
	quotas, err := k.clientset.CoreV1().ResourceQuotas(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, fmt.Errorf("네임스페이스 %s에 ResourceQuota 없음", namespace)
		}
		return nil, fmt.Errorf("ResourceQuota 조회 실패: %w", err)
	}

	if len(quotas.Items) == 0 {
		return nil, fmt.Errorf("네임스페이스 %s에 ResourceQuota 없음", namespace)
	}

	// 첫 번째 ResourceQuota 사용
	quota := quotas.Items[0]

	var cpuUsed, cpuCapacity int64
	var memoryUsed, memoryCapacity int64

	// Hard limits (capacity)
	for key, value := range quota.Spec.Hard {
		switch key {
		case "requests.cpu", "limits.cpu":
			cpuCapacity = value.MilliValue()
		case "requests.memory", "limits.memory":
			memoryCapacity = value.Value()
		}
	}

	// Used resources
	for key, value := range quota.Status.Used {
		switch key {
		case "requests.cpu", "limits.cpu":
			cpuUsed = value.MilliValue()
		case "requests.memory", "limits.memory":
			memoryUsed = value.Value()
		}
	}

	// 퍼센트 계산
	cpuPercent := 0.0
	memoryPercent := 0.0

	if cpuCapacity > 0 {
		cpuPercent = (float64(cpuUsed) / float64(cpuCapacity)) * 100
	}
	if memoryCapacity > 0 {
		memoryPercent = (float64(memoryUsed) / float64(memoryCapacity)) * 100
	}

	return &ResourceUsage{
		CPUPercent:     cpuPercent,
		MemoryPercent:  memoryPercent,
		CPUUsed:        fmt.Sprintf("%dm", cpuUsed),
		MemoryUsed:     formatMemory(memoryUsed),
		CPUCapacity:    fmt.Sprintf("%dm", cpuCapacity),
		MemoryCapacity: formatMemory(memoryCapacity),
	}, nil
}

// GetQueueResourceUsageFromYuniKorn - YuniKorn REST API에서 큐 리소스 사용량 조회
func GetQueueResourceUsageFromYuniKorn(queuePath string) (*ResourceUsage, error) {
	// YuniKorn REST API 엔드포인트
	yuniKornURL := os.Getenv("YUNIKORN_SERVICE_URL")
	if yuniKornURL == "" {
		yuniKornURL = "http://yunikorn-service:9080"
	}

	// 큐 경로 포맷: root.temp → root/temp
	apiURL := fmt.Sprintf("%s/ws/v1/partition/default/queues", yuniKornURL)

	// HTTP GET 요청 생성 (타임아웃 10초)
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get(apiURL)
	if err != nil {
		return nil, fmt.Errorf("YuniKorn API 요청 실패: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("YuniKorn API 오류: status=%d, body=%s", resp.StatusCode, string(body))
	}

	// 응답 파싱
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("YuniKorn API 응답 읽기 실패: %w", err)
	}

	var queueInfo YuniKornQueueInfo
	if err := json.Unmarshal(body, &queueInfo); err != nil {
		return nil, fmt.Errorf("YuniKorn API 응답 파싱 실패: %w", err)
	}

	// 큐 찾기 (재귀적으로 검색)
	targetQueue := findQueueByName(queueInfo.Queues, queuePath)
	if targetQueue == nil {
		return nil, fmt.Errorf("큐를 찾을 수 없음: %s", queuePath)
	}

	// 리소스 사용량 계산
	usedResources := targetQueue.UsedResources
	if usedResources.VCore == 0 && usedResources.Memory == 0 {
		usedResources = targetQueue.AllocatedResources
	}

	maxResources := targetQueue.MaxResources

	// 퍼센트 계산
	cpuPercent := 0.0
	memoryPercent := 0.0

	if maxResources.VCore > 0 {
		cpuPercent = (float64(usedResources.VCore) / float64(maxResources.VCore)) * 100
	}
	if maxResources.Memory > 0 {
		memoryPercent = (float64(usedResources.Memory) / float64(maxResources.Memory)) * 100
	}

	return &ResourceUsage{
		CPUPercent:     cpuPercent,
		MemoryPercent:  memoryPercent,
		CPUUsed:        fmt.Sprintf("%dm", usedResources.VCore),
		MemoryUsed:     formatMemory(int64(usedResources.Memory)),
		CPUCapacity:    fmt.Sprintf("%dm", maxResources.VCore),
		MemoryCapacity: formatMemory(int64(maxResources.Memory)),
	}, nil
}

// findQueueByName - 큐 이름으로 큐 찾기 (재귀적)
func findQueueByName(queues []YuniKornQueue, queueName string) *YuniKornQueue {
	for i := range queues {
		if queues[i].QueueName == queueName {
			return &queues[i]
		}
		if len(queues[i].Children) > 0 {
			if found := findQueueByName(queues[i].Children, queueName); found != nil {
				return found
			}
		}
	}
	return nil
}

// formatMemory - 바이트를 사람이 읽기 쉬운 형식으로 변환
func formatMemory(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// CalculateResourceAllocation - 리소스 할당 계산
// ResourceAllocation 설정에 따라 네임스페이스와 큐를 결정
// source.queue: 할당량을 측정할 큐 (예: root.ias)
// target.queue: submit할 큐 (예: root.temp)
// target: 네임스페이스 리소스쿼터를 측정
func CalculateResourceAllocation(allocation ResourceAllocation) (*ResourceAllocationResult, error) {
	// 비활성화된 경우
	if !allocation.Enabled {
		return &ResourceAllocationResult{
			UseAllocation: false,
			Reason:        "Resource allocation disabled",
		}, nil
	}

	// Source 큐 리소스 사용량 조회 (YuniKorn REST API)
	// allocation.Source.Queue에서 큐 경로 추출 (예: "root.ias")
	sourceUsage, err := GetQueueResourceUsageFromYuniKorn(allocation.Source.Queue)
	if err != nil {
		return &ResourceAllocationResult{
			UseAllocation: false,
			Reason:        fmt.Sprintf("Source queue 리소스 사용량 조회 실패: %v", err),
		}, nil
	}

	// Target 네임스페이스 리소스쿼터 사용량 조회
	k8sClient, err := NewKubernetesClient()
	if err != nil {
		return &ResourceAllocationResult{
			UseAllocation: false,
			Reason:        fmt.Sprintf("Kubernetes client 생성 실패: %v", err),
		}, nil
	}

	targetUsage, err := k8sClient.GetNamespaceResourceQuotaUsage(allocation.Namespace)
	if err != nil {
		return &ResourceAllocationResult{
			UseAllocation: false,
			Reason:        fmt.Sprintf("Target namespace ResourceQuota 조회 실패: %v", err),
		}, nil
	}

	// 조건 평가
	// source.cpu >= allocation.Source.cpu AND
	// source.memory >= allocation.Source.memory AND
	// target.cpu <= allocation.Target.cpu AND
	// target.memory <= allocation.Target.memory

	sourceCPUMet := sourceUsage.CPUPercent >= float64(allocation.Source.CPU)
	sourceMemoryMet := sourceUsage.MemoryPercent >= float64(allocation.Source.Memory)
	targetCPUMet := targetUsage.CPUPercent <= float64(allocation.Target.CPU)
	targetMemoryMet := targetUsage.MemoryPercent <= float64(allocation.Target.Memory)

	conditionMet := sourceCPUMet && sourceMemoryMet && targetCPUMet && targetMemoryMet

	if conditionMet {
		return &ResourceAllocationResult{
			UseAllocation: true,
			Namespace:     allocation.Namespace,
			Queue:         allocation.Target.Queue, // target.queue에 submit
			SourceUsage:   sourceUsage,
			TargetUsage:   targetUsage,
			Reason: fmt.Sprintf("Allocation thresholds met: source.cpu=%.1f%%>=%d%%, source.memory=%.1f%%>=%d%%, target.cpu=%.1f%%<=%d%%, target.memory=%.1f%%<=%d%%",
				sourceUsage.CPUPercent, allocation.Source.CPU,
				sourceUsage.MemoryPercent, allocation.Source.Memory,
				targetUsage.CPUPercent, allocation.Target.CPU,
				targetUsage.MemoryPercent, allocation.Target.Memory),
		}, nil
	}

	return &ResourceAllocationResult{
		UseAllocation: false,
		SourceUsage:   sourceUsage,
		TargetUsage:   targetUsage,
		Reason: fmt.Sprintf("Allocation thresholds not met: source.cpu=%.1f%%>=%d%%(t:%v), source.memory=%.1f%%>=%d%%(t:%v), target.cpu=%.1f%%<=%d%%(t:%v), target.memory=%.1f%%<=%d%%(t:%v)",
			sourceUsage.CPUPercent, allocation.Source.CPU, sourceCPUMet,
			sourceUsage.MemoryPercent, allocation.Source.Memory, sourceMemoryMet,
			targetUsage.CPUPercent, allocation.Target.CPU, targetCPUMet,
			targetUsage.MemoryPercent, allocation.Target.Memory, targetMemoryMet),
	}, nil
}

// IsResourceAllocationEnabled - ResourceAllocation 활성화 여부 확인
func IsResourceAllocationEnabled(spec *ConfigSpec) bool {
	return spec.ResourceAllocation.Enabled
}

// ParseResourceAllocationFromConfig - JSON 문자열에서 ResourceAllocation 파싱
func ParseResourceAllocationFromConfig(jsonStr string) (*ResourceAllocation, error) {
	var allocation ResourceAllocation
	if err := json.Unmarshal([]byte(jsonStr), &allocation); err != nil {
		return nil, fmt.Errorf("ResourceAllocation 파싱 실패: %w", err)
	}
	return &allocation, nil
}

// StringToQuantity - 문자열을 resource.Quantity로 변환
func StringToQuantity(s string) resource.Quantity {
	return resource.MustParse(s)
}

// QuantityToFloat64 - resource.Quantity를 float64로 변환 (CPU의 경우 MilliValue)
func QuantityToFloat64(q resource.Quantity, isCPU bool) float64 {
	if isCPU {
		return float64(q.MilliValue()) / 1000.0
	}
	return float64(q.Value())
}

// Float64ToQuantity - float64를 resource.Quantity로 변환
func Float64ToQuantity(f float64, isCPU bool) resource.Quantity {
	if isCPU {
		return *resource.NewMilliQuantity(int64(f*1000), resource.DecimalSI)
	}
	return *resource.NewQuantity(int64(f), resource.BinarySI)
}

// GetPodResourceUsage - 네임스페이스 내 Pod 리소스 사용량 합계 조회
func (k *KubernetesClient) GetPodResourceUsage(namespace string) (*ResourceUsage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Pod 목록 조회
	pods, err := k.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("Pod 조회 실패: %w", err)
	}

	var totalCPUUsed int64
	var totalMemoryUsed int64

	for _, pod := range pods.Items {
		// Running 상태의 Pod만 계산
		if pod.Status.Phase != "Running" {
			continue
		}

		for _, container := range pod.Spec.Containers {
			if container.Resources.Requests != nil {
				if cpu, ok := container.Resources.Requests["cpu"]; ok {
					totalCPUUsed += cpu.MilliValue()
				}
				if memory, ok := container.Resources.Requests["memory"]; ok {
					totalMemoryUsed += memory.Value()
				}
			}
		}
	}

	// Node Capacity 조회 (전체 클러스터 용량)
	nodes, err := k.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("Node 조회 실패: %w", err)
	}

	var totalCPUCapacity int64
	var totalMemoryCapacity int64

	for _, node := range nodes.Items {
		if cpu, ok := node.Status.Capacity["cpu"]; ok {
			totalCPUCapacity += cpu.MilliValue()
		}
		if memory, ok := node.Status.Capacity["memory"]; ok {
			totalMemoryCapacity += memory.Value()
		}
	}

	// 퍼센트 계산
	cpuPercent := 0.0
	memoryPercent := 0.0

	if totalCPUCapacity > 0 {
		cpuPercent = (float64(totalCPUUsed) / float64(totalCPUCapacity)) * 100
	}
	if totalMemoryCapacity > 0 {
		memoryPercent = (float64(totalMemoryUsed) / float64(totalMemoryCapacity)) * 100
	}

	return &ResourceUsage{
		CPUPercent:     cpuPercent,
		MemoryPercent:  memoryPercent,
		CPUUsed:        fmt.Sprintf("%dm", totalCPUUsed),
		MemoryUsed:     formatMemory(totalMemoryUsed),
		CPUCapacity:    fmt.Sprintf("%dm", totalCPUCapacity),
		MemoryCapacity: formatMemory(totalMemoryCapacity),
	}, nil
}

// GetNodeResourceUsage - 클러스터 전체 리소스 사용량 조회
func (k *KubernetesClient) GetNodeResourceUsage() (*ResourceUsage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nodes, err := k.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("Node 조회 실패: %w", err)
	}

	var totalCPUCapacity int64
	var totalMemoryCapacity int64
	var totalCPUAllocatable int64
	var totalMemoryAllocatable int64

	for _, node := range nodes.Items {
		if cpu, ok := node.Status.Capacity["cpu"]; ok {
			totalCPUCapacity += cpu.MilliValue()
		}
		if memory, ok := node.Status.Capacity["memory"]; ok {
			totalMemoryCapacity += memory.Value()
		}
		if cpu, ok := node.Status.Allocatable["cpu"]; ok {
			totalCPUAllocatable += cpu.MilliValue()
		}
		if memory, ok := node.Status.Allocatable["memory"]; ok {
			totalMemoryAllocatable += memory.Value()
		}
	}

	// 사용량 = Capacity - Allocatable
	totalCPUUsed := totalCPUCapacity - totalCPUAllocatable
	totalMemoryUsed := totalMemoryCapacity - totalMemoryAllocatable

	cpuPercent := 0.0
	memoryPercent := 0.0

	if totalCPUCapacity > 0 {
		cpuPercent = (float64(totalCPUUsed) / float64(totalCPUCapacity)) * 100
	}
	if totalMemoryCapacity > 0 {
		memoryPercent = (float64(totalMemoryUsed) / float64(totalMemoryCapacity)) * 100
	}

	return &ResourceUsage{
		CPUPercent:     cpuPercent,
		MemoryPercent:  memoryPercent,
		CPUUsed:        fmt.Sprintf("%dm", totalCPUUsed),
		MemoryUsed:     formatMemory(totalMemoryUsed),
		CPUCapacity:    fmt.Sprintf("%dm", totalCPUCapacity),
		MemoryCapacity: formatMemory(totalMemoryCapacity),
	}, nil
}

// AtoiSafe - 안전한 문자열을 정수로 변환
func AtoiSafe(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return i
}

// ParseCPUString - CPU 문자열을 milliCore로 변환
func ParseCPUString(cpuStr string) int64 {
	// "100m" → 100, "1" → 1000
	if len(cpuStr) > 0 && cpuStr[len(cpuStr)-1] == 'm' {
		val, _ := strconv.ParseInt(cpuStr[:len(cpuStr)-1], 10, 64)
		return val
	}
	val, _ := strconv.ParseFloat(cpuStr, 64)
	return int64(val * 1000)
}

// ParseMemoryString - 메모리 문자열을 바이트로 변환
func ParseMemoryString(memStr string) int64 {
	// "1Gi" → 1073741824, "1G" → 1000000000
	quantity := resource.MustParse(memStr)
	return quantity.Value()
}
