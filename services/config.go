package services

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Config - 설정 파일 구조체
type Config struct {
	ConfigSpecs []ConfigSpec `json:"config_specs"`
}

// ConfigSpec - 프로비저닝 설정
type ConfigSpec struct {
	ProvisionID         string               `json:"provision_id"`
	Enabled             string               `json:"enabled"`
	ResourceCalculation ResourceCalculation  `json:"resource_calculation"`
	ResourceAllocation  ResourceAllocation   `json:"resource_allocation,omitempty"`
	GangScheduling      GangScheduling       `json:"gang_scheduling"`
	BuildNumber         BuildNumber          `json:"build_number"`
}

// ResourceTier - 리소스 계산 티어
type ResourceTier struct {
	Name     string      `json:"name"`
	MinSize  int64       `json:"min_size,omitempty"`
	MaxSize  int64       `json:"max_size,omitempty"`
	Queue    string      `json:"queue"`
	Executor interface{} `json:"executor"` // 숫자 또는 문자열 지원
	CPU      int         `json:"cpu"`      // executor당 CPU 개수
}

// TierSelectionResult - 티어 선택 결과
type TierSelectionResult struct {
	Queue        string
	Executor     string // executor 개수 (문자열에서 int로 변환 필요)
	ExecutorInt  int    // executor 개수 (정수)
	CPU          int    // executor당 CPU 개수
	TotalSize    int64
	Metadata     *MinIOMetadata
	ObjectCount  int
}

// ResourceCalculation - 리소스 계산 설정
type ResourceCalculation struct {
	Minio string         `json:"minio"`
	Tiers []ResourceTier `json:"tiers"`
}

// Deprecated: 하위 호환성 유지를 위한 필드 (이전 설정 방식 지원)
type LegacyResourceCalculation struct {
	Minio     string `json:"minio"`
	Threshold int64  `json:"threshold"`
	MinQueue  string `json:"min_queue"`
	MaxQueue  string `json:"max_queue"`
}

// GangScheduling - Gang Scheduling 설정
type GangScheduling struct {
	CPU      string `json:"cpu"`
	Memory   string `json:"memory"`
	Executor string `json:"executor"`
}

// BuildNumber - 빌드 번호 설정
type BuildNumber struct {
	Major string `json:"major"`
	Minor string `json:"minor"`
	Patch string `json:"patch"`
}

// AllocationThreshold - 할당율 임계값
type AllocationThreshold struct {
	CPU    int    `json:"cpu"`    // 퍼센트 (예: 50)
	Memory int    `json:"memory"` // 퍼센트 (예: 50)
	Queue  string `json:"queue"`  // 큐 이름 (예: "root.ias", "root.temp")
}

// ResourceAllocation - 리소스 할당 설정
type ResourceAllocation struct {
	Enabled   bool                `json:"enabled"`
	Name      string              `json:"name"`
	Namespace string              `json:"namespace"` // 예: "common"
	Source    AllocationThreshold `json:"source"`    // source 큐 할당율 측정
	Target    AllocationThreshold `json:"target"`    // target 큐 (submit용)
}

// ResourceUsage - 리소스 사용량
type ResourceUsage struct {
	CPUPercent     float64 `json:"cpu_percent"`
	MemoryPercent  float64 `json:"memory_percent"`
	CPUUsed        string  `json:"cpu_used"`
	MemoryUsed     string  `json:"memory_used"`
	CPUCapacity    string  `json:"cpu_capacity"`
	MemoryCapacity string  `json:"memory_capacity"`
}

// ResourceAllocationResult - 리소스 할당 결과
type ResourceAllocationResult struct {
	UseAllocation bool           `json:"use_allocation"`
	Namespace     string         `json:"namespace"`
	Queue         string         `json:"queue"`
	SourceUsage   *ResourceUsage `json:"source_usage,omitempty"`
	TargetUsage   *ResourceUsage `json:"target_usage,omitempty"`
	Reason        string         `json:"reason"`
}

// MinIOMetadata - MinIO 객체 메타데이터
type MinIOMetadata struct {
	Path          string    `json:"path"`
	Size          int64     `json:"size"`
	ETag          string    `json:"etag"`
	LastModified  time.Time `json:"last_modified"`
	ContentType   string    `json:"content_type"`
	StorageClass  string    `json:"storage_class"`
	UserMetadata  map[string]string `json:"user_metadata"`
}

// MinIOConfig - MinIO 연결 설정
type MinIOConfig struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
}

// LoadConfig - config.json 로드
func LoadConfig() (*Config, error) {
	configPath := "./config/config.json"

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("config.json 파일 읽기 실패: %w", err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("config.json 파싱 실패: %w", err)
	}

	return &config, nil
}

// FindProvisionConfig - 프로비저닝 ID에 해당하는 설정 찾기
func FindProvisionConfig(config *Config, provisionID string) (*ConfigSpec, error) {
	for i := range config.ConfigSpecs {
		if config.ConfigSpecs[i].ProvisionID == provisionID {
			return &config.ConfigSpecs[i], nil
		}
	}
	return nil, fmt.Errorf("프로비저닝 ID %s를 찾을 수 없음", provisionID)
}

// IsProvisionEnabled - 프로비저닝이 활성화되어 있는지 확인
// 상위 레벨 enabled가 없는 경우 resource_calculation.enabled 또는 resource_allocation.enabled 확인
func IsProvisionEnabled(spec *ConfigSpec) bool {
	// 상위 레벨 enabled가 "true"인 경우
	if spec.Enabled == "true" {
		return true
	}

	// 상위 레벨 enabled가 없거나 "false"인 경우, 하위 설정 확인
	// resource_calculation.enabled 또는 resource_allocation.enabled가 true이면 활성화
	if spec.ResourceCalculation.Tiers != nil && len(spec.ResourceCalculation.Tiers) > 0 {
		return true
	}
	if spec.ResourceAllocation.Enabled {
		return true
	}

	return false
}

// GetMinioPath - MinIO 경로 생성: {minio_base_path}/{service_id}
// service_id의 트레일링 슬래시 유지 (폴더 모드 지원)
func GetMinioPath(minioBasePath, serviceID string) string {
	return fmt.Sprintf("%s/%s", minioBasePath, serviceID)
}

// BuildMinioPath - config의 minio 값과 service_id를 조합하여 완전한 MinIO 경로 생성
// config minio 값에 <<service_id>> 플레이스홀더가 포함되어 있음
// 예: "1234/5678/<<service_id>>/input/" + service_id "1234-wfbm" => "1234/5678/1234-wfbm/input/"
func BuildMinioPath(minioConfigPath, serviceID string) string {
	// <<service_id>> 플레이스홀더를 찾아서 service_id로 치환
	placeholder := "<<service_id>>"
	if strings.Contains(minioConfigPath, placeholder) {
		return strings.Replace(minioConfigPath, placeholder, serviceID, 1)
	}
	// 플레이스홀더가 없는 경우 기존 동작 (끝에 / 추가)
	return fmt.Sprintf("%s/%s", minioConfigPath, serviceID)
}

// CalculateQueueWithTiers - MinIO 파일/폴더 크기에 따른 3단계 티어 기반 큐 및 executor 계산 및 메타데이터 반환
// minio 경로가 "/"로 끝나면 폴더로 인식하고 모든 오브젝트 크기 합산
// minio 경로가 "/"로 끝나지 않으면 파일로 인식하고 단일 오브젝트 크기 확인
// config의 minio 값에 <<service_id>>가 포함된 경우 service_id로 치환
// 반환값: TierSelectionResult, error
func CalculateQueueWithTiers(minioConfigPath, serviceID string, tiers []ResourceTier) (*TierSelectionResult, error) {
	// MinIO 경로 생성: config의 minio 값에서 <<service_id>>를 service_id로 치환
	minioPath := BuildMinioPath(minioConfigPath, serviceID)

	// MinIO 경로가 "/"로 끝나는지 확인 (폴더 vs 파일 구분)
	var totalSize int64
	var count int
	var metadata *MinIOMetadata
	var err error

	if strings.HasSuffix(minioPath, "/") {
		// 폴더: 해당 경로의 모든 오브젝트 크기 합산
		totalSize, count, err = getMinioFolderSize(minioPath)
		if err != nil {
			// 오류 발생 시 첫 번째 티어를 기본값으로 반환
			defaultTier := getDefaultTier(tiers)
			defaultExecutorStr := "1"
			defaultExecutorInt := 1
			switch v := defaultTier.Executor.(type) {
			case string:
				defaultExecutorStr = v
				defaultExecutorInt, _ = strconv.Atoi(v)
				if defaultExecutorInt == 0 {
					defaultExecutorInt = 1
				}
			case int:
				defaultExecutorInt = v
				defaultExecutorStr = strconv.Itoa(v)
			case float64:
				defaultExecutorInt = int(v)
				defaultExecutorStr = strconv.Itoa(int(v))
			}
			return &TierSelectionResult{
				Queue:        defaultTier.Queue,
				Executor:     defaultExecutorStr,
				ExecutorInt:  defaultExecutorInt,
				CPU:          defaultTier.CPU,
				TotalSize:    0,
				Metadata:     nil,
				ObjectCount:  0,
			}, fmt.Errorf("MinIO 폴더 크기 확인 실패: %w (기본값: %s 사용)", err, defaultTier.Queue)
		}

		// 폴더 메타데이터 생성
		metadata = &MinIOMetadata{
			Path: minioPath,
			Size: totalSize,
		}
	} else {
		// 파일: 단일 오브젝트 메타데이터 확인
		metadata, err = getMinIOMetadata(minioPath)
		if err != nil {
			// 오류 발생 시 첫 번째 티어를 기본값으로 반환
			defaultTier := getDefaultTier(tiers)
			defaultExecutorStr := "1"
			defaultExecutorInt := 1
			switch v := defaultTier.Executor.(type) {
			case string:
				defaultExecutorStr = v
				defaultExecutorInt, _ = strconv.Atoi(v)
				if defaultExecutorInt == 0 {
					defaultExecutorInt = 1
				}
			case int:
				defaultExecutorInt = v
				defaultExecutorStr = strconv.Itoa(v)
			case float64:
				defaultExecutorInt = int(v)
				defaultExecutorStr = strconv.Itoa(int(v))
			}
			return &TierSelectionResult{
				Queue:        defaultTier.Queue,
				Executor:     defaultExecutorStr,
				ExecutorInt:  defaultExecutorInt,
				CPU:          defaultTier.CPU,
				TotalSize:    0,
				Metadata:     nil,
				ObjectCount:  0,
			}, fmt.Errorf("MinIO 파일 크기 확인 실패: %w (기본값: %s 사용)", err, defaultTier.Queue)
		}
		totalSize = metadata.Size
	}

	// 파일 크기에 따라 적절한 티어 선택
	selectedTier := selectTierBySize(totalSize, tiers)

	// executor 값을 문자열로 변환
	executorStr := "1" // 기본값
	switch v := selectedTier.Executor.(type) {
	case string:
		executorStr = v
	case int:
		executorStr = strconv.Itoa(v)
	case float64:
		executorStr = strconv.Itoa(int(v))
	}

	// executor 문자열을 정수로 변환
	executorInt, err := strconv.Atoi(executorStr)
	if err != nil {
		executorInt = 1 // 기본값
	}

	// CPU 값 처리
	cpuInt := selectedTier.CPU
	if cpuInt == 0 {
		// CPU 설정이 없는 경우 기본값 사용
		cpuInt = 1
	}

	return &TierSelectionResult{
		Queue:        selectedTier.Queue,
		Executor:     executorStr,
		ExecutorInt:  executorInt,
		CPU:          cpuInt,
		TotalSize:    totalSize,
		Metadata:     metadata,
		ObjectCount:  count,
	}, nil
}

// selectTierBySize - 파일 크기에 따라 적절한 티어 선택 (3단계 티어)
func selectTierBySize(size int64, tiers []ResourceTier) ResourceTier {
	if len(tiers) == 0 {
		return ResourceTier{Queue: "default", Executor: 1, CPU: 1}
	}

	// 티어를 순회하며 적절한 티어 찾기
	for _, tier := range tiers {
		// MinSize와 MaxSize 범위 확인
		inRange := true

		if tier.MinSize > 0 && size < tier.MinSize {
			inRange = false
		}

		if tier.MaxSize > 0 && size >= tier.MaxSize {
			inRange = false
		}

		if inRange {
			return tier
		}
	}

	// 범위를 벗어난 경우 가장 큰 티어 반환
	return tiers[len(tiers)-1]
}

// getDefaultQueue - 첫 번째 티어의 큐를 반환 (하위 호환성 유지)
// Deprecated: getDefaultTier 사용 권장
func getDefaultQueue(tiers []ResourceTier) string {
	if len(tiers) == 0 {
		return "default"
	}
	return tiers[0].Queue
}

// getDefaultTier - 첫 번째 티어를 기본 티어로 반환
func getDefaultTier(tiers []ResourceTier) ResourceTier {
	if len(tiers) == 0 {
		return ResourceTier{Queue: "default", Executor: 1, CPU: 1}
	}
	return tiers[0]
}

// CalculateQueueWithMetadata - MinIO 파일/폴더 크기에 따른 큐 계산 및 메타데이터 반환 (하위 호환성 유지)
// minio 경로가 "/"로 끝나면 폴더로 인식하고 모든 오브젝트 크기 합산
// minio 경로가 "/"로 끝나지 않으면 파일로 인식하고 단일 오브젝트 크기 확인
// config의 minio 값에 <<service_id>>가 포함된 경우 service_id로 치환
// 반환값: queue, totalSize, metadata, count, error (count는 폴더 내 오브젝트 개수, 파일인 경우 0)
// Deprecated: CalculateQueueWithTiers 사용 권장
func CalculateQueueWithMetadata(minioConfigPath, serviceID string, threshold int64, minQueue, maxQueue string) (string, int64, *MinIOMetadata, int, error) {
	// MinIO 경로 생성: config의 minio 값에서 <<service_id>>를 service_id로 치환
	minioPath := BuildMinioPath(minioConfigPath, serviceID)

	// MinIO 경로가 "/"로 끝나는지 확인 (폴더 vs 파일 구분)
	if strings.HasSuffix(minioPath, "/") {
		// 폴더: 해당 경로의 모든 오브젝트 크기 합산
		totalSize, count, err := getMinioFolderSize(minioPath)
		if err != nil {
			return minQueue, 0, nil, 0, fmt.Errorf("MinIO 폴더 크기 확인 실패: %w (기본값: %s 사용)", err, minQueue)
		}

		// threshold와 비교 (threshold는 바이트 단위)
		var selectedQueue string
		if totalSize < threshold {
			selectedQueue = minQueue
		} else {
			selectedQueue = maxQueue
		}

		// 폴더 메타데이터 생성
		metadata := &MinIOMetadata{
			Path: minioPath,
			Size: totalSize,
		}

		return selectedQueue, totalSize, metadata, count, nil
	} else {
		// 파일: 단일 오브젝트 메타데이터 확인
		metadata, err := getMinIOMetadata(minioPath)
		if err != nil {
			return minQueue, 0, nil, 0, fmt.Errorf("MinIO 파일 크기 확인 실패: %w (기본값: %s 사용)", err, minQueue)
		}

		// threshold와 비교 (threshold는 바이트 단위)
		if metadata.Size < threshold {
			return minQueue, metadata.Size, metadata, 0, nil
		}
		return maxQueue, metadata.Size, metadata, 0, nil
	}
}

// CalculateQueue - MinIO 파일 크기에 따른 큐 계산
func CalculateQueue(minioPath string, threshold int64, minQueue, maxQueue string) (string, int64, error) {
	// MinIO에서 파일 크기 확인 (메타데이터만)
	fileSize, err := getMinIOObjectSize(minioPath)
	if err != nil {
		// 파일이 없거나 읽기 실패 시 기본적으로 minQueue 반환
		return minQueue, 0, fmt.Errorf("MinIO 파일 크기 확인 실패: %w (기본값: %s 사용)", err, minQueue)
	}

	// threshold와 비교 (threshold는 바이트 단위)
	if fileSize < threshold {
		return minQueue, fileSize, nil
	}
	return maxQueue, fileSize, nil
}

// getMinIOMetadata - MinIO에서 객체 메타데이터 가져오기 (다운로드 없이 메타데이터만)
func getMinIOMetadata(minioPath string) (*MinIOMetadata, error) {
	// MinIO 연결 설정 (환경 변수에서 읽기)
	accessKey := os.Getenv("MINIO_ROOT_USER")
	secretKey := os.Getenv("MINIO_ROOT_PASSWORD")

	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("MinIO 환경 변수 설정 안됨 (MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)")
	}

	// MinIO 클라이언트 초기화 (로컬호스트)
	minioEndpoint := os.Getenv("MINIO_ENDPOINT")
	if minioEndpoint == "" {
		minioEndpoint = "localhost:9000"
	}

	// Remove http:// or https:// prefix if present
	minioEndpoint = strings.TrimPrefix(minioEndpoint, "http://")
	minioEndpoint = strings.TrimPrefix(minioEndpoint, "https://")

	minioClient, err := minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, fmt.Errorf("MinIO 클라이언트 초기화 실패: %w", err)
	}

	// minioPath에서 버킷과 객체 이름 파싱 (예: "bucket/object")
	bucket, object, err := parseMinioPath(minioPath)
	if err != nil {
		return nil, fmt.Errorf("MinIO 경로 파싱 실패: %w", err)
	}

	// 객체 메타데이터만 가져오기 (StatObject - 다운로드 없음)
	ctx := context.Background()
	objInfo, err := minioClient.StatObject(ctx, bucket, object, minio.StatObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("MinIO 객체 메타데이터 조회 실패: %w", err)
	}

	// 메타데이터 구조체로 변환
	metadata := &MinIOMetadata{
		Path:         minioPath,
		Size:         objInfo.Size,
		ETag:         objInfo.ETag,
		LastModified: objInfo.LastModified,
		ContentType:  objInfo.ContentType,
		StorageClass: objInfo.StorageClass,
		UserMetadata: objInfo.UserMetadata,
	}

	return metadata, nil
}

// getMinIOObjectSize - MinIO에서 객체 크기만 가져오기 (다운로드 없이 메타데이터만)
func getMinIOObjectSize(minioPath string) (int64, error) {
	metadata, err := getMinIOMetadata(minioPath)
	if err != nil {
		return 0, err
	}
	return metadata.Size, nil
}

// parseMinioPath - MinIO 경로를 버킷과 객체로 파싱
func parseMinioPath(path string) (string, string, error) {
	// "bucket/object/path" 형식에서 버킷과 객체 경로 분리
	parts := splitPath(path, "/")
	if len(parts) < 2 {
		return "", "", fmt.Errorf("잘못된 MinIO 경로 형식: %s (예: bucket/path/to/file)", path)
	}

	bucket := parts[0]
	object := joinPath(parts[1:], "/")

	return bucket, object, nil
}

// splitPath - 경로 분할 헬퍼 함수
func splitPath(s, delimiter string) []string {
	if s == "" {
		return []string{}
	}

	var parts []string
	start := 0

	for i := 0; i < len(s); i++ {
		if i+len(delimiter) <= len(s) && s[i:i+len(delimiter)] == delimiter {
			parts = append(parts, s[start:i])
			start = i + len(delimiter)
			i += len(delimiter) - 1
		}
	}

	parts = append(parts, s[start:])
	return parts
}

// joinPath - 경로 결합 헬퍼 함수
func joinPath(parts []string, delimiter string) string {
	if len(parts) == 0 {
		return ""
	}

	result := parts[0]
	for i := 1; i < len(parts); i++ {
		result += delimiter + parts[i]
	}
	return result
}

// GetExecutorInt - executor 문자열을 정수로 변환
func GetExecutorInt(executorStr string) (int, error) {
	return strconv.Atoi(executorStr)
}

// FormatBytes - 바이트를 사람이 읽기 쉬운 형식으로 변환
func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// getMinioFolderSize - MinIO 폴더(접두사) 내 모든 오브젝트의 크기 합계 계산
func getMinioFolderSize(minioPath string) (int64, int, error) {
	// MinIO 연결 설정 (환경 변수에서 읽기)
	accessKey := os.Getenv("MINIO_ROOT_USER")
	secretKey := os.Getenv("MINIO_ROOT_PASSWORD")

	if accessKey == "" || secretKey == "" {
		return 0, 0, fmt.Errorf("MinIO 환경 변수 설정 안됨 (MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)")
	}

	// MinIO 클라이언트 초기화 (로컬호스트)
	minioEndpoint := os.Getenv("MINIO_ENDPOINT")
	if minioEndpoint == "" {
		minioEndpoint = "localhost:9000"
	}

	// Remove http:// or https:// prefix if present
	minioEndpoint = strings.TrimPrefix(minioEndpoint, "http://")
	minioEndpoint = strings.TrimPrefix(minioEndpoint, "https://")

	minioClient, err := minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("MinIO 클라이언트 초기화 실패: %w", err)
	}

	// minioPath에서 버킷과 접두사 파싱 (예: "bucket/prefix/service_id/")
	bucket, prefix, err := parseMinioPath(minioPath)
	if err != nil {
		return 0, 0, fmt.Errorf("MinIO 경로 파싱 실패: %w", err)
	}

	// 접두사 뒤에 "/"가 없으면 추가 (폴더임을 명확히 하기 위해)
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	// 해당 접두사를 가진 모든 오브젝트 나열
	ctx := context.Background()
	objectCh := minioClient.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true, // 하위 폴더도 모두 검색
	})

	// 모든 오브젝트 크기 합산
	var totalSize int64 = 0
	count := 0

	for object := range objectCh {
		if object.Err != nil {
			return 0, 0, fmt.Errorf("MinIO 객체 목록 조회 실패: %w", object.Err)
		}

		// 폴더 자체(0 크기)는 제외하고 실제 파일만 합산
		if object.Size > 0 {
			totalSize += object.Size
			count++
		}
	}

	// 오브젝트가 하나도 없는 경우
	if count == 0 {
		return 0, 0, fmt.Errorf("폴더에 오브젝트가 없음: %s (총 %d개 오브젝트)", prefix, count)
	}

	return totalSize, count, nil
}
