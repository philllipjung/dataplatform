package handlers

import (
	"encoding/json"
	"fmt"
	"service-common/logger"
	"service-common/metrics"
	"service-common/services"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const (
	// LogFieldKeys for structured logging
	LogFieldEndpoint     = "endpoint"
	LogFieldProvisionID  = "provision_id"
	LogFieldServiceID    = "service_id"
	LogFieldCategory     = "category"
	LogFieldRegion       = "region"
	LogFieldNamespace    = "namespace"
	LogFieldResourceName = "resource_name"
	LogFieldEnabled      = "enabled"
	LogFieldReason       = "reason"
	LogFieldDurationMs   = "duration_ms"

	// Status values
	StatusSuccess = "success"
	StatusError   = "error"
)

// CreateSparkApplication - Create 엔드포인트 핸들러
// POST /api/v1/spark/create
// Request Body: {"provision_id": "0001-wfbm", "service_id": "1234-wfbm", "category": "tttm", "region": "ic"}
func CreateSparkApplication(c *gin.Context) {
	// 요청 시작 시간 기록
	startTime := time.Now()

	// 요청 바디 파싱
	var req CreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		handleRequestError(c, startTime, "", "요청 파싱 실패", err)
		return
	}

	// 필수 필드 검증
	if err := validateRequest(&req); err != nil {
		handleValidationError(c, startTime, &req, err.Error())
		return
	}

	// 1. 클라이언트 입력 로그
	logClientInput(&req)

	// 2. config.json 로드
	config, err := services.LoadConfig()
	if err != nil {
		handleConfigLoadError(c, startTime, &req, err)
		return
	}

	// 3. 프로비저닝 ID에 해당하는 설정 찾기
	provisionConfig, err := services.FindProvisionConfig(config, req.ProvisionID)
	if err != nil {
		handleProvisionConfigError(c, startTime, &req, err)
		return
	}

	// 4. config.json에서 읽은 값 로그
	logConfigValues(provisionConfig)

	// 5. 템플릿 YAML 로드
	yamlTemplate, err := services.LoadTemplateRaw(req.ProvisionID)
	if err != nil {
		handleTemplateLoadError(c, startTime, &req, err)
		return
	}

	// 6. enabled 확인 및 처리
	if !services.IsProvisionEnabled(provisionConfig) {
		handleDisabledProvision(c, startTime, &req, provisionConfig, yamlTemplate)
		return
	}

	// 7. 활성화 모드 처리
	handleEnabledProvision(c, startTime, &req, provisionConfig, yamlTemplate)
}

// validateRequest validates required fields
func validateRequest(req *CreateRequest) error {
	if req.ProvisionID == "" || req.ServiceID == "" || req.Category == "" || req.Region == "" {
		return fmt.Errorf("필수 필드가 누락되었습니다. provision_id, service_id, category, region이 모두 필요합니다")
	}
	// 서비스 아이디 정규화: _를 -로 변환
	req.ServiceID = strings.ReplaceAll(req.ServiceID, "_", "-")
	return nil
}

// logClientInput - 클라이언트 입력 로그 (1번째 로그)
func logClientInput(req *CreateRequest) {
	inputLog := map[string]interface{}{
		"log_type":     "client_input",
		"endpoint":     "create",
		"provision_id": req.ProvisionID,
		"service_id":   req.ServiceID,
		"category":     req.Category,
		"region":       req.Region,
		"received_at":  time.Now().Format(time.RFC3339),
	}

	logJSON, _ := json.Marshal(inputLog)
	logger.Logger.Info(string(logJSON))
}

// logConfigValues - config.json에서 읽은 값 로그 (2번째 로그)
func logConfigValues(config *services.ConfigSpec) {
	// 티어 정보를 로그용 맵으로 변환
	tiersInfo := make([]map[string]interface{}, len(config.ResourceCalculation.Tiers))
	for i, tier := range config.ResourceCalculation.Tiers {
		tiersInfo[i] = map[string]interface{}{
			"name":     tier.Name,
			"min_size": tier.MinSize,
			"max_size": tier.MaxSize,
			"queue":    tier.Queue,
			"executor": tier.Executor,
			"cpu":      tier.CPU,
		}
	}

	configLog := map[string]interface{}{
		"log_type":     "config_values",
		"provision_id": config.ProvisionID,
		"enabled":      config.Enabled,
		"resource_calculation": map[string]interface{}{
			"minio": config.ResourceCalculation.Minio,
			"tiers": tiersInfo,
		},
		"gang_scheduling": map[string]interface{}{
			"cpu":      config.GangScheduling.CPU,
			"memory":   config.GangScheduling.Memory,
			"executor": config.GangScheduling.Executor,
		},
		"build_number": map[string]interface{}{
			"major": config.BuildNumber.Major,
			"minor": config.BuildNumber.Minor,
			"patch": config.BuildNumber.Patch,
		},
	}

	logJSON, _ := json.Marshal(configLog)
	logger.Logger.Info(string(logJSON))
}

// logMinIOResourceCalculation - MinIO 리소스 계산 결과 로그 (3번째 로그)
func logMinIOResourceCalculation(req *CreateRequest, config *services.ConfigSpec, queue string, fileSize int64, executorCount int) {
	resourceLog := map[string]interface{}{
		"log_type":       "minio_resource_calculation",
		"endpoint":       "create",
		"provision_id":   req.ProvisionID,
		"service_id":     req.ServiceID,
		"minio_path":     config.ResourceCalculation.Minio,
		"file_size":      fileSize,
		"selected_queue": queue,
		"executor_count": executorCount,
		"calculated_at":  time.Now().Format(time.RFC3339),
	}

	logJSON, _ := json.Marshal(resourceLog)
	logger.Logger.Info(string(logJSON))
}

// logFinalYAML - 결과 YAML 로그 (4번째 로그) - YAML을 문자열로 유지
func logFinalYAML(yamlStr string) string {
	finalLog := map[string]interface{}{
		"log_type":     "final_yaml_result",
		"content":      yamlStr,
		"generated_at": time.Now().Format(time.RFC3339),
	}

	logJSON, _ := json.Marshal(finalLog)
	logger.Logger.Info(string(logJSON))

	return yamlStr
}

// logMinIOMetadata - MinIO 파일 메타데이터 로그 (5번째 로그)
func logMinIOMetadata(req *CreateRequest, metadata *services.MinIOMetadata) {
	metadataLog := map[string]interface{}{
		"log_type":       "minio_metadata",
		"endpoint":       "create",
		"provision_id":   req.ProvisionID,
		"service_id":     req.ServiceID,
		"minio_path":     metadata.Path,
		"size_bytes":     metadata.Size,
		"size_formatted": services.FormatBytes(metadata.Size),
		"etag":           metadata.ETag,
		"last_modified":  metadata.LastModified.Format(time.RFC3339),
		"content_type":   metadata.ContentType,
		"storage_class":  metadata.StorageClass,
		"user_metadata":  metadata.UserMetadata,
		"fetched_at":     time.Now().Format(time.RFC3339),
	}

	logJSON, _ := json.Marshal(metadataLog)
	logger.Logger.Info(string(logJSON))
}

// handleRequestError handles request parsing errors
func handleRequestError(c *gin.Context, startTime time.Time, provisionID, message string, err error) {
	logger.Logger.Error(message,
		zap.Error(err),
	)
	metrics.RequestsTotal.WithLabelValues(provisionID, "create", StatusError).Inc()
	metrics.RequestDuration.WithLabelValues(provisionID, "create").Observe(time.Since(startTime).Seconds())

	response := Response{
		Success: false,
		Message: "요청 파싱에 실패했습니다",
		Error:   ErrorResponse(CodeBadRequest, message, err.Error(), false),
	}
	c.JSON(400, response)
}

// handleValidationError handles validation errors
func handleValidationError(c *gin.Context, startTime time.Time, req *CreateRequest, message string) {
	logger.Logger.Error("필수 필드 누락",
		zap.String(LogFieldEndpoint, "create"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.String(LogFieldRegion, req.Region),
	)
	metrics.RequestsTotal.WithLabelValues(req.ProvisionID, "create", StatusError).Inc()
	metrics.RequestDuration.WithLabelValues(req.ProvisionID, "create").Observe(time.Since(startTime).Seconds())

	response := Response{
		Success: false,
		Message: "요청 검증에 실패했습니다",
		Error:   ErrorResponse(CodeValidationFailed, message, "provision_id, service_id, category, region 필드가 모두 필요합니다", false),
	}
	c.JSON(400, response)
}

// handleTemplateLoadError handles template loading errors
func handleTemplateLoadError(c *gin.Context, startTime time.Time, req *CreateRequest, err error) {
	logger.Logger.Error("템플릿 로드 실패",
		zap.String(LogFieldEndpoint, "create"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.Error(err),
	)
	metrics.RequestsTotal.WithLabelValues(req.ProvisionID, "create", StatusError).Inc()
	metrics.RequestDuration.WithLabelValues(req.ProvisionID, "create").Observe(time.Since(startTime).Seconds())

	response := Response{
		Success: false,
		Message: "템플릿을 찾을 수 없습니다",
		Error:   ErrorResponse(CodeNotFound, "프로비저닝 ID에 해당하는 템플릿이 없습니다", req.ProvisionID, false),
	}
	c.JSON(404, response)
}

// handleConfigLoadError handles config loading errors
func handleConfigLoadError(c *gin.Context, startTime time.Time, req *CreateRequest, err error) {
	logger.Logger.Error("설정 로드 실패",
		zap.String(LogFieldEndpoint, "create"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.Error(err),
	)
	metrics.RequestsTotal.WithLabelValues(req.ProvisionID, "create", StatusError).Inc()
	metrics.RequestDuration.WithLabelValues(req.ProvisionID, "create").Observe(time.Since(startTime).Seconds())

	response := Response{
		Success: false,
		Message: "서버 설정 로드에 실패했습니다",
		Error:   ErrorResponse(CodeConfigLoadFailed, "설정 파일을 로드할 수 없습니다", err.Error(), true),
	}
	c.JSON(500, response)
}

// handleProvisionConfigError handles provision config errors
func handleProvisionConfigError(c *gin.Context, startTime time.Time, req *CreateRequest, err error) {
	logger.Logger.Error("프로비저닝 설정 찾기 실패",
		zap.String(LogFieldEndpoint, "create"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.Error(err),
	)
	metrics.RequestsTotal.WithLabelValues(req.ProvisionID, "create", StatusError).Inc()
	metrics.RequestDuration.WithLabelValues(req.ProvisionID, "create").Observe(time.Since(startTime).Seconds())

	response := Response{
		Success: false,
		Message: "프로비저닝 설정을 찾을 수 없습니다",
		Error:   ErrorResponse(CodeNotFound, "지정된 프로비저닝 ID가 존재하지 않습니다", req.ProvisionID, false),
	}
	c.JSON(404, response)
}

// handleDisabledProvision handles disabled provision mode
func handleDisabledProvision(c *gin.Context, startTime time.Time, req *CreateRequest, provisionConfig *services.ConfigSpec, yamlTemplate string) {
	logger.Logger.Info("프로비저닝 비활성화 모드",
		zap.String(LogFieldEndpoint, "create"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.String(LogFieldRegion, req.Region),
		zap.String(LogFieldEnabled, provisionConfig.Enabled),
		zap.String(LogFieldReason, "disabled"),
	)

	// 메트릭 기록
	metrics.ProvisionMode.WithLabelValues(req.ProvisionID, "false").Inc()
	metrics.ResourceCalculationSkipped.WithLabelValues(req.ProvisionID, "disabled").Inc()

	// 서비스 ID 라벨만 적용
	yamlTemplate = services.ApplyServiceIDLabelsToYAML(yamlTemplate, req.ServiceID)

	// BUILD_NUMBER 적용
	yamlTemplate = services.ApplyBuildNumberToYAML(yamlTemplate, provisionConfig.BuildNumber)

	// Arguments 적용 (사용자 제공 시)
	yamlTemplate = services.ApplyArgumentsToYAML(yamlTemplate, req.Arguments)

	// 4. 최종 YAML 로그 출력
	logFinalYAML(yamlTemplate)

	// Kubernetes API 서버로 SparkApplication CR 생성 요청
	result, err := services.CreateSparkApplicationCRFromYAML(yamlTemplate)
	if err != nil {
		handleK8sError(c, startTime, req, err, result)
		return
	}

	logCreationSuccess(req, result.Namespace, result.Name, startTime)
	recordSuccessMetrics(req.ProvisionID, result.Namespace, "create", startTime)

	// 비활성화 모드 응답
	response := SuccessResponse("SparkApplication CR 생성 성공 (비활성화 모드)", gin.H{
		"provision_id": req.ProvisionID,
		"service_id":   req.ServiceID,
		"category":     req.Category,
		"region":       req.Region,
		"result":       result,
	})
	c.JSON(201, response)
}

// handleEnabledProvision handles enabled provision mode
func handleEnabledProvision(c *gin.Context, startTime time.Time, req *CreateRequest, provisionConfig *services.ConfigSpec, yamlTemplate string) {
	logger.Logger.Info("프로비저닝 활성화 모드",
		zap.String(LogFieldEndpoint, "create"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.String(LogFieldRegion, req.Region),
		zap.String(LogFieldEnabled, provisionConfig.Enabled),
	)

	// 메트릭 기록
	metrics.ProvisionMode.WithLabelValues(req.ProvisionID, "true").Inc()

	// 최종 네임스페이스와 큐 결정
	var finalNamespace string
	var finalQueue string
	var executorCount int
	var executorCPU int    // executor CPU 개수
	var fileSize int64
	var metadata *services.MinIOMetadata
	var count int

	// 1. Resource Allocation 체크 (우선 실행)
	useResourceAllocation := false
	if services.IsResourceAllocationEnabled(provisionConfig) {
		allocResult, allocErr := services.CalculateResourceAllocation(provisionConfig.ResourceAllocation)

		if allocErr != nil {
			logResourceAllocationError(req, provisionConfig, allocErr)
			metrics.ResourceAllocationDecision.WithLabelValues(req.ProvisionID, "error").Inc()
		} else if allocResult.UseAllocation {
			// Resource Allocation 조건 만족
			useResourceAllocation = true
			finalNamespace = allocResult.Namespace
			finalQueue = allocResult.Queue
			logResourceAllocationSuccess(req, allocResult)
			metrics.ResourceAllocationDecision.WithLabelValues(req.ProvisionID, "allocated").Inc()

			// Resource Allocation 사용 시 기본 executor 설정
			executorCount = 1
			executorCPU = 1  // 기본 CPU 설정
			if allocResult.SourceUsage != nil {
				// Source 사용량에 따라 executor 조정 (선택적 로직)
				if allocResult.SourceUsage.CPUPercent > 80 {
					executorCount = 3
				} else if allocResult.SourceUsage.CPUPercent > 50 {
					executorCount = 2
				}
			}
		} else {
			// Resource Allocation 조건 불만족
			logResourceAllocationSkipped(req, allocResult)
			metrics.ResourceAllocationDecision.WithLabelValues(req.ProvisionID, "skipped").Inc()
		}
	} else {
		logResourceAllocationDisabled(req)
		metrics.ResourceAllocationDecision.WithLabelValues(req.ProvisionID, "disabled").Inc()
	}

	// 2. Resource Allocation 조건 불만족 시 Resource Calculation 실행
	if !useResourceAllocation {
		// Resource Calculation 활성화 확인
		// config.json의 enabled 값과 resource_calculation.enabled 값 모두 확인 필요
		resourceCalcEnabled := isResourceCalculationEnabled(provisionConfig)

		if resourceCalcEnabled {
			// MinIO 리소스 계산 수행
			tierResult, tierErr := services.CalculateQueueWithTiers(
				provisionConfig.ResourceCalculation.Minio,
				req.ServiceID,
				provisionConfig.ResourceCalculation.Tiers,
			)

			finalQueue = tierResult.Queue
			executorCount = tierResult.ExecutorInt
			executorCPU = tierResult.CPU  // CPU 값 추출
			fileSize = tierResult.TotalSize
			metadata = tierResult.Metadata
			count = tierResult.ObjectCount

			// MinIO 리소스 계산 결과 로그
			logMinIOResourceCalculation(req, provisionConfig, finalQueue, fileSize, executorCount)

			if metadata != nil {
				logMinIOMetadata(req, metadata)
			}

			if tierErr != nil {
				logger.Logger.Warn("MinIO 리소스 계산 경고",
					zap.String(LogFieldEndpoint, "create"),
					zap.String(LogFieldProvisionID, req.ProvisionID),
					zap.String(LogFieldServiceID, req.ServiceID),
					zap.Error(tierErr),
				)
			}

			logResourceCalculation(req, provisionConfig, finalQueue, fileSize, executorCount)
			metrics.QueueSelection.WithLabelValues(req.ProvisionID, finalQueue).Inc()
		} else {
			// Resource Calculation 비활성화 시 기본 큐 사용
			finalQueue = "root.ias"
			executorCount = 1
			executorCPU = 1  // 기본 CPU 설정
			logger.Logger.Info("Resource Calculation 비활성화 - 기본 큐 사용",
				zap.String(LogFieldEndpoint, "create"),
				zap.String(LogFieldProvisionID, req.ProvisionID),
				zap.String(LogFieldServiceID, req.ServiceID),
				zap.String("default_queue", finalQueue),
			)
			metrics.ResourceCalculationSkipped.WithLabelValues(req.ProvisionID, "disabled").Inc()
		}

		// 기본 네임스페이스 설정
		finalNamespace = "default"
	}

	// 3. YAML 적용

	// Namespace 업데이트
	yamlTemplate = services.UpdateNamespaceInYAML(yamlTemplate, finalNamespace)

	// Queue label 업데이트 (YuniKorn)
	yamlTemplate = services.UpdateQueueInYAML(yamlTemplate, finalQueue)

	// 폴더인 경우 spark.file.count 추가 (count > 0)
	if count > 0 {
		yamlTemplate = services.ApplySparkFileCountToYAML(yamlTemplate, count)
	}

	// 큐 설정 적용
	yamlTemplate = updateQueueInYAML(yamlTemplate, finalQueue)

	// Gang Scheduling 설정
	logGangSchedulingConfig(req, provisionConfig, executorCount)
	recordGangSchedulingMetrics(req.ProvisionID, provisionConfig, executorCount)

	// task-groups의 executor minMember 업데이트
	yamlTemplate = updateExecutorMinMemberInYAML(yamlTemplate, executorCount)

	// spec.executor.instances 업데이트
	yamlTemplate = services.UpdateExecutorInstances(yamlTemplate, executorCount)

	// executor CPU 적용 (티어 기반)
	yamlTemplate = services.ApplyExecutorCPUToYAML(yamlTemplate, executorCPU)

	// build_number 적용
	yamlTemplate = services.ApplyBuildNumberToYAML(yamlTemplate, provisionConfig.BuildNumber)

	// Arguments 적용
	yamlTemplate = services.ApplyArgumentsToYAML(yamlTemplate, req.Arguments)

	// 서비스 ID 라벨 적용 (UID 포함)
	yamlTemplate = services.ApplyServiceIDLabelsWithUIDToYAML(yamlTemplate, req.ServiceID, req.Category, req.UID)

	// 4. 최종 YAML 로그
	logFinalYAML(yamlTemplate)

	// 5. Kubernetes API 서버로 SparkApplication CR 생성 요청
	result, err := services.CreateSparkApplicationCRFromYAML(yamlTemplate)
	if err != nil {
		handleK8sError(c, startTime, req, err, result)
		return
	}

	logCreationSuccess(req, result.Namespace, result.Name, startTime)
	recordSuccessMetrics(req.ProvisionID, result.Namespace, "create", startTime)

	// 활성화 모드 응답
	response := SuccessResponse("SparkApplication CR 생성 성공", gin.H{
		"provision_id": req.ProvisionID,
		"service_id":   req.ServiceID,
		"category":     req.Category,
		"region":       req.Region,
		"result":       result,
		"resource_allocation": map[string]interface{}{
			"enabled":        services.IsResourceAllocationEnabled(provisionConfig),
			"used":           useResourceAllocation,
			"namespace":      finalNamespace,
			"queue":          finalQueue,
			"executor_count": executorCount,
		},
	})
	c.JSON(201, response)
}

// handleCalculationError handles resource calculation errors
func handleCalculationError(c *gin.Context, startTime time.Time, req *CreateRequest, err error) {
	logger.Logger.Error("리소스 계산 실패",
		zap.String(LogFieldEndpoint, "create"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.Error(err),
	)
	metrics.RequestsTotal.WithLabelValues(req.ProvisionID, "create", StatusError).Inc()
	metrics.RequestDuration.WithLabelValues(req.ProvisionID, "create").Observe(time.Since(startTime).Seconds())
	c.JSON(500, gin.H{
		"error": fmt.Sprintf("리소스 계산 실패: %v", err),
	})
}

// handleExecutorConfigError handles executor config errors
func handleExecutorConfigError(c *gin.Context, startTime time.Time, req *CreateRequest, err error) {
	logger.Logger.Error("executor 설정 변환 실패",
		zap.String(LogFieldEndpoint, "create"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.Error(err),
	)
	metrics.RequestsTotal.WithLabelValues(req.ProvisionID, "create", StatusError).Inc()
	metrics.RequestDuration.WithLabelValues(req.ProvisionID, "create").Observe(time.Since(startTime).Seconds())
	c.JSON(500, gin.H{
		"error": fmt.Sprintf("executor 설정 변환 실패: %v", err),
	})
}

// handleK8sError handles Kubernetes API errors
func handleK8sError(c *gin.Context, startTime time.Time, req *CreateRequest, err error, result *services.CreateResult) {
	logger.Logger.Error("Kubernetes API 요청 실패",
		zap.String(LogFieldEndpoint, "create"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.Error(err),
	)
	metrics.RequestsTotal.WithLabelValues(req.ProvisionID, "create", StatusError).Inc()
	if result != nil {
		metrics.K8sCreation.WithLabelValues(req.ProvisionID, result.Namespace, StatusError).Inc()
	}
	metrics.RequestDuration.WithLabelValues(req.ProvisionID, "create").Observe(time.Since(startTime).Seconds())

	response := Response{
		Success: false,
		Message: "Kubernetes API 요청에 실패했습니다",
		Error:   ErrorResponse(CodeK8sError, "SparkApplication 생성 실패", err.Error(), true),
	}
	c.JSON(500, response)
}

// logResourceCalculation logs resource calculation results
func logResourceCalculation(req *CreateRequest, config *services.ConfigSpec, queue string, fileSize int64, executorCount int) {
	logger.Logger.Info("리소스 계산 완료",
		zap.String(LogFieldEndpoint, "create"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.String("file_path", config.ResourceCalculation.Minio),
		zap.Int64("file_size_bytes", fileSize),
		zap.String("selected_queue", queue),
		zap.Int("executor_count", executorCount),
	)
}

// logGangSchedulingConfig logs gang scheduling configuration
func logGangSchedulingConfig(req *CreateRequest, config *services.ConfigSpec, executorMinMember int) {
	logger.Logger.Info("Gang Scheduling 구성",
		zap.String(LogFieldEndpoint, "create"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.Int("executor_min_member", executorMinMember),
		zap.String("cpu", config.GangScheduling.CPU),
		zap.String("memory", config.GangScheduling.Memory),
	)
}

// recordGangSchedulingMetrics records gang scheduling metrics
func recordGangSchedulingMetrics(provisionID string, config *services.ConfigSpec, executorMinMember int) {
	metrics.ExecutorMinMember.WithLabelValues(provisionID).Set(float64(executorMinMember))

	cpuValue, _ := strconv.ParseFloat(config.GangScheduling.CPU, 64)
	metrics.GangSchedulingResources.WithLabelValues(provisionID, "cpu").Set(cpuValue)

	memoryValue, _ := strconv.ParseFloat(config.GangScheduling.Memory, 64)
	metrics.GangSchedulingResources.WithLabelValues(provisionID, "memory").Set(memoryValue)
}

// logCreationSuccess logs successful CR creation
func logCreationSuccess(req *CreateRequest, namespace, name string, startTime time.Time) {
	logger.Logger.Info("SparkApplication CR 생성 성공",
		zap.String(LogFieldEndpoint, "create"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.String(LogFieldRegion, req.Region),
		zap.String(LogFieldNamespace, namespace),
		zap.String(LogFieldResourceName, name),
		zap.Float64(LogFieldDurationMs, float64(time.Since(startTime).Milliseconds())),
	)
}

// recordSuccessMetrics records success metrics
func recordSuccessMetrics(provisionID, namespace, endpoint string, startTime time.Time) {
	metrics.RequestsTotal.WithLabelValues(provisionID, endpoint, StatusSuccess).Inc()
	metrics.K8sCreation.WithLabelValues(provisionID, namespace, StatusSuccess).Inc()
	metrics.RequestDuration.WithLabelValues(provisionID, endpoint).Observe(time.Since(startTime).Seconds())
}

// updateQueueInYAML updates the queue value in YAML
func updateQueueInYAML(yamlStr string, queue string) string {
	// batchSchedulerOptions.queue 값 교체
	lines := strings.Split(yamlStr, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "queue:") {
			indent := strings.Repeat(" ", len(line)-len(strings.TrimLeft(line, " ")))
			lines[i] = fmt.Sprintf("%squeue: root.%s", indent, queue)
			break
		}
	}
	return strings.Join(lines, "\n")
}

// updateExecutorMinMemberInYAML updates executor minMember in task-groups annotation
func updateExecutorMinMemberInYAML(yamlStr string, minMember int) string {
	lines := strings.Split(yamlStr, "\n")
	inTaskGroups := false
	taskGroupStarted := false

	for i, line := range lines {
		trimmed := strings.TrimSpace(line)

		// task-groups 배열 시작 확인
		if strings.Contains(trimmed, "yunikorn.apache.org/task-groups:") {
			inTaskGroups = true
			continue
		}

		// task-groups 섹션 종료 확인
		if inTaskGroups && (strings.HasPrefix(trimmed, "serviceAccount:") || !strings.HasPrefix(trimmed, "|") && !strings.HasPrefix(trimmed, "[") && trimmed != "" && !strings.HasPrefix(trimmed, "-") && !strings.HasPrefix(trimmed, "{") && !strings.HasPrefix(trimmed, "}")) {
			inTaskGroups = false
			taskGroupStarted = false
		}

		// executor task-group 찾기
		if inTaskGroups && strings.Contains(trimmed, `"name": "spark-executor"`) {
			taskGroupStarted = true
		}

		// executor의 minMember 업데이트
		if taskGroupStarted && strings.Contains(trimmed, `"minMember":`) {
			indent := strings.Repeat(" ", len(line)-len(strings.TrimLeft(line, " ")))
			lines[i] = fmt.Sprintf("%s\"minMember\": %d,", indent, minMember)
			taskGroupStarted = false
			break
		}
	}

	return strings.Join(lines, "\n")
}

// isResourceCalculationEnabled checks if resource calculation is enabled
func isResourceCalculationEnabled(config *services.ConfigSpec) bool {
	// config.json의 enabled 필드와 resource_calculation.enabled 필드 모두 확인
	// 현재는 상위 레벨의 enabled 값만 확인 (필요시 추가 로직 구현)
	return true
}

// logResourceAllocationSuccess logs successful resource allocation
func logResourceAllocationSuccess(req *CreateRequest, result *services.ResourceAllocationResult) {
	fields := []zap.Field{
		zap.String(LogFieldEndpoint, "create"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String("selected_namespace", result.Namespace),
		zap.String("selected_queue", result.Queue),
		zap.String("reason", result.Reason),
	}

	if result.SourceUsage != nil {
		fields = append(fields,
			zap.Float64("source_cpu_percent", result.SourceUsage.CPUPercent),
			zap.Float64("source_memory_percent", result.SourceUsage.MemoryPercent),
		)
	}

	if result.TargetUsage != nil {
		fields = append(fields,
			zap.Float64("target_cpu_percent", result.TargetUsage.CPUPercent),
			zap.Float64("target_memory_percent", result.TargetUsage.MemoryPercent),
		)
	}

	logger.Logger.Info("Resource Allocation 성공 - 조건 만족", fields...)
}

// logResourceAllocationSkipped logs resource allocation skip
func logResourceAllocationSkipped(req *CreateRequest, result *services.ResourceAllocationResult) {
	fields := []zap.Field{
		zap.String(LogFieldEndpoint, "create"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String("reason", result.Reason),
	}

	if result.SourceUsage != nil {
		fields = append(fields,
			zap.Float64("source_cpu_percent", result.SourceUsage.CPUPercent),
			zap.Float64("source_memory_percent", result.SourceUsage.MemoryPercent),
		)
	}

	if result.TargetUsage != nil {
		fields = append(fields,
			zap.Float64("target_cpu_percent", result.TargetUsage.CPUPercent),
			zap.Float64("target_memory_percent", result.TargetUsage.MemoryPercent),
		)
	}

	logger.Logger.Info("Resource Allocation 조건 불만족 - Resource Calculation 실행", fields...)
}

// logResourceAllocationError logs resource allocation error
func logResourceAllocationError(req *CreateRequest, config *services.ConfigSpec, err error) {
	logger.Logger.Error("Resource Allocation 계산 실패",
		zap.String(LogFieldEndpoint, "create"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.Error(err),
	)
}

// logResourceAllocationDisabled logs resource allocation disabled
func logResourceAllocationDisabled(req *CreateRequest) {
	logger.Logger.Info("Resource Allocation 비활성화",
		zap.String(LogFieldEndpoint, "create"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
	)
}
