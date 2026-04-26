package handlers

import (
	"encoding/json"
	"fmt"
	"service-common/logger"
	"service-common/metrics"
	"service-common/services"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// ReferenceRequest - Reference 엔드포인트 요청 파라미터
type ReferenceRequest struct {
	ProvisionID string
	ServiceID   string
	Category    string
	UID         string
	Arguments   string // Optional: 공백으로 구분된 arguments
}

// GetSparkReference - Reference 엔드포인트 핸들러
// GET /api/v1/spark/reference?provision_id=0001-wfbm&service_id=123456&category=tttm
func GetSparkReference(c *gin.Context) {
	// 요청 시작 시간 기록
	startTime := time.Now()

	// 쿼리 파라미터 추출
	req := parseReferenceRequest(c)

	// 필수 파라미터 검증
	if err := validateReferenceRequest(&req); err != nil {
		handleReferenceValidationError(c, startTime, &req, err.Error())
		return
	}

	logReferenceRequestReceived(&req)

	// 1. 템플릿 YAML 로드
	yamlTemplate, err := services.LoadTemplateRaw(req.ProvisionID)
	if err != nil {
		handleReferenceTemplateError(c, startTime, &req, err)
		return
	}

	// 2. config.json 로드
	config, err := services.LoadConfig()
	if err != nil {
		handleReferenceConfigError(c, startTime, &req, err)
		return
	}

	// 3. 프로비저닝 ID에 해당하는 설정 찾기
	provisionConfig, err := services.FindProvisionConfig(config, req.ProvisionID)
	if err != nil {
		handleReferenceProvisionError(c, startTime, &req, err)
		return
	}

	// 4. enabled 확인 및 처리
	if !services.IsProvisionEnabled(provisionConfig) {
		handleReferenceDisabled(c, startTime, &req, provisionConfig, yamlTemplate)
		return
	}

	// 5. 활성화 모드 처리
	handleReferenceEnabled(c, startTime, &req, provisionConfig, yamlTemplate)
}

// parseReferenceRequest extracts request parameters from query string
func parseReferenceRequest(c *gin.Context) ReferenceRequest {
	return ReferenceRequest{
		ProvisionID: c.Query("provision_id"),
		ServiceID:   c.Query("service_id"),
		Category:    c.Query("category"),
		UID:         c.Query("uid"),
		Arguments:   c.Query("arguments"),
	}
}

// validateReferenceRequest validates reference request parameters
func validateReferenceRequest(req *ReferenceRequest) error {
	if req.ProvisionID == "" || req.ServiceID == "" || req.Category == "" || req.UID == "" {
		return fmt.Errorf("필수 파라미터가 누락되었습니다. provision_id, service_id, category, uid가 모두 필요합니다")
	}
	// 서비스 아이디 정규화: _를 -로 변환
	req.ServiceID = strings.ReplaceAll(req.ServiceID, "_", "-")
	return nil
}

// logReferenceRequestReceived logs incoming reference request
func logReferenceRequestReceived(req *ReferenceRequest) {
	logger.Logger.Info("Reference 요청 수신",
		zap.String(LogFieldEndpoint, "reference"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
	)
}

// handleReferenceValidationError handles validation errors
func handleReferenceValidationError(c *gin.Context, startTime time.Time, req *ReferenceRequest, message string) {
	logger.Logger.Error("필수 파라미터 누락",
		zap.String(LogFieldEndpoint, "reference"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
	)
	metrics.RequestsTotal.WithLabelValues(req.ProvisionID, "reference", StatusError).Inc()
	metrics.RequestDuration.WithLabelValues(req.ProvisionID, "reference").Observe(time.Since(startTime).Seconds())

	response := Response{
		Success: false,
		Message: "요청 검증에 실패했습니다",
		Error:   ErrorResponse(CodeValidationFailed, message, "필수 파라미터가 누락되었습니다", false),
	}
	c.JSON(400, response)
}

// handleReferenceTemplateError handles template loading errors
func handleReferenceTemplateError(c *gin.Context, startTime time.Time, req *ReferenceRequest, err error) {
	logger.Logger.Error("템플릿 로드 실패",
		zap.String(LogFieldEndpoint, "reference"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.Error(err),
	)
	metrics.RequestsTotal.WithLabelValues(req.ProvisionID, "reference", StatusError).Inc()
	metrics.RequestDuration.WithLabelValues(req.ProvisionID, "reference").Observe(time.Since(startTime).Seconds())

	response := Response{
		Success: false,
		Message: "템플릿을 찾을 수 없습니다",
		Error:   ErrorResponse(CodeNotFound, "프로비저닝 ID에 해당하는 템플릿이 없습니다", req.ProvisionID, false),
	}
	c.JSON(404, response)
}

// handleReferenceConfigError handles config loading errors
func handleReferenceConfigError(c *gin.Context, startTime time.Time, req *ReferenceRequest, err error) {
	logger.Logger.Error("설정 로드 실패",
		zap.String(LogFieldEndpoint, "reference"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.Error(err),
	)
	metrics.RequestsTotal.WithLabelValues(req.ProvisionID, "reference", StatusError).Inc()
	metrics.RequestDuration.WithLabelValues(req.ProvisionID, "reference").Observe(time.Since(startTime).Seconds())

	response := Response{
		Success: false,
		Message: "서버 설정 로드에 실패했습니다",
		Error:   ErrorResponse(CodeConfigLoadFailed, "설정 파일을 로드할 수 없습니다", err.Error(), true),
	}
	c.JSON(500, response)
}

// handleReferenceProvisionError handles provision config errors
func handleReferenceProvisionError(c *gin.Context, startTime time.Time, req *ReferenceRequest, err error) {
	logger.Logger.Error("프로비저닝 설정 찾기 실패",
		zap.String(LogFieldEndpoint, "reference"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.Error(err),
	)
	metrics.RequestsTotal.WithLabelValues(req.ProvisionID, "reference", StatusError).Inc()
	metrics.RequestDuration.WithLabelValues(req.ProvisionID, "reference").Observe(time.Since(startTime).Seconds())

	response := Response{
		Success: false,
		Message: "프로비저닝 설정을 찾을 수 없습니다",
		Error:   ErrorResponse(CodeNotFound, "지정된 프로비저닝 ID가 존재하지 않습니다", req.ProvisionID, false),
	}
	c.JSON(404, response)
}

// handleReferenceDisabled handles disabled provision mode for reference
func handleReferenceDisabled(c *gin.Context, startTime time.Time, req *ReferenceRequest, provisionConfig *services.ConfigSpec, yamlTemplate string) {
	logger.Logger.Info("프로비저닝 비활성화 모드",
		zap.String(LogFieldEndpoint, "reference"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.String(LogFieldEnabled, provisionConfig.Enabled),
		zap.String(LogFieldReason, "disabled"),
	)

	// 메트릭 기록
	metrics.ProvisionMode.WithLabelValues(req.ProvisionID, "false").Inc()
	metrics.ResourceCalculationSkipped.WithLabelValues(req.ProvisionID, "disabled").Inc()

	// build_number 적용
	yamlTemplate = services.ApplyBuildNumberToYAML(yamlTemplate, provisionConfig.BuildNumber)

	// Arguments 적용 (사용자 제공 시)
	yamlTemplate = services.ApplyArgumentsToYAML(yamlTemplate, req.Arguments)

	// 서비스 ID 라벨 적용
	yamlOutput := services.ApplyServiceIDLabelsToYAML(yamlTemplate, req.ServiceID)

	logReferenceYAMLComplete(req, yamlOutput, startTime, false)
	recordReferenceSuccessMetrics(req.ProvisionID, startTime)

	// 클라이언트에게 YAML 응답
	sendYAMLResponse(c, yamlOutput)
}

// handleReferenceEnabled handles enabled provision mode for reference
func handleReferenceEnabled(c *gin.Context, startTime time.Time, req *ReferenceRequest, provisionConfig *services.ConfigSpec, yamlTemplate string) {
	logger.Logger.Info("프로비저닝 활성화 모드",
		zap.String(LogFieldEndpoint, "reference"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.String(LogFieldEnabled, provisionConfig.Enabled),
	)

	// 메트릭 기록
	metrics.ProvisionMode.WithLabelValues(req.ProvisionID, "true").Inc()

	// 최종 네임스페이스와 큐 결정
	var finalNamespace string
	var finalQueue string
	var executorCount int
	var fileSize int64
	var metadata *services.MinIOMetadata
	var count int

	// 1. Resource Allocation 체크 (우선 실행)
	useResourceAllocation := false
	if services.IsResourceAllocationEnabled(provisionConfig) {
		allocResult, allocErr := services.CalculateResourceAllocation(provisionConfig.ResourceAllocation)

		if allocErr != nil {
			logResourceAllocationErrorReference(req, provisionConfig, allocErr)
			metrics.ResourceAllocationDecision.WithLabelValues(req.ProvisionID, "error").Inc()
		} else if allocResult.UseAllocation {
			// Resource Allocation 조건 만족
			useResourceAllocation = true
			finalNamespace = allocResult.Namespace
			finalQueue = allocResult.Queue
			logResourceAllocationSuccessReference(req, allocResult)
			metrics.ResourceAllocationDecision.WithLabelValues(req.ProvisionID, "allocated").Inc()

			// Resource Allocation 사용 시 기본 executor 설정
			executorCount = 1
			if allocResult.SourceUsage != nil {
				// Source 사용량에 따라 executor 조정
				if allocResult.SourceUsage.CPUPercent > 80 {
					executorCount = 3
				} else if allocResult.SourceUsage.CPUPercent > 50 {
					executorCount = 2
				}
			}
		} else {
			// Resource Allocation 조건 불만족
			logResourceAllocationSkippedReference(req, allocResult)
			metrics.ResourceAllocationDecision.WithLabelValues(req.ProvisionID, "skipped").Inc()
		}
	} else {
		logResourceAllocationDisabledReference(req)
		metrics.ResourceAllocationDecision.WithLabelValues(req.ProvisionID, "disabled").Inc()
	}

	// 2. Resource Allocation 조건 불만족 시 Resource Calculation 실행
	if !useResourceAllocation {
		resourceCalcEnabled := isResourceCalculationEnabledReference(provisionConfig)

		if resourceCalcEnabled {
			// MinIO 리소스 계산 수행
			tierResult, tierErr := services.CalculateQueueWithTiers(
				provisionConfig.ResourceCalculation.Minio,
				req.ServiceID,
				provisionConfig.ResourceCalculation.Tiers,
			)

			finalQueue = tierResult.Queue
			executorCount = tierResult.ExecutorInt
			fileSize = tierResult.TotalSize
			metadata = tierResult.Metadata
			count = tierResult.ObjectCount

			logResourceCalculationReference(req, provisionConfig, finalQueue, fileSize, executorCount)

			if metadata != nil {
				logMinIOMetadataReference(req, metadata)
			}

			if tierErr != nil {
				logger.Logger.Warn("MinIO 리소스 계산 경고",
					zap.String(LogFieldEndpoint, "reference"),
					zap.String(LogFieldProvisionID, req.ProvisionID),
					zap.Error(tierErr),
				)
			}

			metrics.QueueSelection.WithLabelValues(req.ProvisionID, finalQueue).Inc()
		} else {
			// Resource Calculation 비활성화 시 기본 큐 사용
			finalQueue = "root.ias"
			executorCount = 1
			logger.Logger.Info("Resource Calculation 비활성화 - 기본 큐 사용",
				zap.String(LogFieldEndpoint, "reference"),
				zap.String(LogFieldProvisionID, req.ProvisionID),
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

	// 폴더인 경우 spark.file.count 추가
	if count > 0 {
		yamlTemplate = services.ApplySparkFileCountToYAML(yamlTemplate, count)
	}

	// 큐 설정 적용
	yamlTemplate = updateQueueInYAML(yamlTemplate, finalQueue)

	// Gang Scheduling 설정
	logGangSchedulingConfigReference(req, provisionConfig, executorCount)
	recordGangSchedulingMetrics(req.ProvisionID, provisionConfig, executorCount)

	// task-groups의 executor minMember 업데이트
	yamlTemplate = updateExecutorMinMemberInYAML(yamlTemplate, executorCount)

	// spec.executor.instances 업데이트
	yamlTemplate = services.UpdateExecutorInstances(yamlTemplate, executorCount)

	// build_number 적용
	yamlTemplate = services.ApplyBuildNumberToYAML(yamlTemplate, provisionConfig.BuildNumber)

	// Arguments 적용
	yamlTemplate = services.ApplyArgumentsToYAML(yamlTemplate, req.Arguments)

	// 서비스 ID 라벨 적용 (UID 포함)
	yamlOutput := services.ApplyServiceIDLabelsWithUIDToYAML(yamlTemplate, req.ServiceID, req.Category, req.UID)

	logReferenceYAMLComplete(req, yamlOutput, startTime, true)
	recordReferenceSuccessMetrics(req.ProvisionID, startTime)

	// 클라이언트에게 YAML 응답
	sendYAMLResponse(c, yamlOutput)
}

// handleReferenceCalculationError handles resource calculation errors
func handleReferenceCalculationError(c *gin.Context, startTime time.Time, req *ReferenceRequest, err error) {
	logger.Logger.Error("리소스 계산 실패",
		zap.String(LogFieldEndpoint, "reference"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.Error(err),
	)
	metrics.RequestsTotal.WithLabelValues(req.ProvisionID, "reference", StatusError).Inc()
	metrics.RequestDuration.WithLabelValues(req.ProvisionID, "reference").Observe(time.Since(startTime).Seconds())

	response := Response{
		Success: false,
		Message: "리소스 계산에 실패했습니다",
		Error:   ErrorResponse(CodeInternalError, "리소스 계산 중 오류 발생", err.Error(), true),
	}
	c.JSON(500, response)
}

// handleReferenceExecutorError handles executor config errors
func handleReferenceExecutorError(c *gin.Context, startTime time.Time, req *ReferenceRequest, err error) {
	logger.Logger.Error("executor 설정 변환 실패",
		zap.String(LogFieldEndpoint, "reference"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.Error(err),
	)
	metrics.RequestsTotal.WithLabelValues(req.ProvisionID, "reference", StatusError).Inc()
	metrics.RequestDuration.WithLabelValues(req.ProvisionID, "reference").Observe(time.Since(startTime).Seconds())

	response := Response{
		Success: false,
		Message: "Executor 설정 처리에 실패했습니다",
		Error:   ErrorResponse(CodeInternalError, "Executor 설정 변환 중 오류 발생", err.Error(), true),
	}
	c.JSON(500, response)
}

// logResourceCalculationReference logs resource calculation for reference
func logResourceCalculationReference(req *ReferenceRequest, config *services.ConfigSpec, queue string, fileSize int64, executorCount int) {
	logger.Logger.Info("리소스 계산 완료",
		zap.String(LogFieldEndpoint, "reference"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.String("file_path", config.ResourceCalculation.Minio),
		zap.Int64("file_size_bytes", fileSize),
		zap.String("selected_queue", queue),
		zap.Int("executor_count", executorCount),
	)
}

// logGangSchedulingConfigReference logs gang scheduling config for reference
func logGangSchedulingConfigReference(req *ReferenceRequest, config *services.ConfigSpec, executorMinMember int) {
	logger.Logger.Info("Gang Scheduling 구성",
		zap.String(LogFieldEndpoint, "reference"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.Int("executor_min_member", executorMinMember),
		zap.String("cpu", config.GangScheduling.CPU),
		zap.String("memory", config.GangScheduling.Memory),
	)
}

// logReferenceYAMLComplete logs YAML completion with full YAML content
func logReferenceYAMLComplete(req *ReferenceRequest, yamlOutput string, startTime time.Time, enabled bool) {
	mode := "비활성화 모드"
	if enabled {
		mode = "활성화 모드"
	}

	logger.Logger.Info(fmt.Sprintf("YAML 반환 완료 (%s)", mode),
		zap.String(LogFieldEndpoint, "reference"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String(LogFieldCategory, req.Category),
		zap.Float64(LogFieldDurationMs, float64(time.Since(startTime).Milliseconds())),
	)

	// YAML 내용을 로그에 출력
	logger.Logger.Info(fmt.Sprintf("생성된 YAML (%s)", mode),
		zap.String(LogFieldEndpoint, "reference"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.String("content", yamlOutput),
	)
}

// recordReferenceSuccessMetrics records success metrics for reference
func recordReferenceSuccessMetrics(provisionID string, startTime time.Time) {
	metrics.RequestsTotal.WithLabelValues(provisionID, "reference", StatusSuccess).Inc()
	metrics.RequestDuration.WithLabelValues(provisionID, "reference").Observe(time.Since(startTime).Seconds())
}

// sendYAMLResponse sends YAML response to client
func sendYAMLResponse(c *gin.Context, yamlOutput string) {
	c.Header("Content-Type", "application/x-yaml")
	c.String(200, yamlOutput)
}

// logMinIOMetadataReference - MinIO 파일 메타데이터 로그 (5번째 로그)
func logMinIOMetadataReference(req *ReferenceRequest, metadata *services.MinIOMetadata) {
	metadataLog := map[string]interface{}{
		"log_type":       "minio_metadata",
		"endpoint":       "reference",
		"provision_id":   req.ProvisionID,
		"service_id":     req.ServiceID,
		"minio_path":     metadata.Path,
		"size_bytes":     metadata.Size,
		"size_formatted": services.FormatBytes(metadata.Size),
		"etag":           metadata.ETag,
		"last_modified":   metadata.LastModified.Format(time.RFC3339),
		"content_type":   metadata.ContentType,
		"storage_class":  metadata.StorageClass,
		"user_metadata":  metadata.UserMetadata,
		"fetched_at":     time.Now().Format(time.RFC3339),
	}

	logJSON, _ := json.Marshal(metadataLog)
	logger.Logger.Info(string(logJSON))
}

// isResourceCalculationEnabledReference checks if resource calculation is enabled
func isResourceCalculationEnabledReference(config *services.ConfigSpec) bool {
	return true
}

// logResourceAllocationSuccessReference logs successful resource allocation
func logResourceAllocationSuccessReference(req *ReferenceRequest, result *services.ResourceAllocationResult) {
	fields := []zap.Field{
		zap.String(LogFieldEndpoint, "reference"),
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

// logResourceAllocationSkippedReference logs resource allocation skip
func logResourceAllocationSkippedReference(req *ReferenceRequest, result *services.ResourceAllocationResult) {
	fields := []zap.Field{
		zap.String(LogFieldEndpoint, "reference"),
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

// logResourceAllocationErrorReference logs resource allocation error
func logResourceAllocationErrorReference(req *ReferenceRequest, config *services.ConfigSpec, err error) {
	logger.Logger.Error("Resource Allocation 계산 실패",
		zap.String(LogFieldEndpoint, "reference"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
		zap.Error(err),
	)
}

// logResourceAllocationDisabledReference logs resource allocation disabled
func logResourceAllocationDisabledReference(req *ReferenceRequest) {
	logger.Logger.Info("Resource Allocation 비활성화",
		zap.String(LogFieldEndpoint, "reference"),
		zap.String(LogFieldProvisionID, req.ProvisionID),
		zap.String(LogFieldServiceID, req.ServiceID),
	)
}

