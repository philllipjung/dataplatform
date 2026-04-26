package handlers

// CreateRequest - Create 엔드포인트 요청 구조체
type CreateRequest struct {
	ProvisionID string `json:"provision_id" binding:"required"`
	ServiceID   string `json:"service_id" binding:"required"`
	Category    string `json:"category" binding:"required"`
	Region      string `json:"region" binding:"required"`
	UID         string `json:"uid" binding:"required"`
	Arguments   string `json:"arguments" binding:"omitempty"` // Optional: 공백으로 구분된 arguments (예: "111 222 333")
}

// Response - 표준 응답 구조체
type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
	Error   *ErrorInfo  `json:"error,omitempty"`
}

// ErrorInfo - 에러 정보
type ErrorInfo struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
	Retryable bool `json:"retryable"`
}

// 에러 코드 상수
const (
	// 성공 코드 (2xx)
	CodeSuccess           = "SUCCESS"
	CodeCreated           = "CREATED"

	// 비즈니스 에러 코드 (4xx) - 재시도하지 않음
	CodeBadRequest        = "BAD_REQUEST"
	CodeNotFound          = "NOT_FOUND"
	CodeUnprocessable    = "UNPROCESSABLE"
	CodeValidationFailed  = "VALIDATION_FAILED"

	// 기술적 에러 코드 (5xx) - 재시도 가능
	CodeInternalError     = "INTERNAL_ERROR"
	CodeServiceUnavailable = "SERVICE_UNAVAILABLE"
	CodeConfigLoadFailed  = "CONFIG_LOAD_FAILED"
	CodeTemplateLoadFailed = "TEMPLATE_LOAD_FAILED"
	CodeK8sError          = "KUBERNETES_ERROR"
	CodeYuniKornError     = "YUNIKORN_ERROR"
	CodeMinIOError        = "MINIO_ERROR"
)

// ErrorResponse - 에러 응답을 생성하는 함수
func ErrorResponse(code, message, details string, retryable bool) *ErrorInfo {
	return &ErrorInfo{
		Code:      code,
		Message:   message,
		Details:   details,
		Retryable: retryable,
	}
}

// SuccessResponse - 성공 응답을 생성하는 함수
func SuccessResponse(message string, data interface{}) Response {
	return Response{
		Success: true,
		Message: message,
		Data:    data,
	}
}
