package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// YuniKornApplication - YuniKorn 애플리케이션 정보
type YuniKornApplication struct {
	ApplicationID string                `json:"applicationID"`
	QueueName     string                `json:"queueName"`
	QueuePath     string                `json:"queuePath"`
	User          string                `json:"user"`
	State         string                `json:"state"`
	UsedResource  YuniKornResources     `json:"usedResource,omitempty"`
	AllocatedResource YuniKornResources `json:"allocatedResource,omitempty"`
	RejectedMessage string              `json:"rejectedMessage,omitempty"`
	SubmissionTime int64                `json:"submissionTime,omitempty"`
	Containers    []YuniKornContainer   `json:"containers,omitempty"`
}

// YuniKornResources - YuniKorn 리소스 정보
type YuniKornResources struct {
	VCore int64 `json:"vcore"`
	Memory int64 `json:"memory"`
}

// YuniKornContainer - YuniKorn 컨테이너 정보
type YuniKornContainer struct {
	ExecutionType    string            `json:"executionType"`
	ID               string            `json:"id"`
	State            string            `json:"state"`
	AllocatedResource YuniKornResources `json:"allocatedResource,omitempty"`
	UsedResource      YuniKornResources `json:"usedResource,omitempty"`
}

// YuniKornAppsResponse - 애플리케이션 목록 응답
type YuniKornAppsResponse struct {
	Applications []YuniKornApplication `json:"applications,omitempty"`
}

func main() {
	// YuniKorn 서비스 URL
	yuniKornURL := os.Getenv("YUNIKORN_SERVICE_URL")
	if yuniKornURL == "" {
		yuniKornURL = "http://yunikorn-service:9080"
	}

	// HTTP 클라이언트 (타임아웃 10초)
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	fmt.Println("=== YuniKorn Application API Test ===")
	fmt.Printf("YuniKorn URL: %s\n\n", yuniKornURL)

	// 1. 전체 애플리케이션 목록 조회
	fmt.Println("1. GET /ws/v1/partition/default/apps")
	fmt.Println("---")
	testGetAllApps(client, yuniKornURL)
	fmt.Println()

	// 2. 특정 큐의 애플리케이션 목록 조회 (root.ias)
	queuePath := "root.ias"
	fmt.Printf("2. GET /ws/v1/partition/default/queues/%s/apps\n", queuePath)
	fmt.Println("---")
	testGetAppsByQueue(client, yuniKornURL, queuePath)
	fmt.Println()

	// 3. 특정 큐의 애플리케이션 목록 조회 (root.temp)
	queuePath = "root.temp"
	fmt.Printf("3. GET /ws/v1/partition/default/queues/%s/apps\n", queuePath)
	fmt.Println("---")
	testGetAppsByQueue(client, yuniKornURL, queuePath)
	fmt.Println()

	// 4. 애플리케이션 상태별 필터링 (Running 상태만)
	fmt.Println("4. Running 상태의 애플리케이션만 필터링")
	fmt.Println("---")
	testGetRunningApps(client, yuniKornURL)
}

// testGetAllApps - 전체 애플리케이션 목록 조회
func testGetAllApps(client *http.Client, baseURL string) {
	url := fmt.Sprintf("%s/ws/v1/partition/default/apps", baseURL)
	resp, err := client.Get(url)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("ERROR: Status %d\n%s\n", resp.StatusCode, string(body))
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}

	var appsResp YuniKornAppsResponse
	if err := json.Unmarshal(body, &appsResp); err != nil {
		fmt.Printf("ERROR: JSON parse failed: %v\n", err)
		return
	}

	fmt.Printf("Total Applications: %d\n", len(appsResp.Applications))
	if len(appsResp.Applications) > 0 {
		printApps(appsResp.Applications)
	}
}

// testGetAppsByQueue - 특정 큐의 애플리케이션 목록 조회
func testGetAppsByQueue(client *http.Client, baseURL, queuePath string) {
	// 큐 경로 포맷: root.ias → root/ias
	formattedQueue := formatQueuePath(queuePath)
	url := fmt.Sprintf("%s/ws/v1/partition/default/queues/%s/apps", baseURL, formattedQueue)

	resp, err := client.Get(url)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("ERROR: Status %d\n%s\n", resp.StatusCode, string(body))
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}

	var appsResp YuniKornAppsResponse
	if err := json.Unmarshal(body, &appsResp); err != nil {
		fmt.Printf("ERROR: JSON parse failed: %v\n", err)
		return
	}

	fmt.Printf("Applications in queue %s: %d\n", queuePath, len(appsResp.Applications))
	if len(appsResp.Applications) > 0 {
		printApps(appsResp.Applications)
	}
}

// testGetRunningApps - Running 상태의 애플리케이션만 필터링
func testGetRunningApps(client *http.Client, baseURL string) {
	url := fmt.Sprintf("%s/ws/v1/partition/default/apps", baseURL)
	resp, err := client.Get(url)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("ERROR: Status %d\n%s\n", resp.StatusCode, string(body))
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}

	var appsResp YuniKornAppsResponse
	if err := json.Unmarshal(body, &appsResp); err != nil {
		fmt.Printf("ERROR: JSON parse failed: %v\n", err)
		return
	}

	// Running 상태만 필터링
	var runningApps []YuniKornApplication
	for _, app := range appsResp.Applications {
		if app.State == "Running" {
			runningApps = append(runningApps, app)
		}
	}

	fmt.Printf("Running Applications: %d / %d\n", len(runningApps), len(appsResp.Applications))
	if len(runningApps) > 0 {
		printApps(runningApps)
	}
}

// printApps - 애플리케이션 목록 출력
func printApps(apps []YuniKornApplication) {
	for i, app := range apps {
		fmt.Printf("\n[%d] Application ID: %s\n", i+1, app.ApplicationID)
		fmt.Printf("    Queue: %s\n", app.QueuePath)
		fmt.Printf("    User: %s\n", app.User)
		fmt.Printf("    State: %s\n", app.State)

		if app.AllocatedResource.VCore > 0 || app.AllocatedResource.Memory > 0 {
			fmt.Printf("    Allocated: CPU=%dm, Memory=%s\n",
				app.AllocatedResource.VCore, formatMemory(app.AllocatedResource.Memory))
		}

		if app.UsedResource.VCore > 0 || app.UsedResource.Memory > 0 {
			fmt.Printf("    Used: CPU=%dm, Memory=%s\n",
				app.UsedResource.VCore, formatMemory(app.UsedResource.Memory))
		}

		if len(app.Containers) > 0 {
			fmt.Printf("    Containers: %d\n", len(app.Containers))
		}

		if app.RejectedMessage != "" {
			fmt.Printf("    Rejected: %s\n", app.RejectedMessage)
		}
	}
}

// formatQueuePath - 큐 경로 포맷 (root.ias → root/ias)
func formatQueuePath(queuePath string) string {
	result := ""
	for _, c := range queuePath {
		if c == '.' {
			result += "/"
		} else {
			result += string(c)
		}
	}
	return result
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
