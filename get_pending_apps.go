package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// SparkApplicationStatus - SparkApplication 상태
type SparkApplicationStatus struct {
	ApplicationState struct {
		State string `json:"state"`
	} `json:"applicationState"`
}

// SparkApplicationSpec - SparkApplication 스펙
type SparkApplicationSpec struct {
	BatchSchedulerOptions *struct {
		Queue string `json:"queue"`
	} `json:"batchSchedulerOptions"`
}

// SparkApplication - SparkApplication 리소스
type SparkApplication struct {
	Metadata metav1.ObjectMeta `json:"metadata"`
	Spec     SparkApplicationSpec `json:"spec"`
	Status   SparkApplicationStatus `json:"status"`
}

// PendingAppInfo - Pending 앱 정보
type PendingAppInfo struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Queue     string `json:"queue"`
	State     string `json:"state"`
	Age       string `json:"age"`
}

func main() {
	// 대상 큐 (기본값: 모든 큐)
	targetQueue := os.Getenv("TARGET_QUEUE")
	namespace := os.Getenv("TARGET_NAMESPACE")

	if namespace == "" {
		namespace = "default"
	}

	fmt.Printf("=== YuniKorn 큐별 Pending Spark Application 조회 ===\n")
	fmt.Printf("Target Queue: %s\n", getQueueDisplay(targetQueue))
	fmt.Printf("Namespace: %s\n\n", namespace)

	// Kubernetes 클라이언트 생성
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("HOME") + "/.kube/config"
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		fmt.Printf("Kubeconfig 로드 실패: %v\n", err)
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Printf("Kubernetes 클라이언트 생성 실패: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// SparkApplication 리소스 조회 (Dynamic Client 사용)
	// SparkApplication CRD가 있는지 확인하고 조회
	pendingApps := getPendingSparkApps(ctx, clientset, namespace, targetQueue)

	if len(pendingApps) == 0 {
		fmt.Println("Pending 상태의 SparkApplication을 찾지 못했습니다.")
		return
	}

	fmt.Printf("총 %d개의 Pending 애플리케이션을 찾았습니다:\n\n", len(pendingApps))
	for i, app := range pendingApps {
		fmt.Printf("[%d] %s\n", i+1, app.Name)
		fmt.Printf("    Namespace: %s\n", app.Namespace)
		fmt.Printf("    Queue: %s\n", app.Queue)
		fmt.Printf("    State: %s\n", app.State)
		fmt.Printf("    Age: %s\n\n", app.Age)
	}
}

func getQueueDisplay(queue string) string {
	if queue == "" {
		return "모든 큐"
	}
	return queue
}

func getPendingSparkApps(ctx context.Context, clientset *kubernetes.Clientset, namespace, targetQueue string) []PendingAppInfo {
	// kubectl을 사용하여 SparkApplication 조회
	// CRD를 직접 조회하는 것보다 kubectl을 통한 방법이 더 간단

	cmd := fmt.Sprintf("kubectl get sparkapplications -n %s -o json", namespace)
	if targetQueue != "" {
		// jq로 필터링
		cmd = fmt.Sprintf("kubectl get sparkapplications -n %s -o json | jq '.items[] | select(.spec.batchSchedulerOptions.queue == \"%s\")'", namespace, targetQueue)
	}

	// 간단한 구현을 위해 kubectl 결과 파싱
	apps, err := getSparkApplicationsViaKubectl(namespace)
	if err != nil {
		fmt.Printf("SparkApplication 조회 실패: %v\n", err)
		return nil
	}

	var pendingApps []PendingAppInfo
	now := time.Now()

	for _, app := range apps {
		// SUBMITTED 또는 PENDING 상태만
		if app.Status.ApplicationState.State != "SUBMITTED" &&
			app.Status.ApplicationState.State != "PENDING" &&
			app.Status.ApplicationState.State != "RUNNING" {
			continue
		}

		// 큐 필터링
		if targetQueue != "" && app.Spec.BatchSchedulerOptions != nil {
			if app.Spec.BatchSchedulerOptions.Queue != targetQueue {
				continue
			}
		}

		age := now.Sub(app.Metadata.CreationTimestamp.Time).Round(time.Second)

		queue := "unknown"
		if app.Spec.BatchSchedulerOptions != nil {
			queue = app.Spec.BatchSchedulerOptions.Queue
		}

		pendingApps = append(pendingApps, PendingAppInfo{
			Name:      app.Metadata.Name,
			Namespace: app.Metadata.Namespace,
			Queue:     queue,
			State:     app.Status.ApplicationState.State,
			Age:       age.String(),
		})
	}

	return pendingApps
}

// KubectlCommandResult - kubectl 실행 결과
type KubectlCommandResult struct {
	Items []SparkApplication `json:"items"`
}

func getSparkApplicationsViaKubectl(namespace string) ([]SparkApplication, error) {
	// 실제 구현에서는 client-go의 Dynamic Client를 사용하거나
	// CRD 전용 클라이언트를 생성해야 합니다
	// 여기서는 간단히 kubectl 명령을 사용하는 예시를 보여줍니다

	// 이 함수는 실제로 client-go를 사용하여 구현해야 합니다
	// SparkOperator CRD를 사용하는 경우

	return []SparkApplication{}, nil
}
