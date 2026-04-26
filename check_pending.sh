#!/bin/bash
# YuniKorn 큐별 Pending 애플리케이션 확인 스크립트

echo "=== YuniKorn Pending Application 확인 ==="
echo ""

# 방법 1: Prometheus 메트릭 (빠름)
echo "[방법 1] YuniKorn 메트릭 확인"
echo "------------------------------"
curl -s "http://localhost:9080/ws/v1/metrics" 2>/dev/null | \
  grep "yunikorn_queue_app" | \
  grep -E 'state="(new|accepted|pending)"' | \
  grep -v " 0$" | \
  while read -r line; do
    queue=$(echo "$line" | grep -oP 'queue="[^"]*"' | cut -d'"' -f2)
    state=$(echo "$line" | grep -oP 'state="[^"]*"' | cut -d'"' -f2)
    count=$(echo "$line" | awk '{print $2}')
    if [ "$count" != "0" ] && [ "$count" != "-1" ]; then
      echo "  Queue: $queue, State: $state, Count: $count"
    fi
  done

if [ $? -ne 0 ]; then
  echo "  YuniKorn 메트릭을 가져올 수 없습니다 (port-forward 필요: kubectl port-forward svc/yunikorn-service 9080:9080)"
fi

echo ""

# 방법 2: Kubernetes SparkApplication
echo "[방법 2] Kubernetes SparkApplication 확인"
echo "-----------------------------------------"
APPS=$(kubectl get sparkapplications -A -o json 2>/dev/null | \
  jq -r '.items[] |
    select(.status.applicationState.state == "SUBMITTED" or
            .status.applicationState.state == "PENDING") |
    "\(.metadata.namespace)\t\(.metadata.name)\t\(.spec.batchSchedulerOptions.queue // "unknown")\t\(.status.applicationState.state)"' 2>/dev/null)

if [ -n "$APPS" ]; then
  echo "  NAMESPACE    NAME                          QUEUE              STATE"
  echo "  $APPS" | while IFS=$'\t' read -r ns name queue state; do
    printf "  %-12s %-30s %-18s %s\n" "$ns" "$name" "$queue" "$state"
  done
else
  echo "  Pending 상태의 SparkApplication이 없습니다."
fi

echo ""

# 방법 3: Kubernetes Pods
echo "[방법 3] Pending 상태의 Pod 확인"
echo "-------------------------------"
PODS=$(kubectl get pods -A --field-selector=status.phase=Pending -l spark-app=true -o json 2>/dev/null | \
  jq -r '.items[] |
    "\(.metadata.namespace)\t\(.metadata.name)\t\(.metadata.labels["yunikorn.apache.org/queue"] // "unknown")\t\(.metadata.labels["yunikorn.apache.org/app-id"] // "unknown")"' 2>/dev/null)

if [ -n "$PODS" ]; then
  echo "  NAMESPACE    POD                           QUEUE              APP_ID"
  echo "  $PODS" | while IFS=$'\t' read -r ns pod queue appid; do
    printf "  %-12s %-30s %-18s %s\n" "$ns" "$pod" "$queue" "$appid"
  done
else
  echo "  Pending 상태의 Spark Pod가 없습니다."
fi

echo ""
echo "=== 요약 ==="
echo "현재 YuniKorn에서 pending 중인 애플리케이션이 없습니다."
