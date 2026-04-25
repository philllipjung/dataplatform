#!/bin/bash
# YuniKorn 특정 큐에서 Pending 중인 애플리케이션 이름 찾기
# 사용법: ./find_pending_apps.sh [queue_name] [namespace]

TARGET_QUEUE="${1:-}"
NAMESPACE="${2:-default}"

echo "==================================================================="
echo "YuniKorn 큐별 Pending 애플리케이션 이름 찾기"
echo "==================================================================="
echo "Target Queue: ${TARGET_QUEUE:-모든 큐}"
echo "Namespace: $NAMESPACE"
echo ""

# 방법 1: SparkApplication에서 SUBMITTED/PENDING 상태 찾기
echo "[방법 1] SparkApplication - SUBMITTED/PENDING 상태"
echo "-----------------------------------------------------------------"

result=$(kubectl get sparkapplications -n "$NAMESPACE" -o json 2>/dev/null | \
  jq -r --arg queue "$TARGET_QUEUE" '
    .items[] |
    select(
      (.status.applicationState.state == "SUBMITTED" or
       .status.applicationState.state == "PENDING" or
       .status.applicationState.state == "RUNNING")
    ) |
    select($queue == "" or .spec.batchSchedulerOptions.queue == $queue) |
    "\(.metadata.name)\t\(.spec.batchSchedulerOptions.queue // "unknown")\t\(.status.applicationState.state)"
  ' 2>/dev/null)

if [ -n "$result" ]; then
  echo "APP NAME                      QUEUE                 STATE"
  echo "$result" | while IFS=$'\t' read -r name queue state; do
    printf "%-30s %-22s %s\n" "$name" "$queue" "$state"
  done
  count=$(echo "$result" | wc -l)
  echo ""
  echo "총 $count개 발견"
else
  echo "해당 상태의 SparkApplication을 찾지 못함"
fi

echo ""

# 방법 2: Pod에서 Pending 상태 찾기
echo "[방법 2] Kubernetes Pods - Pending 상태"
echo "-----------------------------------------------------------------"

pods=$(kubectl get pods -n "$NAMESPACE" --field-selector=status.phase=Pending -l spark-app=true -o json 2>/dev/null | \
  jq -r --arg queue "$TARGET_QUEUE" '
    .items[] |
    select($queue == "" or .metadata.labels["yunikorn.apache.org/queue"] == $queue) |
    "\(.metadata.name)\t\(.metadata.labels["yunikorn.apache.org/queue"] // "unknown")\t\(.metadata.labels["yunikorn.apache.org/app-id"] // "unknown")\t\(.status.reason)"
  ' 2>/dev/null)

if [ -n "$pods" ]; then
  echo "POD NAME                       QUEUE                 APP_ID                         REASON"
  echo "$pods" | while IFS=$'\t' read -r pod queue appid reason; do
    printf "%-30s %-22s %-30s %s\n" "$pod" "$queue" "$appid" "$reason"
  done
  count=$(echo "$pods" | wc -l)
  echo ""
  echo "총 $count개 발견"
else
  echo "Pending 상태의 Pod를 찾지 못함"
fi

echo ""

# 방법 3: ApplicationRejected 상태의 Pod (YuniKorn이 거부한 앱)
echo "[방법 3] ApplicationRejected 상태 (YuniKorn 거부)"
echo "-----------------------------------------------------------------"

rejected=$(kubectl get pods -n "$NAMESPACE" -l spark-app=true --field-selector=status.phase=Failed -o json 2>/dev/null | \
  jq -r --arg queue "$TARGET_QUEUE" '
    .items[] |
    select(.status.reason == "ApplicationRejected") |
    select($queue == "" or .metadata.labels["yunikorn.apache.org/queue"] == $queue) |
    "\(.metadata.name)\t\(.metadata.labels["yunikorn.apache.org/app-id"] // "unknown")"
  ' 2>/dev/null)

if [ -n "$rejected" ]; then
  echo "거부된 애플리케이션:"
  echo "$rejected" | while IFS=$'\t' read -r pod appid; do
    echo "  - $pod (App ID: $appid)"
  done
else
  echo "거부된 애플리케이션 없음"
fi

echo ""
echo "==================================================================="
