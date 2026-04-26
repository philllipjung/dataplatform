#!/bin/bash

# YuniKorn 특정 큐에서 Pending 중인 Spark Application 조회 스크립트
# 사용법: ./get_pending_apps.sh [queue_name] [namespace]

set -e

TARGET_QUEUE="${1:-}"
NAMESPACE="${2:-default}"

echo "=== YuniKorn 큐별 Pending Spark Application 조회 ==="
echo "Target Queue: ${TARGET_QUEUE:-모든 큐}"
echo "Namespace: $NAMESPACE"
echo ""

# SparkApplication 목록 조회
APPS=$(kubectl get sparkapplications -n "$NAMESPACE" -o json 2>/dev/null)

if [ -z "$APPS" ]; then
    echo "오류: SparkApplication을 가져올 수 없습니다."
    echo "SparkOperator가 설치되어 있는지 확인하세요."
    exit 1
fi

# Pending/SUBMITTED 상태의 앱 필터링
PENDING_APPS=$(echo "$APPS" | jq -r --arg queue "$TARGET_QUEUE" '
    .items[] |
    select(
        (.status.applicationState.state == "SUBMITTED" or
         .status.applicationState.state == "PENDING" or
         .status.applicationState.state == "RUNNING")
    ) |
    select(
        ($queue == "" or .spec.batchSchedulerOptions.queue == $queue)
    ) |
    {
        name: .metadata.name,
        namespace: .metadata.namespace,
        queue: (.spec.batchSchedulerOptions.queue // "unknown"),
        state: .status.applicationState.state,
        appId: (.status.sparkApplicationId // "N/A"),
        submissionTime: (.status.lastSubmissionAttemptTime // "N/A")
    }
')

# 결과 개수 확인
COUNT=$(echo "$PENDING_APPS" | jq -s 'length')

if [ "$COUNT" -eq 0 ]; then
    echo "Pending 상태의 SparkApplication을 찾지 못했습니다."
    echo ""
    echo "전체 SparkApplication 상태:"
    kubectl get sparkapplications -n "$NAMESPACE" 2>/dev/null || echo "  조회 실패"
    exit 0
fi

echo "총 $COUNT개의 애플리케이션을 찾았습니다:"
echo ""

# 상세 정보 출력
echo "$PENDING_APPS" | jq -r '
    "- 이름: \(.name)
      Namespace: \(.namespace)
      Queue: \(.queue)
      State: \(.state)
      App ID: \(.appId)
      Submission Time: \(.submissionTime)
    "
'

# 간단 목록
echo ""
echo "=== 간단 목록 ==="
echo "$PENDING_APPS" | jq -r '.name' | while read -r name; do
    echo "  - $name"
done
