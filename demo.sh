#!/usr/bin/env bash
# Live Demo & Terminal Monitor for xAS PSI-VPA vs Standard VPA (Scenario 2: Marginal-Fit Page Cache Thrashing)
# Usage:
#   ./demo.sh                 # Run with PSI-VPA (sample-psi-vpa-scalingpolicy.yaml)
#   ./demo.sh --standard-vpa  # Run with Standard VPA (sample-standard-vpa-scalingpolicy.yaml)
#   ./demo.sh --watch         # Watch current cluster state without resetting deployment

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="${NAMESPACE:-default}"
APP_NAME="thrashing-app"

MODE="${1:-psi}"
if [[ "${MODE}" == "--standard-vpa" ]]; then
  POLICY_NAME="thrashing-app-standard-vpa"
  POLICY_FILE="${SCRIPT_DIR}/deploy/sample-standard-vpa-scalingpolicy.yaml"
  DECISION_HEADER="xAS STANDARD VPA DECISION"
else
  POLICY_NAME="thrashing-app-psi-vpa"
  POLICY_FILE="${SCRIPT_DIR}/deploy/sample-psi-vpa-scalingpolicy.yaml"
  DECISION_HEADER="xAS PSI-VPA DECISION"
fi

BOLD="\033[1m"
DIM="\033[2m"
GREEN="\033[32m"
YELLOW="\033[33m"
RED="\033[31m"
CYAN="\033[36m"
RESET="\033[0m"

cleanup() {
  echo ""
  echo -e "${BOLD}${CYAN}==> Cleaning up ${APP_NAME} and ScalingPolicies...${RESET}"
  kubectl delete scalingpolicy thrashing-app-psi-vpa thrashing-app-standard-vpa -n "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl delete deployment "${APP_NAME}" -n "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl wait --for=delete pod -l "app=${APP_NAME}" -n "${NAMESPACE}" --timeout=30s >/dev/null 2>&1 || true
  echo -e "${BOLD}${GREEN}==> Cleanup complete (all ${APP_NAME} pods removed).${RESET}"
}

if [[ "${MODE}" != "--watch" ]]; then
  trap cleanup EXIT INT TERM
  echo -e "${BOLD}${CYAN}==> [1/3] Resetting ${APP_NAME} to initial 256Mi limit (with 15s healthy warm-up)...${RESET}"
  kubectl delete scalingpolicy thrashing-app-psi-vpa thrashing-app-standard-vpa -n "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl delete deployment "${APP_NAME}" -n "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl wait --for=delete pod -l "app=${APP_NAME}" -n "${NAMESPACE}" --timeout=30s >/dev/null 2>&1 || true
  kubectl apply -f "${SCRIPT_DIR}/deploy/sample-thrashing-workload.yaml" -n "${NAMESPACE}"

  echo -e "${BOLD}${CYAN}==> [2/3] Waiting for ${APP_NAME} pod to be Ready...${RESET}"
  kubectl rollout status deployment/"${APP_NAME}" -n "${NAMESPACE}" --timeout=90s

  echo -e "${BOLD}${CYAN}==> [3/3] Applying xAS ScalingPolicy (${POLICY_NAME})...${RESET}"
  kubectl apply -f "${POLICY_FILE}" -n "${NAMESPACE}"
fi

START_EPOCH=$(date +%s)

echo ""
echo -e "${BOLD}========================================================================================================================${RESET}"
echo -e "${BOLD}  xAS Live Demo Monitor  |  Workload: ${CYAN}${APP_NAME}${RESET}${BOLD}  |  Policy: ${CYAN}${POLICY_NAME}${RESET}${BOLD}  (Ctrl+C to exit & cleanup)${RESET}"
echo -e "${BOLD}========================================================================================================================${RESET}"
printf "${BOLD}%-6s | %-18s | %-10s | %-8s | %-14s | %-48s${RESET}\n" \
  "TIME" "WORKLOAD PHASE" "MEM LIMIT" "RESTARTS" "P99 LATENCY" "${DECISION_HEADER}"
echo "------------------------------------------------------------------------------------------------------------------------"

while true; do
  NOW_EPOCH=$(date +%s)
  ELAPSED="$((NOW_EPOCH - START_EPOCH))s"

  POD_JSON=$(kubectl get pods -n "${NAMESPACE}" -l "app=${APP_NAME}" -o json 2>/dev/null || echo "{}")
  ACTIVE_POD=$(echo "${POD_JSON}" | jq -c '[.items[]? | select(.metadata.deletionTimestamp == null)] | first // {}')
  POD_NAME=$(echo "${ACTIVE_POD}" | jq -r '.metadata.name // "pending"')
  MEM_LIMIT=$(echo "${ACTIVE_POD}" | jq -r '.spec.containers[0].resources.limits.memory // "-"')
  RESTARTS=$(echo "${ACTIVE_POD}" | jq -r '.status.containerStatuses[0].restartCount // 0')

  PHASE="STARTING"
  P99_MS="-"
  if [[ "${POD_NAME}" != "pending" ]]; then
    LAST_LOG=$(kubectl logs "${POD_NAME}" -n "${NAMESPACE}" --tail=5 2>/dev/null | grep "LATENCY_P99_MS=" | tail -n 1 || true)
    if [[ -n "${LAST_LOG}" ]]; then
      PHASE=$(echo "${LAST_LOG}" | sed -n 's/.*PHASE=\([^ ]*\).*/\1/p')
      P99_MS=$(echo "${LAST_LOG}" | sed -n 's/.*LATENCY_P99_MS=\([0-9.]*\).*/\1 ms/p')
    fi
  fi

  POLICY_JSON=$(kubectl get scalingpolicy "${POLICY_NAME}" -n "${NAMESPACE}" -o json 2>/dev/null || echo "{}")
  REC_MSG=$(echo "${POLICY_JSON}" | jq -r '
    if (.status.recommendations | length) > 0 then
      (.status.recommendations[0].reason // "") as $r |
      (.status.recommendations[0].workloadResources[0].limits.memory // "") as $m |
      if ($r | contains(":")) then
        ($r | sub("^Recommendation generated \\("; "") | sub("\\): "; " -> "))
      else
        ($r + " -> " + $m)
      end
    else
      "Waiting for first scrape..."
    end
  ' | cut -c1-48)

  # Color-code latency and memory limit for quick visual scanning
  LAT_COLOR="${GREEN}"
  if [[ "${P99_MS}" != "-" ]]; then
    P99_NUM="${P99_MS% ms}"
    if awk "BEGIN {exit !(${P99_NUM} > 5.0)}"; then
      LAT_COLOR="${RED}"
    elif awk "BEGIN {exit !(${P99_NUM} > 2.0)}"; then
      LAT_COLOR="${YELLOW}"
    fi
  fi

  MEM_COLOR="${YELLOW}"
  if [[ "${MEM_LIMIT}" != "256Mi" && "${MEM_LIMIT}" != "-" ]]; then
    MEM_COLOR="${GREEN}"
  fi

  printf "%-6s | %-18s | ${MEM_COLOR}%-10s${RESET} | %-8s | ${LAT_COLOR}%-14s${RESET} | %-48s\n" \
    "${ELAPSED}" "${PHASE}" "${MEM_LIMIT}" "${RESTARTS}" "${P99_MS}" "${REC_MSG}"

  sleep 1
done
