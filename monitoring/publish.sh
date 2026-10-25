#!/usr/bin/env bash
# Creates or updates the Skystream dashboard and the alert for when the app's metrics stop arriving.
# Needs GRAFANA_URL and GRAFANA_SERVICE_ACCOUNT_TOKEN (Admin role) in the environment, and jq.
set -euo pipefail
cd "$(dirname "$0")"
: "${GRAFANA_URL:?set GRAFANA_URL, e.g. https://your-stack.grafana.net}"
: "${GRAFANA_SERVICE_ACCOUNT_TOKEN:?set GRAFANA_SERVICE_ACCOUNT_TOKEN}"

api() {
  curl -fsS -H "Authorization: Bearer $GRAFANA_SERVICE_ACCOUNT_TOKEN" -H "Content-Type: application/json" "$@"
}

# The alert queries the stack's Prometheus data source: Grafana Cloud's is named grafanacloud-<stack>-prom.
datasource=${GRAFANA_DATASOURCE_UID:-$(api "$GRAFANA_URL/api/datasources" \
  | jq -r '[.[] | select(.type == "prometheus" and (.name | test("^grafanacloud-.*-prom$")))][0].uid // empty')}
[[ -n $datasource ]] || { echo "No Grafana Cloud Prometheus data source found; set GRAFANA_DATASOURCE_UID" >&2; exit 1; }

if ! api "$GRAFANA_URL/api/folders/skystream" >/dev/null 2>&1; then
  api -X POST "$GRAFANA_URL/api/folders" -d '{"uid": "skystream", "title": "Skystream"}' >/dev/null
fi

jq '{dashboard: ., folderUid: "skystream", overwrite: true}' dashboard.json \
  | api -X POST "$GRAFANA_URL/api/dashboards/db" -d @- | jq -r '"Dashboard: \(env.GRAFANA_URL)\(.url)"'

# Provenance disabled, so the rule stays editable in the UI.
rule=$(jq --arg uid "$datasource" '.data[0].datasourceUid = $uid' alert-rule.json)
if api "$GRAFANA_URL/api/v1/provisioning/alert-rules/skystream-metrics-stopped" >/dev/null 2>&1; then
  api -X PUT -H "X-Disable-Provenance: true" "$GRAFANA_URL/api/v1/provisioning/alert-rules/skystream-metrics-stopped" -d "$rule" >/dev/null
else
  api -X POST -H "X-Disable-Provenance: true" "$GRAFANA_URL/api/v1/provisioning/alert-rules" -d "$rule" >/dev/null
fi
echo "Alert: Skystream metrics stopped"
