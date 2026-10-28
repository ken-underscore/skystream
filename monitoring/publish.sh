#!/usr/bin/env bash
# Creates or updates the Skystream dashboard and the alerts for when the app's metrics stop arriving or ingestion falls
# behind.
# Needs curl 7.76+, jq, and in the environment:
#   GRAFANA_URL                    the stack's Grafana, e.g. https://your-stack.grafana.net
#   GRAFANA_SERVICE_ACCOUNT_TOKEN  a token of a service account with the Admin role
#   ALERT_HOST                     the host the alerts watch, as listed under Host on the dashboard
#   GRAFANA_DATASOURCE_UID         optional: the Prometheus data source, if not Grafana Cloud's grafanacloud-*-prom
set -euo pipefail
cd "$(dirname "$0")"
: "${GRAFANA_URL:?set GRAFANA_URL, e.g. https://your-stack.grafana.net}"
: "${GRAFANA_SERVICE_ACCOUNT_TOKEN:?set GRAFANA_SERVICE_ACCOUNT_TOKEN}"
: "${ALERT_HOST:?set ALERT_HOST to the host the alerts watch, as listed under Host on the dashboard}"
GRAFANA_URL=${GRAFANA_URL%/}

# The token goes in a header file rather than the command line, where other users could see it.
grafana() {
  curl -sS -H @<(printf 'Authorization: Bearer %s\n' "$GRAFANA_SERVICE_ACCOUNT_TOKEN") \
    -H "Content-Type: application/json" "$@"
}

# Grafana's reply, or on failure its explanation, on stderr.
api() {
  local reply
  reply=$(grafana --fail-with-body "$@") || { echo "$reply" >&2; return 1; }
  echo "$reply"
}

exists() {
  [[ $(grafana -o /dev/null -w '%{http_code}' "$1") == 200 ]]
}

datasource=${GRAFANA_DATASOURCE_UID:-$(api "$GRAFANA_URL/api/datasources" \
  | jq -r '[.[] | select(.type == "prometheus" and (.name | test("^grafanacloud-.*-prom$")))][0].uid // empty')}
[[ -n $datasource ]] || { echo "No Grafana Cloud Prometheus data source found; set GRAFANA_DATASOURCE_UID" >&2; exit 1; }

if ! exists "$GRAFANA_URL/api/folders/skystream"; then
  api -X POST "$GRAFANA_URL/api/folders" -d '{"uid": "skystream", "title": "Skystream"}' >/dev/null
fi

jq '{dashboard: ., folderUid: "skystream", overwrite: true}' dashboard.json \
  | api -X POST "$GRAFANA_URL/api/dashboards/db" -d @- | jq -r --arg base "$GRAFANA_URL" '"Dashboard: \($base)\(.url)"'

# Each rule watches one host: another host still reporting, such as a laptop, mustn't hide the server going quiet.
jq -c --arg uid "$datasource" --arg host "$ALERT_HOST" '.[]
  | (.data[] | select(.datasourceUid == "DATASOURCE_UID")) |= (.datasourceUid = $uid | .model.expr |= sub("ALERT_HOST"; $host))' \
  alert-rules.json | while read -r rule; do
  uid=$(jq -r .uid <<<"$rule")
  # Provenance disabled, so the rule stays editable in the UI.
  if exists "$GRAFANA_URL/api/v1/provisioning/alert-rules/$uid"; then
    api -X PUT -H "X-Disable-Provenance: true" "$GRAFANA_URL/api/v1/provisioning/alert-rules/$uid" -d "$rule" >/dev/null
  else
    api -X POST -H "X-Disable-Provenance: true" "$GRAFANA_URL/api/v1/provisioning/alert-rules" -d "$rule" >/dev/null
  fi
  echo "Alert: $(jq -r .title <<<"$rule"), watching $ALERT_HOST"
done
