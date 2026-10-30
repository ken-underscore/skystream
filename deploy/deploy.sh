#!/usr/bin/env bash
# Deploys Skystream on the server: checks out a commit, builds the app and the frontend, and restarts the app on them.
# Run as root: /opt/skystream/app/deploy/deploy.sh [git ref], by default origin/main. Rolling back is deploying an
# earlier commit. First-time setup is in the README's Deploying section.
set -euo pipefail
cd /opt/skystream/app
ref=${1:-origin/main}

git fetch --quiet origin
git checkout --quiet --detach "$ref"
echo "Deploying $(git log -1 --format='%h %s')"

# CI has run the tests on main.
./mvnw --quiet -DskipTests package
(cd src/main/frontend && pnpm install --frozen-lockfile --silent && pnpm --silent build)

# Postgres starts on its own after a reboot; this starts it the first time, and applies Compose changes.
docker compose up --detach --wait postgres-db

# Replaced by renaming, so a running app keeps reading the jar it started with until it restarts.
install -m 644 target/skystream.jar /opt/skystream/skystream.jar.new
mv /opt/skystream/skystream.jar.new /opt/skystream/skystream.jar
install -m 644 deploy/skystream.service /etc/systemd/system/skystream.service
systemctl daemon-reload
systemctl enable --quiet skystream
systemctl restart skystream

# Startup loads the model and checks the schema; ingestion resumes from the saved checkpoint.
for _ in $(seq 60); do
  if curl -sf 127.0.0.1:8081/actuator/health | grep -q '"UP"'; then
    echo "Up: $(git log -1 --format='%h')"
    exit 0
  fi
  sleep 2
done
echo "Not healthy after 2 minutes; see journalctl -u skystream" >&2
exit 1
