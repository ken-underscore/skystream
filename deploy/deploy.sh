#!/usr/bin/env bash
# Deploys Skystream on the server. It checks out a commit, builds the app and the frontend, and restarts the app. Once
# the app is healthy, it switches the served frontend to the new build.
# Run as root: /opt/skystream/app/deploy/deploy.sh [ref]. The ref defaults to main; a branch name means the remote's.
# Rolling back is deploying an earlier commit, as far back as the one that added this script. First-time setup is in
# the README's Deploying section.
set -euo pipefail
cd /opt/skystream/app
export COMPOSE_FILE=compose.yaml:deploy/compose.production.yaml

if [[ ${1:-} != --checked-out ]]; then
  # One deploy at a time: the lock is held through the exec below.
  exec 9>/run/skystream-deploy.lock
  flock -n 9 || { echo "A deploy is already running" >&2; exit 1; }
  git fetch --quiet origin
  ref=${1:-main}
  commit=$(git rev-parse --verify --quiet "origin/$ref^{commit}" || git rev-parse --verify "$ref^{commit}")
  git checkout --quiet --detach "$commit"
  # Runs the script as of that commit, rather than the copy bash already has open.
  exec deploy/deploy.sh --checked-out
fi

commit=$(git rev-parse --short HEAD)
echo "Deploying $(git log -1 --format='%h %s')"

# Every commit on main passed CI's tests before it merged.
./mvnw --quiet -DskipTests clean package
# Only fetches what's missing or changed, so the model matches the commit, rollbacks included.
scripts/fetch-model.sh >/dev/null
# Built beside the served frontend, which keeps serving until the app is healthy.
www=/opt/skystream/www-$commit
(cd src/main/frontend && pnpm install --frozen-lockfile --silent && pnpm --silent build --outDir "$www" --emptyOutDir)

# Postgres starts on its own after a reboot; this starts it the first time, and applies Compose changes.
docker compose up --detach --wait postgres-db

# Both staged before either is swapped in. A running app keeps reading the jar it started with until it restarts.
install -m 644 target/skystream.jar /opt/skystream/skystream.jar.new
install -m 644 deploy/skystream.service /etc/systemd/system/skystream.service.new
mv /opt/skystream/skystream.jar.new /opt/skystream/skystream.jar
mv /etc/systemd/system/skystream.service.new /etc/systemd/system/skystream.service
systemctl daemon-reload
systemctl enable --quiet skystream
systemctl restart skystream

# Healthy means this unit's own process answers, not an earlier copy of the app still holding the port.
healthy() {
  local pid
  pid=$(systemctl show --property MainPID --value skystream)
  [[ $pid != 0 && $(ss -Htlnp 'sport = :8081') == *"pid=$pid,"* ]] \
    && curl -fs --max-time 2 -o /dev/null 127.0.0.1:8081/actuator/health
}

# Startup loads the model and checks the schema; ingestion resumes from the saved checkpoint.
for _ in $(seq 60); do
  if healthy; then
    # Switching a link is atomic, so the new frontend replaces the old one with no gap.
    ln -sfn "$www" /opt/skystream/www.new
    mv -T /opt/skystream/www.new /opt/skystream/www
    find /opt/skystream -maxdepth 1 -name 'www-*' ! -path "$www" -exec rm -rf {} +
    echo "Up: $commit"
    exit 0
  fi
  sleep 2
done
echo "$commit isn't healthy after 2 minutes, so the previous frontend is still served; see journalctl -u skystream" >&2
exit 1
