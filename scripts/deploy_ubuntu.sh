#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="${APP_DIR:-/var/www/aibot}"
REPO_URL="${REPO_URL:-https://github.com/interbirds010/aibot.git}"
BRANCH="${BRANCH:-main}"

if [[ "$(id -u)" -eq 0 ]]; then
  echo "Run this script as the application user, not root." >&2
  exit 1
fi

cd "$APP_DIR"

if [[ ! -d .git ]]; then
  git init
  git remote add origin "$REPO_URL"
else
  git remote set-url origin "$REPO_URL"
fi

git fetch --prune origin "$BRANCH"
git checkout -B "$BRANCH" "origin/$BRANCH"
git pull --ff-only origin "$BRANCH"

test -x venv/bin/python
test -f .env
mkdir -p data logs

venv/bin/python -m pip install \
  --disable-pip-version-check \
  --no-cache-dir \
  --only-binary=:all: \
  -r requirements.txt

venv/bin/python -m compileall -q src
venv/bin/python -c "import fcntl; from src.state_store import exclusive_file_lock; print('fcntl/state_store: OK')"

pm2 startOrReload ecosystem.config.js --update-env
pm2 save

pm2 describe aibot-monitor >/dev/null
pm2 describe aibot-risk-manager >/dev/null
pm2 describe aibot-dashboard >/dev/null
pm2 status
