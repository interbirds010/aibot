# Ubuntu deployment

Target repository: `https://github.com/interbirds010/aibot`

The server must already contain `/var/www/aibot/venv` and a private
`/var/www/aibot/.env`. Neither is committed.

## First deployment

Connect to the server, then run as the application owner:

```bash
cd /var/www/aibot
git init
git remote add origin https://github.com/interbirds010/aibot.git
git fetch origin main
git checkout -B main origin/main
chmod +x scripts/deploy_ubuntu.sh
./scripts/deploy_ubuntu.sh
```

If the public repository is later made private, authenticate Git on the server
with a read-only deploy key before running the commands.

## Subsequent deployments

```bash
cd /var/www/aibot
./scripts/deploy_ubuntu.sh
```

The script uses the existing virtual environment, installs binary wheels
without retaining a pip cache, validates Linux `fcntl` locking, reloads only
the three `aibot-*` PM2 applications, and saves the current PM2 process list.

To enable PM2 itself after reboot, run the command printed by `pm2 startup`
once with the required sudo privileges, then run `pm2 save` again.

## Verification

```bash
cd /var/www/aibot
test -d src && test -d data && test -f ecosystem.config.js
venv/bin/python -m compileall -q src
venv/bin/python -c "import fcntl; from src.state_store import exclusive_file_lock; print('fcntl/state_store: OK')"
pm2 status
pm2 logs aibot-monitor --lines 30 --nostream
pm2 logs aibot-risk-manager --lines 30 --nostream
pm2 logs aibot-dashboard --lines 30 --nostream
```
