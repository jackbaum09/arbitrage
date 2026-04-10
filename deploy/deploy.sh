#!/usr/bin/env bash
# Deploy latest arbitrage scanner code to EC2.
# Usage: bash deploy/deploy.sh

set -euo pipefail

# Defaults target the arbitrage scanner EC2, NOT the pipeline master box
# (3.134.83.29) — that one runs prediction_markets-master and has nothing
# to do with this repo. Override EC2_HOST/SSH_KEY if you ever need to
# deploy this repo elsewhere.
EC2_HOST="${EC2_HOST:-3.18.221.253}"
EC2_USER="${EC2_USER:-ec2-user}"
SSH_KEY="${SSH_KEY:-$HOME/Downloads/arbitrage_test.pem}"
REPO_DIR="/home/$EC2_USER/arbitrage"

if [ ! -f "$SSH_KEY" ]; then
    echo "ERROR: SSH key not found at $SSH_KEY"
    echo "Set SSH_KEY env var to your .pem file path."
    exit 1
fi

SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10"

echo "=== Deploying arbitrage scanner to $EC2_HOST ==="

ssh $SSH_OPTS "$EC2_USER@$EC2_HOST" bash -s <<REMOTE
set -euo pipefail
cd "$REPO_DIR"

echo "--- git pull ---"
git pull --ff-only

echo "--- pip install ---"
source venv/bin/activate
pip install -q -r requirements.txt
deactivate

echo "--- smoke test ---"
source venv/bin/activate
python3 -c "from scanner.detect import scan_all; from execution.risk import RiskLimits; print('Import OK')"
deactivate

echo "--- current crontab ---"
crontab -l | grep run_scanner || echo "(no scanner cron entries)"
REMOTE

echo ""
echo "=== Deploy complete ==="
echo "Monitor: ssh $SSH_OPTS $EC2_USER@$EC2_HOST tail -50 /var/log/arbitrage/scanner.log"
