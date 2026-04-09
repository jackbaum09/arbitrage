#!/usr/bin/env bash
# One-time EC2 setup for the arbitrage scanner.
# Designed to run on the same EC2 instance as prediction_markets-master.
# Usage: bash deploy/setup_ec2.sh

set -euo pipefail

REPO_DIR="$HOME/arbitrage"
LOG_DIR="/var/log/arbitrage"
VENV_DIR="$REPO_DIR/venv"
DEPLOY_DIR="$REPO_DIR/deploy"
AWS_REGION="${AWS_REGION:-us-east-2}"

# Reuse the prediction_markets SNS topic for alerts
PM_SNS_FILE="$HOME/prediction_markets-master/deploy/.sns_topic_arn"

echo "=== Arbitrage Scanner - EC2 Setup ==="

# --- Clone repo if needed ---
if [ ! -d "$REPO_DIR" ]; then
    echo "Cloning arbitrage repo..."
    git clone https://github.com/jackbaum09/arbitrage.git "$REPO_DIR"
else
    echo "Repo exists at $REPO_DIR, pulling latest..."
    cd "$REPO_DIR" && git pull --ff-only
fi

# --- Create virtualenv and install dependencies ---
echo "Setting up Python virtualenv..."
python3.12 -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"
pip install --upgrade pip
pip install -r "$REPO_DIR/requirements.txt"
deactivate

# --- Create .env from template if it doesn't exist ---
if [ ! -f "$REPO_DIR/.env" ]; then
    cat > "$REPO_DIR/.env" <<'ENVEOF'
# Supabase (same as prediction_markets-master)
DB_HOST=
DB_PORT=6543
DB_NAME=postgres
DB_USER=
DB_PASSWORD=

# AWS (for SNS alerts)
AWS_REGION=us-east-2
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=

# SNS alerts for opportunities
SNS_TOPIC_ARN=
ALERT_ROI_THRESHOLD=0.02
ALERTS_ENABLED=false

# Execution (disabled by default)
EXECUTION_ENABLED=false
KALSHI_API_KEY_ID=
KALSHI_PRIVATE_KEY_PATH=
KALSHI_API_MODE=demo
POLYMARKET_PRIVATE_KEY=
ENVEOF
    echo "Created $REPO_DIR/.env — fill in your credentials."
    echo "TIP: Copy DB credentials from $HOME/prediction_markets-master/.env"
else
    echo ".env already exists, skipping."
fi

# --- Create log directory ---
echo "Setting up log directory..."
sudo mkdir -p "$LOG_DIR"
sudo chown "$(whoami):$(id -gn)" "$LOG_DIR"

# --- Link SNS topic from prediction_markets-master ---
if [ -f "$PM_SNS_FILE" ]; then
    cp "$PM_SNS_FILE" "$DEPLOY_DIR/.sns_topic_arn"
    echo "Linked SNS topic ARN from prediction_markets-master"
else
    echo "WARNING: No SNS topic found at $PM_SNS_FILE"
    echo "Create one manually or run prediction_markets-master setup first."
fi

# --- Install crontab ---
# Every 5 minutes during active sports hours (UTC 14-23, 0-4)
echo "Installing cron schedule..."
(crontab -l 2>/dev/null | grep -v "run_scanner.sh"
echo "*/5 14-23 * * * $DEPLOY_DIR/run_scanner.sh"
echo "*/5 0-4 * * * $DEPLOY_DIR/run_scanner.sh"
) | crontab -
echo "Cron installed: every 5min, 10AM–midnight ET (UTC 14–04)"

# --- Make scripts executable ---
chmod +x "$DEPLOY_DIR/run_scanner.sh"
chmod +x "$DEPLOY_DIR/deploy.sh"

echo ""
echo "=== Setup complete ==="
echo "Next steps:"
echo "  1. Fill in $REPO_DIR/.env with your credentials"
echo "  2. Test: $DEPLOY_DIR/run_scanner.sh"
echo "  3. Check logs: tail -50 $LOG_DIR/scanner.log"
