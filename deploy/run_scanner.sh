#!/usr/bin/env bash
# Cron wrapper for the arbitrage scanner.
# Activates venv, runs a single scan, logs output, and alerts on failure via SNS.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
VENV_DIR="$REPO_DIR/venv"
LOG_DIR="/var/log/arbitrage"
LOG_FILE="$LOG_DIR/scanner.log"
AWS_REGION="${AWS_REGION:-us-east-2}"
SNS_TOPIC_ARN_FILE="$SCRIPT_DIR/.sns_topic_arn"

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Timestamp header
echo "========================================" >> "$LOG_FILE"
echo "Scanner run: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Activate virtualenv
source "$VENV_DIR/bin/activate"

# Run a single scan (--once flag)
cd "$REPO_DIR"
python3 main.py --once >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

deactivate

# Log result
if [ $EXIT_CODE -eq 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Scan completed successfully (exit 0)" >> "$LOG_FILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Scan FAILED (exit $EXIT_CODE)" >> "$LOG_FILE"

    # Send SNS alert on failure
    if [ -f "$SNS_TOPIC_ARN_FILE" ]; then
        TOPIC_ARN=$(cat "$SNS_TOPIC_ARN_FILE")
        HOSTNAME=$(hostname)
        TAIL_LOG=$(tail -30 "$LOG_FILE")

        aws sns publish \
            --topic-arn "$TOPIC_ARN" \
            --region "$AWS_REGION" \
            --subject "Arbitrage Scanner FAILED on $HOSTNAME" \
            --message "Arbitrage scanner failed at $(date '+%Y-%m-%d %H:%M:%S') with exit code $EXIT_CODE.

Last 30 lines of log:
$TAIL_LOG" \
            >> "$LOG_FILE" 2>&1 || echo "[$(date '+%Y-%m-%d %H:%M:%S')] WARNING: Failed to send SNS alert" >> "$LOG_FILE"
    fi
fi

exit $EXIT_CODE
