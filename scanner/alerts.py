"""
SNS push notifications for high-ROI arbitrage opportunities.

Publishes to the same SNS topic used by prediction_markets-master
for pipeline failure alerts.

Dedupe: the scanner runs every 5 minutes, but a real arbitrage often
persists across many scans. Re-alerting every scan produces email spam,
so we keep a small JSON state file keyed by Opportunity.opportunity_key
and suppress re-alerts within ALERT_COOLDOWN_HOURS unless the ROI has
improved by at least ALERT_ROI_DELTA since the last alert.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from config import (
    SNS_TOPIC_ARN,
    ALERT_ROI_THRESHOLD,
    ALERTS_ENABLED,
    ALERT_COOLDOWN_HOURS,
    ALERT_ROI_DELTA,
    ALERT_STATE_PATH,
)
from scanner.models import Opportunity

log = logging.getLogger(__name__)

_sns_client = None

# Prune state entries older than this so the file doesn't grow unbounded.
# 24h is comfortably longer than ALERT_COOLDOWN_HOURS (default 6h) so we
# never prune an entry that's still gating alerts.
_STATE_PRUNE_HOURS = 24.0


def _get_sns_client():
    global _sns_client
    if _sns_client is None:
        _sns_client = boto3.client("sns", region_name="us-east-2")
    return _sns_client


def _load_alert_state(path: str) -> dict[str, dict]:
    """
    Load the dedupe state file. Returns an empty dict on any error —
    better to over-alert once than to silently drop signals because the
    state file is corrupt or missing.
    """
    try:
        with open(path, "r") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        return data
    except FileNotFoundError:
        return {}
    except (json.JSONDecodeError, OSError) as e:
        log.warning(f"Could not read alert state at {path}: {e}; starting fresh")
        return {}


def _save_alert_state(path: str, state: dict[str, dict]) -> None:
    """Best-effort atomic write of the dedupe state file."""
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        tmp = f"{path}.tmp"
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except OSError as e:
        log.warning(f"Could not write alert state to {path}: {e}")


def _prune_state(state: dict[str, dict], now: datetime) -> dict[str, dict]:
    """Drop entries whose last_alerted_at is older than _STATE_PRUNE_HOURS."""
    cutoff = _STATE_PRUNE_HOURS * 3600
    pruned = {}
    for key, entry in state.items():
        ts = entry.get("last_alerted_at")
        if not ts:
            continue
        try:
            last = datetime.fromisoformat(ts)
        except (TypeError, ValueError):
            continue
        if (now - last).total_seconds() <= cutoff:
            pruned[key] = entry
    return pruned


def _should_alert(opp: Opportunity, state: dict[str, dict], now: datetime) -> bool:
    """
    Return True if we should fire an SNS alert for this opportunity.

    An opportunity passes dedupe if either:
      - we have no prior alert for its key, OR
      - the last alert was more than ALERT_COOLDOWN_HOURS ago, OR
      - the ROI has improved by at least ALERT_ROI_DELTA since the last alert
    """
    entry = state.get(opp.opportunity_key)
    if not entry:
        return True

    ts = entry.get("last_alerted_at")
    if ts:
        try:
            last = datetime.fromisoformat(ts)
            age_hours = (now - last).total_seconds() / 3600.0
            if age_hours >= ALERT_COOLDOWN_HOURS:
                return True
        except (TypeError, ValueError):
            return True  # corrupt timestamp → re-alert once and rewrite

    last_roi = entry.get("last_roi")
    if isinstance(last_roi, (int, float)):
        # Epsilon guards against floating-point near-misses like 0.06 - 0.05
        # evaluating to 0.00999... and failing a strict >= 0.01 comparison.
        if opp.roi - last_roi >= ALERT_ROI_DELTA - 1e-9:
            return True

    return False


def send_opportunity_alerts(opportunities: list[Opportunity]) -> int:
    """
    Send SNS alerts for new high-ROI verified opportunities.

    Returns the number of alerts sent.
    """
    if not ALERTS_ENABLED:
        return 0

    if not SNS_TOPIC_ARN:
        log.warning("ALERTS_ENABLED but no SNS_TOPIC_ARN configured")
        return 0

    qualifying = [
        opp for opp in opportunities
        if opp.liquidity_verified and opp.roi >= ALERT_ROI_THRESHOLD
    ]

    if not qualifying:
        return 0

    now = datetime.now(timezone.utc)
    state = _prune_state(_load_alert_state(ALERT_STATE_PATH), now)

    to_alert = [opp for opp in qualifying if _should_alert(opp, state, now)]
    suppressed = len(qualifying) - len(to_alert)
    if suppressed:
        log.info(
            f"Alert dedupe: {suppressed} qualifying opp(s) suppressed "
            f"(within {ALERT_COOLDOWN_HOURS}h cooldown, ROI not improved)"
        )

    if not to_alert:
        # Still persist the pruned state so the file stays tidy.
        _save_alert_state(ALERT_STATE_PATH, state)
        return 0

    sent = 0
    client = _get_sns_client()

    for opp in to_alert:
        subject = (
            f"Arb Alert: {opp.roi:.1%} ROI — {opp.outcome} "
            f"({opp.sport.upper()} {opp.market_type})"
        )
        # SNS subject max is 100 chars
        if len(subject) > 100:
            subject = subject[:97] + "..."

        message = (
            f"Arbitrage Opportunity Detected\n"
            f"{'=' * 40}\n\n"
            f"Outcome: {opp.outcome}\n"
            f"Sport: {opp.sport.upper()} | Market: {opp.market_type}\n"
            f"ROI: {opp.roi:.2%}\n"
            f"Net Profit: ${opp.net_profit * 100:.2f} per $100\n\n"
            f"Buy YES on {opp.buy_yes_platform} @ ${opp.buy_yes_price:.4f}\n"
            f"Buy NO on {opp.buy_no_platform} @ ${opp.buy_no_price:.4f}\n"
            f"Total Cost: ${opp.total_cost:.4f} per contract\n\n"
            f"Liquidity: YES depth ${opp.buy_yes_depth:.0f} | NO depth ${opp.buy_no_depth:.0f}\n"
            f"Max Executable Size: ${opp.max_executable_size:.0f}\n"
            f"Capital Required: ${opp.capital_required:.2f}\n\n"
            f"Detected: {now.strftime('%Y-%m-%d %H:%M UTC')}\n"
            f"Verified: Yes\n"
        )

        try:
            client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=subject,
                Message=message,
            )
            sent += 1
            state[opp.opportunity_key] = {
                "last_alerted_at": now.isoformat(),
                "last_roi": float(opp.roi),
                "outcome": opp.outcome,
                "sport": opp.sport,
            }
            log.info(f"Alert sent: {opp.outcome} ({opp.roi:.2%} ROI)")
        except ClientError as e:
            log.error(f"Failed to send alert for {opp.outcome}: {e}")

    _save_alert_state(ALERT_STATE_PATH, state)

    if sent:
        log.info(f"Sent {sent} opportunity alert(s)")

    return sent
