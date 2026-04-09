"""
SNS push notifications for high-ROI arbitrage opportunities.

Publishes to the same SNS topic used by prediction_markets-master
for pipeline failure alerts.
"""

from __future__ import annotations

import logging
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from config import (
    SNS_TOPIC_ARN,
    ALERT_ROI_THRESHOLD,
    ALERTS_ENABLED,
)
from scanner.models import Opportunity

log = logging.getLogger(__name__)

_sns_client = None


def _get_sns_client():
    global _sns_client
    if _sns_client is None:
        _sns_client = boto3.client("sns", region_name="us-east-2")
    return _sns_client


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

    sent = 0
    client = _get_sns_client()

    for opp in qualifying:
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
            f"Detected: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}\n"
            f"Verified: Yes\n"
        )

        try:
            client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=subject,
                Message=message,
            )
            sent += 1
            log.info(f"Alert sent: {opp.outcome} ({opp.roi:.2%} ROI)")
        except ClientError as e:
            log.error(f"Failed to send alert for {opp.outcome}: {e}")

    if sent:
        log.info(f"Sent {sent} opportunity alert(s)")

    return sent
