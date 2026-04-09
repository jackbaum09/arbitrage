"""
Cross-platform outcome matching.

Kalshi and Polymarket use different outcome formats:
  Kalshi:      "Will Buffalo win the Pro Football AFC East Division?"
  Polymarket:  "Buffalo Bills"

  Kalshi:      "Will Aaron Rodgers win the MVP?"
  Polymarket:  "Aaron Rodgers"

This module extracts the entity name from Kalshi's question-style outcomes,
resolves team aliases, and fuzzy-matches against Polymarket's clean outcomes.
"""

from __future__ import annotations

import re
import logging
from thefuzz import fuzz

from scanner.teams import resolve_team

log = logging.getLogger(__name__)

# Minimum fuzzy match score to consider a match
FUZZY_THRESHOLD = 80


def _extract_kalshi_entity(outcome: str) -> str | None:
    """
    Extract the entity (team/player name) from a Kalshi outcome string.

    Patterns handled:
      "Will [Entity] win the ..."
      "Will [Entity] be one of the ..."
      "Will [Entity] have the ..."
      "Will [Entity] lead ..."
    """
    m = re.match(
        r"^Will\s+(.+?)\s+(?:win|be\s+one|have|lead)\b",
        outcome,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).strip()

    # If outcome is already a clean name (not a question), return as-is
    if not outcome.startswith("Will ") and not outcome.startswith("Who "):
        return outcome.strip()

    return None


def match_outcomes(
    kalshi_rows: list[dict],
    polymarket_rows: list[dict],
    sport: str | None = None,
) -> list[tuple[dict, dict]]:
    """
    Match Kalshi rows to Polymarket rows by extracting entity names,
    resolving team aliases, and fuzzy-matching.

    Returns list of (kalshi_row, polymarket_row) pairs.
    """
    matched: list[tuple[dict, dict]] = []

    # Build lookup: normalized PM outcome -> row
    pm_lookup: dict[str, dict] = {}
    for pm in polymarket_rows:
        key = pm["outcome"].strip().lower()
        if key == "other":
            continue  # Skip "Other" bucket outcomes
        pm_lookup[key] = pm

    pm_keys = list(pm_lookup.keys())

    for k_row in kalshi_rows:
        entity = _extract_kalshi_entity(k_row["outcome"])
        if not entity:
            continue

        entity_lower = entity.lower()

        # Step 1: Try team alias resolution (handles city -> full team name)
        resolved = resolve_team(entity, sport)
        if resolved:
            resolved_lower = resolved.lower()
            if resolved_lower in pm_lookup:
                matched.append((k_row, pm_lookup[resolved_lower]))
                continue

        # Step 2: Exact match on extracted entity
        if entity_lower in pm_lookup:
            matched.append((k_row, pm_lookup[entity_lower]))
            continue

        # Step 3: Fuzzy match as fallback (mostly for player names)
        best_match = None
        best_score = 0

        for pm_key in pm_keys:
            # Quick substring check
            if entity_lower in pm_key or pm_key in entity_lower:
                score = fuzz.ratio(entity_lower, pm_key)
                if score > best_score:
                    best_score = score
                    best_match = pm_key
                continue

            # Token sort handles reordering (e.g., "Smith Jr." vs "Jr. Smith")
            score = fuzz.token_sort_ratio(entity_lower, pm_key)
            if score > best_score:
                best_score = score
                best_match = pm_key

        if best_match and best_score >= FUZZY_THRESHOLD:
            matched.append((k_row, pm_lookup[best_match]))

    return matched
