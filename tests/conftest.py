"""
Pytest configuration shared across the arbitrage test suite.

The tests target pure functions in scanner/detect.py and execution/risk.py
and never touch the database or live APIs. We make sure the repo root is
on sys.path so `from scanner.detect import _calculate_fees` resolves the
same way it does when main.py runs.
"""

from __future__ import annotations

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
