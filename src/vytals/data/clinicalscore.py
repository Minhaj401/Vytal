"""Standard ICU early-warning scores from the six canonical vitals.

Single source for every dataset builder in this repo (demo generator,
PhysioNet 2012 importer, HLT-009 importer keeps the dataset's own labels).

NEWS2 (UK Royal College of Physicians, 2017): Scale 1. Supplemental oxygen and
consciousness (ACVPU) are not part of any schema here, so they score as normal
(room air, alert) — the total is a lower bound. Missing vitals score 0.

qSOFA (Sepsis-3): mental status is likewise absent; only RR >= 22 and
SBP <= 100 can contribute, so the total here is 0..2.
"""
from __future__ import annotations

import math


def _pts(x, edges) -> int:
    """First (upper, points) whose upper >= x. None/NaN/unparseable -> 0."""
    if x is None:
        return 0
    try:
        v = float(x)
    except (TypeError, ValueError):  # pd.NA and friends
        return 0
    if math.isnan(v):
        return 0
    for upper, pts in edges:
        if v <= upper:
            return pts
    return edges[-1][1]


def news2(hr=None, spo2=None, rr=None, temp_c=None, sbp=None) -> int:
    return (
        _pts(rr, [(8, 3), (11, 1), (20, 0), (24, 2), (999, 3)])
        + _pts(spo2, [(91, 3), (93, 2), (95, 1), (100, 0)])
        + _pts(sbp, [(90, 3), (100, 2), (110, 1), (219, 0), (999, 3)])
        + _pts(hr, [(40, 3), (50, 1), (90, 0), (110, 1), (130, 2), (999, 3)])
        + _pts(temp_c, [(35.0, 3), (36.0, 1), (38.0, 0), (39.0, 1), (99, 2)])
    )


def qsofa(rr=None, sbp=None) -> int:
    def hit(x, pred) -> bool:
        if x is None:
            return False
        try:
            v = float(x)
        except (TypeError, ValueError):
            return False
        return not math.isnan(v) and pred(v)

    return int(hit(rr, lambda v: v >= 22)) + int(hit(sbp, lambda v: v <= 100))
