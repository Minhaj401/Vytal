#!/usr/bin/env python3
"""Hadoop Streaming reducer — per-parameter totals + abnormal % (sorted by param)."""
import sys

def main():
    cur, total, abnormal = None, 0, 0
    for line in sys.stdin:
        param, kind = line.rstrip("\n").split("\t")
        if param != cur:
            if cur is not None:
                emit(cur, total, abnormal)
            cur, total, abnormal = param, 0, 0
        if kind == "total":
            total += 1
        else:
            abnormal += 1
    if cur is not None:
        emit(cur, total, abnormal)

def emit(param, total, abnormal):
    pct = (100.0 * abnormal / total) if total else 0.0
    print(f"{param}\t{total}\t{abnormal}\t{pct:.2f}")

if __name__ == "__main__":
    main()
