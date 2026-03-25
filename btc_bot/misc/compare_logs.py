#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from typing import Any
import boto3
from io import StringIO
import pandas as pd

'''
python btc_bot/misc/compare_logs.py \
  --bitget-log-group btc_bot \
  --bitget-log-stream ecs/tradebot/c633485700014bd193fb884ea53eda01 \
  --binance-log-group btc_bot \
  --binance-log-stream ecs/tradebot/80c34a62a5e947d785a030553cc2f038 \
  --out-dir binance_bitget_compare
'''

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
LOGGER_CAND_RE = re.compile(r"\[logger_cand\]\s*-\s*(\{.*\})")

FLOAT_COLS = [
    "ms", "ms_cut_value", "micro", "obi10", "ti", "nps", "thin", "range60",
    "persist_micro_ms", "persist_obi10_ms",
    "score_ml", "size_mult", "entry_price", "exit_price",
    "pnl_raw_bps", "fees_bps", "pnl_bps", "holding_s",
    "gap_range60", "gap_obi10", "gap_micro", "gap_ti", "gap_nps", "gap_thin",
]
TIME_COLS = ["ts", "entry_time", "exit_time"]


def strip_ansi(s: str) -> str:
    return ANSI_RE.sub("", s)

def read_cloudwatch_stream(log_group: str, log_stream: str) -> list[str]:
    client = boto3.client("logs")

    events = []
    next_token = None

    while True:
        kwargs = {
            "logGroupName": log_group,
            "logStreamName": log_stream,
            "limit": 10000,
            "startFromHead": True,
        }

        if next_token:
            kwargs["nextToken"] = next_token

        resp = client.get_log_events(**kwargs)

        events.extend([e["message"] for e in resp["events"]])

        new_token = resp.get("nextForwardToken")
        if new_token == next_token:
            break

        next_token = new_token

    return events

def read_file_content(path_str: str) -> str:
    if path_str.startswith("s3://"):
        s3 = boto3.client("s3")
        bucket, key = path_str.replace("s3://", "").split("/", 1)
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8", errors="ignore")
    else:
        with open(path_str, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()

def parse_logger_cand_events_from_lines(lines: list[str], source: str, default_venue: str) -> pd.DataFrame:
    rows = []

    for raw_line in lines:
        line = strip_ansi(raw_line).strip()

        if "[logger_cand]" not in line:
            continue

        m = LOGGER_CAND_RE.search(line)
        if not m:
            continue

        try:
            payload = json.loads(m.group(1))
        except:
            continue

        if "venue" not in payload or not payload["venue"]:
            payload["venue"] = default_venue

        payload["_source"] = source
        rows.append(payload)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    for c in FLOAT_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    for c in TIME_COLS:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")

    return df

def safe_mean(s: pd.Series) -> float | None:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.mean())

def safe_median(s: pd.Series) -> float | None:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.median())

def safe_sum(s: pd.Series) -> float | None:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.sum())

def fmt(x: Any, nd: int = 3) -> str:
    if x is None:
        return "NA"
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
        return "NA"
    if isinstance(x, float):
        return f"{x:.{nd}f}"
    return str(x)

def compute_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    out_rows: list[dict[str, Any]] = []

    for venue, g in df.groupby("venue", dropna=False):
        accepts = g[g["event"] == "accept"].copy()
        near_miss = g[g["event"] == "near_miss"].copy() if "near_miss" in set(g["event"]) else g.iloc[0:0].copy()
        opened = g[g["event"] == "trade_opened"].copy()
        closed = g[g["event"] == "trade_closed"].copy()

        n_accept = len(accepts)
        n_near_miss = len(near_miss)
        n_opened = len(opened)
        n_closed = len(closed)

        accept_to_open = (n_opened / n_accept) if n_accept else None

        wins = None
        if n_closed:
            wins = float((closed["pnl_bps"] > 0).mean())

        row = {
            "venue": venue,
            "n_accept": n_accept,
            "n_near_miss": n_near_miss,
            "n_trade_opened": n_opened,
            "n_trade_closed": n_closed,
            "accept_to_open_rate": accept_to_open,
            "avg_accept_ms": safe_mean(accepts["ms"]) if "ms" in accepts.columns else None,
            "avg_accept_nps": safe_mean(accepts["nps"]) if "nps" in accepts.columns else None,
            "avg_accept_range60": safe_mean(accepts["range60"]) if "range60" in accepts.columns else None,
            "avg_accept_thin": safe_mean(accepts["thin"]) if "thin" in accepts.columns else None,
            "avg_open_score_ml": safe_mean(opened["score_ml"]) if "score_ml" in opened.columns else None,
            "avg_closed_pnl_raw_bps": safe_mean(closed["pnl_raw_bps"]) if "pnl_raw_bps" in closed.columns else None,
            "avg_closed_fees_bps": safe_mean(closed["fees_bps"]) if "fees_bps" in closed.columns else None,
            "avg_closed_pnl_bps": safe_mean(closed["pnl_bps"]) if "pnl_bps" in closed.columns else None,
            "median_closed_pnl_bps": safe_median(closed["pnl_bps"]) if "pnl_bps" in closed.columns else None,
            "sum_closed_pnl_bps": safe_sum(closed["pnl_bps"]) if "pnl_bps" in closed.columns else None,
            "win_rate_closed": wins,
            "avg_holding_s": safe_mean(closed["holding_s"]) if "holding_s" in closed.columns else None,
        }
        out_rows.append(row)

    return pd.DataFrame(out_rows).sort_values("venue").reset_index(drop=True)

def compare_side_by_side(summary: pd.DataFrame) -> str:
    if summary.empty:
        return "No logger_cand JSON events found."

    idx = summary.set_index("venue")
    venues = list(idx.index)

    cols = [
        "n_accept",
        "n_near_miss",
        "n_trade_opened",
        "n_trade_closed",
        "accept_to_open_rate",
        "avg_accept_ms",
        "avg_accept_nps",
        "avg_accept_range60",
        "avg_accept_thin",
        "avg_open_score_ml",
        "avg_closed_pnl_raw_bps",
        "avg_closed_fees_bps",
        "avg_closed_pnl_bps",
        "median_closed_pnl_bps",
        "sum_closed_pnl_bps",
        "win_rate_closed",
        "avg_holding_s",
    ]

    lines = []
    header = ["metric"] + venues
    widths = [max(len(h), 24) for h in header]

    def row_line(parts: list[str]) -> str:
        return " | ".join(p.ljust(w) for p, w in zip(parts, widths))

    lines.append(row_line(header))
    lines.append("-" * sum(widths) + "-" * (3 * (len(widths) - 1)))

    for c in cols:
        vals = [fmt(idx.loc[v, c], 4) if c.endswith("_rate") or "pnl" in c or c.endswith("_ms") or c.endswith("_s")
                else fmt(idx.loc[v, c], 3) for v in venues]
        lines.append(row_line([c] + vals))

    return "\n".join(lines)

def build_trade_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    closed = df[df["event"] == "trade_closed"].copy()
    if closed.empty:
        return closed

    keep = [
        "venue", "trade_id", "symbol", "side", "router_branch",
        "score_ml", "size_mult", "entry_time", "exit_time",
        "entry_price", "exit_price", "pnl_raw_bps", "fees_bps", "pnl_bps",
        "holding_s", "reason",
    ]
    
    keep = [c for c in keep if c in closed.columns]
    return closed[keep].sort_values(["venue", "entry_time"]).reset_index(drop=True)

def main() -> None:
    ap = argparse.ArgumentParser(description="Compare Binance vs Bitget logger_cand logs")
    ap.add_argument("--out-dir", default="", help="Optional output directory for CSV files")
    ap.add_argument("--binance-log-group")
    ap.add_argument("--binance-log-stream")
    ap.add_argument("--bitget-log-group")
    ap.add_argument("--bitget-log-stream")
    args = ap.parse_args()

    if args.binance_log_group:
        lines_bin = read_cloudwatch_stream(args.binance_log_group, args.binance_log_stream)
        df_bin = parse_logger_cand_events_from_lines(
            lines_bin,
            source=args.binance_log_stream,
            default_venue="binance",
        )
    else:
        df_bin = pd.DataFrame()

    if args.bitget_log_group:
        lines_bit = read_cloudwatch_stream(args.bitget_log_group, args.bitget_log_stream)
        df_bit = parse_logger_cand_events_from_lines(
            lines_bit,
            source=args.bitget_log_stream,
            default_venue="bitget",
        )
    else:
        df_bit = pd.DataFrame()

    df = pd.concat([df_bin, df_bit], ignore_index=True) if (not df_bin.empty or not df_bit.empty) else pd.DataFrame()

    if df.empty:
        print("No logger_cand JSON events found in the provided files.")
        return

    summary = compute_summary(df)
    trades = build_trade_table(df)

    print("\n=== SUMMARY ===")
    print(compare_side_by_side(summary))

    if not trades.empty:
        print("\n=== CLOSED TRADES ===")
        print(trades.to_string(index=False))

    if args.out_dir:
        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        df.to_csv(out_dir / "all_events.csv", index=False)
        summary.to_csv(out_dir / "summary.csv", index=False)
        trades.to_csv(out_dir / "closed_trades.csv", index=False)

        accepts = df[df["event"] == "accept"].copy()
        if not accepts.empty:
            accepts.to_csv(out_dir / "accepts.csv", index=False)

        near_miss = df[df["event"] == "near_miss"].copy()
        if not near_miss.empty:
            near_miss.to_csv(out_dir / "near_miss.csv", index=False)

        opened = df[df["event"] == "trade_opened"].copy()
        if not opened.empty:
            opened.to_csv(out_dir / "trade_opened.csv", index=False)

        print(f"\nCSV written to: {out_dir}")


if __name__ == "__main__":
    main()