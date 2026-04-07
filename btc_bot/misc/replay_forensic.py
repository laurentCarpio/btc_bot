from __future__ import annotations

import argparse
import asyncio
import gzip
import io
import json
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import boto3
import numpy as np
import pandas as pd

'''
python -m btc_bot.misc.replay_forensic \
  --bucket tradebot-config-tokyo \
  --raw-prefix data/live_merged_daily_raw \
  --candle-parquet s3://tradebot-config-tokyo/data/bougie/BTCUSDT/BTCUSDT-1m-2026.parquet \
  --venue binance \
  --symbol BTCUSDT \
  --start 2026-04-02T14:00:00+00:00 \
  --end 2026-04-02T14:40:00+00:00 \
  --out-dir s3://tradebot-config-tokyo/debug/replay_forensic_compare/2026-04-02_loss
'''

UTC = timezone.utc


# =============================================================================
# Generic utils
# =============================================================================

def parse_ts(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def dt_to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=UTC).isoformat()


def is_s3_uri(path: str) -> bool:
    return str(path).startswith("s3://")


def parse_s3_uri(uri: str) -> tuple[str, str]:
    raw = uri[len("s3://"):]
    bucket, _, key = raw.partition("/")
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI: {uri}")
    return bucket, key


def s3_join(root: str, name: str) -> str:
    return f"{root.rstrip('/')}/{name}"


def safe_to_dict(x: Any) -> Any:
    if x is None:
        return None

    if isinstance(x, datetime):
        return x.isoformat()

    if isinstance(x, Path):
        return str(x)

    if isinstance(x, np.generic):
        return x.item()

    if isinstance(x, np.ndarray):
        return x.tolist()

    if is_dataclass(x):
        return {k: safe_to_dict(v) for k, v in asdict(x).items()}

    if isinstance(x, dict):
        return {str(k): safe_to_dict(v) for k, v in x.items()}

    if isinstance(x, (list, tuple, set)):
        return [safe_to_dict(v) for v in x]

    if hasattr(x, "model_dump"):
        try:
            return safe_to_dict(x.model_dump())
        except Exception:
            pass

    if hasattr(x, "__dict__"):
        try:
            return {str(k): safe_to_dict(v) for k, v in vars(x).items()}
        except Exception:
            pass

    return x


def write_jsonl_gz_any(path: str, rows: Iterable[dict[str, Any]], s3=None) -> str:
    if is_s3_uri(path):
        if s3 is None:
            s3 = boto3.client("s3")
        bucket, key = parse_s3_uri(path)
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            for row in rows:
                payload = json.dumps(safe_to_dict(row), ensure_ascii=False) + "\n"
                gz.write(payload.encode("utf-8"))
        buf.seek(0)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=buf.getvalue(),
            ContentType="application/json",
            ContentEncoding="gzip",
        )
        return path

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(p, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(safe_to_dict(row), ensure_ascii=False) + "\n")
    return str(p)


def write_json_any(path: str, obj: dict[str, Any], s3=None) -> str:
    data = json.dumps(safe_to_dict(obj), indent=2, ensure_ascii=False).encode("utf-8")

    if is_s3_uri(path):
        if s3 is None:
            s3 = boto3.client("s3")
        bucket, key = parse_s3_uri(path)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType="application/json",
        )
        return path

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    return str(p)


def _local_join(base: str, filename: str) -> str:
    p = Path(base)
    p.mkdir(parents=True, exist_ok=True)
    return str(p / filename)


# =============================================================================
# Replay dataclasses
# =============================================================================

@dataclass
class ReplayEvent:
    ts_ms: int
    source: str
    kind: str
    payload: dict[str, Any]


@dataclass
class DecisionEvent:
    ts_ms: int
    event: str
    venue: str
    symbol: str
    payload: dict[str, Any]


# =============================================================================
# S3 loaders
# =============================================================================

def list_keys(s3, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token: Optional[str] = None

    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token

        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".jsonl.gz"):
                keys.append(key)

        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")

    return sorted(keys)


def iter_jsonl_gz_from_s3_key(s3, bucket: str, key: str) -> Iterable[dict[str, Any]]:
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read()

    with gzip.GzipFile(fileobj=io.BytesIO(raw), mode="rb") as gz:
        for line in gz:
            line = line.decode("utf-8").strip()
            if not line:
                continue
            yield json.loads(line)


def record_ts_ms(rec: dict[str, Any], kind: str) -> int:
    if kind == "trade":
        return int(rec.get("trade_time_ms", 0) or 0)
    return int(rec.get("event_time_ms", 0) or 0)


def load_raw_events_from_daily_merged(
    *,
    bucket: str,
    prefix_root: str,
    source: str,
    symbol: str,
    start_dt: datetime,
    end_dt: datetime,
    s3=None,
    output_name: str = "data.jsonl.gz",
) -> list[ReplayEvent]:
    s3 = s3 or boto3.client("s3")
    start_ms = dt_to_ms(start_dt)
    end_ms = dt_to_ms(end_dt)

    cur_day = datetime(start_dt.year, start_dt.month, start_dt.day, tzinfo=UTC)
    last_day = datetime(end_dt.year, end_dt.month, end_dt.day, tzinfo=UTC)

    days: list[str] = []
    while cur_day <= last_day:
        days.append(cur_day.strftime("%Y-%m-%d"))
        cur_day += timedelta(days=1)

    out: list[ReplayEvent] = []

    for kind in ("book", "trade"):
        for d in days:
            key = (
                f"{prefix_root.strip('/')}/"
                f"source={source}/kind={kind}/symbol={symbol}/day={d}/"
                f"{output_name}"
            )

            resp = s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
            if resp.get("KeyCount", 0) == 0:
                continue

            for rec in iter_jsonl_gz_from_s3_key(s3, bucket, key):
                ts_ms = record_ts_ms(rec, kind)
                if ts_ms < start_ms or ts_ms > end_ms:
                    continue
                out.append(
                    ReplayEvent(
                        ts_ms=ts_ms,
                        source=source,
                        kind=kind,
                        payload=rec,
                    )
                )

    out.sort(key=lambda x: (x.ts_ms, 0 if x.kind == "book" else 1))
    return out


def load_candle_events_from_parquet(
    *,
    parquet_path: str,
    start_dt: datetime,
    end_dt: datetime,
) -> list[ReplayEvent]:
    df = pd.read_parquet(parquet_path)

    if "timestamp" not in df.columns:
        raise ValueError(f"Parquet missing 'timestamp' column: {parquet_path}")

    ts = pd.to_datetime(df["timestamp"], utc=True)
    df = df.assign(timestamp=ts)

    mask = (df["timestamp"] >= start_dt) & (df["timestamp"] <= end_dt)
    df = df.loc[mask].copy()

    out: list[ReplayEvent] = []
    for _, row in df.iterrows():
        ts_ms = int(row["timestamp"].timestamp() * 1000)
        out.append(
            ReplayEvent(
                ts_ms=ts_ms,
                source="binance",
                kind="candle1m",
                payload={
                    "ts_ms": ts_ms,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row["volume"]),
                },
            )
        )

    return out


# =============================================================================
# Adapter + tracing
# =============================================================================

class BtcBotAdapter:
    def __init__(self, venue: str, symbol: str):
        self.venue = venue
        self.symbol = symbol
        self.decision_events: list[DecisionEvent] = []
        self._ready = False
        self.orchestrator = None
        self.engine = None
        self.cfg = None
        self.vol_edges = None
        self.ms_cut_monitor = None

        self.candidate_trace: list[dict[str, Any]] = []
        self.router_trace: list[dict[str, Any]] = []
        self.ml_trace: list[dict[str, Any]] = []
        self.entry_trace: list[dict[str, Any]] = []
        self.timer_trace: list[dict[str, Any]] = []

    def _jsonable(self, x: Any) -> Any:
        return safe_to_dict(x)

    def _append_trace(self, buf: list[dict[str, Any]], row: dict[str, Any]) -> None:
        buf.append(self._jsonable(row))

    def emit(self, ts_ms: int, event: str, payload: dict[str, Any]) -> None:
        self.decision_events.append(
            DecisionEvent(
                ts_ms=ts_ms,
                event=event,
                venue=self.venue,
                symbol=self.symbol,
                payload=payload,
            )
        )

    async def warmup(self, start_dt: datetime) -> None:
        from btc_bot.live.bootstrap import build_live_config, load_live_vol_bucket_edges
        from btc_bot.live.engine import LiveEngine
        from btc_bot.live.ml.artifacts import load_xgb_artifact_from_run
        from btc_bot.live.ml.inference import MLInferenceService
        from btc_bot.live.orchestrator import LiveOrchestrator
        from btc_bot.live.regime.atr_live import ATRLiveBuilder
        from btc_bot.live.stage0.bucketizer import LiveBucketizer
        from btc_bot.live.stage0.detector import Stage0Detector
        from btc_bot.live.stage0.features import Stage0FeatureBuilder
        from btc_bot.live.stage0.ms_cut_monitor import MSCutMonitor

        ml_run_root = (
            "s3://tradebot-config-tokyo/research/ms_edge/ml/runs/"
            "run_id=stageb_ml_low_c1e8bb16ef"
        )

        self.cfg = build_live_config()
        self.vol_edges = load_live_vol_bucket_edges()

        self.ms_cut_monitor = MSCutMonitor(
            base_cut=self.cfg.thresholds.ms_cut_value,
            maxlen=5000,
            min_obs=200,
            q_low=95.0,
            q_high=97.0,
        )

        detector = Stage0Detector(
            self.cfg.stage0,
            self.cfg.thresholds,
            symbol=self.symbol,
            venue=self.venue,
            ms_cut_monitor=self.ms_cut_monitor,
        )

        artifact = load_xgb_artifact_from_run(
            run_root=ml_run_root,
            threshold=0.70,
        )
        ml_service = MLInferenceService(artifact)

        engine = LiveEngine(
            cfg=self.cfg,
            detector=detector,
            ml_service=ml_service,
        )

        bucketizer = LiveBucketizer(
            freq_ms=self.cfg.stage0.freq_ms,
            max_level=15,
        )

        feature_builder = Stage0FeatureBuilder(self.cfg.stage0)

        atr_builder = ATRLiveBuilder(
            atr_n=self.cfg.regime.atr_n,
            tf_minutes=15,
            vol_bucket_edges=self.vol_edges,
        )

        self.orchestrator = LiveOrchestrator(
            engine=engine,
            bucketizer=bucketizer,
            feature_builder=feature_builder,
            atr_builder=atr_builder,
        )
        self.engine = engine

        self._patch_engine_hooks()
        self._ready = True

    def _patch_engine_hooks(self) -> None:
        original_open_trade = self.engine.executor.open_trade
        original_close_trade = self.engine.executor.close_trade

        def patched_open_trade(entry_decision, entry_price: float):
            trade = original_open_trade(entry_decision, entry_price)
            self.emit(
                int(trade.entry_time.timestamp() * 1000),
                "trade_opened",
                {
                    "trade_id": trade.trade_id,
                    "side": trade.side,
                    "router_branch": trade.router_branch,
                    "score_ml": trade.score_ml,
                    "size_mult": trade.size_mult,
                    "entry_price": trade.entry_price,
                    "entry_time": trade.entry_time.isoformat(),
                },
            )
            return trade

        def patched_close_trade(trade, exit_price, exit_time, pnl_bps, exit_reason, pnl_raw_bps, fees_bps):
            original_close_trade(
                trade=trade,
                exit_price=exit_price,
                exit_time=exit_time,
                pnl_bps=pnl_bps,
                exit_reason=exit_reason,
                pnl_raw_bps=pnl_raw_bps,
                fees_bps=fees_bps,
            )
            self.emit(
                int(exit_time.timestamp() * 1000),
                "trade_closed",
                {
                    "trade_id": trade.trade_id,
                    "side": trade.side,
                    "router_branch": trade.router_branch,
                    "score_ml": trade.score_ml,
                    "size_mult": trade.size_mult,
                    "entry_price": trade.entry_price,
                    "exit_price": exit_price,
                    "entry_time": trade.entry_time.isoformat(),
                    "exit_time": exit_time.isoformat(),
                    "pnl_bps": pnl_bps,
                    "pnl_raw_bps": pnl_raw_bps,
                    "fees_bps": fees_bps,
                    "reason": exit_reason,
                },
            )

        self.engine.executor.open_trade = patched_open_trade
        self.engine.executor.close_trade = patched_close_trade

        original_ml_score = self.engine.ml_service.score

        def patched_ml_score(feature_map):
            artifact = self.engine.ml_service.artifact

            ordered_row = None
            if artifact is not None:
                ordered_row = {
                    c: float(feature_map.get(c, 0.0))
                    for c in artifact.feature_cols
                }

            out = original_ml_score(feature_map)

            self._append_trace(
                self.ml_trace,
                {
                    "venue": self.venue,
                    "symbol": self.symbol,
                    "feature_map_raw": dict(feature_map),
                    "feature_map_ordered": ordered_row,
                    "ml_decision": self._jsonable(out),
                    "artifact_threshold": None if artifact is None else float(artifact.threshold),
                    "artifact_feature_cols": None if artifact is None else list(artifact.feature_cols),
                },
            )
            return out

        self.engine.ml_service.score = patched_ml_score

        original_detect = self.engine.detector.detect

        def patched_detect(snap):
            det = self.engine.detector
            thr = det.thr
            cfg = det.cfg

            checks = {
                "dir0": int(snap.dir0) != 0,
                "spread_rel": snap.spread_rel_5m <= thr.spread_rel_max,
                "spread_ticks": snap.spread_ticks_1s <= thr.spread_ticks_max,
                "range60": snap.range_60s_bps >= thr.range60s_min,
                "obi10": abs(snap.OBI_10) >= thr.obi10_abs_min,
                "micro": abs(snap.micro_bias_bps) >= thr.micro_abs_min,
                "Ntot": snap.Ntot > 0.0,
                "TI": abs(snap.TI) >= thr.ti_abs_min,
                "nps": snap.nps >= thr.nps_min,
                "persist": (
                    snap.persist_micro_ms >= cfg.persist_ms_min
                    or snap.persist_obi10_ms >= cfg.persist_ms_min
                ),
                "thin": snap.thinning_opp_3 >= thr.thin_min,
                "sign_micro": np.sign(snap.micro_bias_bps) == int(snap.dir0),
                "sign_ti": np.sign(snap.TI) == int(snap.dir0),
            }

            ms = det.compute_ms(snap)
            gaps = det._compute_gaps(snap)
            pass_all_hard_pre = bool(all(checks.values()))
            failed = [k for k, v in checks.items() if not v]

            out = original_detect(snap)

            self._append_trace(
                self.candidate_trace,
                {
                    "ts_ms": int(snap.timestamp.timestamp() * 1000),
                    "ts_iso": snap.timestamp.isoformat(),
                    "venue": self.venue,
                    "symbol": self.symbol,
                    "snapshot": {
                        "dir0": int(snap.dir0),
                        "mid": float(snap.mid),
                        "spread_bps": float(snap.spread_bps),
                        "spread_ticks_1s": float(snap.spread_ticks_1s),
                        "spread_rel_5m": float(snap.spread_rel_5m),
                        "range_60s_bps": float(snap.range_60s_bps),
                        "micro_bias_bps": float(snap.micro_bias_bps),
                        "OBI_10": float(snap.OBI_10),
                        "TI": float(snap.TI),
                        "Ntot": float(snap.Ntot),
                        "nps": float(snap.nps),
                        "persist_micro_ms": float(snap.persist_micro_ms),
                        "persist_obi10_ms": float(snap.persist_obi10_ms),
                        "thinning_opp_3": float(snap.thinning_opp_3),
                    },
                    "thresholds": {
                        "spread_rel_max": float(thr.spread_rel_max),
                        "spread_ticks_max": float(thr.spread_ticks_max),
                        "range60s_min": float(thr.range60s_min),
                        "obi10_abs_min": float(thr.obi10_abs_min),
                        "micro_abs_min": float(thr.micro_abs_min),
                        "ti_abs_min": float(thr.ti_abs_min),
                        "nps_min": float(thr.nps_min),
                        "thin_min": float(thr.thin_min),
                        "ms_cut_value": float(getattr(det, "ms_cut_value", np.nan)),
                    },
                    "config": {
                        "persist_ms_min": float(cfg.persist_ms_min),
                    },
                    "checks": checks,
                    "failed": failed,
                    "gaps": {k: float(v) for k, v in gaps.items()},
                    "ms_pre": float(ms),
                    "pass_all_hard_pre": pass_all_hard_pre,
                    "candidate_out": self._jsonable(out),
                },
            )
            return out

        self.engine.detector.detect = patched_detect

        original_on_stage0_snapshot = self.engine.on_stage0_snapshot

        def patched_on_stage0_snapshot(snap, atr_bps, vol_bucket):
            trade_before = set(self.engine.open_trades.keys())
            out = original_on_stage0_snapshot(snap, atr_bps, vol_bucket)
            trade_after = set(self.engine.open_trades.keys())

            new_trade_ids = sorted(trade_after - trade_before)
            active_trade_ids = sorted(trade_after)

            router_branch = None
            trade_dir = None
            if out is not None:
                router_branch = getattr(out, "router_branch", None)
                trade_dir = getattr(out, "side", None)

            self._append_trace(
                self.router_trace,
                {
                    "ts_ms": int(snap.timestamp.timestamp() * 1000),
                    "ts_iso": snap.timestamp.isoformat(),
                    "venue": self.venue,
                    "symbol": self.symbol,
                    "atr_bps": float(atr_bps),
                    "vol_bucket": str(vol_bucket),
                    "returned_trade": self._jsonable(out),
                    "router_branch": router_branch,
                    "trade_dir": trade_dir,
                    "new_trade_ids": new_trade_ids,
                    "active_trade_ids": active_trade_ids,
                },
            )

            self._append_trace(
                self.entry_trace,
                {
                    "ts_ms": int(snap.timestamp.timestamp() * 1000),
                    "ts_iso": snap.timestamp.isoformat(),
                    "venue": self.venue,
                    "symbol": self.symbol,
                    "mid": float(snap.mid),
                    "atr_bps": float(atr_bps),
                    "vol_bucket": str(vol_bucket),
                    "trade_opened": out is not None,
                    "trade": self._jsonable(out),
                },
            )
            return out

        self.engine.on_stage0_snapshot = patched_on_stage0_snapshot

        original_on_timer = self.engine.on_timer

        def patched_on_timer(now_ts, current_mid: float):
            before = {
                tid: {
                    "trade_id": tid,
                    "symbol": t.symbol,
                    "side": t.side,
                    "entry_time": t.entry_time.isoformat(),
                    "entry_price": float(t.entry_price),
                    "router_branch": t.router_branch,
                    "score_ml": t.score_ml,
                    "size_mult": t.size_mult,
                }
                for tid, t in self.engine.open_trades.items()
            }

            out = original_on_timer(now_ts, current_mid)

            after = {
                tid: {
                    "trade_id": tid,
                    "symbol": t.symbol,
                    "side": t.side,
                    "entry_time": t.entry_time.isoformat(),
                    "entry_price": float(t.entry_price),
                    "router_branch": t.router_branch,
                    "score_ml": t.score_ml,
                    "size_mult": t.size_mult,
                }
                for tid, t in self.engine.open_trades.items()
            }

            closed_ids = sorted(set(before.keys()) - set(after.keys()))
            remaining_ids = sorted(after.keys())

            self._append_trace(
                self.timer_trace,
                {
                    "ts_ms": int(now_ts.timestamp() * 1000),
                    "ts_iso": now_ts.isoformat(),
                    "venue": self.venue,
                    "symbol": self.symbol,
                    "current_mid": float(current_mid),
                    "open_trades_before": list(before.values()),
                    "open_trades_after": list(after.values()),
                    "closed_trade_ids": closed_ids,
                    "remaining_trade_ids": remaining_ids,
                },
            )
            return out

        self.engine.on_timer = patched_on_timer

    async def on_book(self, ts_ms: int, payload: dict[str, Any]) -> None:
        if not self._ready or self.orchestrator is None:
            return

        await self.orchestrator.on_book_update(
            symbol=self.symbol,
            ts_ms=ts_ms,
            bids=payload.get("bids", []),
            asks=payload.get("asks", []),
            action=payload.get("action", "update"),
        )

    async def on_trade(self, ts_ms: int, payload: dict[str, Any]) -> None:
        if not self._ready or self.orchestrator is None:
            return

        price = payload.get("price")
        size = payload.get("qty", payload.get("size"))
        side = payload.get("side")
        if price is None or size is None or side is None:
            return

        await self.orchestrator.on_trade_update(
            symbol=self.symbol,
            ts_ms=ts_ms,
            price=float(price),
            size=float(size),
            side=str(side),
        )

    async def on_candle_1m(self, ts_ms: int, payload: dict[str, Any]) -> None:
        if not self._ready or self.orchestrator is None:
            return

        await self.orchestrator.on_candle_1m_update(
            symbol=self.symbol,
            ts_ms=ts_ms,
            open_=float(payload["open"]),
            high=float(payload["high"]),
            low=float(payload["low"]),
            close=float(payload["close"]),
            volume=float(payload["volume"]),
        )

    async def finalize(self) -> None:
        return None

    def build_runtime_snapshot(self) -> dict[str, Any]:
        ms_snapshot = None
        if self.ms_cut_monitor is not None and hasattr(self.ms_cut_monitor, "get_shadow_snapshot"):
            try:
                snap = self.ms_cut_monitor.get_shadow_snapshot()
                ms_snapshot = safe_to_dict(snap)
            except Exception:
                ms_snapshot = None

        return {
            "venue": self.venue,
            "symbol": self.symbol,
            "cfg": safe_to_dict(self.cfg),
            "stage0_cfg": safe_to_dict(getattr(self.cfg, "stage0", None)),
            "thresholds": safe_to_dict(getattr(self.cfg, "thresholds", None)),
            "regime_cfg": safe_to_dict(getattr(self.cfg, "regime", None)),
            "vol_edges": safe_to_dict(self.vol_edges),
            "ms_cut_snapshot": ms_snapshot,
        }


# =============================================================================
# Replay runner
# =============================================================================

async def run_replay(
    *,
    adapter: BtcBotAdapter,
    events: list[ReplayEvent],
    start_dt: datetime,
) -> list[DecisionEvent]:
    await adapter.warmup(start_dt)

    for ev in events:
        if ev.kind == "candle1m":
            await adapter.on_candle_1m(ev.ts_ms, ev.payload)
        elif ev.kind == "book":
            await adapter.on_book(ev.ts_ms, ev.payload)
        elif ev.kind == "trade":
            await adapter.on_trade(ev.ts_ms, ev.payload)

    await adapter.finalize()
    return adapter.decision_events


def summarize_decisions(decisions: list[DecisionEvent]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for d in decisions:
        counts[d.event] = counts.get(d.event, 0) + 1
    return {
        "n_events": len(decisions),
        "counts": counts,
        "first_ts_ms": decisions[0].ts_ms if decisions else None,
        "last_ts_ms": decisions[-1].ts_ms if decisions else None,
    }


def normalize_decision_row(d: DecisionEvent) -> dict[str, Any]:
    return {
        "ts_ms": d.ts_ms,
        "ts_iso": ms_to_iso(d.ts_ms),
        "venue": d.venue,
        "symbol": d.symbol,
        "event": d.event,
        **safe_to_dict(d.payload),
    }


def build_replay_rows(events: list[ReplayEvent]) -> list[dict[str, Any]]:
    return [
        {
            "ts_ms": ev.ts_ms,
            "ts_iso": ms_to_iso(ev.ts_ms),
            "source": ev.source,
            "kind": ev.kind,
            "payload": safe_to_dict(ev.payload),
        }
        for ev in events
    ]


def write_outputs(
    *,
    s3,
    out_root: str,
    replay_rows: list[dict[str, Any]],
    decision_rows: list[dict[str, Any]],
    summary: dict[str, Any],
    run_meta: dict[str, Any],
    candidate_trace: list[dict[str, Any]],
    router_trace: list[dict[str, Any]],
    ml_trace: list[dict[str, Any]],
    entry_trace: list[dict[str, Any]],
    timer_trace: list[dict[str, Any]],
    venue: str,
    symbol: str,
) -> dict[str, str]:
    prefix_name = f"{venue}_{symbol}"

    mapping = {
        "events": f"{prefix_name}_events.jsonl.gz",
        "decisions": f"{prefix_name}_decisions.jsonl.gz",
        "summary": f"{prefix_name}_summary.json",
        "run_meta": f"{prefix_name}_run_meta.json",
        "candidate_trace": f"{prefix_name}_candidate_trace.jsonl.gz",
        "router_trace": f"{prefix_name}_router_trace.jsonl.gz",
        "ml_trace": f"{prefix_name}_ml_trace.jsonl.gz",
        "entry_trace": f"{prefix_name}_entry_trace.jsonl.gz",
        "timer_trace": f"{prefix_name}_timer_trace.jsonl.gz",
    }

    outputs: dict[str, str] = {}

    def out_path(filename: str) -> str:
        return s3_join(out_root, filename) if is_s3_uri(out_root) else _local_join(out_root, filename)

    outputs["events"] = write_jsonl_gz_any(out_path(mapping["events"]), replay_rows, s3=s3)
    outputs["decisions"] = write_jsonl_gz_any(out_path(mapping["decisions"]), decision_rows, s3=s3)
    outputs["summary"] = write_json_any(out_path(mapping["summary"]), summary, s3=s3)
    outputs["run_meta"] = write_json_any(out_path(mapping["run_meta"]), run_meta, s3=s3)

    outputs["candidate_trace"] = write_jsonl_gz_any(out_path(mapping["candidate_trace"]), candidate_trace, s3=s3)
    outputs["router_trace"] = write_jsonl_gz_any(out_path(mapping["router_trace"]), router_trace, s3=s3)
    outputs["ml_trace"] = write_jsonl_gz_any(out_path(mapping["ml_trace"]), ml_trace, s3=s3)
    outputs["entry_trace"] = write_jsonl_gz_any(out_path(mapping["entry_trace"]), entry_trace, s3=s3)
    outputs["timer_trace"] = write_jsonl_gz_any(out_path(mapping["timer_trace"]), timer_trace, s3=s3)

    return outputs


# =============================================================================
# CLI
# =============================================================================

async def async_main() -> None:
    parser = argparse.ArgumentParser(description="Replay forensic from daily merged raw + 1m candles")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--raw-prefix", required=True, help="Ex: data/live_merged_daily_raw")
    parser.add_argument("--candle-parquet", required=True, help="Local path or s3:// URI")
    parser.add_argument("--venue", required=True, choices=["binance", "bitget"])
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--pad-before-s", type=int, default=120)
    parser.add_argument("--pad-after-s", type=int, default=120)
    parser.add_argument("--atr-warmup-min", type=int, default=300)
    parser.add_argument("--out-dir", default="forensic_out")
    args = parser.parse_args()

    forensic_start = parse_ts(args.start)
    forensic_end = parse_ts(args.end)

    raw_start = forensic_start - timedelta(seconds=args.pad_before_s)
    raw_end = forensic_end + timedelta(seconds=args.pad_after_s)

    candle_start = raw_start - timedelta(minutes=args.atr_warmup_min)
    candle_end = raw_end

    s3 = boto3.client("s3")

    raw_events = load_raw_events_from_daily_merged(
        bucket=args.bucket,
        prefix_root=args.raw_prefix,
        source=args.venue,
        symbol=args.symbol,
        start_dt=raw_start,
        end_dt=raw_end,
        s3=s3,
    )

    candle_events = load_candle_events_from_parquet(
        parquet_path=args.candle_parquet,
        start_dt=candle_start,
        end_dt=candle_end,
    )

    all_events = candle_events + raw_events
    kind_order = {"candle1m": 0, "book": 1, "trade": 2}
    all_events.sort(key=lambda x: (x.ts_ms, kind_order.get(x.kind, 9)))

    adapter = BtcBotAdapter(venue=args.venue, symbol=args.symbol)
    decisions = await run_replay(
        adapter=adapter,
        events=all_events,
        start_dt=raw_start,
    )

    replay_rows = build_replay_rows(all_events)
    decision_rows = [normalize_decision_row(d) for d in decisions]
    summary = summarize_decisions(decisions)

    run_meta = {
        "venue": args.venue,
        "symbol": args.symbol,
        "bucket": args.bucket,
        "raw_prefix": args.raw_prefix,
        "candle_parquet": args.candle_parquet,
        "forensic_start": forensic_start.isoformat(),
        "forensic_end": forensic_end.isoformat(),
        "raw_window_start": raw_start.isoformat(),
        "raw_window_end": raw_end.isoformat(),
        "candle_warmup_start": candle_start.isoformat(),
        "candle_warmup_end": candle_end.isoformat(),
        "loaded_raw_events": len(raw_events),
        "loaded_candle_events": len(candle_events),
        "loaded_all_events": len(all_events),
        "runtime_snapshot": adapter.build_runtime_snapshot(),
        "trace_counts": {
            "candidate_trace": len(adapter.candidate_trace),
            "router_trace": len(adapter.router_trace),
            "ml_trace": len(adapter.ml_trace),
            "entry_trace": len(adapter.entry_trace),
            "timer_trace": len(adapter.timer_trace),
        },
    }

    outputs = write_outputs(
        s3=s3,
        out_root=args.out_dir,
        replay_rows=replay_rows,
        decision_rows=decision_rows,
        summary=summary,
        run_meta=run_meta,
        candidate_trace=adapter.candidate_trace,
        router_trace=adapter.router_trace,
        ml_trace=adapter.ml_trace,
        entry_trace=adapter.entry_trace,
        timer_trace=adapter.timer_trace,
        venue=args.venue,
        symbol=args.symbol,
    )

    print("=== REPLAY FORENSIC ===")
    print(f"venue={args.venue}")
    print(f"symbol={args.symbol}")
    print(f"raw_window_start={raw_start.isoformat()}")
    print(f"raw_window_end={raw_end.isoformat()}")
    print(f"candle_warmup_start={candle_start.isoformat()}")
    print(f"loaded_raw_events={len(raw_events)}")
    print(f"loaded_candle_events={len(candle_events)}")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    for k, v in outputs.items():
        print(f"{k}_out={v}")


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()