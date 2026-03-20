from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from collections import deque
from typing import Deque, Optional

import numpy as np
import pandas as pd

from btc_bot.live.models import RegimeSnapshot


@dataclass(slots=True)
class Candle1m:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass(slots=True)
class CandleTF:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


class ATRLiveBuilder:
    """
    Build ATR on higher timeframe bars from incoming 1m candles.

    Logic aligned with compute_atr_bps_from_1m:
      - resample 1m -> tf bars
      - TR = max(high-low, abs(high-prev_close), abs(low-prev_close))
      - ATR = rolling mean over atr_n bars
      - atr_bps = atr / close * 1e4
    """

    def __init__(
        self,
        atr_n: int = 14,
        tf_minutes: int = 15,
        vol_bucket_edges: list[float] | None = None,
    ) -> None:
        self.atr_n = int(atr_n)
        self.tf_minutes = int(tf_minutes)
        self.vol_bucket_edges = list(vol_bucket_edges or [])

        self.current_tf_bar: Optional[CandleTF] = None
        self.completed_tf_bars: Deque[CandleTF] = deque(maxlen=max(200, atr_n * 5))
        self.tr_values: Deque[float] = deque(maxlen=max(200, atr_n * 5))

    def update_1m_candle(self, candle: Candle1m) -> Optional[RegimeSnapshot]:
        tf_end = self._tf_bucket_end(candle.timestamp)

        if self.current_tf_bar is None:
            self.current_tf_bar = CandleTF(
                timestamp=tf_end,
                open=float(candle.open),
                high=float(candle.high),
                low=float(candle.low),
                close=float(candle.close),
                volume=float(candle.volume),
            )
            return None

        # same tf bar
        if tf_end == self.current_tf_bar.timestamp:
            self.current_tf_bar.high = max(self.current_tf_bar.high, float(candle.high))
            self.current_tf_bar.low = min(self.current_tf_bar.low, float(candle.low))
            self.current_tf_bar.close = float(candle.close)
            self.current_tf_bar.volume += float(candle.volume)
            return None

        # tf bar changed => finalize previous bar
        finalized = self.current_tf_bar
        self.completed_tf_bars.append(finalized)

        atr_bps = self._compute_latest_atr_bps()
        regime = None
        if atr_bps is not None:
            vol_bucket = self.bucket_from_atr(atr_bps)
            regime = RegimeSnapshot(
                timestamp=finalized.timestamp,
                atr_bps=float(atr_bps),
                vol_bucket=vol_bucket,
                vol_regime3=self.regime3_from_bucket(vol_bucket),
            )

        # start new tf bar
        self.current_tf_bar = CandleTF(
            timestamp=tf_end,
            open=float(candle.open),
            high=float(candle.high),
            low=float(candle.low),
            close=float(candle.close),
            volume=float(candle.volume),
        )

        return regime

    def _compute_latest_atr_bps(self) -> Optional[float]:
        if len(self.completed_tf_bars) == 0:
            return None

        bars = list(self.completed_tf_bars)
        cur = bars[-1]

        if len(bars) == 1:
            prev_close = cur.close
        else:
            prev_close = bars[-2].close

        tr = max(
            cur.high - cur.low,
            abs(cur.high - prev_close),
            abs(cur.low - prev_close),
        )
        self.tr_values.append(float(tr))

        if len(self.tr_values) < self.atr_n:
            return None

        atr = float(np.mean(list(self.tr_values)[-self.atr_n:]))
        if cur.close <= 0:
            return None

        atr_bps = (atr / cur.close) * 1e4
        return float(atr_bps)

    def bucket_from_atr(self, atr_bps: float) -> str:
        if not self.vol_bucket_edges:
            # fallback coarse behavior
            return "b2"

        k = 0
        for edge in self.vol_bucket_edges:
            if atr_bps <= edge:
                return f"b{k}"
            k += 1
        return f"b{len(self.vol_bucket_edges)}"

    @staticmethod
    def regime3_from_bucket(vol_bucket: str) -> str:
        if vol_bucket in ("b0", "b1"):
            return "low"
        if vol_bucket == "b2":
            return "mid"
        if vol_bucket in ("b3", "b4"):
            return "high"
        return "other"

    def _tf_bucket_end(self, ts: datetime) -> datetime:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        minute = ts.minute
        tf = self.tf_minutes

        bucket_minute = (minute // tf + 1) * tf
        hour = ts.hour
        day = ts.date()

        if bucket_minute < 60:
            return ts.replace(
                minute=bucket_minute,
                second=0,
                microsecond=0,
            )

        # rollover next hour
        dt = pd.Timestamp(ts).to_pydatetime()
        dt = dt.replace(minute=0, second=0, microsecond=0)
        dt = dt + pd.Timedelta(hours=1)
        return dt