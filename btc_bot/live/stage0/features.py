from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Deque, Optional

import numpy as np

from btc_bot.live.config import Stage0Config
from btc_bot.live.models import Stage0Snapshot
from btc_bot.live.stage0.bucketizer import CompletedBucket


@dataclass(slots=True)
class RollingPoint:
    timestamp: datetime
    value: float


@dataclass(slots=True)
class FlowPoint:
    timestamp: datetime
    notional_buy: float
    notional_sell: float
    ntr: int


class Stage0FeatureBuilder:
    def __init__(self, cfg: Stage0Config) -> None:
        self.cfg = cfg

        self.flow_window: Deque[FlowPoint] = deque()
        self.spread_ticks_window_1s: Deque[RollingPoint] = deque()
        self.spread_ticks_window_5m: Deque[RollingPoint] = deque()
        self.mid_window_60s: Deque[RollingPoint] = deque()
        self.mid_window_10m: Deque[RollingPoint] = deque()
        self.opp_depth_window_2s: Deque[RollingPoint] = deque()

        self.last_micro_sign: int = 0
        self.last_micro_sign_ts: Optional[datetime] = None
        self.last_obi_sign: int = 0
        self.last_obi_sign_ts: Optional[datetime] = None

    def build_snapshot(self, bucket: CompletedBucket) -> Optional[Stage0Snapshot]:
        bid0 = float(bucket.bid_0_price)
        ask0 = float(bucket.ask_0_price)
        if bid0 <= 0 or ask0 <= 0:
            return None

        ts = bucket.bucket_end
        mid = (bid0 + ask0) / 2.0
        spread_bps = ((ask0 - bid0) / (mid + 1e-12)) * 1e4
        spread_ticks = (ask0 - bid0) / (self.cfg.tick_size + 1e-12)

        bid0s = float(bucket.bid_0_size)
        ask0s = float(bucket.ask_0_size)
        micro = (ask0 * bid0s + bid0 * ask0s) / (bid0s + ask0s + 1e-12)
        micro_bias_bps = ((micro - mid) / (mid + 1e-12)) * 1e4

        dbid = {}
        dask = {}
        for k in self.cfg.depths:
            dbid[k] = sum(float(bucket.bid_sizes.get(i, 0.0)) for i in range(k + 1))
            dask[k] = sum(float(bucket.ask_sizes.get(i, 0.0)) for i in range(k + 1))

        obi = {}
        for k in self.cfg.depths:
            obi[k] = (dbid[k] - dask[k]) / (dbid[k] + dask[k] + 1e-9)

        self._push_flow(
            ts=ts,
            notional_buy=float(bucket.notional_buy),
            notional_sell=float(bucket.notional_sell),
            ntr=int(bucket.ntr),
        )
        self._trim_flow(ts)

        nb = sum(x.notional_buy for x in self.flow_window)
        ns = sum(x.notional_sell for x in self.flow_window)
        ntr_roll = sum(x.ntr for x in self.flow_window)
        ntot = nb + ns
        ti = (nb - ns) / (ntot + 1e-9)
        nps = ntr_roll / max(self.cfg.trades_window_s, 1e-9)

        self._push_rolling(self.spread_ticks_window_1s, ts, spread_ticks)
        self._trim_rolling(self.spread_ticks_window_1s, ts, seconds=1)

        spread_ticks_1s = self._median(self.spread_ticks_window_1s)

        self._push_rolling(self.spread_ticks_window_5m, ts, spread_ticks_1s)
        self._trim_rolling(self.spread_ticks_window_5m, ts, seconds=self.cfg.spread_roll_med_s)

        spread_ticks_med_5m = self._median(self.spread_ticks_window_5m)
        if spread_ticks_med_5m <= 0:
            spread_rel_5m = 1.0
        else:
            spread_rel_5m = spread_ticks_1s / spread_ticks_med_5m

        self._push_rolling(self.mid_window_60s, ts, mid)
        self._trim_rolling(self.mid_window_60s, ts, seconds=60)
        range_60s_bps = self._range_bps(self.mid_window_60s, mid)

        self._push_rolling(self.mid_window_10m, ts, mid)
        self._trim_rolling(self.mid_window_10m, ts, seconds=600)
        range_10m_bps = self._range_bps(self.mid_window_10m, mid)

        obi10 = float(obi.get(10, 0.0))
        dir0 = int(np.sign(ti + 0.5 * np.sign(micro_bias_bps)))

        persist_micro_ms = self._compute_persist_ms(
            ts=ts,
            sign_value=int(np.sign(micro_bias_bps)),
            last_sign_attr="last_micro_sign",
            last_ts_attr="last_micro_sign_ts",
            max_ms=self.cfg.persist_window_ms,
        )

        persist_obi10_ms = self._compute_persist_ms(
            ts=ts,
            sign_value=int(np.sign(obi10)),
            last_sign_attr="last_obi_sign",
            last_ts_attr="last_obi_sign_ts",
            max_ms=self.cfg.persist_window_ms,
        )

        dopp_3 = float(dask.get(3, 0.0)) if dir0 > 0 else float(dbid.get(3, 0.0))
        self._push_rolling(self.opp_depth_window_2s, ts, dopp_3)
        self._trim_rolling(self.opp_depth_window_2s, ts, seconds=self.cfg.thinning_window_s)
        med_opp_3 = self._median(self.opp_depth_window_2s)
        thinning_opp_3 = (med_opp_3 - dopp_3) / (med_opp_3 + 1e-9) if med_opp_3 > 0 else 0.0

        snapshot = Stage0Snapshot(
            timestamp=ts,
            bid_0_price=bid0,
            ask_0_price=ask0,
            mid=mid,
            spread_bps=float(spread_bps),
            spread_ticks_1s=float(spread_ticks_1s),
            spread_rel_5m=float(spread_rel_5m),
            micro_bias_bps=float(micro_bias_bps),
            OBI_10=obi10,
            TI=float(ti),
            nps=float(nps),
            Ntot=float(ntot),
            MS=0.0,  # computed later by detector
            dir0=dir0,
            persist_micro_ms=float(persist_micro_ms),
            persist_obi10_ms=float(persist_obi10_ms),
            thinning_opp_3=float(thinning_opp_3),
            range_60s_bps=float(range_60s_bps),
            range_10m_bps=float(range_10m_bps),
        )
        return snapshot

    def _push_flow(self, ts: datetime, notional_buy: float, notional_sell: float, ntr: int) -> None:
        self.flow_window.append(
            FlowPoint(
                timestamp=ts,
                notional_buy=notional_buy,
                notional_sell=notional_sell,
                ntr=ntr,
            )
        )

    def _trim_flow(self, now_ts: datetime) -> None:
        cutoff = now_ts - timedelta(seconds=self.cfg.trades_window_s)
        while self.flow_window and self.flow_window[0].timestamp < cutoff:
            self.flow_window.popleft()

    def _push_rolling(self, dq: Deque[RollingPoint], ts: datetime, value: float) -> None:
        dq.append(RollingPoint(timestamp=ts, value=float(value)))

    def _trim_rolling(self, dq: Deque[RollingPoint], now_ts: datetime, seconds: float) -> None:
        cutoff = now_ts - timedelta(seconds=seconds)
        while dq and dq[0].timestamp < cutoff:
            dq.popleft()

    def _median(self, dq: Deque[RollingPoint]) -> float:
        if not dq:
            return 0.0
        vals = [x.value for x in dq]
        return float(np.median(vals))

    def _range_bps(self, dq: Deque[RollingPoint], ref_mid: float) -> float:
        if not dq or ref_mid <= 0:
            return 0.0
        vals = [x.value for x in dq]
        return float((max(vals) - min(vals)) / (ref_mid + 1e-12) * 1e4)

    def _compute_persist_ms(
        self,
        ts: datetime,
        sign_value: int,
        last_sign_attr: str,
        last_ts_attr: str,
        max_ms: int,
    ) -> float:
        last_sign = getattr(self, last_sign_attr)
        last_ts = getattr(self, last_ts_attr)

        if sign_value == 0:
            setattr(self, last_sign_attr, 0)
            setattr(self, last_ts_attr, ts)
            return 0.0

        if last_ts is None or sign_value != last_sign:
            setattr(self, last_sign_attr, sign_value)
            setattr(self, last_ts_attr, ts)
            return 0.0

        dt_ms = (ts - last_ts).total_seconds() * 1000.0
        dt_ms = max(0.0, min(float(max_ms), dt_ms))
        return float(dt_ms)