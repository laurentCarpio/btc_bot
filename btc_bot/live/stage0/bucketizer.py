from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from collections import deque
from typing import Deque, Optional


@dataclass(slots=True)
class BookLevelState:
    bid_prices: dict[int, float] = field(default_factory=dict)
    ask_prices: dict[int, float] = field(default_factory=dict)
    bid_sizes: dict[int, float] = field(default_factory=dict)
    ask_sizes: dict[int, float] = field(default_factory=dict)

    def best_bid(self) -> float:
        return float(self.bid_prices.get(0, 0.0))

    def best_ask(self) -> float:
        return float(self.ask_prices.get(0, 0.0))

    def bid_size(self, level: int) -> float:
        return float(self.bid_sizes.get(level, 0.0))

    def ask_size(self, level: int) -> float:
        return float(self.ask_sizes.get(level, 0.0))


@dataclass(slots=True)
class TradeAgg:
    bucket_end: datetime
    notional_buy: float = 0.0
    notional_sell: float = 0.0
    qty_buy: float = 0.0
    qty_sell: float = 0.0
    ntr: int = 0

    def add_trade(self, price: float, qty: float, is_buy: bool) -> None:
        notional = float(price) * float(qty)
        if is_buy:
            self.notional_buy += notional
            self.qty_buy += float(qty)
        else:
            self.notional_sell += notional
            self.qty_sell += float(qty)
        self.ntr += 1


@dataclass(slots=True)
class CompletedBucket:
    bucket_end: datetime
    bid_0_price: float
    ask_0_price: float
    bid_0_size: float
    ask_0_size: float
    bid_sizes: dict[int, float]
    ask_sizes: dict[int, float]
    notional_buy: float
    notional_sell: float
    qty_buy: float
    qty_sell: float
    ntr: int


def floor_to_bucket(ts: datetime, freq_ms: int) -> datetime:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    step_us = freq_ms * 1000
    epoch_us = int(ts.timestamp() * 1_000_000)
    floored = (epoch_us // step_us) * step_us
    return datetime.fromtimestamp(floored / 1_000_000, tz=timezone.utc)

def right_edge_bucket(ts: datetime, freq_ms: int) -> datetime:
    return floor_to_bucket(ts, freq_ms) + timedelta(milliseconds=freq_ms)


class LiveBucketizer:
    def __init__(self, freq_ms: int = 250, max_level: int = 15) -> None:
        self.freq_ms = int(freq_ms)
        self.max_level = int(max_level)

        self.book = BookLevelState()
        self.current_bucket_end: Optional[datetime] = None
        self.current_trade_agg: Optional[TradeAgg] = None

        self.trade_timestamps: Deque[datetime] = deque()
        self.min_trade_seen_ts: Optional[datetime] = None
        self.max_trade_seen_ts: Optional[datetime] = None

    def update_book(
        self,
        ts: datetime,
        bid_prices: dict[int, float],
        ask_prices: dict[int, float],
        bid_sizes: dict[int, float],
        ask_sizes: dict[int, float],
    ) -> None:
        bucket_end = right_edge_bucket(ts, self.freq_ms)
        self._ensure_bucket(bucket_end)

        self.book.bid_prices = {
            i: float(bid_prices[i]) for i in range(self.max_level + 1) if i in bid_prices
        }
        self.book.ask_prices = {
            i: float(ask_prices[i]) for i in range(self.max_level + 1) if i in ask_prices
        }
        self.book.bid_sizes = {
            i: float(bid_sizes[i]) for i in range(self.max_level + 1) if i in bid_sizes
        }
        self.book.ask_sizes = {
            i: float(ask_sizes[i]) for i in range(self.max_level + 1) if i in ask_sizes
        }

    def update_trade(self, ts: datetime, price: float, qty: float, is_buy: bool) -> None:
        bucket_end = right_edge_bucket(ts, self.freq_ms)
        self._ensure_bucket(bucket_end)

        if self.current_trade_agg is None:
            self.current_trade_agg = TradeAgg(bucket_end=bucket_end)

        self.current_trade_agg.add_trade(price=price, qty=qty, is_buy=is_buy)

        self.trade_timestamps.append(ts)

        if self.min_trade_seen_ts is None or ts < self.min_trade_seen_ts:
            self.min_trade_seen_ts = ts

        if self.max_trade_seen_ts is None or ts > self.max_trade_seen_ts:
            self.max_trade_seen_ts = ts

        self._trim_trade_timestamps(now_ts=ts, window_s=2.0)

    def maybe_close_bucket(self, now_ts: datetime) -> Optional[CompletedBucket]:
        if self.current_bucket_end is None:
            return None

        if now_ts < self.current_bucket_end:
            return None

        bid0 = self.book.best_bid()
        ask0 = self.book.best_ask()

        if bid0 <= 0 or ask0 <= 0 or bid0 >= ask0:
            self._roll_to_next_bucket()
            return None
        
        spread_ticks = (ask0 - bid0) / 0.1
        if spread_ticks > 5:
            self._roll_to_next_bucket()
            return None

        agg = self.current_trade_agg
        if agg is None:
            agg = TradeAgg(bucket_end=self.current_bucket_end)

        out = CompletedBucket(
            bucket_end=self.current_bucket_end,
            bid_0_price=bid0,
            ask_0_price=ask0,
            bid_0_size=self.book.bid_size(0),
            ask_0_size=self.book.ask_size(0),
            bid_sizes={i: self.book.bid_size(i) for i in range(self.max_level + 1)},
            ask_sizes={i: self.book.ask_size(i) for i in range(self.max_level + 1)},
            notional_buy=agg.notional_buy,
            notional_sell=agg.notional_sell,
            qty_buy=agg.qty_buy,
            qty_sell=agg.qty_sell,
            ntr=agg.ntr,
        )

        self._roll_to_next_bucket()
        return out

    def _ensure_bucket(self, bucket_end: datetime) -> None:
        if self.current_bucket_end is None:
            self.current_bucket_end = bucket_end
            self.current_trade_agg = TradeAgg(bucket_end=bucket_end)
            return

        if bucket_end > self.current_bucket_end:
            # on n'avance que d'un bucket logique à la fois via maybe_close_bucket / _roll_to_next_bucket
            while self.current_bucket_end < bucket_end:
                self._roll_to_next_bucket()

    def _roll_to_next_bucket(self) -> None:
        if self.current_bucket_end is None:
            return
        self.current_bucket_end = self.current_bucket_end + timedelta(milliseconds=self.freq_ms)
        self.current_trade_agg = TradeAgg(bucket_end=self.current_bucket_end)

    def _trim_trade_timestamps(self, now_ts: datetime, window_s: float) -> None:
        cutoff = now_ts - timedelta(seconds=float(window_s))
        while self.trade_timestamps and self.trade_timestamps[0] < cutoff:
            self.trade_timestamps.popleft()

    def has_full_trade_window(self, window_s: float = 2.0) -> bool:
        if self.min_trade_seen_ts is None or self.max_trade_seen_ts is None:
            return False
        return (self.max_trade_seen_ts - self.min_trade_seen_ts).total_seconds() >= float(window_s)