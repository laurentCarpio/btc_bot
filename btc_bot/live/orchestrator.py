from __future__ import annotations

from datetime import datetime, timezone

from btc_bot.live.models import Stage0Snapshot
from btc_bot.live.regime.atr_live import ATRLiveBuilder, Candle1m
from btc_bot.live.stage0.bucketizer import LiveBucketizer
from btc_bot.live.stage0.features import Stage0FeatureBuilder
from btc_bot.live.logging.trade_logger import logger_pub


class LiveOrchestrator:
    def __init__(
        self,
        engine,
        bucketizer: LiveBucketizer,
        feature_builder: Stage0FeatureBuilder,
        atr_builder: ATRLiveBuilder,
    ):
        self.engine = engine
        self.bucketizer = bucketizer
        self.feature_builder = feature_builder
        self.atr_builder = atr_builder
        self.logger = logger_pub

        self.book_ready = False
        self.trade_ready = False
        self.atr_ready = False

        self.last_regime = None

        self.last_candle_1m_by_symbol = {}

    async def on_book_update(self, symbol: str, ts_ms: int, bids: list, asks: list, action: str):
        ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

        if not self.book_ready:
            if len(bids) >= 15 and len(asks) >= 15:
                self.book_ready = True
                self.logger.info("[LiveOrchestrator] book_ready=True")

        bid_prices = {i: float(px) for i, (px, _sz) in enumerate(bids[:16])}
        ask_prices = {i: float(px) for i, (px, _sz) in enumerate(asks[:16])}
        bid_sizes = {i: float(sz) for i, (_px, sz) in enumerate(bids[:16])}
        ask_sizes = {i: float(sz) for i, (_px, sz) in enumerate(asks[:16])}

        closed = self.bucketizer.maybe_close_bucket(ts)
        if closed is not None:
            self.logger.debug(
                f"[LiveOrchestrator] bucket_closed(book-pre) "
                f"bucket_end={closed.bucket_end.isoformat()} "
                f"bid0={closed.bid_0_price} ask0={closed.ask_0_price} ntr={closed.ntr}"
            )
            snap = self.feature_builder.build_snapshot(closed)
            if snap is None:
                self.logger.debug(
                    f"[LiveOrchestrator] snapshot_none bucket_end={closed.bucket_end.isoformat()}"
                )
            await self._emit_stage0_if_ready(snap)

        self.bucketizer.update_book(
            ts=ts,
            bid_prices=bid_prices,
            ask_prices=ask_prices,
            bid_sizes=bid_sizes,
            ask_sizes=ask_sizes,
        )

    async def on_trade_update(self, symbol: str, ts_ms: int, price: float, size: float, side: str):
        ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        closed = self.bucketizer.maybe_close_bucket(ts)
        if closed is not None:
            self.logger.debug(
                f"[LiveOrchestrator] bucket_closed(trade-pre) "
                f"bucket_end={closed.bucket_end.isoformat()} "
                f"bid0={closed.bid_0_price} ask0={closed.ask_0_price} ntr={closed.ntr}"
            )
            snap = self.feature_builder.build_snapshot(closed)
            if snap is None:
                self.logger.info(
                    f"[LiveOrchestrator] snapshot_none bucket_end={closed.bucket_end.isoformat()}"
                )
            await self._emit_stage0_if_ready(snap)

        self.bucketizer.update_trade(
            ts=ts,
            price=float(price),
            qty=float(size),
            is_buy=(str(side).lower() == "buy"),
        )

        trade_window_ready = self.bucketizer.has_full_trade_window()

        if not self.trade_ready and trade_window_ready:
            self.trade_ready = True
            self.logger.info("[LiveOrchestrator] trade_ready=True")
            
    async def on_candle_1m_update(
        self,
        symbol: str,
        ts_ms: int,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: float,
    ):
        ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

        new_candle = Candle1m(
            timestamp=ts,
            open=float(open_),
            high=float(high),
            low=float(low),
            close=float(close),
            volume=float(volume),
        )

        prev_candle = self.last_candle_1m_by_symbol.get(symbol)

        if prev_candle is None:
            self.last_candle_1m_by_symbol[symbol] = new_candle
            return

        # stale / out-of-order candle -> ignore
        if new_candle.timestamp < prev_candle.timestamp:
            self.logger.debug(
                f"[LiveOrchestrator] ignore stale candle symbol={symbol} "
                f"new={new_candle.timestamp.isoformat()} "
                f"prev={prev_candle.timestamp.isoformat()}"
            )
            return

        # same minute -> replace current building candle
        if new_candle.timestamp == prev_candle.timestamp:
            self.last_candle_1m_by_symbol[symbol] = new_candle
            return

        # nouvelle minute -> l’ancienne minute est maintenant finalisée
        maybe_regime = self.atr_builder.update_1m_candle(prev_candle)

        if maybe_regime is not None:
            if self.last_regime is None or maybe_regime.timestamp != self.last_regime.timestamp:
                self.last_regime = maybe_regime
                if not self.atr_ready:
                    self.atr_ready = True
                    self.logger.info("[LiveOrchestrator] atr_ready=True")
                self.logger.debug(f"[LiveOrchestrator] regime updated: {self.last_regime}")

        # on stocke maintenant la nouvelle minute courante
        self.last_candle_1m_by_symbol[symbol] = new_candle
        
    async def _emit_stage0_if_ready(self, snap: Stage0Snapshot | None):
        if snap is None:
            return

        if not (self.book_ready and self.trade_ready and self.atr_ready):
            self.logger.debug(
                "[LiveOrchestrator] warmup not ready "
                f"(book={self.book_ready}, trade={self.trade_ready}, atr={self.atr_ready})"
            )
            return

        if self.last_regime is None:
            self.logger.debug("[LiveOrchestrator] Skip Stage0 emit: regime not ready yet")
            return

        self.logger.debug(
            "[LiveOrchestrator] stage0_emit "
            f"ts={snap.timestamp.isoformat()} "
            f"mid={snap.mid:.2f} "
            f"spread_ticks_1s={snap.spread_ticks_1s:.4f} "
            f"micro={snap.micro_bias_bps:.6f} "
            f"obi10={snap.OBI_10:.6f} "
            f"ti={snap.TI:.6f} "
            f"nps={snap.nps:.2f} "
            f"thin={snap.thinning_opp_3:.6f} "
            f"range60={snap.range_60s_bps:.4f} "
            f"atr_bps={self.last_regime.atr_bps:.4f} "
            f"vol_bucket={self.last_regime.vol_bucket}"
        )
        self.engine.on_stage0_snapshot(
            snap,
            atr_bps=self.last_regime.atr_bps,
            vol_bucket=self.last_regime.vol_bucket,
        )