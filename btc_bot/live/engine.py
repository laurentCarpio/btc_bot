from __future__ import annotations

from btc_bot.live.config import get_default_config
from btc_bot.live.execution.paper_executor import PaperExecutor
from btc_bot.live.logging.trade_logger import logger_pub
from btc_bot.live.ml.inference import MLInferenceService
from btc_bot.live.models import EntryDecision, Stage0Snapshot, MLDecision, RegimeSnapshot
from btc_bot.live.router.router import BranchRouter
from btc_bot.live.stage0.detector import Stage0Detector
from btc_bot.live.trade.lifecycle import TradeLifecycleManager
from btc_bot.live.trade.sizing import SimpleSizer


class LiveEngine:
    def __init__(self, detector: Stage0Detector, ml_service: MLInferenceService) -> None:
        self.cfg = get_default_config()
        self.detector = detector
        self.ml_service = ml_service
        self.router = BranchRouter(mr_bucket=self.cfg.regime.mr_bucket)
        self.sizer = SimpleSizer()
        self.lifecycle = TradeLifecycleManager(self.cfg.exit)
        self.executor = PaperExecutor()
        self.logger = logger_pub
        self.open_trades = {}

    def _has_open_trade_for_symbol(self, symbol: str) -> bool:
        return any(t.symbol == symbol and t.status == "OPEN" for t in self.open_trades.values())

    def _entry_block_reason(self, symbol: str) -> str | None:
        if self.cfg.risk.one_position_per_symbol and self._has_open_trade_for_symbol(symbol):
            return "existing_open_position"
        return None
    
    def on_stage0_snapshot(self, snap: Stage0Snapshot, atr_bps: float, vol_bucket: str):
        candidate = self.detector.detect(snap)
        self.logger.debug(f"[LiveEngine] candidate : {candidate}")

        if not candidate.pass_all_hard:
            return None

        block_reason = self._entry_block_reason(candidate.symbol)
        if block_reason is not None:
            entry_decision = EntryDecision(
                should_enter=False,
                reason=block_reason,
                symbol=candidate.symbol,
                timestamp=candidate.timestamp,
                side=0,
                router_branch="NONE",
                score=None,
                size_mult=0.0,
            )
            self.logger.info(f"[LiveEngine] entry_decision : {entry_decision}")
            return None

        regime = RegimeSnapshot(
            timestamp=snap.timestamp,
            atr_bps=float(atr_bps),
            vol_bucket=vol_bucket,
            vol_regime3=(
                "low" if vol_bucket in ("b0", "b1")
                else "mid" if vol_bucket == "b2"
                else "high" if vol_bucket in ("b3", "b4")
                else "other"
            ),
        )

        router_decision = self.router.decide(candidate.dir0, regime)

        feature_map = dict(candidate.features)
        feature_map["atr_bps"] = float(atr_bps)

        if router_decision.router_branch != "BO":
            ml_decision = MLDecision(
                enabled=False,
                score=None,
                accepted=True,
                threshold=None,
            )
        else:
            ml_decision = self.ml_service.score(feature_map)

        size_mult = self.sizer.size_from_ml(ml_decision)

        entry_decision = EntryDecision(
            should_enter=size_mult > 0,
            reason="ml_accept" if size_mult > 0 else "ml_reject",
            symbol=candidate.symbol,
            timestamp=candidate.timestamp,
            side=router_decision.trade_dir,
            router_branch=router_decision.router_branch,
            score=ml_decision.score,
            size_mult=size_mult,
        )

        if not entry_decision.should_enter:
            self.logger.info(f" [LiveEngine] entry_decision: {entry_decision}")
            return None

        self.logger.info(f" [LiveEngine] entry_decision: {entry_decision}")
        trade = self.executor.open_trade(entry_decision, entry_price=snap.mid)
        self.open_trades[trade.trade_id] = trade
        return trade

    def on_timer(self, now_ts, current_mid: float):
        closed = []

        for trade_id, trade in list(self.open_trades.items()):
            exit_decision = self.lifecycle.evaluate(
                trade=trade,
                now_ts=now_ts,
                current_price=current_mid,
            )
            if exit_decision.should_exit:
                self.executor.close_trade(
                    trade=trade,
                    exit_price=current_mid,
                    exit_time=now_ts,
                )
                self.logger.info(f"[LiveEngine] exit: {trade.trade_id} , {exit_decision}")
                closed.append(trade_id)

        for trade_id in closed:
            self.open_trades.pop(trade_id, None)