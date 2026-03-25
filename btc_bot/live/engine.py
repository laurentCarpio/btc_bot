from __future__ import annotations

from btc_bot.live.execution.paper_executor import PaperExecutor
from btc_bot.live.logging.trade_logger import logger_pub, logger_cand, log_candidate_event
from btc_bot.live.ml.inference import MLInferenceService
from btc_bot.live.models import EntryDecision, Stage0Snapshot, MLDecision, RegimeSnapshot
from btc_bot.live.router.router import BranchRouter
from btc_bot.live.stage0.detector import Stage0Detector
from btc_bot.live.trade.lifecycle import TradeLifecycleManager
from btc_bot.live.trade.pnl import compute_pnl_bps
from btc_bot.live.trade.sizing import SimpleSizer


class LiveEngine:
    def __init__(self, cfg, detector: Stage0Detector, ml_service: MLInferenceService) -> None:
        self.cfg = cfg
        self.detector = detector
        self.ml_service = ml_service
        self.router = BranchRouter(mr_bucket=self.cfg.regime.mr_bucket)
        self.sizer = SimpleSizer()

        self.roundtrip_fees_bps = float(self.cfg.stage0.fee_maker_bps) + float(self.cfg.stage0.fee_taker_bps)

        self.lifecycle = TradeLifecycleManager(
            self.cfg.exit,
            roundtrip_fees_bps=self.roundtrip_fees_bps,
        )

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
        self.logger.info(f"[LiveEngine] router_decision: {router_decision}")

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

        self.logger.info(f"[LiveEngine] ml_decision: {ml_decision}")

        size_mult = self.sizer.size_from_ml(ml_decision)

        if router_decision.router_branch != "BO":
            reason = "mr_accept" if size_mult > 0 else "mr_reject"
        else:
            reason = "ml_accept" if size_mult > 0 else "ml_reject"

        entry_decision = EntryDecision(
            should_enter=size_mult > 0,
            reason=reason,
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

        self.logger.info(f"[LiveEngine] entry_decision: {entry_decision}")

        trade = self.executor.open_trade(entry_decision, entry_price=snap.mid)
        self.open_trades[trade.trade_id] = trade

        self.logger.info(
            f"[LiveEngine] trade_opened: "
            f"trade_id={trade.trade_id} "
            f"symbol={trade.symbol} "
            f"side={trade.side} "
            f"router_branch={trade.router_branch} "
            f"score_ml={trade.score_ml} "
            f"size_mult={trade.size_mult} "
            f"entry_time={trade.entry_time.isoformat()} "
            f"entry_price={trade.entry_price:.6f}"
        )

        log_candidate_event(
            logger_cand,
            {
                "venue": getattr(self.detector, "venue", "unknown"),
                "event": "trade_opened",
                "trade_id": trade.trade_id,
                "symbol": trade.symbol,
                "side": int(trade.side),
                "router_branch": trade.router_branch,
                "score_ml": None if trade.score_ml is None else float(trade.score_ml),
                "size_mult": float(trade.size_mult),
                "entry_time": trade.entry_time.isoformat(),
                "entry_price": float(trade.entry_price),
            },
        )

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
                pnl_raw_bps = compute_pnl_bps(
                    entry_price=trade.entry_price,
                    current_price=current_mid,
                    side=trade.side,
                )

                fees_bps = float(self.roundtrip_fees_bps)

                self.executor.close_trade(
                    trade=trade,
                    exit_price=current_mid,
                    exit_time=now_ts,
                    pnl_bps=exit_decision.pnl_bps,
                    exit_reason=exit_decision.reason,
                    pnl_raw_bps=pnl_raw_bps,
                    fees_bps=fees_bps,
                )

                holding_s = (now_ts - trade.entry_time).total_seconds()

                self.logger.info(
                    f"[LiveEngine] trade_closed: "
                    f"trade_id={trade.trade_id} "
                    f"symbol={trade.symbol} "
                    f"side={trade.side} "
                    f"router_branch={trade.router_branch} "
                    f"score_ml={trade.score_ml} "
                    f"size_mult={trade.size_mult} "
                    f"entry_time={trade.entry_time.isoformat()} "
                    f"exit_time={now_ts.isoformat()} "
                    f"entry_price={trade.entry_price:.6f} "
                    f"exit_price={float(current_mid):.6f} "
                    f"pnl_raw_bps={float(pnl_raw_bps):.6f} "
                    f"fees_bps={fees_bps:.6f} "
                    f"pnl_bps={float(exit_decision.pnl_bps):.6f} "
                    f"holding_s={holding_s:.3f} "
                    f"reason={exit_decision.reason}"
                )

                log_candidate_event(
                    logger_cand,
                    {
                        "venue": getattr(self.detector, "venue", "unknown"),
                        "event": "trade_closed",
                        "trade_id": trade.trade_id,
                        "symbol": trade.symbol,
                        "side": int(trade.side),
                        "router_branch": trade.router_branch,
                        "score_ml": None if trade.score_ml is None else float(trade.score_ml),
                        "size_mult": float(trade.size_mult),
                        "entry_time": trade.entry_time.isoformat(),
                        "exit_time": now_ts.isoformat(),
                        "entry_price": float(trade.entry_price),
                        "exit_price": float(current_mid),
                        "pnl_raw_bps": float(pnl_raw_bps),
                        "fees_bps": float(fees_bps),
                        "pnl_bps": float(exit_decision.pnl_bps),
                        "holding_s": float(holding_s),
                        "reason": str(exit_decision.reason),
                    },
                )

                closed.append(trade_id)

        for trade_id in closed:
            self.open_trades.pop(trade_id, None) 