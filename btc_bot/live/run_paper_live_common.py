from __future__ import annotations

import asyncio

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
from btc_bot.live.logging.trade_logger import logger_pub


ML_RUN_ROOT = "s3://tradebot-config-tokyo/research/ms_edge/ml/runs/run_id=stageb_ml_low_c1e8bb16ef"


async def run_paper_live(
    *,
    symbol: str,
    venue: str,
    ws_client,
    bridge_cls,
    logger,
) -> None:
    cfg = build_live_config()
    vol_edges = load_live_vol_bucket_edges()

    logger.info(f"[run_paper_live:{venue}] cfg.stage0={cfg.stage0}")
    logger.info(f"[run_paper_live:{venue}] cfg.thresholds={cfg.thresholds}")
    logger.info(f"[run_paper_live:{venue}] vol_edges={vol_edges}")

    ms_cut_monitor = MSCutMonitor(
        base_cut=cfg.thresholds.ms_cut_value,
        maxlen=5000,
        min_obs=200,
        q_low=95.0,
        q_high=97.0,
    )

    detector = Stage0Detector(
        cfg.stage0,
        cfg.thresholds,
        symbol=symbol,
        venue=venue,
        ms_cut_monitor=ms_cut_monitor,
    )

    artifact = load_xgb_artifact_from_run(
        run_root=ML_RUN_ROOT,
        threshold=0.70,
    )
    ml_service = MLInferenceService(artifact)

    engine = LiveEngine(
        cfg=cfg,
        detector=detector,
        ml_service=ml_service,
    )

    bucketizer = LiveBucketizer(
        freq_ms=cfg.stage0.freq_ms,
        max_level=15,
    )
    feature_builder = Stage0FeatureBuilder(cfg.stage0)

    atr_builder = ATRLiveBuilder(
        atr_n=cfg.regime.atr_n,
        tf_minutes=15,
        vol_bucket_edges=vol_edges,
    )

    orchestrator = LiveOrchestrator(
        engine=engine,
        bucketizer=bucketizer,
        feature_builder=feature_builder,
        atr_builder=atr_builder,
    )

    bridge = bridge_cls(
        ws_pub_client=ws_client,
        live_orchestrator=orchestrator,
        logger=logger,
    )

    await bridge.connect()

    logger.info(f"[run_paper_live:{venue}] ✅ live paper pipeline started")

    try:
        while True:
            await asyncio.sleep(60)

            # 🔥 AJOUT ICI
            if hasattr(bridge, "flush_spread_stats"):
                bridge.flush_spread_stats()

            if hasattr(bridge, "flush_trade_stats"):
                bridge.flush_trade_stats()

            live_q95 = "NA"
            live_q97 = "NA"
            eff_q95 = cfg.thresholds.ms_cut_value
            eff_q97 = cfg.thresholds.ms_cut_value
            n_obs = 0

            if ms_cut_monitor is not None:
                snap = ms_cut_monitor.get_shadow_snapshot()
                n_obs = snap.n_obs

                if snap.live_q95 is not None:
                    live_q95 = f"{snap.live_q95:.6f}"
                if snap.live_q97 is not None:
                    live_q97 = f"{snap.live_q97:.6f}"

                eff_q95 = snap.effective_cut_q95
                eff_q97 = snap.effective_cut_q97

            logger.info(
                f"[run_paper_live:{venue}] heartbeat "
                f"book_ready={orchestrator.book_ready} "
                f"trade_ready={orchestrator.trade_ready} "
                f"atr_ready={orchestrator.atr_ready} "
                f"last_regime={orchestrator.last_regime} "
                f"ms_shadow_n_obs={n_obs} "
                f"base_cut={cfg.thresholds.ms_cut_value:.6f} "
                f"live_q95={live_q95} "
                f"live_q97={live_q97} "
                f"effective_cut_q95={eff_q95:.6f} "
                f"effective_cut_q97={eff_q97:.6f}"
            )
    except asyncio.CancelledError:
        logger.info(f"[run_paper_live:{venue}] cancelled")
        raise