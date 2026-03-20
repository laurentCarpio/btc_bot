from __future__ import annotations

import asyncio

from btc_bot.live.bootstrap import build_live_config, load_live_vol_bucket_edges
from btc_bot.live.bitget.bitget_live_socket_bridge import BitgetLiveSocketBridge
from btc_bot.live.bitget.ws_public_client import WsPublicClient
from btc_bot.live.engine import LiveEngine
from btc_bot.live.logging.trade_logger import logger_pub
from btc_bot.live.ml.artifacts import load_xgb_artifact_from_run
from btc_bot.live.ml.inference import MLInferenceService
from btc_bot.live.orchestrator import LiveOrchestrator
from btc_bot.live.regime.atr_live import ATRLiveBuilder
from btc_bot.live.stage0.bucketizer import LiveBucketizer
from btc_bot.live.stage0.detector import Stage0Detector
from btc_bot.live.stage0.features import Stage0FeatureBuilder


SYMBOL = "BTCUSDT"
ML_RUN_ROOT = "s3://tradebot-config-tokyo/research/ms_edge/ml/runs/run_id=stageb_ml_high_f3abb4ce18"


async def main():
    cfg = build_live_config()
    vol_edges = load_live_vol_bucket_edges()

    logger_pub.info(f"[run_paper_live] cfg.stage0={cfg.stage0}")
    logger_pub.info(f"[run_paper_live] cfg.thresholds={cfg.thresholds}")
    logger_pub.info(f"[run_paper_live] vol_edges={vol_edges}")

    detector = Stage0Detector(cfg.stage0, cfg.thresholds, symbol=SYMBOL)

    artifact = load_xgb_artifact_from_run(
        run_root=ML_RUN_ROOT,
        threshold=0.70,
    )
    ml_service = MLInferenceService(artifact)

    engine = LiveEngine(
        detector=detector,
        ml_service=ml_service
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
        atr_builder=atr_builder
    )

    ws_client = WsPublicClient(symbol=SYMBOL)

    bridge = BitgetLiveSocketBridge(
        ws_pub_client=ws_client,
        live_orchestrator=orchestrator,
        logger=logger_pub,
    )

    await bridge.connect()

    logger_pub.info("[run_paper_live] ✅ live paper pipeline started")

    try:
        while True:
            await asyncio.sleep(60)
            logger_pub.info(
                "[run_paper_live] heartbeat "
                f"book_ready={orchestrator.book_ready} "
                f"trade_ready={orchestrator.trade_ready} "
                f"atr_ready={orchestrator.atr_ready} "
                f"last_regime={orchestrator.last_regime}"
            )
    except asyncio.CancelledError:
        logger_pub.info("[run_paper_live] cancelled")
        raise


if __name__ == "__main__":
    asyncio.run(main())