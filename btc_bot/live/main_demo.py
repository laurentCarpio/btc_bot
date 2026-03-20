from datetime import datetime, timedelta, timezone

from btc_bot.live.bootstrap import build_live_config, load_live_vol_bucket_edges
from btc_bot.live.engine import LiveEngine
from btc_bot.live.ml.artifacts import load_xgb_artifact_from_run
from btc_bot.live.ml.inference import MLInferenceService
from btc_bot.live.stage0.bucketizer import LiveBucketizer
from btc_bot.live.stage0.detector import Stage0Detector
from btc_bot.live.stage0.features import Stage0FeatureBuilder
from btc_bot.live.regime.atr_live import ATRLiveBuilder, Candle1m

def warm_atr_regime(
    atr_builder: ATRLiveBuilder,
    start_ts: datetime,
    n_1m_candles: int = 240,
):
    regime = None
    base = 100000.0

    for i in range(n_1m_candles):
        ts = (start_ts + timedelta(minutes=i)).replace(second=0, microsecond=0)

        # volatilité assez élevée pour tomber plutôt en b3/b4 selon edges
        open_ = base + i * 8.0
        high = open_ + 120.0
        low = open_ - 110.0
        close = open_ + 25.0

        candle = Candle1m(
            timestamp=ts,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=10.0,
        )
        maybe_regime = atr_builder.update_1m_candle(candle)
        if maybe_regime is not None:
            regime = maybe_regime

    return regime

def build_warm_snapshot(
    bucketizer: LiveBucketizer,
    feature_builder: Stage0FeatureBuilder,
    start_ts: datetime,
    n_buckets: int = 12,
):
    snap = None

    base_bid = 100000.0

    for i in range(n_buckets):
        ts = start_ts + timedelta(milliseconds=250 * i)

        # drift plus fort pour créer un vrai range_60s_bps
        bid0 = base_bid + 15.0 * i
        ask0 = bid0 + 0.099  # légèrement sous 1 tick effectif

        bid_sizes = {
            0: 500.0,
            1: 480.0,
            2: 460.0,
            3: 440.0,
            4: 420.0,
            5: 400.0,
            10: 350.0,
            14: 300.0,
        }

        ask_scale = max(0.01, 1.0 - 0.14 * i)
        ask_sizes = {
            0: 5.0 * ask_scale,
            1: 4.0 * ask_scale,
            2: 3.0 * ask_scale,
            3: 2.5 * ask_scale,
            4: 2.0 * ask_scale,
            5: 1.8 * ask_scale,
            10: 1.2 * ask_scale,
            14: 1.0 * ask_scale,
        }

        bucketizer.update_book(
            ts=ts,
            bid_prices={
                0: bid0,
                1: bid0 - 0.1,
                2: bid0 - 0.2,
                3: bid0 - 0.3,
                4: bid0 - 0.4,
                5: bid0 - 0.5,
                10: bid0 - 1.0,
                14: bid0 - 1.4,
            },
            ask_prices={
                0: ask0,
                1: ask0 + 0.1,
                2: ask0 + 0.2,
                3: ask0 + 0.3,
                4: ask0 + 0.4,
                5: ask0 + 0.5,
                10: ask0 + 1.0,
                14: ask0 + 1.4,
            },
            bid_sizes=bid_sizes,
            ask_sizes=ask_sizes,
        )

        # flow très asymétrique acheteur pour pousser TI > 0.98
        bucketizer.update_trade(ts=ts, price=ask0, qty=4.0 + 0.3 * i, is_buy=True)
        bucketizer.update_trade(ts=ts, price=ask0, qty=3.0 + 0.2 * i, is_buy=True)

        # micro sell minuscule
        bucketizer.update_trade(ts=ts, price=bid0, qty=0.03, is_buy=False)

        closed = bucketizer.maybe_close_bucket(ts + timedelta(milliseconds=300))
        if closed is not None:
            snap = feature_builder.build_snapshot(closed)

    return snap

def main():
    cfg = build_live_config()
    edges = load_live_vol_bucket_edges()

    print("[cfg.stage0]", cfg.stage0)
    print("[cfg.thresholds]", cfg.thresholds)
    print("[vol_bucket_edges]", edges)

    bucketizer = LiveBucketizer(
        freq_ms=cfg.stage0.freq_ms,
        max_level=15,
    )
    
    feature_builder = Stage0FeatureBuilder(cfg.stage0)
    detector = Stage0Detector(cfg.stage0, cfg.thresholds, symbol=cfg.symbol)

    RUN_ROOT = "s3://tradebot-config-tokyo/research/ms_edge/ml/runs/run_id=stageb_ml_high_f3abb4ce18"
    artifact = load_xgb_artifact_from_run(
        run_root=RUN_ROOT,
        threshold=0.50,  # garde 0.50 pour le demo complet
    )
    ml_service = MLInferenceService(artifact)
    engine = LiveEngine(detector, ml_service)

    atr_builder = ATRLiveBuilder(
        atr_n=cfg.regime.atr_n,
        tf_minutes=15,
        vol_bucket_edges=edges,
    )

    t0 = datetime.now(timezone.utc)

    regime = warm_atr_regime(
        atr_builder=atr_builder,
        start_ts=t0 - timedelta(hours=8),
        n_1m_candles=240,
    )

    if regime is None:
        print("❌ No ATR regime built")
        return

    print("[regime]", regime)

    print("\n--- STEP 1: ENTRY ---")
    snap = build_warm_snapshot(
        bucketizer=bucketizer,
        feature_builder=feature_builder,
        start_ts=t0,
        n_buckets=8,
    )

    print("[snapshot]", snap)
    if snap is None:
        print("❌ No Stage0Snapshot built")
        return

    trade = engine.on_stage0_snapshot(
        snap,
        atr_bps=regime.atr_bps,
        vol_bucket=regime.vol_bucket,
    )

    if not trade:
        print("❌ No trade opened")
        return

    print("✅ Trade opened:", trade.trade_id)    

    print("\n--- STEP 1B: TRY SECOND ENTRY WHILE FIRST IS OPEN ---")
    trade_blocked = engine.on_stage0_snapshot(
        snap,
        atr_bps=15.0,
        vol_bucket="b3",
    )
    print("second trade result:", trade_blocked)

    print("\n--- STEP 2: +30s (should hold) ---")
    t1 = trade.entry_time + timedelta(seconds=30)
    engine.on_timer(t1, current_mid=100.2)

    print("\n--- STEP 3: +60s ---")
    t2 = trade.entry_time + timedelta(seconds=60)
    print("Scenario A: negative PnL → should exit")
    engine.on_timer(t2, current_mid=100095.0)

    print("\n--- REOPEN TRADE FOR SCENARIO B ---")
    t0_b = t0 + timedelta(minutes=10)

    snap_b = build_warm_snapshot(
        bucketizer=bucketizer,
        feature_builder=feature_builder,
        start_ts=t0_b,
        n_buckets=8,
    )
    print("[snapshot]", snap_b)

    if snap_b is None:
        print("❌ No Stage0Snapshot for scenario B")
        return

    trade_b = engine.on_stage0_snapshot(
        snap_b,
        atr_bps=regime.atr_bps,
        vol_bucket=regime.vol_bucket,
    )

    if not trade_b:
        print("❌ No trade opened for scenario B")
        return

    t2_b = trade_b.entry_time + timedelta(seconds=60)
    t3_b = trade_b.entry_time + timedelta(seconds=180)

    print("\n--- STEP 4: +60s positive → hold ---")
    engine.on_timer(t2_b, current_mid=100115.0)

    print("\n--- STEP 5: +180s → forced exit ---")
    engine.on_timer(t3_b, current_mid=100130.0)
    
if __name__ == "__main__":
    main()