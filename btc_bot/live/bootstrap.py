from __future__ import annotations

from btc_bot.live.config import LiveConfig, get_default_config
from btc_bot.live.logging.trade_logger import logger_pub
from btc_bot.live.stage0.config_loader import (
    load_stage0_config_from_s3,
    load_thresholds_from_s3,
    load_vol_bucket_edges_from_s3,
)


STAGE0_CFG_S3 = (
    "s3://tradebot-config-tokyo/research/ms_edge/stage0_thresholds/stage0_cfg_prod.json"
)

STAGE0_THRESHOLDS_S3 = (
    "s3://tradebot-config-tokyo/research/ms_edge/stage0_thresholds/stage0_thresholds_prod.json"
)

VOL_BUCKET_EDGES_S3 = (
    "s3://tradebot-config-tokyo/research/ms_edge/stage0_thresholds/vol_bucket_edges_prod.json"
)

def load_live_vol_bucket_edges() -> list[float]:
    edges = load_vol_bucket_edges_from_s3(VOL_BUCKET_EDGES_S3)
    logger_pub.info(f"[bootstrap] loaded vol bucket edges: {edges}")
    return edges

def build_live_config() -> LiveConfig:
    cfg = get_default_config()

    cfg.stage0 = load_stage0_config_from_s3(STAGE0_CFG_S3)
    cfg.thresholds = load_thresholds_from_s3(STAGE0_THRESHOLDS_S3)

    logger_pub.info(
        f"[bootstrap] loaded stage0 cfg: "
        f"trades_window_s={cfg.stage0.trades_window_s} "
        f"persist_window_ms={cfg.stage0.persist_window_ms} "
        f"thinning_window_s={cfg.stage0.thinning_window_s} "
        f"tick_size={cfg.stage0.tick_size} "
        f"spread_roll_med_s={cfg.stage0.spread_roll_med_s} "
        f"freq_ms={cfg.stage0.freq_ms} "
        f"depths={cfg.stage0.depths} "
        f"persist_ms_min={cfg.stage0.persist_ms_min}"
    )

    logger_pub.info(
        f"[bootstrap] loaded thresholds: "
        f"range60s_min={cfg.thresholds.range60s_min:.4f} "
        f"obi10_abs_min={cfg.thresholds.obi10_abs_min:.6f} "
        f"micro_abs_min={cfg.thresholds.micro_abs_min:.6f} "
        f"ti_abs_min={cfg.thresholds.ti_abs_min:.6f} "
        f"nps_min={cfg.thresholds.nps_min:.2f} "
        f"thin_min={cfg.thresholds.thin_min:.6f} "
        f"ms_cut_value={cfg.thresholds.ms_cut_value:.6f}"
    )

    return cfg