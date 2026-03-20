from __future__ import annotations

from btc_bot.live.config import LiveConfig, get_default_config
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
    return load_vol_bucket_edges_from_s3(VOL_BUCKET_EDGES_S3)

def build_live_config() -> LiveConfig:
    cfg = get_default_config()

    cfg.stage0 = load_stage0_config_from_s3(STAGE0_CFG_S3)
    cfg.thresholds = load_thresholds_from_s3(STAGE0_THRESHOLDS_S3)

    return cfg