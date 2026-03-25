from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

@dataclass(slots=True)
class RiskConfig:
    one_position_per_symbol: bool = True

@dataclass(slots=True)
class Stage0Config:
    trades_window_s: float = 2.0
    persist_window_ms: int = 800
    thinning_window_s: float = 2.0
    tick_size: float = 0.1
    spread_roll_med_s: int = 300
    freq_ms: int = 250
    depths: tuple[int, ...] = (1, 3, 5, 10, 14)
    persist_ms_min: int = 250
    fee_maker_bps: float = 2.0
    fee_taker_bps: float = 6.0


@dataclass(slots=True)
class ThresholdConfig:
    spread_ticks_max: float = 999.0
    spread_rel_max: float = 999.0
    range60s_min: float = 0.0
    micro_abs_min: float = 0.0
    obi10_abs_min: float = 0.0
    ti_abs_min: float = 0.0
    nps_min: float = 0.0
    thin_min: float = 0.0

    med_micro_abs: float = 0.0
    mad_micro_abs: float = 1.0
    med_obi10_abs: float = 0.0
    mad_obi10_abs: float = 1.0
    med_ti_abs: float = 0.0
    mad_ti_abs: float = 1.0
    med_thin: float = 0.0
    mad_thin: float = 1.0
    med_spread: float = 0.0
    mad_spread: float = 1.0
    med_nps: float = 0.0
    mad_nps: float = 1.0
    ms_cut_value: float = float("-inf")


@dataclass(slots=True)
class RegimeConfig:
    atr_tf: str = "15min"
    atr_n: int = 14
    n_vol_buckets: int = 5
    mr_bucket: str = "b2"

@dataclass(slots=True)
class ExitConfig:
    check_negative_at_s: int = 60
    hard_exit_at_s: int = 180
    negative_buffer_bps: float = 1.0


@dataclass(slots=True)
class MLConfig:
    enabled: bool = True
    model_path: str = ""
    score_threshold: float = 0.70
    feature_cols: List[str] = field(default_factory=lambda: [
        "MS",
        "micro_bias_bps",
        "abs_micro_bias_bps",
        "OBI_10",
        "abs_OBI_10",
        "TI",
        "abs_TI",
        "thinning_opp_3",
        "persist_micro_ms",
        "persist_obi10_ms",
        "range_60s_bps",
        "atr_bps",
    ])


@dataclass(slots=True)
class LiveConfig:
    symbol: str = "BTCUSDT"
    mode: str = "paper"
    stage0: Stage0Config = field(default_factory=Stage0Config)
    thresholds: ThresholdConfig = field(default_factory=ThresholdConfig)
    regime: RegimeConfig = field(default_factory=RegimeConfig)
    exit: ExitConfig = field(default_factory=ExitConfig)
    ml: MLConfig = field(default_factory=MLConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)


def get_default_config() -> LiveConfig:
    return LiveConfig()