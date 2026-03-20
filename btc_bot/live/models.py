from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Dict, Any


@dataclass(slots=True)
class Stage0Snapshot:
    timestamp: object
    bid_0_price: float
    ask_0_price: float
    mid: float
    spread_bps: float
    spread_ticks_1s: float
    spread_rel_5m: float
    micro_bias_bps: float
    OBI_10: float
    TI: float
    nps: float
    Ntot: float
    MS: float
    dir0: int
    persist_micro_ms: float
    persist_obi10_ms: float
    thinning_opp_3: float
    range_60s_bps: float
    range_10m_bps: float


@dataclass(slots=True)
class Stage0Candidate:
    timestamp: object
    symbol: str
    dir0: int
    ms: float
    features: Dict[str, float]
    pass_all_hard: bool
    pass_ms_keep: bool


@dataclass(slots=True)
class RegimeSnapshot:
    timestamp: object
    atr_bps: float
    vol_bucket: str
    vol_regime3: str


@dataclass(slots=True)
class RouterDecision:
    router_branch: str
    trade_dir: int


@dataclass(slots=True)
class MLDecision:
    enabled: bool
    score: Optional[float]
    accepted: bool
    threshold: Optional[float]


@dataclass(slots=True)
class EntryDecision:
    should_enter: bool
    reason: str
    symbol: str
    timestamp: object
    side: int
    router_branch: str
    score: Optional[float]
    size_mult: float


@dataclass(slots=True)
class OpenTrade:
    trade_id: str
    symbol: str
    entry_time: object
    entry_price: float
    side: int
    router_branch: str
    score_ml: Optional[float]
    size_mult: float
    status: str = "OPEN"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExitDecision:
    should_exit: bool
    reason: str
    timestamp: object
    pnl_bps: float