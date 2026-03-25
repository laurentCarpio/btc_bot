from __future__ import annotations


def compute_pnl_bps(entry_price: float, current_price: float, side: int) -> float:
    if entry_price <= 0 or current_price <= 0 or side == 0:
        return 0.0
    return float(side * (current_price / entry_price - 1.0) * 1e4)


def compute_pnl_net_bps(entry_price: float, current_price: float, side: int, fees_bps: float) -> float:
    pnl_raw_bps = compute_pnl_bps(entry_price=entry_price, current_price=current_price, side=side)
    return float(pnl_raw_bps - float(fees_bps))