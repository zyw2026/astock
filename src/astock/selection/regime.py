from __future__ import annotations

from astock.logic_pool.models import MarketRegime


def classify_historical_regime(main_net_inflow: float | None) -> MarketRegime:
    value = main_net_inflow or 0.0
    if value <= -8e10:
        return "panic"
    if value <= -4e10:
        return "weak_rotation"
    if value >= 1e10:
        return "trend"
    return "rotation"


def classify_current_regime(market_overview: dict, recent_market_fund_flow: dict) -> tuple[MarketRegime, dict]:
    row = (market_overview.get("rows") or [{}])[0]
    leaders = row.get("industry_fund_flow_leaders") or []
    breadth = sum(1 for item in leaders if (item.get("pct_chg") or 0) >= 2)
    avg_pct = sum((item.get("pct_chg") or 0) for item in leaders) / len(leaders) if leaders else 0.0
    flow_rows = recent_market_fund_flow.get("rows") or []
    latest_flow = flow_rows[-1] if flow_rows else {}
    main_net_inflow = latest_flow.get("main_net_inflow") or 0.0

    if main_net_inflow <= -8e10 and breadth <= 1:
        regime: MarketRegime = "panic"
    elif main_net_inflow <= -4e10 and breadth <= 3:
        regime = "weak_rotation"
    elif breadth >= 3 and avg_pct >= 2.0:
        regime = "trend" if main_net_inflow > 0 else "rotation"
    else:
        regime = "rotation"
    return regime, {
        "main_net_inflow": main_net_inflow,
        "industry_leader_count": len(leaders),
        "strong_industry_count": breadth,
        "avg_leader_pct_chg": avg_pct,
    }
