from __future__ import annotations

from collections.abc import Iterable

from astock.logic_pool.models import LogicSpec, MarketRegime


class LogicRegistry:
    def __init__(self, specs: Iterable[LogicSpec] | None = None) -> None:
        self._specs: dict[str, LogicSpec] = {}
        for spec in specs or ():
            self.register(spec)

    def register(self, spec: LogicSpec) -> None:
        if spec.logic_id in self._specs:
            raise ValueError(f"duplicate logic_id: {spec.logic_id}")
        self._specs[spec.logic_id] = spec

    def get(self, logic_id: str) -> LogicSpec:
        return self._specs[logic_id]

    def all(self) -> list[LogicSpec]:
        return list(self._specs.values())

    def by_regime(self, regime: MarketRegime) -> list[LogicSpec]:
        return [spec for spec in self._specs.values() if regime in spec.regime_whitelist]
