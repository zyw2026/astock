from __future__ import annotations

import time

import httpx

from astock.app.settings import settings


class AksMcpRestClient:
    """Thin REST client boundary around the deployed aks-mcp service."""

    def __init__(self, base_url: str | None = None, timeout: float | None = None) -> None:
        self.base_url = (base_url or settings.aks_mcp_base_url).rstrip("/")
        self.timeout = timeout or settings.http_timeout_sec
        self.max_retries = settings.http_max_retries
        self.retry_backoff_sec = settings.http_retry_backoff_sec
        self.rate_limit_wait_sec = settings.http_rate_limit_wait_sec

    def post(self, path: str, payload: dict) -> dict:
        last_error: Exception | None = None
        with httpx.Client(base_url=self.base_url, timeout=self.timeout) as client:
            for attempt in range(1, self.max_retries + 1):
                try:
                    response = client.post(path, json=payload)
                except httpx.HTTPError as exc:
                    last_error = exc
                    if attempt == self.max_retries:
                        break
                    time.sleep(self.retry_backoff_sec * attempt)
                    continue

                if response.status_code == 429 and attempt < self.max_retries:
                    retry_after = response.headers.get("Retry-After")
                    wait_sec = float(retry_after) if retry_after else self.rate_limit_wait_sec
                    time.sleep(wait_sec)
                    continue

                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    detail = response.text[:500]
                    raise RuntimeError(
                        f"aks-mcp request failed: path={path} status={response.status_code} payload={payload} detail={detail}"
                    ) from exc
                return response.json()
        raise RuntimeError(f"aks-mcp request failed after retries: path={path} payload={payload}") from last_error

    def query_tool(self, tool_name: str, payload: dict | None = None) -> dict:
        return self.post(f"/api/v1/query/{tool_name}", payload or {})

    def readyz(self) -> bool:
        with httpx.Client(base_url=self.base_url, timeout=self.timeout) as client:
            response = client.get("/readyz")
            response.raise_for_status()
            return True

    def market_overview(self, *, top_n: int = 5) -> dict:
        return self.query_tool("market_overview", {"top_n": top_n})

    def market_fund_flow(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
    ) -> dict:
        payload: dict = {}
        if start_date:
            payload["start_date"] = start_date
        if end_date:
            payload["end_date"] = end_date
        if limit is not None:
            payload["limit"] = limit
        return self.query_tool("market_fund_flow", payload)

    def sector_fund_flow_rank(self, *, sector_type: str, limit: int = 20) -> dict:
        return self.query_tool("sector_fund_flow_rank", {"sector_type": sector_type, "limit": limit})

    def stock_list(
        self,
        *,
        symbol: list[str] | None = None,
        keyword: str | None = None,
        listed_status: str | list[str] | None = "active",
        limit: int = 100,
        cursor: str | None = None,
        fields: list[str] | None = None,
    ) -> dict:
        payload: dict = {"limit": limit}
        if symbol is not None:
            payload["symbol"] = symbol
        if keyword is not None:
            payload["keyword"] = keyword
        if listed_status is not None:
            payload["listed_status"] = listed_status
        if cursor is not None:
            payload["cursor"] = cursor
        if fields is not None:
            payload["fields"] = fields
        return self.query_tool("stock_list", payload)

    def trade_calendar(
        self,
        *,
        trade_date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        is_open: bool | None = None,
        limit: int = 100,
        cursor: str | None = None,
        fields: list[str] | None = None,
    ) -> dict:
        payload: dict = {"limit": limit}
        if trade_date is not None:
            payload["trade_date"] = trade_date
        if start_date is not None:
            payload["start_date"] = start_date
        if end_date is not None:
            payload["end_date"] = end_date
        if is_open is not None:
            payload["is_open"] = is_open
        if cursor is not None:
            payload["cursor"] = cursor
        if fields is not None:
            payload["fields"] = fields
        return self.query_tool("trade_calendar", payload)

    def stock_hist(
        self,
        *,
        symbol: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        adjust: str = "none",
        fields: list[str] | None = None,
    ) -> dict:
        payload: dict = {"symbol": symbol, "adjust": adjust}
        if start_date:
            payload["start_date"] = start_date
        if end_date:
            payload["end_date"] = end_date
        if limit is not None:
            payload["limit"] = limit
        if fields:
            payload["fields"] = fields
        return self.query_tool("stock_hist", payload)

    def stock_fund_flow(self, *, symbol: list[str], start_date: str | None = None, end_date: str | None = None) -> dict:
        payload: dict = {"symbol": symbol}
        if start_date:
            payload["start_date"] = start_date
        if end_date:
            payload["end_date"] = end_date
        return self.query_tool("stock_fund_flow", payload)

    def stock_indicators(
        self,
        *,
        symbol: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        fields: list[str] | None = None,
    ) -> dict:
        payload: dict = {"symbol": symbol}
        if start_date:
            payload["start_date"] = start_date
        if end_date:
            payload["end_date"] = end_date
        if limit is not None:
            payload["limit"] = limit
        if fields:
            payload["fields"] = fields
        return self.query_tool("stock_indicators", payload)

    def stock_selector(
        self,
        *,
        strategy: str,
        trade_date: str | None = None,
        limit: int = 20,
        listed_status: str | list[str] | None = "active",
        **filters,
    ) -> dict:
        payload: dict = {"strategy": strategy, "limit": limit}
        if trade_date:
            payload["trade_date"] = trade_date
        if listed_status is not None:
            payload["listed_status"] = listed_status
        payload.update({key: value for key, value in filters.items() if value is not None})
        return self.query_tool("stock_selector", payload)

    def paginate_stock_list(self, *, listed_status: str | list[str] | None = "active", limit: int = 100) -> list[dict]:
        cursor = None
        rows: list[dict] = []
        while True:
            payload = self.stock_list(
                listed_status=listed_status,
                limit=limit,
                cursor=cursor,
                fields=["symbol", "name", "listed_status"],
            )
            rows.extend(payload.get("rows", []))
            cursor = (payload.get("page_info") or {}).get("next_cursor")
            if not cursor:
                break
        return rows

    def paginate_trade_calendar(
        self,
        *,
        start_date: str,
        end_date: str,
        is_open: bool | None = None,
        limit: int = 200,
    ) -> list[dict]:
        cursor = None
        rows: list[dict] = []
        while True:
            payload = self.trade_calendar(
                start_date=start_date,
                end_date=end_date,
                is_open=is_open,
                limit=limit,
                cursor=cursor,
                fields=["trade_date", "is_open"],
            )
            rows.extend(payload.get("rows", []))
            cursor = (payload.get("page_info") or {}).get("next_cursor")
            if not cursor:
                break
        return rows

    @staticmethod
    def rows_frame(response: dict):
        import polars as pl

        rows = response.get("rows", [])
        return pl.DataFrame(rows) if rows else pl.DataFrame()
