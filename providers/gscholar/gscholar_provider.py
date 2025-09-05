import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union


class GscholarMetricsProvider:
    def __init__(self, config: Union[Dict[str, Any], List[Dict[str, Any]]]):
        # Config can be dict with embedded data or raw dict shaped like SerpAPI author payload
        self.data: Dict[str, Any] = {}
        if isinstance(config, list):
            # Not expected for scholar; wrap as data
            self.data = {"articles": config}
        elif isinstance(config, dict):
            # If the file was a raw array at top-level, prior loader gives list; otherwise dict
            self.data = config
        else:
            self.data = {}

        # Normalize key roots if nested under "data"
        if "data" in self.data and isinstance(self.data["data"], dict):
            self.data = self.data["data"]

        self.author: Dict[str, Any] = self.data.get("author") or {}
        self.articles: List[Dict[str, Any]] = self.data.get("articles") or []
        self.cited_by: Dict[str, Any] = self.data.get("cited_by") or {}

    # --- helpers ---
    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _parse_year(value: Any) -> Optional[int]:
        try:
            return int(str(value).strip())
        except Exception:
            return None

    @staticmethod
    def _years_between(dt_a: datetime, dt_b: datetime) -> float:
        return (dt_b - dt_a).days / 365.25

    # --- metrics ---
    def metric_is_profile(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        has_name = bool((self.author.get("name") or "").strip())
        has_articles = bool(self.articles)
        return has_name or has_articles

    def metric_has_verified_email(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        email_field = self.author.get("email") or ""
        return isinstance(email_field, str) and email_field.lower().startswith("verified email")

    def metric_has_profile_photo(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        thumb = self.author.get("thumbnail")
        return bool(thumb)

    def metric_affiliation(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> Optional[str]:
        aff = self.author.get("affiliations")
        return aff if aff else None

    def metric_homepage_url(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> Optional[str]:
        # Some payloads use "website"
        url = self.author.get("website") or self.author.get("homepage")
        return url if url else None

    def _extract_cited_by_table(self) -> Dict[str, Dict[str, int]]:
        # Expect structure: cited_by.table = [ {"citations": {"all": X, "since_2020": Y}}, {"h_index": {...}}, {"i10_index": {...}} ]
        table = self.cited_by.get("table") or []
        out: Dict[str, Dict[str, int]] = {}
        for entry in table:
            if not isinstance(entry, dict):
                continue
            for key, val in entry.items():
                if isinstance(val, dict):
                    out[key] = {
                        k: int(v) for k, v in val.items() if isinstance(v, (int, float, str)) and str(v).isdigit()
                    }
        return out

    def metric_citations_all(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        table = self._extract_cited_by_table()
        return int(table.get("citations", {}).get("all") or 0)

    def metric_citations_5y(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        table = self._extract_cited_by_table()
        val = table.get("citations", {}).get("since_2020")
        return int(val or 0)

    def metric_h_index_all(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        table = self._extract_cited_by_table()
        return int(table.get("h_index", {}).get("all") or 0)

    def metric_h_index_5y(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        table = self._extract_cited_by_table()
        val = table.get("h_index", {}).get("since_2020")
        return int(val or 0)

    def metric_i10_index_all(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        table = self._extract_cited_by_table()
        return int(table.get("i10_index", {}).get("all") or 0)

    def metric_i10_index_5y(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        table = self._extract_cited_by_table()
        val = table.get("i10_index", {}).get("since_2020")
        return int(val or 0)

    def metric_articles_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return len(self.articles or [])

    def metric_last_pub_age_years(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> Optional[float]:
        if not self.articles:
            return None
        # Articles have a string year; choose max by year
        latest_year: Optional[int] = None
        for a in self.articles:
            y = self._parse_year(a.get("year"))
            if y is not None and (latest_year is None or y > latest_year):
                latest_year = y
        if latest_year is None:
            return None
        # Assume publication around mid-year for approximation
        pub_dt = datetime(latest_year, 7, 1, tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        years = self._years_between(pub_dt, now)
        return round(years, 2)

    def get_metric(self, metric_name: str, window_value: Optional[int], window_unit: Optional[str]) -> Any:
        method_name = f"metric_{metric_name}"
        method = getattr(self, method_name, None)
        if not method:
            raise ValueError(f"Unsupported metric: {metric_name}")
        return method(window_value, window_unit)


