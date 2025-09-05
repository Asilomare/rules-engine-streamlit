import json
from typing import Any, Dict, List, Optional


class FiverrMetricsProvider:
    def __init__(self, config_or_items: Any):
        # Accept dict with data or raw list
        if isinstance(config_or_items, dict):
            self.data: Dict[str, Any] = config_or_items.get("data") or config_or_items
        elif isinstance(config_or_items, list):
            self.data = {"items": config_or_items}
        else:
            try:
                self.data = json.loads(str(config_or_items))
            except Exception:
                raise ValueError("Unsupported Fiverr provider config type")

    # --- helpers ---
    @staticmethod
    def _is_truthy(value: Any) -> bool:
        return bool(value) and str(value).strip().lower() not in {"none", "null", "na"}

    def _get(self, *keys: str, default: Any = None) -> Any:
        cur: Any = self.data
        for key in keys:
            if not isinstance(cur, dict):
                return default
            cur = cur.get(key)
            if cur is None:
                return default
        return cur

    @staticmethod
    def _len_list(value: Any) -> int:
        return len(value) if isinstance(value, list) else 0

    @staticmethod
    def _to_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return default

    # --- boolean metrics (EXISTS/EQ TRUE) ---
    def metric_profile_activated(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return bool(self._get("seller", "user", "isActivationCompleted", default=False))

    def metric_profile_image_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self._get("seller", "user", "profileImageUrl"))

    def metric_verified_seller(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return bool(self._get("seller", "isVerified", default=False))

    def metric_notable_clients_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._len_list(self._get("seller", "notableClients", default=[])) > 0

    def metric_intro_video_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self._get("seller", "introVideo", "url"))

    def metric_seller_is_active(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return bool(self._get("seller", "isActive", default=False))

    def metric_vacation_mode(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return bool(self._get("seller", "isOnVacation", default=False))

    # --- numeric metrics (RANGE/GT/LT etc.) ---
    def metric_languages_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return self._len_list(self._get("seller", "user", "languages", default=[]))

    def metric_skills_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return self._len_list(self._get("seller", "activeStructuredSkills", default=[]))

    def metric_certifications_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return self._len_list(self._get("seller", "certifications", default=[]))

    def metric_educations_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return self._len_list(self._get("seller", "activeEducations", default=[]))

    def metric_hourly_rate_cents(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return self._to_int(self._get("seller", "hourlyRate", "priceInCents", default=0), 0)

    def metric_approved_gigs_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return self._to_int(self._get("seller", "approvedGigsCount", default=0), 0)

    def metric_portfolio_projects_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return self._to_int(self._get("seller", "portfolios", "totalCount", default=0), 0)

    def metric_rating_score(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> float:
        return self._to_float(self._get("seller", "rating", "score", default=0.0), 0.0)

    def metric_rating_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return self._to_int(self._get("seller", "rating", "count", default=0), 0)

    def metric_response_time_hours(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> float:
        return self._to_float(self._get("seller", "responseTime", "inHours", default=0), 0.0)

    def metric_buyer_reviews_total_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return self._to_int(self._get("reviewsData", "buying_reviews", "total_count", default=0), 0)

    def metric_buyer_average_rating(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> float:
        return self._to_float(self._get("reviewsData", "buying_reviews", "average_valuation", default=0.0), 0.0)

    # --- dispatcher ---
    def get_metric(self, metric_name: str, window_value: Optional[int], window_unit: Optional[str]) -> Any:
        method_name = f"metric_{metric_name}"
        method = getattr(self, method_name, None)
        if not method:
            raise ValueError(f"Unsupported metric: {metric_name}")
        return method(window_value, window_unit)


