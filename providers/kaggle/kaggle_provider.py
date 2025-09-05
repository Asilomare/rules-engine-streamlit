import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class KaggleMetricsProvider:
    def __init__(self, config_or_data: Any):
        if isinstance(config_or_data, dict):
            self.data: Dict[str, Any] = config_or_data.get("data") or config_or_data
        elif isinstance(config_or_data, list):
            self.data = {"items": config_or_data}
        else:
            try:
                self.data = json.loads(str(config_or_data))
            except Exception:
                raise ValueError("Unsupported Kaggle provider config type")

        # Kaggle JSON shape places user data under userProfile
        self.profile: Dict[str, Any] = self.data.get("userProfile") or {}

    # --- helpers ---
    @staticmethod
    def _is_truthy(value: Any) -> bool:
        return bool(value) and str(value).strip().lower() not in {"none", "null", "na"}

    @staticmethod
    def _parse_iso8601(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        s = str(value).strip()
        try:
            if s.endswith("Z"):
                s = s.replace("Z", "+00:00")
            return datetime.fromisoformat(s)
        except Exception:
            return None

    @staticmethod
    def _months_between(a: datetime, b: datetime) -> int:
        if b < a:
            a, b = b, a
        years = b.year - a.year
        months = b.month - a.month
        total = years * 12 + months
        if b.day < a.day:
            total -= 1
        return max(total, 0)

    @staticmethod
    def _days_between(a: datetime, b: datetime) -> int:
        return abs((b - a).days)

    # --- boolean metrics (EXISTS) ---
    def metric_profile_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self.profile.get("userId"))

    def metric_avatar_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self.profile.get("userAvatarUrl"))

    def metric_bio_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self.profile.get("bio"))

    def metric_github_username_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self.profile.get("gitHubUserName"))

    def metric_twitter_username_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self.profile.get("twitterUserName"))

    def metric_linkedin_url_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self.profile.get("linkedInUrl"))

    def metric_website_url_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self.profile.get("websiteUrl"))

    def metric_rare_badge_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        badges = self.profile.get("badges") or []
        if isinstance(badges, list):
            for b in badges:
                bd = (b or {}).get("badge") or {}
                if isinstance(bd, dict) and ("rarityIndex" in bd) and bool(bd.get("rarityIndex")):
                    return True
        return False

    # --- numeric metrics (RANGE) ---
    def metric_account_age_months(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        joined = self._parse_iso8601(self.profile.get("userJoinDate"))
        if not joined:
            return 0
        now = datetime.now(timezone.utc)
        return self._months_between(joined, now)

    def metric_last_active_days(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        last_active = self._parse_iso8601(self.profile.get("userLastActive"))
        if not last_active:
            return 10_000
        now = datetime.now(timezone.utc)
        return self._days_between(last_active, now)

    def metric_datasets_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        try:
            return int(self.profile.get("totalDatasets") or 0)
        except Exception:
            return 0

    def metric_notebooks_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        try:
            return int(self.profile.get("totalKernels") or 0)
        except Exception:
            return 0

    def metric_discussions_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        try:
            return int(self.profile.get("totalDiscussions") or 0)
        except Exception:
            return 0

    def metric_models_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        try:
            return int(self.profile.get("totalModels") or 0)
        except Exception:
            return 0

    def metric_hosted_competitions_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        try:
            return int(self.profile.get("totalHostedCompetitions") or 0)
        except Exception:
            return 0

    def metric_writeups_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        try:
            return int(self.profile.get("totalWriteUps") or 0)
        except Exception:
            return 0

    def metric_followers_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        try:
            return int(self.profile.get("totalUsersFollowingMe") or 0)
        except Exception:
            return 0

    def metric_badges_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        badges = self.profile.get("badges") or []
        return len(badges) if isinstance(badges, list) else 0

    # --- dispatcher ---
    def get_metric(self, metric_name: str, window_value: Optional[int], window_unit: Optional[str]) -> Any:
        method_name = f"metric_{metric_name}"
        method = getattr(self, method_name, None)
        if not method:
            raise ValueError(f"Unsupported metric: {metric_name}")
        return method(window_value, window_unit)


