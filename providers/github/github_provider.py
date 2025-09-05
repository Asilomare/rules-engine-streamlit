import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional


class GitHubMetricsProvider:
    def __init__(self, config_or_payload):
        # Expect the static payload produced by fetch_github_data.py
        if isinstance(config_or_payload, dict) and (config_or_payload.get("computed_metrics") or config_or_payload.get("repos")):
            self.payload = config_or_payload
        else:
            # If provided a path or other structure, try to read provider_github.json
            here = os.path.dirname(os.path.realpath(__file__))
            provider_path = os.path.join(here, "provider_github.json")
            with open(provider_path, "r", encoding="utf-8") as f:
                self.payload = json.load(f)

    def get_user(self) -> Dict[str, Any]:
        return self.payload.get("user", {})

    def get_repos(self) -> List[Dict[str, Any]]:
        return self.payload.get("repos", [])

    @staticmethod
    def _parse_github_datetime(dt_str: str) -> datetime:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))

    @staticmethod
    def _since_datetime(window_value: Optional[int], window_unit: Optional[str]) -> Optional[datetime]:
        if not window_value or not window_unit:
            return None
        now = datetime.now(timezone.utc)
        unit = window_unit.lower()
        if unit in ("day", "days"):
            return now - timedelta(days=window_value)
        if unit in ("hour", "hours"):
            return now - timedelta(hours=window_value)
        if unit in ("week", "weeks"):
            return now - timedelta(weeks=window_value)
        if unit in ("month", "months"):
            return now - timedelta(days=30 * window_value)
        if unit in ("year", "years"):
            return now - timedelta(days=365 * window_value)
        return None

    def metric_user_reachable(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        user = self.get_user()
        url = user.get("url") or f"https://api.github.com/users/{user.get('login')}"
        _ = self._get(url)
        return True

    def metric_public_repos_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return int(self.payload.get("computed_metrics", {}).get("public_repos_count", len(self.get_repos())))

    def metric_non_fork_ratio(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> float:
        return float(self.payload.get("computed_metrics", {}).get("non_fork_ratio", 0.0))

    def metric_recent_repos_pushed_count(self, window_value: Optional[int], window_unit: Optional[str]) -> int:
        # Use 90d precomputed when matches window; otherwise approximate via repo timestamps
        if window_value == 90 and (window_unit or "").lower().startswith("day"):
            return int(self.payload.get("computed_metrics", {}).get("recent_repos_pushed_90d", 0))
        repos = self.get_repos()
        since_dt = self._since_datetime(window_value, window_unit)
        if not since_dt:
            return 0
        return sum(1 for r in repos if r.get("pushed_at") and self._parse_github_datetime(r["pushed_at"]) >= since_dt)

    def metric_stars_total_sum(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return int(self.payload.get("computed_metrics", {}).get("stars_total_sum", 0))

    def metric_max_repo_stars(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return int(self.payload.get("computed_metrics", {}).get("max_repo_stars", 0))

    def metric_language_distinct_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return int(self.payload.get("computed_metrics", {}).get("language_distinct_count", 0))

    def metric_commits_by_author_count(self, window_value: Optional[int], window_unit: Optional[str]) -> int:
        if window_value == 90 and (window_unit or "").lower().startswith("day"):
            return int(self.payload.get("computed_metrics", {}).get("commits_by_author_90d", 0))
        return 0

    def metric_pr_issue_involvement_count(self, window_value: Optional[int], window_unit: Optional[str]) -> int:
        if window_value == 180 and (window_unit or "").lower().startswith("day"):
            return int(self.payload.get("computed_metrics", {}).get("pr_issue_involvement_180d", 0))
        return 0

    def metric_authored_items_count(self, window_value: Optional[int], window_unit: Optional[str]) -> int:
        if window_value == 365 and (window_unit or "").lower().startswith("day"):
            return int(self.payload.get("computed_metrics", {}).get("authored_items_365d", 0))
        return 0

    def metric_owner_repos_updated_count(self, window_value: Optional[int], window_unit: Optional[str]) -> int:
        if window_value == 365 and (window_unit or "").lower().startswith("day"):
            return int(self.payload.get("computed_metrics", {}).get("owner_repos_updated_365d", 0))
        # fallback approximation
        return self.metric_recent_repos_pushed_count(window_value, window_unit)

    def metric_indexed_code_files_total(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return int(self.payload.get("computed_metrics", {}).get("indexed_code_files_total", 0))

    def get_metric(self, metric_name: str, window_value: Optional[int], window_unit: Optional[str]) -> Any:
        method_name = f"metric_{metric_name}"
        method = getattr(self, method_name, None)
        if not method:
            raise ValueError(f"Unsupported metric: {metric_name}")
        return method(window_value, window_unit)
