import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class GitlabMetricsProvider:
    def __init__(self, config_or_items: Any):
        # Accept dict with embedded data, or a raw list of project items, or JSON string
        if isinstance(config_or_items, dict):
            # common keys: {"data": [...]}, {"items": [...]}, {"projects": [...]}
            data = (
                config_or_items.get("data")
                or config_or_items.get("items")
                or config_or_items.get("projects")
                or config_or_items
            )
            if isinstance(data, list):
                self.items: List[Dict[str, Any]] = data
            else:
                # If dict-shaped, try to interpret a single project object as a one-item list
                self.items = [data] if isinstance(data, dict) else []
        elif isinstance(config_or_items, list):
            self.items = config_or_items
        else:
            try:
                parsed = json.loads(str(config_or_items))
                if isinstance(parsed, list):
                    self.items = parsed
                elif isinstance(parsed, dict):
                    inner = parsed.get("data") or parsed.get("items") or parsed.get("projects") or parsed
                    self.items = inner if isinstance(inner, list) else ([inner] if isinstance(inner, dict) else [])
                else:
                    self.items = []
            except Exception:
                raise ValueError("Unsupported GitLab provider config type")

    # --- helpers ---
    @staticmethod
    def _parse_dt(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            # GitLab returns ISO 8601 strings, sometimes with Z
            # Ensure tz-aware UTC
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    @staticmethod
    def _truthy(value: Any) -> bool:
        return bool(value) and str(value).strip().lower() not in {"none", "null", "na"}

    # --- metrics ---
    def metric_projects_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return len(self.items) if isinstance(self.items, list) else 0

    def metric_any_public_project(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        for proj in self.items:
            visibility = str(proj.get("visibility") or "").strip().lower()
            if visibility == "public":
                return True
        return False

    def metric_any_readme_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        for proj in self.items:
            if self._truthy(proj.get("readme_url")) or self._truthy(proj.get("readmeUrl")):
                return True
        return False

    def metric_total_star_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        total = 0
        for proj in self.items:
            try:
                total += int(proj.get("star_count") or proj.get("starCount") or 0)
            except Exception:
                continue
        return total

    def metric_total_forks_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        total = 0
        for proj in self.items:
            try:
                total += int(proj.get("forks_count") or proj.get("forksCount") or 0)
            except Exception:
                continue
        return total

    def metric_has_ci_enabled(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        for proj in self.items:
            jobs_enabled = proj.get("jobs_enabled")
            builds_access = str(proj.get("builds_access_level") or "").strip().lower()
            if (isinstance(jobs_enabled, bool) and jobs_enabled) or builds_access == "enabled":
                return True
        return False

    def metric_last_activity_days_since(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        now = datetime.now(timezone.utc)
        min_days = None
        for proj in self.items:
            last_activity = self._parse_dt(proj.get("last_activity_at") or proj.get("lastActivityAt"))
            if last_activity is None:
                continue
            delta_days = (now - last_activity).days
            min_days = delta_days if min_days is None else min(min_days, delta_days)
        # If nothing had a parsable timestamp, treat as very inactive
        return min_days if min_days is not None else 10_000

    # --- dispatcher ---
    def get_metric(self, metric_name: str, window_value: Optional[int], window_unit: Optional[str]) -> Any:
        method_name = f"metric_{metric_name}"
        method = getattr(self, method_name, None)
        if not method:
            raise ValueError(f"Unsupported metric: {metric_name}")
        return method(window_value, window_unit)


