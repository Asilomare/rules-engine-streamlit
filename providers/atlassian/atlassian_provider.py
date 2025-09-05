import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional


class AtlassianMetricsProvider:
    def __init__(self, config_or_items: Any):
        # Accept raw list of issues, dict with 'data' or 'issues', or path-like config
        self._issues: List[Dict[str, Any]] = []

        if isinstance(config_or_items, list):
            self._issues = config_or_items
        elif isinstance(config_or_items, dict):
            # Common shapes: { "data": [...] } or Jira search payload with key 'issues'
            if "data" in config_or_items:
                data = config_or_items["data"]
                if isinstance(data, list):
                    self._issues = data
                elif isinstance(data, dict) and isinstance(data.get("issues"), list):
                    self._issues = data.get("issues") or []
                else:
                    # If 'data' is a single issue object, wrap it
                    self._issues = [data]
            elif isinstance(config_or_items.get("issues"), list):
                self._issues = config_or_items.get("issues") or []
            else:
                # Fallback: treat dict as a single issue
                self._issues = [config_or_items]
        else:
            # If given a string, try treating as a JSON file path
            if isinstance(config_or_items, str) and os.path.exists(config_or_items):
                with open(config_or_items, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    self._issues = data
                elif isinstance(data, dict) and isinstance(data.get("issues"), list):
                    self._issues = data.get("issues") or []
                else:
                    self._issues = [data]
            else:
                raise ValueError("Unsupported Atlassian provider config type")

    @staticmethod
    def _parse_jira_datetime(dt_str: Optional[str]) -> Optional[datetime]:
        if not dt_str or not isinstance(dt_str, str):
            return None
        s = dt_str.strip()
        # Normalize timezone like +0000 to +00:00 and handle trailing Z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        else:
            s = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", s)
        try:
            return datetime.fromisoformat(s)
        except Exception:
            return None

    @staticmethod
    def _since_datetime(window_value: Optional[int], window_unit: Optional[str]) -> Optional[datetime]:
        if not window_value or not window_unit:
            return None
        now = datetime.now(timezone.utc)
        unit = (window_unit or "").lower()
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

    # Metric helpers
    def _iter_fields(self) -> List[Dict[str, Any]]:
        return [i.get("fields", {}) for i in self._issues if isinstance(i, dict)]

    # Metrics
    def metric_issues_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return bool(self._issues)

    def metric_issues_total_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return len(self._issues)

    def metric_recent_issues_created_count(self, window_value: Optional[int], window_unit: Optional[str]) -> int:
        since_dt = self._since_datetime(window_value, window_unit)
        if not since_dt:
            return 0
        total = 0
        for issue in self._issues:
            created = self._parse_jira_datetime(issue.get("fields", {}).get("created"))
            if created and created >= since_dt:
                total += 1
        return total

    def metric_recent_issues_updated_count(self, window_value: Optional[int], window_unit: Optional[str]) -> int:
        since_dt = self._since_datetime(window_value, window_unit)
        if not since_dt:
            return 0
        total = 0
        for issue in self._issues:
            updated = self._parse_jira_datetime(issue.get("fields", {}).get("updated"))
            if updated and updated >= since_dt:
                total += 1
        return total

    def metric_comments_total_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        total = 0
        for f in self._iter_fields():
            total += int((f.get("comment", {}) or {}).get("total", 0) or 0)
        return total

    def metric_worklogs_total_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        total = 0
        for f in self._iter_fields():
            total += int((f.get("worklog", {}) or {}).get("total", 0) or 0)
        return total

    def metric_projects_distinct_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        projects = set()
        for f in self._iter_fields():
            proj = f.get("project") or {}
            key = proj.get("key") or proj.get("id")
            if key is not None:
                projects.add(str(key))
        return len(projects)

    def metric_changelog_entries_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        total = 0
        for issue in self._issues:
            histories = (issue.get("changelog", {}) or {}).get("histories")
            if isinstance(histories, list):
                total += len(histories)
            else:
                # Fall back to 'total' if provided
                total += int((issue.get("changelog", {}) or {}).get("total", 0) or 0)
        return total

    def get_metric(self, metric_name: str, window_value: Optional[int], window_unit: Optional[str]) -> Any:
        method_name = f"metric_{metric_name}"
        method = getattr(self, method_name, None)
        if not method:
            raise ValueError(f"Unsupported metric: {metric_name}")
        return method(window_value, window_unit)


