import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class StackMetricsProvider:
    def __init__(self, config_or_items: Any):
        # Accept either dict with data, raw dict in provider shape, or raw list
        if isinstance(config_or_items, dict):
            self.data: Dict[str, Any] = config_or_items.get("data") or config_or_items
        elif isinstance(config_or_items, list):
            self.data = {"items": config_or_items}
        else:
            try:
                # Try JSON parsing from a string
                self.data = json.loads(str(config_or_items))
            except Exception:
                raise ValueError("Unsupported Stack provider config type")

    # --- helpers ---
    @staticmethod
    def _is_truthy(value: Any) -> bool:
        return bool(value) and str(value).strip().lower() not in {"none", "null", "na"}

    def _profile_item(self) -> Optional[Dict[str, Any]]:
        profile_container = self.data.get("profile") or {}
        items = profile_container.get("items") if isinstance(profile_container, dict) else None
        if isinstance(items, list) and items:
            return items[0]
        return None

    def _answers(self) -> List[Dict[str, Any]]:
        ans = self.data.get("answers") or {}
        items = ans.get("items") if isinstance(ans, dict) else None
        return items if isinstance(items, list) else []

    def _questions(self) -> List[Dict[str, Any]]:
        q = self.data.get("questions") or {}
        items = q.get("items") if isinstance(q, dict) else None
        return items if isinstance(items, list) else []

    def _tags(self) -> List[Dict[str, Any]]:
        t = self.data.get("tags") or {}
        items = t.get("items") if isinstance(t, dict) else None
        return items if isinstance(items, list) else []

    # --- boolean metrics (EXISTS) ---
    def metric_profile_image_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        user = self._profile_item() or {}
        return self._is_truthy(user.get("profile_image"))

    def metric_profile_link_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        user = self._profile_item() or {}
        return self._is_truthy(user.get("link"))

    def metric_display_name_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        user = self._profile_item() or {}
        return self._is_truthy(user.get("display_name"))

    def metric_is_suspended_or_private(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        user = self._profile_item() or {}
        user_type = str(user.get("user_type") or "").strip().lower()
        # Stack Exchange API does not directly expose suspended/private via this minimal payload.
        # Conservatively return False unless explicit signals appear.
        return user_type in {"does_not_exist", "suspended", "blocked"}

    # --- numeric metrics (RANGE) ---
    def metric_account_age_years(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> float:
        user = self._profile_item() or {}
        created_ts = user.get("creation_date")
        if not created_ts:
            return 0.0
        try:
            created_dt = datetime.fromtimestamp(int(created_ts), tz=timezone.utc)
        except Exception:
            return 0.0
        now = datetime.now(timezone.utc)
        age_days = (now - created_dt).days
        return round(age_days / 365.25, 2)

    def metric_last_access_days(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> float:
        user = self._profile_item() or {}
        last_access_ts = user.get("last_access_date")
        if not last_access_ts:
            return 999999.0
        try:
            last_access_dt = datetime.fromtimestamp(int(last_access_ts), tz=timezone.utc)
        except Exception:
            return 999999.0
        now = datetime.now(timezone.utc)
        return float((now - last_access_dt).days)

    def metric_reputation_total(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        user = self._profile_item() or {}
        try:
            return int(user.get("reputation") or 0)
        except Exception:
            return 0

    def metric_badge_gold_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        user = self._profile_item() or {}
        counts = user.get("badge_counts") or {}
        try:
            return int(counts.get("gold") or 0)
        except Exception:
            return 0

    def metric_badge_silver_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        user = self._profile_item() or {}
        counts = user.get("badge_counts") or {}
        try:
            return int(counts.get("silver") or 0)
        except Exception:
            return 0

    def metric_badge_bronze_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        user = self._profile_item() or {}
        counts = user.get("badge_counts") or {}
        try:
            return int(counts.get("bronze") or 0)
        except Exception:
            return 0

    def metric_answer_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return len(self._answers())

    def metric_question_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return len(self._questions())

    def metric_upvote_ratio(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> float:
        # Best-effort: compute from answers if up_vote_count/down_vote_count are present; else 0.0
        total_up = 0
        total_down = 0
        for ans in self._answers():
            try:
                total_up += int(ans.get("up_vote_count") or 0)
                total_down += int(ans.get("down_vote_count") or 0)
            except Exception:
                continue
        denom = total_up + total_down
        if denom <= 0:
            return 0.0
        return round(total_up / float(denom), 4)

    def metric_votes_cast_total(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        # Without explicit votes cast data, conservative default 0
        return 0

    def metric_answers_questions_ratio(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> float:
        answers_n = self.metric_answer_count(None, None)
        questions_n = self.metric_question_count(None, None)
        if questions_n <= 0:
            if answers_n <= 0:
                return 0.0
            # If no questions but some answers, treat as high ratio
            return float(answers_n)
        return round(answers_n / float(questions_n), 4)

    def metric_top_tags_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return len(self._tags())

    def metric_downvote_share(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> float:
        total_up = 0
        total_down = 0
        for ans in self._answers():
            try:
                total_up += int(ans.get("up_vote_count") or 0)
                total_down += int(ans.get("down_vote_count") or 0)
            except Exception:
                continue
        total_votes = total_up + total_down
        if total_votes <= 0:
            return 0.0
        return round(total_down / float(total_votes), 4)

    # --- dispatcher ---
    def get_metric(self, metric_name: str, window_value: Optional[int], window_unit: Optional[str]) -> Any:
        method_name = f"metric_{metric_name}"
        method = getattr(self, method_name, None)
        if not method:
            raise ValueError(f"Unsupported metric: {metric_name}")
        return method(window_value, window_unit)


