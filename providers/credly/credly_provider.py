import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union


class CredlyMetricsProvider:
    def __init__(self, config: Union[Dict[str, Any], List[Dict[str, Any]]]):
        # Accept either a dict (with embedded data or path) or a raw list of badges
        self.details: Dict[str, Any] = {}
        self.badges: List[Dict[str, Any]] = []
        self.badges = config
        
    # --- helpers ---
    @staticmethod
    def _parse_date(dt_str: Optional[str]) -> Optional[datetime]:
        if not dt_str:
            return None
        try:
            # Incoming example: '2024-03-05'
            return datetime.fromisoformat(dt_str).replace(tzinfo=timezone.utc)
        except Exception:
            return None

    # --- metrics ---
    def metric_profile_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return bool(self.details.get("url") or self.details.get("username") or any(self.badges))

    def metric_uuid_resolved(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        # Treat presence of badges as resolution success
        return bool(self.badges)

    def metric_profile_http_ok(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return bool(self.badges)

    def metric_pagination_complete(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return True

    def metric_fetch_errors(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return 0

    def metric_badge_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return len(self.badges)

    def metric_latest_badge_age_months(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> Optional[float]:
        if not self.badges:
            return None
        now = datetime.now(timezone.utc)
        latest_dt: Optional[datetime] = None
        for b in self.badges:
            issued_str = b.get("issued_at") or b.get("issued_at_date")
            dt = self._parse_date(issued_str)
            if dt and (latest_dt is None or dt > latest_dt):
                latest_dt = dt
        if not latest_dt:
            return None
        delta_days = (now - latest_dt).days
        return round(delta_days / 30.0, 2)

    def metric_tier1_badge_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        # Without a definitive tier taxonomy, infer Tier-1 via issuer or badge keywords
        keywords = ["professional", "expert", "architect", "engineer"]
        count = 0
        for b in self.badges:
            name = (b.get("badgeName") or "").lower()
            if any(k in name for k in keywords):
                count += 1
        return count

    def metric_issuer_primary_entity_distinct_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        ids = set()
        for b in self.badges:
            issuer = b.get("issuer") or {}
            entities = issuer.get("entities") or []
            for ent in entities:
                try:
                    if ent.get("primary"):
                        entity = ent.get("entity") or {}
                        ent_id = entity.get("id")
                        if ent_id:
                            ids.add(ent_id)
                except Exception:
                    continue
        return len(ids)

    def metric_top_issuer_concentration_pct(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> float:
        if not self.badges:
            return 0.0
        counts: Dict[str, int] = {}
        for b in self.badges:
            issuer = b.get("issuerName") or b.get("issuer") or "unknown"
            counts[issuer] = counts.get(issuer, 0) + 1
        top = max(counts.values())
        return round((top / len(self.badges)) * 100.0, 2)

    def metric_evidence_badge_ratio(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> float:
        # Percent of badges with evidence array length > 0
        if not self.badges:
            return 0.0
        with_evidence = 0
        for b in self.badges:
            ev = b.get("evidence")
            if isinstance(ev, list) and len(ev) > 0:
                with_evidence += 1
        return round((with_evidence / len(self.badges)) * 100.0, 2)

    def metric_skills_unique_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        skills = set()
        for b in self.badges:
            templ = b.get("badge_template") or {}
            sks = templ.get("skills") or []
            for s in sks:
                name = s.get("name") if isinstance(s, dict) else None
                if name:
                    skills.add(name)
        return len(skills)

    def metric_expired_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        now = datetime.now(timezone.utc)
        count = 0
        for b in self.badges:
            exp_str = b.get("expires_at") or b.get("expires_at_date")
            dt = self._parse_date(exp_str)
            if dt and dt < now:
                count += 1
        return count

    def metric_revoked_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return sum(1 for b in self.badges if str(b.get("state", "")).lower() == "revoked")

    def metric_future_dated_issue_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        now = datetime.now(timezone.utc)
        count = 0
        for b in self.badges:
            issued_str = b.get("issuedOn") or b.get("issued_at_date")
            dt = self._parse_date(issued_str)
            if dt and dt > now:
                count += 1
        return count

    def metric_expiry_present_ratio(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> float:
        if not self.badges:
            return 0.0
        with_expiry = 0
        for b in self.badges:
            if b.get("expires_at") or b.get("expires_at_date"):
                with_expiry += 1
        return round((with_expiry / len(self.badges)) * 100.0, 2)

    def metric_template_completeness_ratio(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> float:
        # Infer via presence of key fields per badge
        if not self.badges:
            return 0.0
        fields = ["badgeName", "issuerName", "issuedOn", "url"]
        completeness = []
        for b in self.badges:
            present = sum(1 for f in fields if bool(b.get(f)))
            completeness.append((present / len(fields)) * 100.0)
        return round(sum(completeness) / len(completeness), 2)

    def metric_verification_https_ratio(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> float:
        if not self.badges:
            return 0.0
        # use badge_template.image_url or evidence URLs? For now, verify public_url style is https via image_url fields
        https_count = 0
        for b in self.badges:
            url = b.get("image_url") or ((b.get("badge_template") or {}).get("image_url"))
            if isinstance(url, str) and url.startswith("https://"):
                https_count += 1
        return round((https_count / len(self.badges)) * 100.0, 2)

    def metric_skill_cloud_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return False

    def metric_skill_data_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return False

    def metric_skill_security_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return False

    def metric_badge_verification_url_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        # In this schema, verification via public URL isn't provided directly; infer via evidence presence
        for b in self.badges:
            ev = b.get("evidence")
            if isinstance(ev, list) and len(ev) > 0:
                return True
        return False

    def metric_badge_issuer_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        for b in self.badges:
            issuer = b.get("issuer") or {}
            entities = issuer.get("entities") or []
            if any(ent.get("primary") and (ent.get("entity") or {}).get("id") for ent in entities):
                return True
        return False

    def metric_badge_date_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return any(b.get("issued_at") or b.get("issued_at_date") for b in self.badges)

    def metric_badge_image_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        for b in self.badges:
            if b.get("image_url"):
                return True
            templ = b.get("badge_template") or {}
            if templ.get("image_url"):
                return True
        return False

    def metric_badge_template_url_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        for b in self.badges:
            templ = b.get("badge_template") or {}
            if templ.get("url"):
                return True
        return False

    def metric_badge_is_expired(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self.metric_expired_count(window_value, window_unit) > 0

    def metric_badge_is_revoked(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self.metric_revoked_count(window_value, window_unit) > 0

    def metric_badge_issue_in_future(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        now = datetime.now(timezone.utc)
        for b in self.badges:
            dt = self._parse_date(b.get("issued_at") or b.get("issued_at_date"))
            if dt and dt > now:
                return True
        return False

    def metric_badge_expiry_before_issue(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        for b in self.badges:
            issue_dt = self._parse_date(b.get("issued_at") or b.get("issued_at_date"))
            exp_dt = self._parse_date(b.get("expires_at") or b.get("expires_at_date"))
            if issue_dt and exp_dt and exp_dt < issue_dt:
                return True
        return False

    def get_metric(self, metric_name: str, window_value: Optional[int], window_unit: Optional[str]) -> Any:
        method_name = f"metric_{metric_name}"
        method = getattr(self, method_name, None)
        if not method:
            raise ValueError(f"Unsupported metric: {metric_name}")
        return method(window_value, window_unit)
