import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


class LinkedinMetricsProvider:
    def __init__(self, config_or_data):
        # Accept either a dict with data or a raw dict representing the profile
        if isinstance(config_or_data, dict):
            # If wrapped as {"data": {...}}, unwrap it
            self.data: Dict[str, Any] = config_or_data.get("data") or config_or_data
        elif isinstance(config_or_data, list):
            # Not expected for LinkedIn, but allow storing as generic container
            self.data = {"items": config_or_data}
        else:
            # Attempt JSON parse if a string path or JSON string is passed
            try:
                self.data = json.loads(str(config_or_data))
            except Exception:
                raise ValueError("Unsupported Linkedin provider config type")

    # --- helpers ---
    @staticmethod
    def _is_truthy(value: Any) -> bool:
        return bool(value) and str(value).strip().lower() not in {"none", "null", "na"}

    def _get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    @staticmethod
    def _parse_month_name(token: str) -> Optional[int]:
        months = {
            "jan": 1, "january": 1,
            "feb": 2, "february": 2,
            "mar": 3, "march": 3,
            "apr": 4, "april": 4,
            "may": 5,
            "jun": 6, "june": 6,
            "jul": 7, "july": 7,
            "aug": 8, "august": 8,
            "sep": 9, "sept": 9, "september": 9,
            "oct": 10, "october": 10,
            "nov": 11, "november": 11,
            "dec": 12, "december": 12,
        }
        t = token.strip().lower()
        return months.get(t)

    @staticmethod
    def _to_datetime(year: int, month: Optional[int]) -> datetime:
        month_value = month if month and 1 <= month <= 12 else 6
        return datetime(year=year, month=month_value, day=15, tzinfo=timezone.utc)

    @classmethod
    def _parse_year_month(cls, s: str) -> Optional[datetime]:
        s = (s or "").strip()
        if not s:
            return None
        if s.lower() == "present":
            return datetime.now(timezone.utc)
        m = re.match(r"^([A-Za-z]{3,9})\s+(\d{4})$", s)
        if m:
            month = cls._parse_month_name(m.group(1))
            year = int(m.group(2))
            if year >= 1900:
                return cls._to_datetime(year, month)
        m2 = re.match(r"^(\d{4})$", s)
        if m2:
            year = int(m2.group(1))
            if year >= 1900:
                return cls._to_datetime(year, None)
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

    @classmethod
    def _parse_caption_dates_and_duration(cls, caption: str) -> Tuple[Optional[datetime], Optional[datetime], Optional[int]]:
        start_dt: Optional[datetime] = None
        end_dt: Optional[datetime] = None
        duration_months: Optional[int] = None
        parts = [p.strip() for p in (caption or "").split("·")] if caption else []
        if parts:
            range_part = parts[0]
            if "-" in range_part:
                lr = [x.strip() for x in range_part.split("-")]
                if len(lr) >= 2:
                    start_dt = cls._parse_year_month(lr[0])
                    end_dt = cls._parse_year_month(lr[1])
        if len(parts) >= 2:
            dur = parts[1].lower()
            years = 0
            months = 0
            my = re.search(r"(\d+)\s*yr[s]?", dur)
            if my:
                years = int(my.group(1))
            mmo = re.search(r"(\d+)\s*mo[s]?", dur)
            if mmo:
                months = int(mmo.group(1))
            if years or months:
                duration_months = years * 12 + months
        return start_dt, end_dt, duration_months

    def _collect_experience_intervals(self) -> List[Tuple[datetime, datetime]]:
        exps = self._get("experiences") or []
        intervals: List[Tuple[datetime, datetime]] = []
        now = datetime.now(timezone.utc)
        if isinstance(exps, list):
            for exp in exps:
                caption = str((exp or {}).get("caption") or "").strip()
                start_dt, end_dt, duration_months = self._parse_caption_dates_and_duration(caption)
                if start_dt and end_dt:
                    intervals.append((start_dt, end_dt))
                elif duration_months is not None:
                    # Back-compute start if only duration is present
                    try:
                        years = duration_months // 12
                        months = duration_months % 12
                        year = now.year - years
                        month = now.month - months
                        while month <= 0:
                            month += 12
                            year -= 1
                        start_guess = datetime(year, month, 15, tzinfo=timezone.utc)
                    except Exception:
                        start_guess = now
                    intervals.append((start_guess, now))
        return intervals

    @staticmethod
    def _merge_intervals(intervals: List[Tuple[datetime, datetime]]) -> List[Tuple[datetime, datetime]]:
        if not intervals:
            return []
        ordered = sorted(intervals, key=lambda x: x[0])
        merged: List[Tuple[datetime, datetime]] = []
        cur_start, cur_end = ordered[0]
        for s, e in ordered[1:]:
            if s <= cur_end:
                if e > cur_end:
                    cur_end = e
            else:
                merged.append((cur_start, cur_end))
                cur_start, cur_end = s, e
        merged.append((cur_start, cur_end))
        return merged

    # --- boolean metrics (EXISTS) ---
    def metric_profile_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self._get("linkedinUrl"))

    def metric_profile_image_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        if self._is_truthy(self._get("profilePic")):
            return True
        dims: List[Dict[str, Any]] = self._get("profilePicAllDimensions") or []
        return any(self._is_truthy(d.get("url")) for d in dims if isinstance(d, dict))

    def metric_about_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self._get("about"))

    def metric_location_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return any(
            self._is_truthy(self._get(k))
            for k in ("addressWithCountry", "addressWithoutCountry", "addressCountryOnly")
        )

    def metric_country_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self._get("addressCountryOnly"))

    def metric_public_email_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self._get("email"))

    def metric_current_company_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self._get("companyName"))

    def metric_current_title_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self._get("jobTitle"))

    def metric_has_https_website(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        candidates: List[str] = []
        company_website = self._get("companyWebsite")
        if isinstance(company_website, str):
            candidates.append(company_website)
        creator = self._get("creatorWebsite") or {}
        if isinstance(creator, dict):
            if isinstance(creator.get("link"), str):
                candidates.append(creator["link"]) 
            if isinstance(creator.get("name"), str):
                candidates.append(creator["name"]) 
        company_linkedin = self._get("companyLinkedin")
        if isinstance(company_linkedin, str):
            candidates.append(company_linkedin)
        # Consider https only
        return any(isinstance(u, str) and u.strip().lower().startswith("https://") for u in candidates)

    def metric_public_profile_custom_slug(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        # Heuristic: presence of publicIdentifier implies a custom slug is available
        return self._is_truthy(self._get("publicIdentifier"))

    def metric_name_username_distinct(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        full_name = str(self._get("fullName") or "").strip().lower().replace(" ", "")
        public_identifier = str(self._get("publicIdentifier") or "").strip().lower()
        if not full_name or not public_identifier:
            return False
        return full_name != public_identifier

    def metric_background_image_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        # Not present in sample; support key if available
        return self._is_truthy(self._get("backgroundImageUrl")) or self._is_truthy(self._get("background_image_url"))

    def metric_public_profile_url_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self._get("linkedinUrl"))

    def metric_creator_mode_on(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        val = self._get("creatorModeOn")
        return bool(val) if val is not None else False

    def metric_open_to_work(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        val = self._get("openToWork")
        return bool(val) if val is not None else False

    def metric_provides_services(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        val = self._get("providesServices")
        return bool(val) if val is not None else False

    def metric_premium_badge(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        val = self._get("premiumBadge")
        return bool(val) if val is not None else False

    # --- numeric metrics (RANGE) ---
    def metric_headline_length_chars(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return len(str(self._get("headline") or ""))

    def metric_about_length_chars(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        return len(str(self._get("about") or ""))

    def metric_current_role_tenure_months(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        yrs = self._get("currentJobDurationInYrs")
        try:
            months = int(round(float(yrs) * 12)) if yrs is not None else 0
        except Exception:
            months = 0
        return months

    def metric_experience_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        exps = self._get("experiences") or []
        return len(exps) if isinstance(exps, list) else 0

    def metric_education_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        edus = self._get("educations") or []
        return len(edus) if isinstance(edus, list) else 0

    def metric_languages_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        langs = self._get("languages") or []
        return len(langs) if isinstance(langs, list) else 0

    def metric_followers_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        try:
            return int(self._get("followers") or 0)
        except Exception:
            return 0

    def metric_websites_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        values: List[str] = []
        for key in ("companyWebsite",):
            v = self._get(key)
            if isinstance(v, str):
                values.append(v)
        creator = self._get("creatorWebsite") or {}
        if isinstance(creator, dict):
            for key in ("link", "name"):
                v = creator.get(key)
                if isinstance(v, str):
                    values.append(v)
        company_linkedin = self._get("companyLinkedin")
        if isinstance(company_linkedin, str):
            values.append(company_linkedin)
        # Heuristic: count items that look like domains/URLs
        def looks_like_site(s: str) -> bool:
            s_l = s.strip().lower()
            return s_l.startswith("http://") or s_l.startswith("https://") or ("." in s_l and " " not in s_l)
        unique_sites = {s.strip() for s in values if isinstance(s, str) and looks_like_site(s)}
        return len(unique_sites)

    # --- company related ---
    def metric_company_name_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self._get("companyName"))

    def metric_company_industry_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self._get("companyIndustry"))

    def metric_company_website_present(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        return self._is_truthy(self._get("companyWebsite"))

    def metric_company_website_https_ok(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> bool:
        site = self._get("companyWebsite")
        return isinstance(site, str) and site.strip().lower().startswith("https://")

    # --- counts from lists ---
    def metric_certifications_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        items = self._get("licenseAndCertificates") or []
        return len(items) if isinstance(items, list) else 0

    def metric_skills_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        items = self._get("skills") or []
        return len(items) if isinstance(items, list) else 0

    def metric_honors_awards_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        items = self._get("honorsAndAwards") or []
        return len(items) if isinstance(items, list) else 0

    def metric_recommendations_received_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        items = self._get("recommendations") or []
        # If structure differentiates received vs given, refine later; use total length as conservative proxy
        return len(items) if isinstance(items, list) else 0

    def metric_recommendations_given_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        items = self._get("recommendationsGiven") or []
        return len(items) if isinstance(items, list) else 0

    def metric_top_skill_endorsements_total(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        top = self._get("topSkillsByEndorsements") or []
        total = 0
        if isinstance(top, list):
            for s in top:
                try:
                    total += int(s.get("endorsements") or s.get("count") or 0)
                except Exception:
                    continue
        return total

    def metric_featured_items_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        # If a dedicated 'featured' list exists, prefer it; otherwise 0
        items = self._get("featured") or []
        return len(items) if isinstance(items, list) else 0

    # --- experience-derived ---
    def metric_total_experience_years(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> float:
        intervals = self._collect_experience_intervals()
        if intervals:
            merged = self._merge_intervals(intervals)
            total_months = sum(self._months_between(s, e) for s, e in merged)
            return round(total_months / 12.0, 2)
        yrs = self._get("currentJobDurationInYrs")
        try:
            return round(float(yrs or 0.0), 2)
        except Exception:
            return 0.0

    def metric_job_changes_last2y_count(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        intervals = self._collect_experience_intervals()
        if not intervals:
            return 0
        now = datetime.now(timezone.utc)
        threshold = datetime(year=now.year - 2, month=now.month, day=now.day, tzinfo=timezone.utc)
        starts = [s for s, _ in intervals if s >= threshold]
        return len(starts)

    def metric_experience_gap_max_months(self, window_value: Optional[int] = None, window_unit: Optional[str] = None) -> int:
        intervals = self._collect_experience_intervals()
        if not intervals or len(intervals) == 1:
            return 0
        ordered = sorted(intervals, key=lambda x: x[0])
        max_gap = 0
        prev_end = ordered[0][1]
        for s, e in ordered[1:]:
            if s > prev_end:
                gap = self._months_between(prev_end, s)
                if gap > max_gap:
                    max_gap = gap
            if e > prev_end:
                prev_end = e
        return max_gap

    # --- dispatcher ---
    def get_metric(self, metric_name: str, window_value: Optional[int], window_unit: Optional[str]) -> Any:
        method_name = f"metric_{metric_name}"
        method = getattr(self, method_name, None)
        if not method:
            raise ValueError(f"Unsupported metric: {metric_name}")
        return method(window_value, window_unit)


