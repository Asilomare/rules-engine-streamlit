import csv
import json
import os
import importlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# Optional runtime deps in Lambda environments
try:
    import boto3  # type: ignore
    from botocore.exceptions import ClientError  # type: ignore
except Exception:  # pragma: no cover - local tools may not have boto3
    boto3 = None
    ClientError = Exception

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None


@dataclass
class Rule:
    id: str
    category: str
    label: str
    metric: str
    operator: str
    value_low: Optional[float]
    value_high: Optional[float]
    value_exact: Optional[Any]
    unit: Optional[str]
    window_value: Optional[int]
    window_unit: Optional[str]
    weight: float
    is_gate: bool


@dataclass
class BusinessRuleOption:
    # One row in br_[provider].csv representing a single Likert option for a business rule group
    source: str
    group_id: str
    parent_id: Optional[str]
    priority: Optional[int]
    match_type: Optional[str]
    category: str
    business_rule_label: str
    likert_value: int
    likert_label: str
    all_of: List[str]
    any_of: List[str]
    none_of: List[str]
    threshold_attr: Optional[str]
    weight: float


def _parse_id_list(cell: Optional[str]) -> List[str]:
    if not cell:
        return []
    # Support comma-separated values, ignore empties, trim whitespace
    values = [v.strip() for v in str(cell).split(",")]
    return [v for v in values if v]


def load_business_rules_from_csv(provider_name: str, rules_dir: str) -> List[BusinessRuleOption]:
    """Loads business rules for a specific provider from a CSV file, if present.

    Schema (columns):
    source,id,parentId,priority,match_type,category,businessRuleLabel,
    likert_value,likert_label,ALL_OF,ANY_OF,NONE_OF,threshold_attr,weight
    """
    br_path = os.path.join(rules_dir, f"providers/{provider_name}/br_{provider_name}.csv")
    if not os.path.exists(br_path):
        return []

    options: List[BusinessRuleOption] = []
    with open(br_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            options.append(
                BusinessRuleOption(
                    source=(row.get("source") or "").strip(),
                    group_id=(row.get("id") or "").strip(),
                    parent_id=((row.get("parentId") or "").strip() or None),
                    priority=int(row["priority"]) if row.get("priority") and row["priority"].strip() else None,
                    match_type=(row.get("match_type") or "").strip() or None,
                    category=(row.get("category") or "").strip(),
                    business_rule_label=(row.get("businessRuleLabel") or "").strip(),
                    likert_value=int(row["likert_value"]) if row.get("likert_value") else 0,
                    likert_label=(row.get("likert_label") or "").strip(),
                    all_of=_parse_id_list(row.get("ALL_OF")),
                    any_of=_parse_id_list(row.get("ANY_OF")),
                    none_of=_parse_id_list(row.get("NONE_OF")),
                    threshold_attr=(row.get("threshold_attr") or "").strip() or None,
                    weight=float(row["weight"]) if row.get("weight") and row["weight"].strip() else 0.0,
                )
            )
    return options


def evaluate_business_rules(atomic_log: List[Dict[str, Any]], br_options: List[BusinessRuleOption]) -> Tuple[float, List[Dict[str, Any]]]:
    """Evaluates business rules using the atomic evaluation log.

    Returns a (business_score, business_log) tuple. One selected option per group contributes its weight.
    """
    if not br_options:
        return 0.0, []

    matched_atomic_ids = {str(entry.get("id")) for entry in atomic_log if entry.get("matched")}

    # group options by (group_id)
    groups: Dict[str, List[BusinessRuleOption]] = {}
    for opt in br_options:
        groups.setdefault(opt.group_id, []).append(opt)

    business_score = 0.0
    business_log: List[Dict[str, Any]] = []

    def option_satisfied(opt: BusinessRuleOption) -> bool:
        if opt.all_of:
            if not all(rule_id in matched_atomic_ids for rule_id in opt.all_of):
                return False
        if opt.any_of:
            if not any(rule_id in matched_atomic_ids for rule_id in opt.any_of):
                return False
        if opt.none_of:
            if any(rule_id in matched_atomic_ids for rule_id in opt.none_of):
                return False
        return True

    for group_id, options in groups.items():
        # choose best satisfied option per group: highest likert_value, then highest weight, then lowest priority
        satisfied: List[BusinessRuleOption] = [opt for opt in options if option_satisfied(opt)]
        chosen: Optional[BusinessRuleOption] = None
        if satisfied:
            satisfied.sort(key=lambda o: (o.likert_value, o.weight, -(o.priority or 0)), reverse=True)
            chosen = satisfied[0]

        group_label = next((opt.business_rule_label for opt in options if opt.business_rule_label), "")
        group_category = next((opt.category for opt in options if opt.category), "")

        business_log.append({
            "group_id": group_id,
            "label": group_label,
            "category": group_category,
            "chosen": None if not chosen else {
                "likert_value": chosen.likert_value,
                "likert_label": chosen.likert_label,
                "weight": chosen.weight,
                "all_of": chosen.all_of,
                "any_of": chosen.any_of,
                "none_of": chosen.none_of,
            },
            "satisfied_options": [
                {
                    "likert_value": o.likert_value,
                    "likert_label": o.likert_label,
                    "weight": o.weight,
                } for o in satisfied
            ],
        })

        if chosen:
            business_score += chosen.weight

    return business_score, business_log


def load_rules_from_csv(provider_name: str, rules_dir: str) -> List[Rule]:
    """Loads rules for a specific provider from a CSV file."""
    rules_path = os.path.join(rules_dir, f"providers/{provider_name}/rules_{provider_name}.csv")
    if not os.path.exists(rules_path):
        raise FileNotFoundError(f"Rules file not found for provider '{provider_name}' at {rules_path}")

    rules: List[Rule] = []
    with open(rules_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Parse value_exact allowing booleans or strings
            raw_exact = (row.get("value_exact") or "").strip()
            parsed_exact: Optional[Any]
            if raw_exact == "":
                parsed_exact = None
            else:
                upper_exact = raw_exact.upper()
                if upper_exact in ("TRUE", "FALSE"):
                    parsed_exact = True if upper_exact == "TRUE" else False
                else:
                    try:
                        parsed_exact = float(raw_exact)
                    except ValueError:
                        parsed_exact = raw_exact
            rules.append(
                Rule(
                    id=row["id"].strip(),
                    category=row["category"].strip(),
                    label=row["label"].strip(),
                    metric=row["metric"].strip(),
                    operator=row["operator"].strip().upper(),
                    value_low=float(row["value_low"]) if row.get("value_low") and row["value_low"].strip() else None,
                    value_high=float(row["value_high"]) if row.get("value_high") and row["value_high"].strip() else None,
                    value_exact=parsed_exact,
                    unit=(row.get("unit") or "").strip() or None,
                    window_value=int(row["window_value"]) if row.get("window_value") and row["window_value"].strip() else None,
                    window_unit=(row.get("window_unit") or "").strip() or None,
                    weight=float(row["weight"]) if row.get("weight") and row["weight"].strip() else 0.0,
                    is_gate=(row.get("is_gate", "FALSE").strip().upper() == "TRUE"),
                )
            )
    return rules


# --- Dynamic Google Sheets-backed loader with S3 caching ---

def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    try:
        return os.environ.get(name, default)
    except Exception:
        return default


def _s3_client_optional():
    if boto3 is None:
        return None
    try:
        return boto3.client("s3")
    except Exception:
        return None


def _read_cached_rules_from_s3(bucket: str, key: str) -> Optional[str]:
    client = _s3_client_optional()
    if not client or not bucket or not key:
        return None
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8")
    except ClientError:
        return None
    except Exception:
        return None


def _write_cached_rules_to_s3(bucket: str, key: str, body: str) -> None:
    client = _s3_client_optional()
    if not client or not bucket or not key:
        return
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"), ContentType="text/csv")
    except Exception:
        # Fail soft; caller will use in-memory version regardless
        pass


def _csv_string_to_dicts(csv_text: str) -> List[Dict[str, Any]]:
    reader = csv.DictReader(csv_text.splitlines())
    return [row for row in reader]


def _validate_rules_rows(rows: List[Dict[str, Any]]) -> bool:
    required = {"id", "category", "label", "metric", "operator", "weight"}
    if not rows:
        return False
    headers = set(rows[0].keys())
    if not required.issubset(headers):
        return False
    # Spot-check a few rows for parseability of numeric fields if present
    for row in rows[:10]:
        for k in ("value_low", "value_high", "weight"):
            v = (row.get(k) or "").strip()
            if v == "":
                continue
            try:
                float(v)
            except Exception:
                return False
    return True


def _validate_business_rows(rows: List[Dict[str, Any]]) -> bool:
    required = {"id", "category", "businessRuleLabel", "likert_value", "weight"}
    if not rows:
        return False
    headers = set(rows[0].keys())
    if not required.issubset(headers):
        return False
    for row in rows[:10]:
        for k in ("likert_value", "weight"):
            v = (row.get(k) or "").strip()
            if v == "":
                continue
            try:
                float(v)
            except Exception:
                return False
    return True


def _fetch_sheet_values(spreadsheet_id: str, sheet_name: str, api_key: str) -> Optional[List[List[str]]]:
    if not requests:
        return None
    try:
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{sheet_name}"
        resp = requests.get(url, params={"key": api_key}, timeout=15)
        resp.raise_for_status()
        data = resp.json() or {}
        values = data.get("values")
        if isinstance(values, list) and values:
            return values
        return None
    except Exception:
        return None


def _sheet_values_to_csv_text(values: List[List[str]]) -> str:
    from io import StringIO
    buf = StringIO()
    writer = csv.writer(buf)
    for row in values:
        writer.writerow(row)
    return buf.getvalue()


def _load_csv_rows_dynamic(provider_name: str, kind: str, fallback_path: str) -> Optional[List[Dict[str, Any]]]:
    """Attempt to load CSV rows from S3 cache or Google Sheets.

    kind: "rules" or "br"
    fallback_path: local file path to use if dynamic config is missing or fetch fails
    """
    spreadsheet_id = _env("RULES_SPREADSHEET_ID")
    api_key = _env("GOOGLE_SHEETS_API_KEY")
    bucket = _env("RULES_CACHE_BUCKET") or _env("REPO_ARCHIVES_BUCKET_NAME")
    prefix = _env("RULES_CACHE_PREFIX", "rules-engine-cache") or "rules-engine-cache"
    ttl_seconds_str = _env("RULES_CACHE_TTL_SECONDS", "86400")
    try:
        ttl_seconds = int(ttl_seconds_str) if ttl_seconds_str else 86400
    except Exception:
        ttl_seconds = 86400

    cache_key = f"{prefix}/{kind}_{provider_name}.csv"

    # 1) If no dynamic config, skip to local
    if not spreadsheet_id or not api_key:
        if os.path.exists(fallback_path):
            with open(fallback_path, "r", encoding="utf-8") as f:
                return _csv_string_to_dicts(f.read())
        return None

    # 2) Try fresh cache if not older than TTL
    client = _s3_client_optional()
    fresh_cache: Optional[str] = None
    if client and bucket:
        try:
            head = client.head_object(Bucket=bucket, Key=cache_key)
            last_modified = head.get("LastModified")
            if last_modified:
                age = (datetime.now(timezone.utc) - last_modified).total_seconds()
                if age < ttl_seconds:
                    cached_text = _read_cached_rules_from_s3(bucket, cache_key)
                    if cached_text:
                        return _csv_string_to_dicts(cached_text)
        except ClientError:
            pass
        except Exception:
            pass

    # 3) Fetch from Sheets and validate
    sheet_prefix = "a-" if kind == "rules" else "br-"
    sheet_name = f"{sheet_prefix}{provider_name}"
    values = _fetch_sheet_values(spreadsheet_id, sheet_name, api_key)
    if values:
        csv_text = _sheet_values_to_csv_text(values)
        rows = _csv_string_to_dicts(csv_text)
        is_valid = _validate_rules_rows(rows) if kind == "rules" else _validate_business_rows(rows)
        if is_valid:
            if bucket:
                _write_cached_rules_to_s3(bucket, cache_key, csv_text)
            return rows

    # 4) Fallback to any existing cache (even if stale)
    if bucket:
        cached_text = _read_cached_rules_from_s3(bucket, cache_key)
        if cached_text:
            try:
                rows = _csv_string_to_dicts(cached_text)
                is_valid = _validate_rules_rows(rows) if kind == "rules" else _validate_business_rows(rows)
                if is_valid:
                    return rows
            except Exception:
                pass

    # 5) Final fallback to local repo CSV
    if os.path.exists(fallback_path):
        with open(fallback_path, "r", encoding="utf-8") as f:
            return _csv_string_to_dicts(f.read())

    return None


def load_rules(provider_name: str, rules_dir: str) -> List[Rule]:
    """Dynamic loader that prefers Google Sheets + S3 cache, falls back to local CSV.

    This function mirrors load_rules_from_csv but uses dynamic caching when configured.
    """
    fallback_path = os.path.join(rules_dir, f"providers/{provider_name}/rules_{provider_name}.csv")
    rows = _load_csv_rows_dynamic(provider_name, "rules", fallback_path)
    if rows is None:
        # Keep original behavior (raise) if file missing and no dynamic
        return load_rules_from_csv(provider_name, rules_dir)

    rules: List[Rule] = []
    for row in rows:
        raw_exact = (row.get("value_exact") or "").strip()
        parsed_exact: Optional[Any]
        if raw_exact == "":
            parsed_exact = None
        else:
            upper_exact = raw_exact.upper()
            if upper_exact in ("TRUE", "FALSE"):
                parsed_exact = True if upper_exact == "TRUE" else False
            else:
                try:
                    parsed_exact = float(raw_exact)
                except ValueError:
                    parsed_exact = raw_exact
        rules.append(
            Rule(
                id=(row.get("id") or "").strip(),
                category=(row.get("category") or "").strip(),
                label=(row.get("label") or "").strip(),
                metric=(row.get("metric") or "").strip(),
                operator=(row.get("operator") or "").strip().upper(),
                value_low=float(row["value_low"]) if row.get("value_low") and str(row["value_low"]).strip() else None,
                value_high=float(row["value_high"]) if row.get("value_high") and str(row["value_high"]).strip() else None,
                value_exact=parsed_exact,
                unit=(row.get("unit") or "").strip() or None,
                window_value=int(row["window_value"]) if row.get("window_value") and str(row["window_value"]).strip() else None,
                window_unit=(row.get("window_unit") or "").strip() or None,
                weight=float(row["weight"]) if row.get("weight") and str(row["weight"]).strip() else 0.0,
                is_gate=(str(row.get("is_gate", "FALSE")).strip().upper() == "TRUE"),
            )
        )
    return rules


def load_business_rules(provider_name: str, rules_dir: str) -> List[BusinessRuleOption]:
    fallback_path = os.path.join(rules_dir, f"providers/{provider_name}/br_{provider_name}.csv")
    rows = _load_csv_rows_dynamic(provider_name, "br", fallback_path)
    if rows is None:
        return load_business_rules_from_csv(provider_name, rules_dir)

    options: List[BusinessRuleOption] = []
    for row in rows:
        options.append(
            BusinessRuleOption(
                source=(row.get("source") or "").strip(),
                group_id=(row.get("id") or "").strip(),
                parent_id=((row.get("parentId") or "").strip() or None),
                priority=int(row["priority"]) if row.get("priority") and str(row["priority"]).strip() else None,
                match_type=(row.get("match_type") or "").strip() or None,
                category=(row.get("category") or "").strip(),
                business_rule_label=(row.get("businessRuleLabel") or "").strip(),
                likert_value=int(row["likert_value"]) if row.get("likert_value") else 0,
                likert_label=(row.get("likert_label") or "").strip(),
                all_of=_parse_id_list(row.get("ALL_OF")),
                any_of=_parse_id_list(row.get("ANY_OF")),
                none_of=_parse_id_list(row.get("NONE_OF")),
                threshold_attr=(row.get("threshold_attr") or "").strip() or None,
                weight=float(row["weight"]) if row.get("weight") and str(row["weight"]).strip() else 0.0,
            )
        )
    return options


def get_provider_instance(provider_name: str, config_or_token: Dict[str, Any]) -> Any:
    """Dynamically imports and instantiates a provider class."""
    try:
        # Try both common naming conventions: provider_<name>.py and <name>_provider.py
        candidate_modules = [
            f"providers.{provider_name}.provider_{provider_name}",
            f"providers.{provider_name}.{provider_name}_provider",
        ]
        last_error: Optional[Exception] = None
        provider_module = None
        for module_name in candidate_modules:
            try:
                provider_module = importlib.import_module(module_name)
                break
            except Exception as e:
                last_error = e
                continue
        if provider_module is None:
            raise ImportError(str(last_error) if last_error else f"No module found for {provider_name}")

        provider_class_name = None
        for name in dir(provider_module):
            if name.endswith("MetricsProvider"):
                provider_class_name = name
                break
        
        if not provider_class_name:
            raise AttributeError(f"No class ending with 'MetricsProvider' found in module '{module_name}'")
            
        provider_class = getattr(provider_module, provider_class_name)
        return provider_class(config_or_token)
    except (ImportError, AttributeError) as e:
        raise ValueError(f"Could not load provider '{provider_name}': {e}")


def evaluate_rules(provider: Any, rules: List[Rule]) -> Tuple[float, List[Dict[str, Any]]]:
    score = 0.0
    log: List[Dict[str, Any]] = []

    for rule in rules:
        try:
            metric_value = provider.get_metric(rule.metric, rule.window_value, rule.window_unit)
        except Exception as e:
            log.append({
                "id": rule.id,
                "category": rule.category,
                "label": rule.label,
                "metric": rule.metric,
                "matched": False,
                "weight_applied": 0.0,
                "metric_value": None,
                "error": str(e),
                "exception_type": type(e).__name__,
            })
            continue

        matched = False
        # print('Operator: ', rule.operator)
        # print('Metric Value: ', type(metric_value), metric_value)
        if rule.operator == "EXISTS":
            matched = bool(metric_value)
        elif rule.operator == "MISSING":
            matched = not bool(metric_value)
        elif rule.operator == "EQ":
            target = rule.value_exact
            val = metric_value
            # handle boolean strings
            if isinstance(target, (str,)):
                t = target.strip().upper()
                if t in ("TRUE", "FALSE"):
                    target = True if t == "TRUE" else False
            matched = (val == target)
        elif rule.operator == "NE":
            target = rule.value_exact
            if isinstance(target, (str,)):
                t = target.strip().upper()
                if t in ("TRUE", "FALSE"):
                    target = True if t == "TRUE" else False
            matched = (metric_value != target)
        elif rule.operator in ("LT", "LTE", "GT", "GTE"):
            try:
                mv = float(metric_value)
            except Exception:
                mv = None
            if mv is None or rule.value_exact is None:
                matched = False
            else:
                thr = float(rule.value_exact)
                if rule.operator == "LT":
                    matched = mv < thr
                elif rule.operator == "LTE":
                    matched = mv <= thr
                elif rule.operator == "GT":
                    matched = mv > thr
                else:
                    matched = mv >= thr
        elif rule.operator in ("RANGE", "BETWEEN"):
            # print('Value Low: ', type(rule.value_low), rule.value_low)
            # print('Value High: ', type(rule.value_high), rule.value_high)
            low_ok = rule.value_low is None or (metric_value is not None and metric_value >= rule.value_low)
            high_ok = rule.value_high is None or (metric_value is not None and metric_value <= rule.value_high)
            # print('Low OK: ', low_ok)
            # print('High OK: ', high_ok)
            matched = bool(low_ok and high_ok)
        else:
            matched = False

        # print('Weight: ', rule.weight)

        applied = rule.weight if matched else 0.0
        score += applied
        # print('Label: ', rule.label)
        # print('Matched: ', matched)

        # if matched:
        #     print('Score: ', applied)

        # print('\n')
        log.append({
            "id": rule.id,
            "category": rule.category,
            "label": rule.label,
            "metric": rule.metric,
            "operator": rule.operator,
            "thresholds": {
                "value_low": rule.value_low,
                "value_high": rule.value_high,
                "value_exact": rule.value_exact,
            },
            "window": {
                "value": rule.window_value,
                "unit": rule.window_unit,
            },
            "metric_value": metric_value,
            "matched": matched,
            "weight_applied": applied,
            "is_gate": rule.is_gate,
        })

    return score, log


def read_provider_config(provider_path: str) -> Dict[str, Any]:
    with open(provider_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


# --- Dynamic evaluation using aggregated data from assessment pipeline ---

def _build_provider_payloads_from_aggregated(aggregated_data: Dict[str, Any]) -> Dict[str, Any]:
    """Map aggregated_data provider analyses into rules-engine provider payloads.

    Only providers with sufficient data are included.
    """
    out: Dict[str, Any] = {}

    gh = aggregated_data.get("github_analysis") or {}
    if gh:
        user = gh.get("user_details") or {}
        repos = gh.get("analyzed_repositories") or []
        # Map to minimal shape expected by GitHub provider
        mapped_repos: List[Dict[str, Any]] = []
        for r in repos:
            mapped_repos.append({
                "pushed_at": r.get("pushed_at"),
                "stargazers_count": r.get("stars_count", 0),
                "forks_count": r.get("forks_count", 0),
            })
        out["github"] = {
            "user": {
                "login": user.get("username"),
                "html_url": f"https://github.com/{user.get('username')}" if user.get("username") else None,
            },
            "repos": mapped_repos,
        }

    gl = aggregated_data.get("gitlab_analysis") or {}
    if gl:
        projects = gl.get("analyzed_repositories") or []
        out["gitlab"] = [{
            "star_count": p.get("stars_count", 0),
            "forks_count": p.get("forks_count", 0),
            "last_activity_at": p.get("last_activity_at"),
            "visibility": "public",
        } for p in projects]

    so = aggregated_data.get("stackoverflow_analysis") or {}
    if so:
        # analyzer returns { analysis: {...}, raw_payload: {...} } or just analysis
        analysis = so.get("analysis") or so.get("analysis_data") or so
        profile = analysis.get("profile_data") or {}
        answers = analysis.get("top_answers") or []
        tags = analysis.get("top_tags") or []
        out["stack"] = {
            "profile": {"items": [profile]},
            "answers": {"items": answers},
            "questions": {"items": []},
            "tags": {"items": tags},
        }

    li = aggregated_data.get("linkedin_analysis") or {}
    if li:
        out["linkedin"] = li.get("raw_data_summary") or li

    kg = aggregated_data.get("kaggle_analysis") or {}
    if kg:
        out["kaggle"] = kg

    sch = aggregated_data.get("google_scholar_analysis") or {}
    if sch:
        out["gscholar"] = sch

    # credly/fiverr often require raw payload shapes; include only if present
    credly_data = aggregated_data.get("credly_analysis")
    if credly_data:
        if isinstance(credly_data, str):
            try:
                parsed = json.loads(credly_data)
            except Exception:
                parsed = None
        else:
            parsed = credly_data
        if isinstance(parsed, list) and parsed:
            out["credly"] = parsed

    fiverr_data = aggregated_data.get("fiverr_analysis")
    if fiverr_data:
        if isinstance(fiverr_data, str):
            try:
                parsed_fv = json.loads(fiverr_data)
            except Exception:
                parsed_fv = None
        else:
            parsed_fv = fiverr_data
        if isinstance(parsed_fv, dict) and parsed_fv:
            out["fiverr"] = parsed_fv

    return out


def evaluate_dynamic_from_aggregated(aggregated_data: Dict[str, Any]) -> Dict[str, Any]:
    """Evaluate rules for providers present in aggregated_data.

    Uses dynamic Google Sheets loading with S3 caching when configured.
    """
    script_dir = os.path.dirname(os.path.realpath(__file__))
    provider_payloads = _build_provider_payloads_from_aggregated(aggregated_data)

    total_score = 0.0
    all_provider_results: Dict[str, Any] = {}

    for provider_name, payload in provider_payloads.items():
        try:
            # Prefer dynamic rules; fall back to repo CSVs.
            rules = load_rules(provider_name, script_dir)
            provider = get_provider_instance(provider_name, payload)

            atomic_score, atomic_log = evaluate_rules(provider, rules)
            br_options = load_business_rules(provider_name, script_dir)
            business_score, business_log = evaluate_business_rules(atomic_log, br_options)

            provider_score_total = atomic_score + business_score
            total_score += provider_score_total

            all_provider_results[provider_name] = {
                "score": round(provider_score_total, 6),
                "score_atomic": round(atomic_score, 6),
                "score_business": round(business_score, 6),
                "business": business_log,
                "errors": [entry for entry in atomic_log if entry.get("error")],
            }
        except Exception as e:
            all_provider_results[provider_name] = {
                "error": str(e),
                "exception_type": type(e).__name__,
            }

    return {
        "total_score": round(total_score, 6),
        "evaluated_at": datetime.now(timezone.utc).isoformat() + "Z",
        "provider_results": all_provider_results,
    }


def main() -> None:
    script_dir = os.path.dirname(os.path.realpath(__file__))
    
    provider_paths = [
        os.path.join(script_dir, "providers/github/provider_github.json"), 
        os.path.join(script_dir, "providers/credly/provider_credly.json"),
        os.path.join(script_dir, "providers/linkedin/provider_linkedin.json"),
        os.path.join(script_dir, "providers/fiverr/provider_fiverr.json"),
        os.path.join(script_dir, "providers/kaggle/provider_kaggle.json"),
        os.path.join(script_dir, "providers/gitlab/provider_gitlab.json"),
        os.path.join(script_dir, "providers/gscholar/provider_gscholar.json"),
        os.path.join(script_dir, "providers/stack/provider_stack.json"),

        # NOT INCLUDING ATLASSIAN, 
        # ticket tracking should not impact a user's score
        # os.path.join(script_dir, "providers/atlassian/provider_atlassian.json"),
    ]

    provider_weights = {
        "github": 1.0,
        "credly": 1.0,
        "linkedin": 1.0,
        "fiverr": 1.0,
        "kaggle": 1.0,
        "gitlab": 1.0,
        "gscholar": 1.0,
        "stack": 1.0,
        # "atlassian": 1.0,
    }

    total_score = 0.0
    all_provider_results = {}

    for provider_path in provider_paths:
        provider_name_from_file = ""
        try:
            filename = os.path.basename(provider_path)
            if filename.startswith("provider_") and filename.endswith(".json"):
                provider_name_from_file = filename[len("provider_"):-len(".json")]
            else:
                raise ValueError(f"Invalid provider file name format: {filename}")

            rules = load_rules_from_csv(provider_name_from_file, script_dir)
            config = read_provider_config(provider_path)
            provider = get_provider_instance(provider_name_from_file, config)

            # 1) Atomic evaluation
            atomic_score, atomic_log = evaluate_rules(provider, rules)
            errors = [entry for entry in atomic_log if entry.get("error")]

            # 2) Business rules on top of atomic
            br_options = load_business_rules_from_csv(provider_name_from_file, script_dir)
            business_score, business_log = evaluate_business_rules(atomic_log, br_options)

            provider_score_total = atomic_score + business_score
            
            provider_weight = provider_weights.get(provider_name_from_file, 1.0)
            weighted_score = provider_score_total * provider_weight
            total_score += weighted_score

            all_provider_results[provider_name_from_file] = {
                # Back-compat: score = atomic + business
                "score": round(provider_score_total, 6),
                "weighted_score": round(weighted_score, 6),
                "weight": provider_weight,
                "score_atomic": round(atomic_score, 6),
                "score_business": round(business_score, 6),
                # "log": log,
                "errors": errors,
                # Uncomment to include detailed business rule results in output
                "business": business_log,
            }
        except Exception as e:
            provider_name = provider_name_from_file or os.path.basename(provider_path)
            all_provider_results[provider_name] = {
                "error": str(e),
                "exception_type": type(e).__name__,
            }

    output = {
        "total_score": round(total_score, 6),
        "evaluated_at": datetime.now(timezone.utc).isoformat() + "Z",
        "provider_results": all_provider_results,
    }

    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
