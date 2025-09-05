import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests


def _headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _get(url: str, token: str, params: Optional[Dict[str, Any]] = None, max_retries: int = 3, timeout_sec: int = 20) -> requests.Response:
    attempt = 0
    while attempt <= max_retries:
        try:
            resp = requests.get(url, headers=_headers(token), params=params, timeout=timeout_sec)
            if resp.status_code in (403, 429):
                msg = None
                try:
                    msg = resp.text
                except Exception:
                    msg = None
                is_rate_limited = "rate limit" in (msg or "").lower() or resp.headers.get("X-RateLimit-Remaining") == "0"
                if is_rate_limited and attempt < max_retries:
                    reset_header = resp.headers.get("X-RateLimit-Reset")
                    sleep_seconds = (2 ** attempt)
                    if reset_header:
                        try:
                            reset_ts = int(reset_header)
                            now_ts = int(time.time())
                            until_reset = max(0, reset_ts - now_ts)
                            if until_reset <= 60:
                                sleep_seconds = max(sleep_seconds, until_reset)
                        except Exception:
                            pass
                    time.sleep(sleep_seconds)
                    attempt += 1
                    continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            status = getattr(e.response, "status_code", None)
            should_retry = status in (403, 429) or status is None
            if attempt < max_retries and should_retry:
                time.sleep(2 ** attempt)
                attempt += 1
                continue
            raise
    raise RuntimeError("Exhausted retries")


def fetch_user(token: str) -> Dict[str, Any]:
    return _get("https://api.github.com/user", token).json()


def fetch_repos(token: str) -> List[Dict[str, Any]]:
    repos: List[Dict[str, Any]] = []
    page = 1
    while True:
        resp = _get(
            "https://api.github.com/user/repos",
            token,
            params={"type": "owner", "sort": "updated", "per_page": 100, "page": page},
        )
        batch = resp.json()
        if not batch:
            break
        repos.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    # Keep only public, non-archived
    cleaned: List[Dict[str, Any]] = []
    for r in repos:
        if r.get("private") or r.get("archived"):
            continue
        cleaned.append({
            "full_name": r.get("full_name"),
            "language": r.get("language"),
            "stargazers_count": int(r.get("stargazers_count", 0)),
            "fork": bool(r.get("fork")),
            "pushed_at": r.get("pushed_at"),
        })
    return cleaned


def parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None


def since_dt(window_value: Optional[int], window_unit: Optional[str]) -> Optional[datetime]:
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


def count_recent_repos_pushed(repos: List[Dict[str, Any]], days: int) -> int:
    threshold = datetime.now(timezone.utc) - timedelta(days=days)
    return sum(1 for r in repos if r.get("pushed_at") and (parse_iso(r["pushed_at"]) or datetime.min.replace(tzinfo=timezone.utc)) >= threshold)


def commits_by_author_90d(token: str, login: str, repos: List[Dict[str, Any]]) -> int:
    since_iso = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
    total = 0
    for r in repos:
        full_name = r.get("full_name")
        if not full_name:
            continue
        page = 1
        while True:
            resp = _get(
                f"https://api.github.com/repos/{full_name}/commits",
                token,
                params={"author": login, "since": since_iso, "per_page": 100, "page": page},
            )
            commits = resp.json()
            total += len(commits)
            if len(commits) < 100:
                break
            page += 1
    return total


def search_count(token: str, q: str) -> int:
    try:
        resp = _get("https://api.github.com/search/issues", token, params={"q": q, "per_page": 1})
        return int(resp.json().get("total_count", 0))
    except Exception:
        return 0


def code_search_count(token: str, login: str) -> int:
    resp = _get("https://api.github.com/search/code", token, params={"q": f"user:{login}", "per_page": 1})
    try:
        return int(resp.json().get("total_count", 0))
    except Exception:
        return 0


def build_payload(user: Dict[str, Any], repos: List[Dict[str, Any]], token: str) -> Dict[str, Any]:
    login = user.get("login") or ""
    public_repos_count = len(repos)
    non_forks = sum(1 for r in repos if not r.get("fork"))
    non_fork_ratio = (non_forks / public_repos_count) if public_repos_count > 0 else 0.0
    recent_repos_pushed_90d = count_recent_repos_pushed(repos, 90)
    stars_total_sum = sum(int(r.get("stargazers_count", 0)) for r in repos)
    max_repo_stars = max((int(r.get("stargazers_count", 0)) for r in repos), default=0)
    language_distinct_count = len({r.get("language") for r in repos if r.get("language")})
    owner_repos_updated_365d = count_recent_repos_pushed(repos, 365)

    commits_90d = commits_by_author_90d(token, login, repos)

    since_180 = (datetime.now(timezone.utc) - timedelta(days=180)).date().isoformat()
    # Use separate issue/PR queries to satisfy Search API
    pr_issue_involvement_180d = (
        search_count(token, f"involves:{login} updated:>={since_180} is:issue")
        + search_count(token, f"involves:{login} updated:>={since_180} is:pull-request")
    )

    since_365 = (datetime.now(timezone.utc) - timedelta(days=365)).date().isoformat()
    authored_items_365d = (
        search_count(token, f"author:{login} created:>={since_365} is:issue")
        + search_count(token, f"author:{login} created:>={since_365} is:pull-request")
    )

    indexed_code_files_total = code_search_count(token, login)

    return {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "user": {
            "login": user.get("login"),
            "id": user.get("id"),
            "name": user.get("name"),
            "html_url": user.get("html_url"),
            "created_at": user.get("created_at"),
            "avatar_url": user.get("avatar_url"),
        },
        "repos": repos,
        "computed_metrics": {
            "public_repos_count": public_repos_count,
            "non_fork_ratio": round(non_fork_ratio, 6),
            "recent_repos_pushed_90d": recent_repos_pushed_90d,
            "stars_total_sum": stars_total_sum,
            "max_repo_stars": max_repo_stars,
            "language_distinct_count": language_distinct_count,
            "owner_repos_updated_365d": owner_repos_updated_365d,
            "commits_by_author_90d": commits_90d,
            "pr_issue_involvement_180d": pr_issue_involvement_180d,
            "authored_items_365d": authored_items_365d,
            "indexed_code_files_total": indexed_code_files_total,
        },
    }


def main() -> None:
    here = os.path.dirname(os.path.realpath(__file__))
    provider_json_path = os.path.join(here, "provider_github.json")
    out_path = provider_json_path  # overwrite provider file with static content

    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        # Fallback: read from provider file if it contains accessToken
        try:
            with open(provider_json_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            token = cfg.get("accessToken") or cfg.get("access_token")
        except Exception:
            token = None
    if not token:
        raise RuntimeError("Missing GitHub token. Provide via env GITHUB_TOKEN or provider_github.json accessToken.")

    user = fetch_user(token)
    repos = fetch_repos(token)
    payload = build_payload(user, repos, token)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote static GitHub data to {out_path}")


if __name__ == "__main__":
    main()


