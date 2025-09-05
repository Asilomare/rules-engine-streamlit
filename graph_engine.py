"""
Interactive graphs for rules engine outputs (atomic + business rules).

How to run:
    1) Install deps (once):
       pip install streamlit plotly pandas

    2) Start the app from the repo root:
       streamlit run backend/rules_engine/graph_engine.py

Notes:
    - This app evaluates providers on-the-fly using the same engine logic,
      then visualizes the atomic evaluation log and business rule log.
    - Providers are discovered from providers/*/provider_*.json (excludes atlassian).
"""

import os
import sys
import glob
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st
import json


# Ensure we can import the local engine and its provider modules
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.append(SCRIPT_DIR)

import rules_engine as engine  # noqa: E402


def discover_provider_paths(base_dir: str) -> List[str]:
    pattern = os.path.join(base_dir, "providers", "*", "provider_*.json")
    all_paths = glob.glob(pattern)
    # Exclude atlassian per engine note
    return [p for p in all_paths if "/atlassian/" not in p]


def _find_url_in_obj(obj: Any, substrings: List[str]) -> Optional[str]:
    try:
        if isinstance(obj, str):
            for s in substrings:
                if s in obj:
                    return obj
            return None
        if isinstance(obj, dict):
            for v in obj.values():
                found = _find_url_in_obj(v, substrings)
                if found:
                    return found
            return None
        if isinstance(obj, list):
            for v in obj:
                found = _find_url_in_obj(v, substrings)
                if found:
                    return found
            return None
    except Exception:
        return None
    return None


def extract_profile_url(provider_name: str, config: Any) -> Optional[str]:
    name = provider_name.lower()
    try:
        if name == "github":
            return (config.get("user") or {}).get("html_url")
        if name == "credly":
            # Prefer earner_path if present
            if isinstance(config, list) and config:
                for item in config:
                    earner_path = item.get("earner_path")
                    if earner_path:
                        return f"https://www.credly.com{earner_path}"
            # fallback search
            return _find_url_in_obj(config, ["credly.com"]) or None
        if name == "linkedin":
            # Common fields: public_profile_url, url
            candidate = (config.get("public_profile_url") or config.get("url")) if isinstance(config, dict) else None
            return candidate or _find_url_in_obj(config, ["linkedin.com"]) or None
        if name == "fiverr":
            return _find_url_in_obj(config, ["fiverr.com"]) or None
        if name == "kaggle":
            return _find_url_in_obj(config, ["kaggle.com"]) or None
        if name == "gitlab":
            # GitLab user often has web_url
            if isinstance(config, dict) and "web_url" in config:
                return config.get("web_url")
            return _find_url_in_obj(config, ["gitlab.com"]) or None
        if name == "gscholar":
            return _find_url_in_obj(config, ["scholar.google"]) or None
        if name == "stack":
            return _find_url_in_obj(config, ["stackoverflow.com"]) or None
    except Exception:
        return None
    return None


def evaluate_all_providers(base_dir: str) -> Dict[str, Any]:
    provider_paths = discover_provider_paths(base_dir)
    results: Dict[str, Any] = {}
    total_score = 0.0

    for provider_path in provider_paths:
        provider_name = os.path.basename(provider_path)
        if provider_name.startswith("provider_") and provider_name.endswith(".json"):
            provider_name = provider_name[len("provider_"):-len(".json")]
        else:
            continue

        try:
            rules = engine.load_rules_from_csv(provider_name, base_dir)
            config = engine.read_provider_config(provider_path)
            provider = engine.get_provider_instance(provider_name, config)

            atomic_score, atomic_log = engine.evaluate_rules(provider, rules)
            br_options = engine.load_business_rules_from_csv(provider_name, base_dir)
            business_score, business_log = engine.evaluate_business_rules(atomic_log, br_options)

            provider_score_total = atomic_score + business_score
            total_score += provider_score_total

            results[provider_name] = {
                "score": round(provider_score_total, 6),
                "score_atomic": round(atomic_score, 6),
                "score_business": round(business_score, 6),
                "atomic_log": atomic_log,
                "business_log": business_log,
                "profile_url": extract_profile_url(provider_name, config),
            }
        except Exception as e:
            results[provider_name] = {
                "error": str(e),
                "exception_type": type(e).__name__,
            }

    return {
        "total_score": round(total_score, 6),
        "providers": results,
    }


def atomic_log_to_df(providers_result: Dict[str, Any]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for provider_name, data in providers_result.items():
        if not isinstance(data, dict) or "atomic_log" not in data:
            continue
        for entry in data["atomic_log"]:
            row = {
                "provider": provider_name,
                "id": entry.get("id"),
                "category": entry.get("category"),
                "label": entry.get("label"),
                "metric": entry.get("metric"),
                "operator": entry.get("operator"),
                "metric_value": entry.get("metric_value"),
                "matched": bool(entry.get("matched")),
                "weight_applied": float(entry.get("weight_applied", 0.0)),
                "is_gate": bool(entry.get("is_gate", False)),
            }
            rows.append(row)
    if not rows:
        return pd.DataFrame(columns=[
            "provider", "id", "category", "label", "metric",
            "operator", "metric_value", "matched", "weight_applied", "is_gate",
        ])
    df = pd.DataFrame(rows)

    def _safe_display_value(val: Any) -> str:
        if val is None:
            return ""
        if isinstance(val, (int, float, bool, str)):
            return str(val)
        try:
            return json.dumps(val)
        except Exception:
            return str(val)

    # Ensure Arrow-friendly representation for mixed-type values
    df["metric_value"] = df["metric_value"].apply(_safe_display_value)
    return df


def business_log_to_df(providers_result: Dict[str, Any]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for provider_name, data in providers_result.items():
        if not isinstance(data, dict) or "business_log" not in data:
            continue
        for group in data["business_log"]:
            chosen = group.get("chosen") or {}
            rows.append({
                "provider": provider_name,
                "group_id": group.get("group_id"),
                "label": group.get("label"),
                "category": group.get("category"),
                "chosen_likert_value": chosen.get("likert_value"),
                "chosen_likert_label": chosen.get("likert_label"),
                "chosen_weight": chosen.get("weight"),
                "satisfied_count": len(group.get("satisfied_options", [])),
            })
    if not rows:
        return pd.DataFrame(columns=[
            "provider", "group_id", "label", "category",
            "chosen_likert_value", "chosen_likert_label", "chosen_weight", "satisfied_count",
        ])
    return pd.DataFrame(rows)


def render_summary(header: str, eval_result: Dict[str, Any]) -> None:
    st.subheader(header)
    total = eval_result.get("total_score", 0.0)
    providers = eval_result.get("providers", {})
    cols = st.columns(3)
    cols[0].metric("Total Score", f"{total:.2f}")
    cols[1].metric("Providers", f"{len(providers)}")
    with cols[2]:
        df = pd.DataFrame([
            {
                "provider": name,
                "total": data.get("score", 0.0),
                "atomic": data.get("score_atomic", 0.0),
                "business": data.get("score_business", 0.0),
            }
            for name, data in providers.items()
            if isinstance(data, dict)
        ])
        if not df.empty:
            fig = px.bar(
                df.melt(id_vars=["provider"], value_vars=["atomic", "business"], var_name="type", value_name="score"),
                x="provider", y="score", color="type", barmode="stack",
                title="Scores by Provider (Atomic + Business)", text_auto=True,
            )
            st.plotly_chart(fig, use_container_width=True)


def render_atomic_section(atomic_df: pd.DataFrame) -> None:
    st.subheader("Atomic Rules")
    if atomic_df.empty:
        st.info("No atomic log data available.")
        return

    # Filters
    providers = sorted(atomic_df["provider"].unique())
    categories = sorted([c for c in atomic_df["category"].dropna().unique()])
    fcols = st.columns(4)
    sel_providers = fcols[0].multiselect("Providers", providers, default=providers)
    sel_cats = fcols[1].multiselect("Categories", categories, default=categories)
    show_matched_only = fcols[2].checkbox("Matched only", value=True)
    search_label = fcols[3].text_input("Search label", value="")

    df = atomic_df.copy()
    df = df[df["provider"].isin(sel_providers)]
    if sel_cats:
        df = df[df["category"].isin(sel_cats)]
    if show_matched_only:
        df = df[df["matched"]]
    if search_label.strip():
        s = search_label.strip().lower()
        df = df[df["label"].str.lower().str.contains(s, na=False)]

    if df.empty:
        st.warning("No rows match the current filters.")
        return

    # Charts
    by_cat = df.groupby(["provider", "category"], as_index=False)["weight_applied"].sum()
    fig1 = px.bar(by_cat, x="category", y="weight_applied", color="provider", barmode="group",
                  title="Sum of Applied Weights by Category")
    st.plotly_chart(fig1, use_container_width=True)

    top_rules = df.sort_values("weight_applied", ascending=False).head(25)
    fig2 = px.bar(top_rules, x="weight_applied", y="label", color="provider", orientation="h",
                  title="Top Rules by Applied Weight", hover_data=["metric", "operator", "metric_value"]) 
    st.plotly_chart(fig2, use_container_width=True)

    with st.expander("Atomic log table"):
        st.dataframe(df.sort_values(["provider", "category", "label"]).reset_index(drop=True), use_container_width=True)


def render_business_section(business_df: pd.DataFrame) -> None:
    st.subheader("Business Rules")
    if business_df.empty:
        st.info("No business log data available.")
        return

    # Filters
    providers = sorted(business_df["provider"].unique())
    categories = sorted([c for c in business_df["category"].dropna().unique()])
    fcols = st.columns(3)
    sel_providers = fcols[0].multiselect("Providers", providers, default=providers)
    sel_cats = fcols[1].multiselect("Categories", categories, default=categories)
    min_likert = fcols[2].slider("Min Likert", min_value=0, max_value=int(business_df["chosen_likert_value"].fillna(0).max() or 5), value=0)

    df = business_df.copy()
    df = df[df["provider"].isin(sel_providers)]
    if sel_cats:
        df = df[df["category"].isin(sel_cats)]
    df = df[df["chosen_likert_value"].fillna(0) >= min_likert]

    if df.empty:
        st.warning("No groups match the current filters.")
        return

    by_cat = df.groupby(["provider", "category"], as_index=False)["chosen_weight"].sum()
    fig1 = px.bar(by_cat, x="category", y="chosen_weight", color="provider", barmode="group",
                  title="Sum of Business Weights by Category")
    st.plotly_chart(fig1, use_container_width=True)

    # Distribution of likert values
    fig2 = px.histogram(df, x="chosen_likert_value", color="provider", barmode="group",
                        title="Distribution of Chosen Likert Values")
    st.plotly_chart(fig2, use_container_width=True)

    with st.expander("Business log table"):
        st.dataframe(df.sort_values(["provider", "category", "group_id"]).reset_index(drop=True), use_container_width=True)


def main() -> None:
    st.set_page_config(page_title="Rules Engine Graphs", layout="wide")
    st.title("Rules Engine Graphs")

    base_dir = SCRIPT_DIR
    with st.spinner("Evaluating providers..."):
        eval_result = evaluate_all_providers(base_dir)

    render_summary("Overview", eval_result)

    providers_result = eval_result.get("providers", {})
    atomic_df = atomic_log_to_df(providers_result)
    business_df = business_log_to_df(providers_result)

    tabs = st.tabs(["Atomic", "Business", "Profiles", "Download"])
    with tabs[0]:
        render_atomic_section(atomic_df)
    with tabs[1]:
        render_business_section(business_df)
    with tabs[2]:
        st.subheader("Profiles")
        providers = eval_result.get("providers", {})
        links = []
        for name, data in providers.items():
            if not isinstance(data, dict):
                continue
            url = data.get("profile_url")
            if url:
                links.append((name, url))
        if links:
            for name, url in sorted(links):
                st.markdown(f"- [{name}]({url})")
        else:
            st.info("No profile links detected.")
    with tabs[3]:
        st.download_button(
            label="Download raw evaluation JSON",
            data=pd.Series(eval_result).to_json(),
            file_name="rules_evaluation.json",
            mime="application/json",
        )


if __name__ == "__main__":
    main()


