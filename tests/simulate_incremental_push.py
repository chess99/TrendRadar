# input: local sqlite dbs under output/news plus config files (.env.local optional)
# output: stdout preview of incremental push batches; no network or writes
# pos: dev tool for dry-run incremental push preview; update header and tests/README
#!/usr/bin/env python3
# coding=utf-8
# ruff: noqa: E402
"""
Dry-run incremental push preview using local SQLite data.

This script:
- forces local storage backend
- loads config + frequency words
- reads today's data (and uses yesterday for incremental detection)
- renders the same message batches used by wework
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from trendradar.context import AppContext
from trendradar.core import detect_latest_new_titles_from_storage, load_config
from trendradar.core.analyzer import convert_keyword_stats_to_platform_stats


def _load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[len("export ") :].strip()
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _force_local_overrides() -> None:
    os.environ["STORAGE_BACKEND"] = "local"
    os.environ["PULL_ENABLED"] = "false"
    os.environ["AI_ANALYSIS_ENABLED"] = "false"
    os.environ["AI_TRANSLATION_ENABLED"] = "false"


def _extract_results_from_news_data(
    news_data, current_platform_ids: Optional[List[str]]
) -> Tuple[Dict, Dict, Dict]:
    if not news_data or not news_data.items:
        return {}, {}, {}

    all_results: Dict[str, Dict] = {}
    id_to_name: Dict[str, str] = {}
    title_info: Dict[str, Dict] = {}

    for source_id, news_list in news_data.items.items():
        if current_platform_ids is not None and source_id not in current_platform_ids:
            continue
        source_name = news_data.id_to_name.get(source_id, source_id)
        id_to_name[source_id] = source_name
        all_results.setdefault(source_id, {})
        title_info.setdefault(source_id, {})

        for item in news_list:
            title = item.title
            ranks = getattr(item, "ranks", [item.rank])
            first_time = getattr(item, "first_time", item.crawl_time)
            last_time = getattr(item, "last_time", item.crawl_time)
            count = getattr(item, "count", 1)
            rank_timeline = getattr(item, "rank_timeline", [])

            all_results[source_id][title] = {
                "ranks": ranks,
                "url": item.url or "",
                "mobileUrl": item.mobile_url or "",
            }

            title_info[source_id][title] = {
                "first_time": first_time,
                "last_time": last_time,
                "count": count,
                "ranks": ranks,
                "url": item.url or "",
                "mobileUrl": item.mobile_url or "",
                "rank_timeline": rank_timeline,
            }

    return all_results, id_to_name, title_info


def _resolve_dates(storage_manager, today_arg: Optional[str], yesterday_arg: Optional[str]) -> Tuple[str, str]:
    backend = storage_manager.get_backend()
    if today_arg and yesterday_arg:
        return today_arg, yesterday_arg
    if today_arg:
        today_date = today_arg
        yesterday_date = (
            backend._get_configured_time() - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        return today_date, yesterday_date

    today_date = backend._format_date_folder(None)
    yesterday_date = (backend._get_configured_time() - timedelta(days=1)).strftime("%Y-%m-%d")
    return today_date, yesterday_date


def main() -> int:
    parser = argparse.ArgumentParser(description="Dry-run incremental push preview (local data).")
    parser.add_argument("--today", help="Override today's date (YYYY-MM-DD).")
    parser.add_argument("--yesterday", help="Override yesterday's date (YYYY-MM-DD).")
    parser.add_argument(
        "--format",
        default="wework",
        choices=["wework", "dingtalk", "feishu", "telegram", "ntfy", "bark", "slack"],
        help="Render format for preview output.",
    )
    parser.add_argument(
        "--env-file",
        default=".env.local",
        help="Optional env file to load (defaults to .env.local).",
    )
    args = parser.parse_args()

    _load_env_file(Path(args.env_file))
    _force_local_overrides()

    config = load_config()
    ctx = AppContext(config)
    storage_manager = ctx.get_storage_manager()

    today_date, yesterday_date = _resolve_dates(storage_manager, args.today, args.yesterday)
    today_db = Path("output") / "news" / f"{today_date}.db"
    yesterday_db = Path("output") / "news" / f"{yesterday_date}.db"

    print("=== Incremental Push Dry-Run ===")
    print(f"today_date: {today_date} (exists={today_db.exists()})")
    print(f"yesterday_date: {yesterday_date} (exists={yesterday_db.exists()})")

    today_data = storage_manager.get_today_all_data(today_date)
    yesterday_data = storage_manager.get_today_all_data(yesterday_date)

    current_platform_ids = ctx.platform_ids
    results, today_id_to_name, title_info = _extract_results_from_news_data(
        today_data, current_platform_ids
    )

    id_to_name = {p["id"]: p.get("name", p["id"]) for p in ctx.platforms}
    id_to_name.update(today_id_to_name)
    if yesterday_data and yesterday_data.id_to_name:
        id_to_name.update(yesterday_data.id_to_name)

    new_titles = detect_latest_new_titles_from_storage(storage_manager, current_platform_ids)
    word_groups, filter_words, global_filters = ctx.load_frequency_words()

    stats, total_titles = ctx.count_frequency(
        results=results,
        word_groups=word_groups,
        filter_words=filter_words,
        id_to_name=id_to_name,
        title_info=title_info,
        new_titles=new_titles,
        mode="incremental",
        global_filters=global_filters,
        quiet=True,
    )

    if ctx.display_mode == "platform" and stats:
        stats = convert_keyword_stats_to_platform_stats(
            stats,
            ctx.weight_config,
            ctx.rank_threshold,
        )

    report_data = ctx.prepare_report(
        stats=stats,
        failed_ids=[],
        new_titles=new_titles,
        id_to_name=id_to_name,
        mode="incremental",
    )

    last_push_time = storage_manager.get_last_push_time()
    total_new = sum(len(titles) for titles in new_titles.values()) if new_titles else 0
    matched_count = sum(len(stat.get("titles", [])) for stat in stats) if stats else 0

    print(f"last_push_time: {last_push_time or 'None'}")
    print(f"new_titles_total: {total_new}")
    print(f"matched_news_total: {matched_count}")
    print("report_mode: incremental")
    print(f"display_mode: {ctx.display_mode}")
    print(f"render_format: {args.format}")

    batches = ctx.split_content(
        report_data=report_data,
        format_type=args.format,
        update_info=None,
        mode="incremental",
        rss_items=None,
        rss_new_items=None,
        ai_content=None,
        standalone_data=None,
        ai_stats=None,
        report_type="增量分析",
    )

    print(f"batches: {len(batches)}")
    for index, batch in enumerate(batches, 1):
        header = f"\n--- batch {index}/{len(batches)} ---"
        print(header)
        print(batch)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
