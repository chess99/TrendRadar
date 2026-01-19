# coding=utf-8
"""
数据处理模块

提供数据读取、保存和检测功能：
- save_titles_to_file: 保存标题到 TXT 文件
- read_all_today_titles: 从存储后端读取当天所有标题
- detect_latest_new_titles: 检测最新批次的新增标题

Author: TrendRadar Team
"""

from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple


def save_titles_to_file(
    results: Dict,
    id_to_name: Dict,
    failed_ids: List,
    output_path: str,
    clean_title_func: Callable[[str], str],
) -> str:
    """
    保存标题到 TXT 文件

    Args:
        results: 抓取结果 {source_id: {title: title_data}}
        id_to_name: ID 到名称的映射
        failed_ids: 失败的 ID 列表
        output_path: 输出文件路径
        clean_title_func: 标题清理函数

    Returns:
        str: 保存的文件路径
    """
    # 确保目录存在
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        for id_value, title_data in results.items():
            # id | name 或 id
            name = id_to_name.get(id_value)
            if name and name != id_value:
                f.write(f"{id_value} | {name}\n")
            else:
                f.write(f"{id_value}\n")

            # 按排名排序标题
            sorted_titles = []
            for title, info in title_data.items():
                cleaned_title = clean_title_func(title)
                if isinstance(info, dict):
                    ranks = info.get("ranks", [])
                    url = info.get("url", "")
                    mobile_url = info.get("mobileUrl", "")
                else:
                    ranks = info if isinstance(info, list) else []
                    url = ""
                    mobile_url = ""

                rank = ranks[0] if ranks else 1
                sorted_titles.append((rank, cleaned_title, url, mobile_url))

            sorted_titles.sort(key=lambda x: x[0])

            for rank, cleaned_title, url, mobile_url in sorted_titles:
                line = f"{rank}. {cleaned_title}"

                if url:
                    line += f" [URL:{url}]"
                if mobile_url:
                    line += f" [MOBILE:{mobile_url}]"
                f.write(line + "\n")

            f.write("\n")

        if failed_ids:
            f.write("==== 以下ID请求失败 ====\n")
            for id_value in failed_ids:
                f.write(f"{id_value}\n")

    return output_path


def read_all_today_titles_from_storage(
    storage_manager,
    current_platform_ids: Optional[List[str]] = None,
) -> Tuple[Dict, Dict, Dict]:
    """
    从存储后端读取当天所有标题（SQLite 数据）

    Args:
        storage_manager: 存储管理器实例
        current_platform_ids: 当前监控的平台 ID 列表（用于过滤）

    Returns:
        Tuple[Dict, Dict, Dict]: (all_results, id_to_name, title_info)
    """
    try:
        news_data = storage_manager.get_today_all_data()

        if not news_data or not news_data.items:
            return {}, {}, {}

        all_results = {}
        final_id_to_name = {}
        title_info = {}

        for source_id, news_list in news_data.items.items():
            # 按平台过滤
            if current_platform_ids is not None and source_id not in current_platform_ids:
                continue

            # 获取来源名称
            source_name = news_data.id_to_name.get(source_id, source_id)
            final_id_to_name[source_id] = source_name

            if source_id not in all_results:
                all_results[source_id] = {}
                title_info[source_id] = {}

            for item in news_list:
                title = item.title
                ranks = getattr(item, 'ranks', [item.rank])
                first_time = getattr(item, 'first_time', item.crawl_time)
                last_time = getattr(item, 'last_time', item.crawl_time)
                count = getattr(item, 'count', 1)
                rank_timeline = getattr(item, 'rank_timeline', [])

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

        return all_results, final_id_to_name, title_info

    except Exception as e:
        print(f"[存储] 从存储后端读取数据失败: {e}")
        return {}, {}, {}


def read_all_today_titles(
    storage_manager,
    current_platform_ids: Optional[List[str]] = None,
    quiet: bool = False,
) -> Tuple[Dict, Dict, Dict]:
    """
    读取当天所有标题（从存储后端）

    Args:
        storage_manager: 存储管理器实例
        current_platform_ids: 当前监控的平台 ID 列表（用于过滤）
        quiet: 是否静默模式（不打印日志）

    Returns:
        Tuple[Dict, Dict, Dict]: (all_results, id_to_name, title_info)
    """
    all_results, final_id_to_name, title_info = read_all_today_titles_from_storage(
        storage_manager, current_platform_ids
    )

    if not quiet:
        if all_results:
            total_count = sum(len(titles) for titles in all_results.values())
            print(f"[存储] 已从存储后端读取 {total_count} 条标题")
        else:
            print("[存储] 当天暂无数据")

    return all_results, final_id_to_name, title_info


def detect_latest_new_titles_from_storage(
    storage_manager,
    current_platform_ids: Optional[List[str]] = None,
) -> Dict:
    """
    从存储后端检测新增标题（从上次推送到现在的所有新增，跨天支持）

    修改说明：
    - 原逻辑：只检测最新一次抓取的新增标题
    - 新逻辑：检测从上次推送时间之后的所有新增标题（包括昨天和今天的数据）
    - 时间跨度限制：最多26小时，避免数据爆炸
    - 如果今天未推送过，则检测从昨天第一次抓取之后的所有新增标题

    Args:
        storage_manager: 存储管理器实例
        current_platform_ids: 当前监控的平台 ID 列表（用于过滤）

    Returns:
        Dict: 新增标题 {source_id: {title: title_data}}
    """
    try:
        from datetime import datetime, timedelta

        # 获取后端实例以访问日期格式化方法
        backend = storage_manager.get_backend()
        
        # 获取今天和昨天的日期
        today_date = backend._format_date_folder(None)
        yesterday_date = (backend._get_configured_time() - timedelta(days=1)).strftime("%Y-%m-%d")

        # 读取今天和昨天的数据
        today_data = storage_manager.get_today_all_data(today_date)
        yesterday_data = storage_manager.get_today_all_data(yesterday_date)

        # 合并数据
        all_data_items = {}
        all_data_id_to_name = {}
        
        if today_data and today_data.items:
            for source_id, news_list in today_data.items.items():
                if source_id not in all_data_items:
                    all_data_items[source_id] = []
                all_data_items[source_id].extend(news_list)
            all_data_id_to_name.update(today_data.id_to_name)
        
        if yesterday_data and yesterday_data.items:
            for source_id, news_list in yesterday_data.items.items():
                if source_id not in all_data_items:
                    all_data_items[source_id] = []
                all_data_items[source_id].extend(news_list)
            all_data_id_to_name.update(yesterday_data.id_to_name)

        if not all_data_items:
            # 没有历史数据（第一次抓取），不应该有"新增"标题
            return {}

        # 获取上次推送时间（全局查询）
        last_push_time_str = storage_manager.get_last_push_time()
        
        # 确定基准时间：如果推送过，使用上次推送时间；否则使用昨天第一次抓取时间
        if last_push_time_str:
            # 推送过，使用上次推送时间作为基准
            try:
                push_datetime = datetime.strptime(last_push_time_str, "%Y-%m-%d %H:%M:%S")
                # 获取当前时间
                backend = storage_manager.get_backend()
                current_time = backend._get_configured_time()
                
                # 时间跨度限制：最多26小时
                time_diff = current_time - push_datetime.replace(tzinfo=current_time.tzinfo)
                max_hours = 26
                if time_diff.total_seconds() > max_hours * 3600:
                    # 如果超过26小时，将基准时间调整为26小时前
                    base_datetime = current_time - timedelta(hours=max_hours)
                    base_time_str = base_datetime.strftime("%Y-%m-%d %H:%M")
                    print(f"[增量检测] 上次推送时间超过{max_hours}小时，限制为{max_hours}小时内的数据")
                else:
                    # 构建用于比较的时间字符串（格式：YYYY-MM-DD HH:MM）
                    base_time_str = push_datetime.strftime("%Y-%m-%d %H:%M")
                
                print(f"[增量检测] 使用上次推送时间作为基准: {last_push_time_str}")
            except (ValueError, TypeError) as e:
                print(f"[增量检测] 解析上次推送时间失败: {e}，使用昨天第一次抓取时间作为基准")
                base_time_str = None
        else:
            # 从未推送过，使用昨天第一次抓取时间作为基准
            base_time_str = None
            print("[增量检测] 从未推送过，检测从昨天第一次抓取之后的所有新增")

        # 收集所有标题及其首次出现时间
        all_titles_with_time = {}
        for source_id, news_list in all_data_items.items():
            if current_platform_ids is not None and source_id not in current_platform_ids:
                continue

            if source_id not in all_titles_with_time:
                all_titles_with_time[source_id] = {}

            for item in news_list:
                first_time = getattr(item, 'first_time', item.crawl_time)
                title = item.title

                # 如果标题已存在，保留更早的首次出现时间
                if title not in all_titles_with_time[source_id]:
                    all_titles_with_time[source_id][title] = {
                        "first_time": first_time,
                        "ranks": [item.rank],
                        "url": item.url or "",
                        "mobileUrl": item.mobile_url or "",
                    }
                else:
                    # 如果这个标题的首次出现时间更早，更新它
                    if first_time < all_titles_with_time[source_id][title]["first_time"]:
                        all_titles_with_time[source_id][title]["first_time"] = first_time

        # 如果没有基准时间（从未推送过），需要找到昨天第一次抓取时间
        if base_time_str is None:
            # 找到所有标题中最早的首次出现时间（优先从昨天的数据中找）
            earliest_time = None
            for source_id, titles in all_titles_with_time.items():
                for title_data in titles.values():
                    first_time = title_data["first_time"]
                    # 优先选择昨天的数据
                    if first_time.startswith(yesterday_date):
                        if earliest_time is None or first_time < earliest_time:
                            earliest_time = first_time
            
            # 如果昨天没有数据，使用今天最早的数据
            if earliest_time is None:
                for source_id, titles in all_titles_with_time.items():
                    for title_data in titles.values():
                        first_time = title_data["first_time"]
                        if earliest_time is None or first_time < earliest_time:
                            earliest_time = first_time
            
            if earliest_time:
                # 提取时间部分用于比较
                if ' ' in earliest_time:
                    base_time_str = earliest_time.split()[0] + " " + earliest_time.split()[1][:5]
                else:
                    base_time_str = earliest_time[:16]  # YYYY-MM-DD HH:MM
                print(f"[增量检测] 使用第一次抓取时间作为基准: {earliest_time}")

        if not base_time_str:
            # 无法确定基准时间，返回空
            return {}

        # 检测新增标题：首次出现时间晚于基准时间的标题
        new_titles = {}
        for source_id, titles in all_titles_with_time.items():
            source_new_titles = {}
            for title, title_data in titles.items():
                first_time = title_data["first_time"]
                # 比较时间：如果首次出现时间晚于基准时间，则是新增标题
                # 时间格式：YYYY-MM-DD HH:MM:SS 或 YYYY-MM-DD HH:MM
                if ' ' in first_time:
                    first_time_str = first_time.split()[0] + " " + first_time.split()[1][:5]
                else:
                    first_time_str = first_time[:16]
                
                if first_time_str > base_time_str:
                    source_new_titles[title] = {
                        "ranks": title_data["ranks"],
                        "url": title_data["url"],
                        "mobileUrl": title_data["mobileUrl"],
                    }

            if source_new_titles:
                new_titles[source_id] = source_new_titles

        return new_titles

    except Exception as e:
        print(f"[存储] 从存储后端检测新标题失败: {e}")
        import traceback
        traceback.print_exc()
        return {}


def detect_latest_new_titles(
    storage_manager,
    current_platform_ids: Optional[List[str]] = None,
    quiet: bool = False,
) -> Dict:
    """
    检测新增标题（从上次推送到现在的所有新增）

    修改说明：
    - 原逻辑：只检测最新一次抓取的新增标题
    - 新逻辑：检测从上次推送时间之后的所有新增标题
    - 如果今天未推送过，则检测从当天第一次抓取之后的所有新增标题

    Args:
        storage_manager: 存储管理器实例
        current_platform_ids: 当前监控的平台 ID 列表（用于过滤）
        quiet: 是否静默模式（不打印日志）

    Returns:
        Dict: 新增标题 {source_id: {title: title_data}}
    """
    new_titles = detect_latest_new_titles_from_storage(storage_manager, current_platform_ids)
    if new_titles and not quiet:
        total_new = sum(len(titles) for titles in new_titles.values())
        print(f"[存储] 从存储后端检测到 {total_new} 条新增标题（从上次推送到现在）")
    return new_titles
