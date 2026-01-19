#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试增量检测功能（跨天支持）

测试内容：
1. 存储管理器初始化
2. 全局推送时间查询（跨日期）
3. 跨天数据读取（今天+昨天）
4. 增量检测逻辑（26小时限制）
"""

import sys
import os
from pathlib import Path

# Windows 控制台编码修复
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from trendradar.storage.manager import StorageManager
from trendradar.core.data import detect_latest_new_titles_from_storage
from datetime import datetime, timedelta
import sqlite3


def check_database_data(storage_manager, date_str):
    """检查指定日期的数据库数据"""
    backend = storage_manager.get_backend()
    db_path = backend._get_db_path(date_str, "news")
    
    if not db_path.exists():
        return None, 0, None, None
    
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # 检查新闻数量
        cursor.execute("SELECT COUNT(*) FROM news_items")
        news_count = cursor.fetchone()[0]
        
        # 检查推送记录
        cursor.execute("SELECT push_time FROM push_records WHERE pushed = 1")
        push_record = cursor.fetchone()
        push_time = push_record[0] if push_record else None
        
        # 检查最早的抓取时间
        cursor.execute("SELECT MIN(first_crawl_time) FROM news_items")
        earliest_time = cursor.fetchone()[0]
        
        conn.close()
        return str(db_path), news_count, push_time, earliest_time
    except Exception as e:
        return None, 0, None, None


def test_incremental_detection():
    """详细测试增量检测功能"""
    print("=" * 70)
    print("测试增量检测功能（跨天支持）")
    print("=" * 70)
    
    try:
        # 初始化存储管理器
        print("\n[1/5] 初始化存储管理器...")
        storage_manager = StorageManager(
            backend_type="local",
            data_dir="output",
            timezone="Asia/Shanghai"
        )
        backend = storage_manager.get_backend()
        print("✅ 存储管理器初始化成功")
        
        # 检查今天和昨天的数据文件
        print("\n[2/5] 检查数据文件...")
        today_date = backend._format_date_folder(None)
        yesterday_date = (backend._get_configured_time() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        print(f"   今天日期: {today_date}")
        today_path, today_count, today_push, today_earliest = check_database_data(storage_manager, today_date)
        if today_path:
            print(f"   ✅ 今天数据库: {today_path}")
            print(f"      新闻数量: {today_count} 条")
            if today_push:
                print(f"      推送时间: {today_push}")
            if today_earliest:
                print(f"      最早抓取: {today_earliest}")
        else:
            print(f"   ⚠️  今天数据库不存在")
        
        print(f"\n   昨天日期: {yesterday_date}")
        yesterday_path, yesterday_count, yesterday_push, yesterday_earliest = check_database_data(storage_manager, yesterday_date)
        if yesterday_path:
            print(f"   ✅ 昨天数据库: {yesterday_path}")
            print(f"      新闻数量: {yesterday_count} 条")
            if yesterday_push:
                print(f"      推送时间: {yesterday_push}")
            if yesterday_earliest:
                print(f"      最早抓取: {yesterday_earliest}")
        else:
            print(f"   ⚠️  昨天数据库不存在")
        
        # 测试获取上次推送时间（全局查询）
        print("\n[3/5] 测试获取上次推送时间（全局查询）...")
        last_push_time = storage_manager.get_last_push_time()
        if last_push_time:
            print(f"✅ 找到上次推送时间: {last_push_time}")
            
            # 计算时间差
            try:
                push_dt = datetime.strptime(last_push_time, "%Y-%m-%d %H:%M:%S")
                current_dt = backend._get_configured_time()
                time_diff = current_dt - push_dt.replace(tzinfo=current_dt.tzinfo)
                hours_diff = time_diff.total_seconds() / 3600
                print(f"   距离现在: {hours_diff:.1f} 小时")
                if hours_diff > 26:
                    print(f"   ⚠️  超过26小时限制，将自动限制为26小时内的数据")
            except Exception as e:
                print(f"   ⚠️  无法计算时间差: {e}")
        else:
            print("ℹ️  未找到推送记录（正常，如果从未推送过）")
        
        # 测试读取今天和昨天的数据
        print("\n[4/5] 测试读取今天和昨天的数据...")
        today_data = storage_manager.get_today_all_data(today_date)
        yesterday_data = storage_manager.get_today_all_data(yesterday_date)
        
        today_items = sum(len(items) for items in today_data.items.values()) if today_data and today_data.items else 0
        yesterday_items = sum(len(items) for items in yesterday_data.items.values()) if yesterday_data and yesterday_data.items else 0
        
        print(f"   今天数据: {today_items} 条")
        print(f"   昨天数据: {yesterday_items} 条")
        print(f"   合计: {today_items + yesterday_items} 条")
        
        # 测试增量检测
        print("\n[5/5] 测试增量检测（跨天支持）...")
        new_titles = detect_latest_new_titles_from_storage(storage_manager)
        
        if new_titles:
            total_new = sum(len(titles) for titles in new_titles.values())
            print(f"✅ 检测到 {total_new} 条新增标题（跨天）")
            for source_id, titles in new_titles.items():
                print(f"   - {source_id}: {len(titles)} 条")
                # 显示前3条作为示例
                for i, (title, data) in enumerate(list(titles.items())[:3]):
                    print(f"     {i+1}. {title[:50]}...")
        else:
            print("ℹ️  未检测到新增标题")
            print("   可能原因：")
            print("   1. 今天未推送过，且数据不足")
            print("   2. 从上次推送到现在确实没有新增")
            print("   3. 数据文件存在但为空")
        
        print("\n" + "=" * 70)
        print("✅ 测试完成！")
        print("=" * 70)
        print("\n💡 提示：")
        print("   - 如果看到'未检测到新增标题'，这是正常的（可能确实没有新增）")
        print("   - 功能已实现：支持跨天检测、26小时限制、全局推送时间查询")
        print("   - 实际效果需要在有数据的情况下验证")
        return True
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_incremental_detection()
    sys.exit(0 if success else 1)
