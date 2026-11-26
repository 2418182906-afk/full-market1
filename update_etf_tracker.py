#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
宽基 ETF 规模追踪 & 资金底背离监测系统 (QTYX V3.6.6 亿元口径版)
数据源：
1. 五大宽基指数日线：腾讯财经日K接口 (sh000688, sh000852, sh000905, sh000016, sh000300)
2. 宽基ETF每日份额：上海证券交易所、深圳证券交易所及 broad_market.py
3. 宽基ETF日线收盘价/净值代理：腾讯财经日K接口（用于将“亿份”换算为“亿元”）

用法：
- 更新数据并生成独立HTML看板：
  python3 update_etf_tracker.py
- 启动本地交互Web服务：
  python3 update_etf_tracker.py --serve --port 8765
"""

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import requests

from broad_market import ALL_CODES, CODE_TO_GROUP, GROUPS, sync_broad_market, build_broad_payload

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
CACHE_PATH = os.path.join(DATA_DIR, "etf_share_cache.json")
HTML_OUT_PATH = os.path.join(BASE_DIR, "etf_share_tracker.html")

START_DATE = "2024-06-03"

INDICES_META = [
    {"code": "sh000688", "name": "科创50指数", "group": "科创50", "color": "#2b6cb0"},
    {"code": "sh000852", "name": "中证1000指数", "group": "中证1000", "color": "#82c91e"},
    {"code": "sh000905", "name": "中证500指数", "group": "中证500", "color": "#495057"},
    {"code": "sh000016", "name": "上证50指数", "group": "上证50", "color": "#fd7e14"},
    {"code": "sh000300", "name": "沪深300指数", "group": "沪深300", "color": "#15aabf"},
]

ETFS_META = [
    {
        "code": "510300",
        "name": "沪深300华泰",
        "full_name": "华泰柏瑞沪深300ETF",
        "group": "沪深300",
        "index_code": "sh000300",
        "index_name": "沪深300指数",
        "color": "#e6b800",
        "single_thresh": 20.0,
        "five_day_thresh": 50.0,
        "low_pct": 20.0,
        "high_pct": 80.0,
    },
    {
        "code": "510330",
        "name": "沪深300华夏",
        "full_name": "华夏沪深300ETF",
        "group": "沪深300",
        "index_code": "sh000300",
        "index_name": "沪深300指数",
        "color": "#e64980",
        "single_thresh": 14.0,
        "five_day_thresh": 35.0,
        "low_pct": 20.0,
        "high_pct": 80.0,
    },
    {
        "code": "510500",
        "name": "中证500南方",
        "full_name": "南方中证500ETF",
        "group": "中证500",
        "index_code": "sh000905",
        "index_name": "中证500指数",
        "color": "#7950f2",
        "single_thresh": 10.0,
        "five_day_thresh": 25.0,
        "low_pct": 20.0,
        "high_pct": 80.0,
    },
    {
        "code": "512500",
        "name": "中证500华夏",
        "full_name": "华夏中证500ETF",
        "group": "中证500",
        "index_code": "sh000905",
        "index_name": "中证500指数",
        "color": "#20c997",
        "single_thresh": 4.0,
        "five_day_thresh": 10.0,
        "low_pct": 20.0,
        "high_pct": 80.0,
    },
    {
        "code": "512100",
        "name": "中证1000南方",
        "full_name": "南方中证1000ETF",
        "group": "中证1000",
        "index_code": "sh000852",
        "index_name": "中证1000指数",
        "color": "#4c6ef5",
        "single_thresh": 7.0,
        "five_day_thresh": 30.0,
        "low_pct": 20.0,
        "high_pct": 80.0,
    },
    {
        "code": "560010",
        "name": "中证1000广发",
        "full_name": "广发中证1000ETF",
        "group": "中证1000",
        "index_code": "sh000852",
        "index_name": "中证1000指数",
        "color": "#94d82d",
        "single_thresh": 5.0,
        "five_day_thresh": 10.0,
        "low_pct": 20.0,
        "high_pct": 80.0,
    },
    {
        "code": "510050",
        "name": "上证50华夏",
        "full_name": "华夏上证50ETF",
        "group": "上证50",
        "index_code": "sh000016",
        "index_name": "上证50指数",
        "color": "#495057",
        "single_thresh": 15.0,
        "five_day_thresh": 35.0,
        "low_pct": 20.0,
        "high_pct": 80.0,
    },
    {
        "code": "588000",
        "name": "科创50华夏",
        "full_name": "华夏上证科创板50ETF",
        "group": "科创50",
        "index_code": "sh000688",
        "index_name": "科创50指数",
        "color": "#f76707",
        "single_thresh": 6.0,
        "five_day_thresh": 30.0,
        "low_pct": 20.0,
        "high_pct": 80.0,
    },
]


def _to_sec_symbol(code: str) -> str:
    c = str(code).strip().zfill(6)
    return f"sz{c}" if c.startswith(("15", "16", "18", "00", "30")) else f"sh{c}"


def _fetch_kline_close_map(session: requests.Session, symbol: str, start_date: str = START_DATE) -> dict:
    """从腾讯财经获取日K收盘价映射 {YYYY-MM-DD: close_float}"""
    url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={symbol},day,{start_date},2029-12-31,1500,"
    out = {}
    for attempt in range(3):
        try:
            resp = session.get(url, timeout=15).json()
            days = resp.get("data", {}).get(symbol, {}).get("day", [])
            for d in days:
                if isinstance(d, list) and len(d) >= 3 and d[0] >= start_date:
                    out[d[0]] = round(float(d[2]), 4)
            return out
        except Exception:
            time.sleep(0.25 * (attempt + 1))
    return out


def _convert_broad_payload_to_yi_yuan(broad_payload: dict, price_maps: dict) -> dict:
    """将 broad_market 的份额（亿份）乘以对应 ETF 当日收盘价，换算为规模（亿元）"""
    if not isinstance(broad_payload, dict):
        return {}

    b_dates = broad_payload.get("dates", [])
    by_date = broad_payload.get("by_date", {})
    groups = broad_payload.get("groups", [])

    # 补充每只 ETF 的当日价格与当日规模（亿元）
    new_by_date = {}
    for day, rows in by_date.items():
        if not isinstance(rows, dict):
            continue
        day_out = {}
        for code, rec in rows.items():
            if not isinstance(rec, dict):
                continue
            c = str(code).strip().zfill(6)
            shares_yi = float(rec.get("shares_10k", 0.0)) / 10000.0
            px = price_maps.get(c, {}).get(day)
            amount_yi_yuan = round(shares_yi * px, 4) if px is not None else None
            item = dict(rec)
            item["shares_yi"] = round(shares_yi, 4)
            item["price"] = px
            item["amount_yi_yuan"] = amount_yi_yuan
            day_out[c] = item
        new_by_date[day] = day_out

    new_groups = []
    for g in groups:
        gid = g.get("id")
        series_yuan = []
        for day in b_dates:
            day_rows = new_by_date.get(day, {})
            vals = [
                r["amount_yi_yuan"]
                for code, r in day_rows.items()
                if str(code).zfill(6) in CODE_TO_GROUP
                and CODE_TO_GROUP[str(code).zfill(6)] == gid
                and r.get("amount_yi_yuan") is not None
            ]
            series_yuan.append(round(sum(vals), 4) if vals else None)

        latest_idx = next((i for i in range(len(series_yuan) - 1, -1, -1) if series_yuan[i] is not None), None)
        prev_idx = (
            next((i for i in range(latest_idx - 1, -1, -1) if series_yuan[i] is not None), None)
            if latest_idx is not None
            else None
        )
        latest_val = series_yuan[latest_idx] if latest_idx is not None else None
        prev_val = series_yuan[prev_idx] if prev_idx is not None else None

        details_yuan = []
        if latest_idx is not None:
            lday = b_dates[latest_idx]
            for code, rec in new_by_date.get(lday, {}).items():
                c = str(code).zfill(6)
                if c in CODE_TO_GROUP and CODE_TO_GROUP[c] == gid:
                    details_yuan.append(
                        {
                            "code": c,
                            "name": rec.get("name", ""),
                            "exchange": rec.get("exchange", ""),
                            "shares_yi": rec.get("shares_yi"),
                            "price": rec.get("price"),
                            "amount": rec.get("amount_yi_yuan"),
                        }
                    )
        details_yuan.sort(key=lambda x: (x["amount"] is None, -(x["amount"] or 0.0)))

        new_g = dict(g)
        new_g["series"] = series_yuan
        new_g["latest"] = latest_val
        new_g["change1"] = (
            round(latest_val - prev_val, 4)
            if latest_val is not None and prev_val is not None
            else None
        )
        new_g["details"] = details_yuan
        new_groups.append(new_g)

    total_series_yuan = []
    for day in b_dates:
        day_rows = new_by_date.get(day, {})
        vals = [
            r["amount_yi_yuan"]
            for code, r in day_rows.items()
            if str(code).zfill(6) in CODE_TO_GROUP and r.get("amount_yi_yuan") is not None
        ]
        total_series_yuan.append(round(sum(vals), 4) if vals else None)

    t_latest_idx = next(
        (i for i in range(len(total_series_yuan) - 1, -1, -1) if total_series_yuan[i] is not None),
        None,
    )
    t_prev_idx = (
        next((i for i in range(t_latest_idx - 1, -1, -1) if total_series_yuan[i] is not None), None)
        if t_latest_idx is not None
        else None
    )
    t_latest = total_series_yuan[t_latest_idx] if t_latest_idx is not None else None
    t_prev = total_series_yuan[t_prev_idx] if t_prev_idx is not None else None

    out_payload = dict(broad_payload)
    out_payload["unit"] = "亿元"
    out_payload["groups"] = new_groups
    out_payload["total_series"] = total_series_yuan
    out_payload["total_latest"] = t_latest
    out_payload["total_change1"] = (
        round(t_latest - t_prev, 4)
        if t_latest is not None and t_prev is not None
        else None
    )
    out_payload["note"] = (
        "单位：亿元（按各ETF当日收盘价由份额折算：亿份 × 收盘价 = 亿元）。"
        "本页面仅统计固定代码白名单中的35只指定宽基ETF。"
        "缺失净值或缺失披露日不以前值填充。"
    )
    return out_payload


def sync_data(force_full: bool = False) -> dict:
    """增量同步五大宽基指数日线、上交所8只宽基ETF份额及全市场35只ETF折算规模（亿元）"""
    os.makedirs(DATA_DIR, exist_ok=True)
    cache = {}
    if os.path.exists(CACHE_PATH) and not force_full:
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                cache = json.load(f)
        except Exception:
            cache = {}

    etf_by_date = cache.get("etf_by_date", {})
    indices_data = cache.get("indices_data", {})
    etf_prices_data = cache.get("etf_prices_data", {})

    session = requests.Session()
    all_dates_set = set()

    for item in INDICES_META:
        code = item["code"]
        price_map = _fetch_kline_close_map(session, code, START_DATE)
        series = indices_data.get(code, {})
        for d, close_val in price_map.items():
            series[d] = round(close_val, 2)
            all_dates_set.add(d)
        indices_data[code] = series

    # 同步8只核心ETF行情价格
    for etf_meta in ETFS_META:
        c = etf_meta["code"]
        px_map = _fetch_kline_close_map(session, _to_sec_symbol(c), START_DATE)
        existing_px = etf_prices_data.get(c, {})
        existing_px.update(px_map)
        etf_prices_data[c] = existing_px

    all_trading_dates = sorted(list(all_dates_set))
    target_codes = {e["code"] for e in ETFS_META}

    recent_recheck = set(all_trading_dates[-3:]) if all_trading_dates else set()
    missing_dates = [
        d for d in all_trading_dates
        if (d not in etf_by_date)
        or (len(etf_by_date.get(d, {})) < len(target_codes))
        or (d in recent_recheck)
    ]

    sse_url = "https://query.sse.com.cn/commonQuery.do"
    headers = {"Referer": "https://www.sse.com.cn/", "User-Agent": "Mozilla/5.0"}

    def fetch_sse_date(d: str):
        params = {
            "isPagination": "true",
            "pageHelp.pageSize": "10000",
            "pageHelp.pageNo": "1",
            "sqlId": "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L",
            "STAT_DATE": d,
        }
        for attempt in range(3):
            try:
                r = requests.get(sse_url, params=params, headers=headers, timeout=12)
                js = r.json()
                out = {}
                for x in js.get("result", []):
                    sc = x.get("SEC_CODE")
                    if sc in target_codes:
                        shares_yi = float(x["TOT_VOL"]) / 10000.0
                        px = etf_prices_data.get(sc, {}).get(d)
                        out[sc] = round(shares_yi * px, 4) if px is not None else round(shares_yi, 4)
                return d, out
            except Exception:
                time.sleep(0.25 * (attempt + 1))
        return d, {}

    if missing_dates:
        print(f"[Sync] 正在从上交所同步 {len(missing_dates)} 个交易日的ETF规模数据...")
        with ThreadPoolExecutor(max_workers=16) as ex:
            for d, res in ex.map(fetch_sse_date, missing_dates):
                if res:
                    etf_by_date[d] = res

    valid_dates = sorted([d for d in etf_by_date.keys() if etf_by_date[d]])

    # 同步35只全市场白名单ETF份额并获取全白名单收盘价，统一折算为亿元
    broad_cache = sync_broad_market(force_full=force_full)
    broad_raw_payload = build_broad_payload(broad_cache)

    all_price_maps = {c: dict(etf_prices_data.get(c, {})) for c in target_codes}

    def fetch_broad_px(c: str):
        return c, _fetch_kline_close_map(session, _to_sec_symbol(c), START_DATE)

    extra_codes = [c for c in sorted(ALL_CODES) if c not in all_price_maps]
    if extra_codes:
        with ThreadPoolExecutor(max_workers=10) as ex:
            for c, px_map in ex.map(fetch_broad_px, extra_codes):
                all_price_maps[c] = px_map

    broad_payload = _convert_broad_payload_to_yi_yuan(broad_raw_payload, all_price_maps)

    new_cache = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "start_date": valid_dates[0] if valid_dates else START_DATE,
        "end_date": valid_dates[-1] if valid_dates else "",
        "total_days": len(valid_dates),
        "indices_meta": INDICES_META,
        "etfs_meta": ETFS_META,
        "dates": valid_dates,
        "indices_data": indices_data,
        "etf_prices_data": etf_prices_data,
        "etf_by_date": {d: etf_by_date[d] for d in valid_dates},
        "broad_market": broad_payload,
    }

    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(new_cache, f, ensure_ascii=False, indent=2)

    print(
        f"[Sync] 数据同步完成: {new_cache['start_date']} ~ {new_cache['end_date']} "
        f"(共 {new_cache['total_days']} 个交易日，口径：亿元)"
    )
    return new_cache


def build_frontend_payload(cache: dict) -> dict:
    """将缓存数据整理为前端高密度列式JSON结构（统一口径：亿元）"""
    dates = cache["dates"]
    indices_series = {}
    for idx_meta in INDICES_META:
        c = idx_meta["code"]
        raw_map = cache["indices_data"].get(c, {})
        vals = []
        last_val = None
        for d in dates:
            v = raw_map.get(d, last_val)
            if v is not None:
                last_val = v
            vals.append(v)
        indices_series[c] = vals

    etf_series = {}
    px_cache = cache.get("etf_prices_data", {})
    for etf_meta in ETFS_META:
        c = etf_meta["code"]
        px_map = px_cache.get(c, {})
        vals = []
        last_val = None
        for d in dates:
            day_map = cache["etf_by_date"].get(d, {})
            raw_v = day_map.get(c)
            if raw_v is not None:
                # 若缓存中已是乘以价格后的亿元，px_map[d] 存在时保持一致
                v = raw_v
                last_val = v
            else:
                v = last_val
            vals.append(round(v, 4) if v is not None else None)
        etf_series[c] = vals

    return {
        "updated_at": cache["updated_at"],
        "start_date": cache["start_date"],
        "end_date": cache["end_date"],
        "total_days": cache["total_days"],
        "dates": dates,
        "indices_meta": INDICES_META,
        "etfs_meta": ETFS_META,
        "indices_series": indices_series,
        "etf_series": etf_series,
        "broad_market": cache.get("broad_market", {}),
    }


HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>宽基ETF规模追踪 · 市场底背离监测系统 (QTYX-V3.6.6 亿元版)</title>
  <script src="./echarts.min.js"></script>
  <script>
    if (typeof window.echarts === 'undefined') {
      document.write('<script src="https://registry.npmmirror.com/echarts/5.5.1/files/dist/echarts.min.js"><\/script>');
    }
  </script>
  <style>
    :root {
      --bg-page: #f4f6fa;
      --bg-card: #ffffff;
      --bg-subtle: #f8fafd;
      --bg-hover: #eef3fb;
      --text-primary: #141b2d;
      --text-secondary: #4a5568;
      --text-muted: #7b879c;
      --border-color: #e2e8f0;
      --border-strong: #cbd5e1;
      --accent: #1d4ed8;
      --accent-soft: rgba(29, 78, 216, 0.08);
      --up-red: #dc2626;
      --up-red-bg: rgba(220, 38, 38, 0.09);
      --down-green: #16a34a;
      --down-green-bg: rgba(22, 163, 74, 0.09);
      --warn-amber: #d97706;
      --warn-amber-bg: rgba(217, 119, 6, 0.11);
      --purple-fire: #7c3aed;
      --purple-fire-bg: rgba(124, 58, 237, 0.10);
      --shadow-sm: 0 1px 3px rgba(15, 23, 42, 0.05), 0 1px 2px rgba(15, 23, 42, 0.03);
      --shadow-md: 0 8px 24px rgba(15, 23, 42, 0.06);
      --radius-lg: 14px;
      --radius-md: 10px;
      --font-sans: "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", -apple-system, BlinkMacSystemFont, sans-serif;
      --font-mono: "JetBrains Mono", "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
    }

    [data-theme="dark"] {
      --bg-page: #0b1120;
      --bg-card: #111827;
      --bg-subtle: #1e293b;
      --bg-hover: #263449;
      --text-primary: #f1f5f9;
      --text-secondary: #cbd5e1;
      --text-muted: #94a3b8;
      --border-color: #1f2937;
      --border-strong: #334155;
      --accent: #3b82f6;
      --accent-soft: rgba(59, 130, 246, 0.15);
      --up-red: #f87171;
      --up-red-bg: rgba(248, 113, 113, 0.14);
      --down-green: #4ade80;
      --down-green-bg: rgba(74, 222, 128, 0.14);
      --warn-amber: #fbbf24;
      --warn-amber-bg: rgba(251, 191, 36, 0.14);
      --purple-fire: #a78bfa;
      --purple-fire-bg: rgba(167, 139, 250, 0.15);
      --shadow-sm: 0 2px 6px rgba(0, 0, 0, 0.3);
      --shadow-md: 0 10px 30px rgba(0, 0, 0, 0.45);
    }

    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

    body {
      font-family: var(--font-sans);
      background-color: var(--bg-page);
      color: var(--text-primary);
      line-height: 1.5;
      padding-bottom: 48px;
      transition: background-color 0.2s ease, color 0.2s ease;
    }

    .top-header {
      background: var(--bg-card);
      border-bottom: 1px solid var(--border-color);
      position: sticky;
      top: 0;
      z-index: 100;
      box-shadow: var(--shadow-sm);
    }
    .header-inner {
      max-width: 1560px;
      margin: 0 auto;
      padding: 14px 24px;
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
    }
    .brand-group {
      display: flex;
      align-items: center;
      gap: 14px;
    }
    .brand-badge {
      background: linear-gradient(135deg, #1e40af, #2563eb);
      color: #fff;
      font-weight: 700;
      font-size: 13px;
      padding: 6px 11px;
      border-radius: 8px;
      letter-spacing: 0.5px;
      box-shadow: 0 2px 8px rgba(37, 99, 235, 0.28);
    }
    .brand-title h1 {
      font-size: 19px;
      font-weight: 700;
      letter-spacing: -0.2px;
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .brand-subtitle {
      font-size: 12.5px;
      color: var(--text-muted);
      margin-top: 2px;
    }

    .nav-actions {
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 8px;
    }
    .nav-jump {
      display: flex;
      background: var(--bg-subtle);
      padding: 3px;
      border-radius: 9px;
      border: 1px solid var(--border-color);
      margin-right: 6px;
    }
    .nav-jump a {
      text-decoration: none;
      color: var(--text-secondary);
      font-size: 12.5px;
      font-weight: 600;
      padding: 5px 11px;
      border-radius: 6px;
      transition: all 0.15s;
    }
    .nav-jump a:hover {
      color: var(--accent);
      background: var(--bg-card);
    }

    .btn {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-size: 12.5px;
      font-weight: 600;
      padding: 7px 13px;
      border-radius: 8px;
      border: 1px solid var(--border-color);
      background: var(--bg-card);
      color: var(--text-primary);
      cursor: pointer;
      transition: all 0.15s ease;
      font-family: inherit;
    }
    .btn:hover {
      background: var(--bg-hover);
      border-color: var(--border-strong);
    }
    .btn-primary {
      background: var(--accent);
      color: #ffffff;
      border-color: var(--accent);
    }
    .btn-primary:hover {
      background: #1e40af;
      border-color: #1e40af;
    }

    .container {
      max-width: 1560px;
      margin: 0 auto;
      padding: 18px 24px 0;
    }

    .guide-banner {
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-left: 4px solid var(--accent);
      border-radius: var(--radius-md);
      padding: 12px 18px;
      margin-bottom: 18px;
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      box-shadow: var(--shadow-sm);
    }
    .guide-rules {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 16px;
      font-size: 13px;
      color: var(--text-secondary);
    }
    .rule-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      background: var(--bg-subtle);
      padding: 4px 10px;
      border-radius: 6px;
      border: 1px solid var(--border-color);
      font-weight: 500;
    }
    .guide-warn {
      font-size: 12px;
      color: var(--warn-amber);
      background: var(--warn-amber-bg);
      padding: 4px 10px;
      border-radius: 6px;
      font-weight: 600;
    }

    .etf-grid {
      display: grid;
      grid-template-columns: repeat(8, 1fr);
      gap: 12px;
      margin-bottom: 20px;
    }
    @media (max-width: 1400px) {
      .etf-grid {
        grid-template-columns: repeat(4, 1fr);
      }
    }
    @media (max-width: 768px) {
      .etf-grid {
        grid-template-columns: repeat(2, 1fr);
      }
    }
    .etf-card {
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-md);
      padding: 12px 13px;
      box-shadow: var(--shadow-sm);
      cursor: pointer;
      transition: transform 0.15s ease, box-shadow 0.15s ease, border-color 0.15s ease;
    }
    .etf-card:hover {
      transform: translateY(-2px);
      box-shadow: var(--shadow-md);
      border-color: var(--accent);
    }
    .etf-card-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 6px;
    }
    .etf-card-name {
      font-weight: 700;
      font-size: 13px;
      color: var(--text-primary);
      display: flex;
      align-items: center;
      gap: 5px;
      white-space: nowrap;
    }
    .color-dot {
      width: 9px;
      height: 9px;
      border-radius: 50%;
      display: inline-block;
      flex-shrink: 0;
    }
    .etf-card-code {
      font-family: var(--font-mono);
      font-size: 11.5px;
      color: var(--text-muted);
      background: var(--bg-subtle);
      padding: 1px 6px;
      border-radius: 4px;
    }
    .etf-card-share {
      font-family: var(--font-mono);
      font-size: 19px;
      font-weight: 700;
      margin: 4px 0 6px;
      letter-spacing: -0.5px;
    }
    .etf-card-share span {
      font-size: 11.5px;
      font-weight: 500;
      color: var(--text-muted);
      margin-left: 3px;
    }
    .etf-card-metrics {
      display: flex;
      justify-content: space-between;
      font-size: 11.5px;
      color: var(--text-secondary);
      margin-bottom: 7px;
      font-family: var(--font-mono);
    }
    .pct-bar-wrap {
      height: 5px;
      background: var(--bg-subtle);
      border-radius: 999px;
      overflow: hidden;
      margin-bottom: 5px;
    }
    .pct-bar-fill {
      height: 100%;
      border-radius: 999px;
      transition: width 0.3s ease;
    }
    .etf-card-foot {
      display: flex;
      align-items: center;
      justify-content: space-between;
      font-size: 11px;
      color: var(--text-muted);
    }

    .section-card {
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-lg);
      padding: 20px 22px;
      margin-bottom: 22px;
      box-shadow: var(--shadow-sm);
    }
    .section-header {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding-bottom: 14px;
      border-bottom: 1px solid var(--border-color);
      margin-bottom: 16px;
    }
    .section-title-group {
      display: flex;
      flex-direction: column;
      gap: 3px;
    }
    .section-title {
      font-size: 17px;
      font-weight: 700;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .section-num {
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 12px;
      font-weight: 700;
      padding: 2px 8px;
      border-radius: 6px;
    }
    .section-desc {
      font-size: 12.5px;
      color: var(--text-muted);
    }

    .toolbar-row {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 12px;
    }
    .pill-group {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 6px;
    }
    .pill-label {
      font-size: 12px;
      font-weight: 600;
      color: var(--text-muted);
      margin-right: 4px;
    }
    .pill-btn {
      font-size: 12px;
      font-weight: 600;
      padding: 5px 11px;
      border-radius: 7px;
      border: 1px solid var(--border-color);
      background: var(--bg-subtle);
      color: var(--text-secondary);
      cursor: pointer;
      transition: all 0.15s ease;
    }
    .pill-btn:hover {
      border-color: var(--accent);
      color: var(--accent);
    }
    .pill-btn.active {
      background: var(--accent);
      color: #ffffff;
      border-color: var(--accent);
    }

    .checkbox-label {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-size: 12.5px;
      font-weight: 600;
      color: var(--text-secondary);
      cursor: pointer;
      user-select: none;
      background: var(--bg-subtle);
      padding: 5px 11px;
      border-radius: 7px;
      border: 1px solid var(--border-color);
    }

    .chart-box-overview {
      width: 100%;
      height: 580px;
    }
    .chart-box-single {
      width: 100%;
      height: 540px;
    }
    .stacked-charts-grid {
      display: flex;
      flex-direction: column;
      gap: 24px;
    }
    .stacked-chart-item {
      border: 1px solid var(--border-color);
      border-radius: var(--radius-md);
      padding: 14px 16px;
      background: var(--bg-subtle);
    }

    .style-tabs {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .style-tab {
      display: inline-flex;
      align-items: center;
      gap: 7px;
      padding: 7px 14px;
      border-radius: 8px;
      font-size: 13px;
      font-weight: 700;
      border: 1px solid var(--border-color);
      background: var(--bg-subtle);
      color: var(--text-secondary);
      cursor: pointer;
      transition: all 0.15s;
    }
    .style-tab:hover {
      border-color: var(--accent);
      color: var(--text-primary);
    }
    .style-tab.active {
      background: var(--accent);
      color: #fff;
      border-color: var(--accent);
      box-shadow: 0 2px 8px rgba(29, 78, 216, 0.25);
    }

    .insight-strip {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 10px;
      margin-bottom: 14px;
      padding: 11px 14px;
      background: var(--bg-subtle);
      border-radius: var(--radius-md);
      border: 1px solid var(--border-color);
    }
    .insight-item {
      display: flex;
      flex-direction: column;
      gap: 2px;
    }
    .insight-label {
      font-size: 11.5px;
      color: var(--text-muted);
    }
    .insight-val {
      font-size: 14.5px;
      font-weight: 700;
      font-family: var(--font-mono);
    }

    .mono-cell {
      font-family: var(--font-mono);
    }
    .val-pos {
      color: var(--up-red);
      font-weight: 700;
    }
    .val-neg {
      color: var(--down-green);
      font-weight: 700;
    }

    .broad-market-panel {
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-lg);
      padding: 20px 22px;
      margin: 0 0 22px;
      box-shadow: var(--shadow-sm);
    }
    .broad-market-head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 12px;
      margin-bottom: 14px;
    }
    .broad-market-head h2 {
      font-size: 18px;
      font-weight: 750;
    }
    .broad-market-note {
      color: var(--text-muted);
      font-size: 12.5px;
      margin-top: 4px;
    }
    .broad-kpis {
      display: grid;
      grid-template-columns: repeat(4, minmax(140px, 1fr));
      gap: 10px;
      margin: 14px 0 16px;
    }
    .broad-kpi {
      border: 1px solid var(--border-color);
      border-radius: 10px;
      padding: 12px 14px;
      background: var(--bg-subtle);
    }
    .broad-kpi .k-label {
      color: var(--text-muted);
      font-size: 12px;
    }
    .broad-kpi .k-value {
      font: 700 20px var(--font-mono);
      margin-top: 5px;
    }
    .broad-tabs-bar {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 12px;
      padding: 10px 12px;
      background: var(--bg-subtle);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-md);
    }
    .broad-chart-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 8px;
      margin: 6px 0 4px;
    }
    .broad-chart-title {
      font-size: 14.5px;
      font-weight: 700;
      color: var(--text-primary);
    }
    .broad-chart-unit {
      font-size: 12px;
      color: var(--text-muted);
    }
    .broad-table-wrap {
      overflow: auto;
      margin-top: 14px;
      border: 1px solid var(--border-color);
      border-radius: var(--radius-md);
    }
    .broad-table {
      border-collapse: collapse;
      width: 100%;
      min-width: 720px;
      font-size: 13px;
    }
    .broad-table th, .broad-table td {
      padding: 10px 12px;
      border-bottom: 1px solid var(--border-color);
      text-align: right;
      white-space: nowrap;
    }
    .broad-table th:first-child, .broad-table td:first-child {
      text-align: left;
    }
    .broad-table th {
      color: var(--text-muted);
      font-weight: 650;
      background: var(--bg-subtle);
    }
    .broad-table tr.active-series-row td {
      background: var(--accent-soft);
    }
    .broad-expand {
      border: 0;
      background: none;
      color: var(--accent);
      cursor: pointer;
      font: inherit;
      font-weight: 700;
    }
    .broad-detail {
      display: none;
      background: var(--bg-subtle);
    }
    .broad-detail.open {
      display: table-row;
    }
    .broad-detail td {
      text-align: left;
      white-space: normal;
      padding: 10px 14px;
    }
    .broad-chip {
      display: inline-block;
      border: 1px solid var(--border-color);
      background: var(--bg-card);
      border-radius: 999px;
      padding: 2px 9px;
      margin: 2px 6px 2px 0;
      font-size: 11.5px;
      color: var(--text-secondary);
    }
    .broad-status {
      font-size: 12px;
      color: var(--text-muted);
      margin-top: 10px;
      line-height: 1.6;
    }
    @media (max-width: 850px) {
      .broad-kpis {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }

    .toast {
      position: fixed;
      bottom: 24px;
      right: 24px;
      background: #1e293b;
      color: #fff;
      padding: 10px 16px;
      border-radius: 9px;
      font-size: 13px;
      font-weight: 600;
      box-shadow: 0 10px 25px rgba(0, 0, 0, 0.25);
      z-index: 1200;
      display: none;
    }
  </style>
</head>
<body>
  <header class="top-header">
    <div class="header-inner">
      <div class="brand-group">
        <span class="brand-badge">QTYX V3.6.6</span>
        <div class="brand-title">
          <h1>宽基 ETF 规模追踪 · 市场底背离监测系统</h1>
          <div class="brand-subtitle" id="metaSubtitle">加载中...</div>
        </div>
      </div>

      <div class="nav-actions">
        <nav class="nav-jump">
          <a href="#sec-broad-market">七大宽基系列汇总</a>
          <a href="#sec-overview">1. 全市场总览图</a>
          <a href="#sec-single">2. 单ETF资金共振</a>
        </nav>
        <button class="btn" id="btnThemeToggle" title="切换研报浅色/量化深色主题">🌓 切换主题</button>
        <button class="btn btn-primary" id="btnSyncLatest" title="数据由 GitHub Actions 每个交易日自动更新">⏱ 每日自动更新</button>
      </div>
    </div>
  </header>

  <main class="container">
    <div class="guide-banner">
      <div class="guide-rules">
        <span class="rule-pill">📏 <strong>看图统一规则</strong>：虚线 = 宽基指数走势，实线 = ETF 规模（起点归一到 100）</span>
        <span class="rule-pill">📉 <strong>同步创新低</strong>：指数跌 + 规模跌 → 资金出逃，抛压未结束</span>
        <span class="rule-pill">📈 <strong>资金底背离</strong>：指数持续回落 + 规模止跌拐头向上 → 大资金在低位主动申购承接</span>
      </div>
      <div class="guide-warn">⚠️ 提示：资金底先行 ≠ 价格底立刻到来，二者常有磨底时间差，仅供宏观观测辅助</div>
    </div>

    <section class="broad-market-panel" id="sec-broad-market">
      <div class="broad-market-head">
        <div>
          <h2>沪深两市宽基 ETF 全系列规模汇总</h2>
          <div class="broad-market-note">上证50 · 沪深300 · 中证500 · 中证1000 · 中证A500 · 科创宽基 · 创业板（创业板指+创业板50）</div>
        </div>
        <span class="rule-pill">按基金代码去重 · 单位：亿元</span>
      </div>

      <div class="broad-tabs-bar">
        <div class="style-tabs" id="broadSeriesTabs"></div>
        <div class="pill-group" id="broadRangeGroup">
          <span class="pill-label">区间：</span>
          <button class="pill-btn active" data-range="ALL">全部</button>
          <button class="pill-btn" data-range="YTD">2026年以来</button>
          <button class="pill-btn" data-range="6M">近6月</button>
          <button class="pill-btn" data-range="3M">近3月</button>
          <button class="pill-btn" data-range="1M">近1月</button>
        </div>
      </div>

      <div class="broad-kpis" id="broadMarketKpis"></div>

      <div class="broad-chart-header">
        <div class="broad-chart-title" id="broadChartTitle">七大系列合计总规模走势 &amp; 每日规模变化</div>
        <div class="broad-chart-unit">上图：累计总规模（亿元） · 下图：较前一有效披露日增减（亿元，红增绿减）</div>
      </div>
      <div id="broadMarketChart" style="height:440px;width:100%;"></div>

      <div class="broad-table-wrap">
        <table class="broad-table">
          <thead>
            <tr>
              <th>系列（点击行可直接切换上方图表）</th>
              <th>纳入ETF数量</th>
              <th>最新总规模（亿元）</th>
              <th>较前一披露日变化（亿元）</th>
              <th>明细</th>
            </tr>
          </thead>
          <tbody id="broadMarketRows"></tbody>
        </table>
      </div>
      <div class="broad-status" id="broadMarketStatus"></div>
    </section>

    <section class="etf-grid" id="etfSnapshotGrid"></section>

    <section class="section-card" id="sec-overview">
      <div class="section-header">
        <div class="section-title-group">
          <div class="section-title">
            <span class="section-num">板块一</span>
            主力资金动向：全市场宽基指数 &amp; ETF 规模归一对比
          </div>
          <div class="section-desc">
            五大宽基指数（虚线）+ 八大旗舰宽基 ETF 规模（实线，单位：亿元）横向同框对比
          </div>
        </div>
        <div class="pill-group" id="overviewPeriodGroup">
          <span class="pill-label">区间快捷缩放：</span>
          <button class="pill-btn active" data-range="ALL">全部区间</button>
          <button class="pill-btn" data-range="YTD">2026年以来</button>
          <button class="pill-btn" data-range="6M">近6个月</button>
          <button class="pill-btn" data-range="3M">近3个月</button>
          <button class="pill-btn" data-range="1M">近1个月</button>
        </div>
      </div>

      <div class="toolbar-row">
        <div class="pill-group" id="overviewPresetGroup">
          <span class="pill-label">线条组合预设：</span>
          <button class="pill-btn active" data-preset="ALL">📊 全部显示 (5指数+8ETF)</button>
          <button class="pill-btn" data-preset="ONLY_ETF">📈 仅看8只ETF规模 (实线)</button>
          <button class="pill-btn" data-preset="ONLY_IDX">📉 仅看5大宽基指数 (虚线)</button>
          <button class="pill-btn" data-preset="STAR50">🚀 科创50底背离典型</button>
          <button class="pill-btn" data-preset="LARGE_CAP">🏛️ 大盘宽基 (沪深300+上证50)</button>
          <button class="pill-btn" data-preset="SMID_CAP">⚡ 中小盘成长 (中证500+1000)</button>
        </div>
        <label class="checkbox-label" title="开启后，拖动底部时间滑块时，以当前视野左端起点动态重新归一为 100">
          <input type="checkbox" id="chkDynamicRebase" />
          缩放区间起点动态重算基准=100
        </label>
      </div>

      <div id="chartOverview" class="chart-box-overview"></div>
    </section>

    <section class="section-card" id="sec-single">
      <div class="section-header">
        <div class="section-title-group">
          <div class="section-title">
            <span class="section-num">板块二</span>
            单宽基 / 单 ETF 资金共振独立观测图
          </div>
          <div class="section-desc">
            上图：左轴指数点位 + 右轴 ETF 规模（亿元）；下图：每日规模变化柱状图（红柱增加 / 绿柱减少，单位：亿元）
          </div>
        </div>
        <div class="pill-group">
          <button class="btn" id="btnToggleExpandMode">📜 切换为：五大宽基全部展开视图</button>
        </div>
      </div>

      <div class="toolbar-row" id="singleTabToolbar">
        <div class="style-tabs" id="styleTabsContainer"></div>
        <div class="pill-group" id="singleEtfSubSelect"></div>
      </div>

      <div class="insight-strip" id="singleInsightStrip"></div>
      <div id="chartSingle" class="chart-box-single"></div>
      <div id="stackedChartsContainer" class="stacked-charts-grid" style="display: none;"></div>
    </section>
  </main>

  <div class="toast" id="toastMsg"></div>

  <script>
    window.ETF_TRACKER_DATA = __PAYLOAD_JSON__;
  </script>
  <script>
    (function () {
      const DATA = window.ETF_TRACKER_DATA;
      const dates = DATA.dates;
      const N = dates.length;

      function getThemeColors() {
        const isDark = document.documentElement.getAttribute("data-theme") === "dark";
        return {
          isDark,
          text: isDark ? "#e2e8f0" : "#1e293b",
          subText: isDark ? "#94a3b8" : "#64748b",
          gridLine: isDark ? "rgba(148, 163, 184, 0.13)" : "#f1f5f9",
          axisLine: isDark ? "#334155" : "#cbd5e1",
          tooltipBg: isDark ? "rgba(15, 23, 42, 0.94)" : "rgba(255, 255, 255, 0.96)",
        };
      }

      let activeBroadSeriesId = "TOTAL";
      let broadZoomStart = 0;
      let broadZoomEnd = 100;
      let broadChartInstance = null;

      function computeDiffFromPrevValid(series) {
        const diff = new Array(series.length).fill(null);
        let prevValid = null;
        for (let i = 0; i < series.length; i++) {
          const cur = series[i];
          if (cur == null) {
            diff[i] = null;
          } else if (prevValid == null) {
            diff[i] = 0;
            prevValid = cur;
          } else {
            diff[i] = +(cur - prevValid).toFixed(4);
            prevValid = cur;
          }
        }
        return diff;
      }

      function renderBroadMarket() {
        const B = DATA.broad_market || {};
        const groups = B.groups || [];
        const bDates = B.dates || [];
        const fmt = (v) =>
          v == null ? "—" : Number(v).toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        const sign = (v) => (v == null ? "—" : (v > 0 ? "+" : "") + fmt(v));

        const tabsEl = document.getElementById("broadSeriesTabs");
        const tabItems = [
          { id: "TOTAL", name: "七大系列合计", color: "#1d4ed8", count: groups.reduce((a, g) => a + (g.count || 0), 0) },
          ...groups.map((g) => ({ id: g.id, name: g.name, color: g.color || "#1d4ed8", count: g.count || 0 })),
        ];

        tabsEl.innerHTML = tabItems
          .map(
            (t) => `
            <button class="style-tab ${activeBroadSeriesId === t.id ? "active" : ""}" data-series-id="${t.id}">
              <span class="color-dot" style="background:${activeBroadSeriesId === t.id ? "#ffffff" : t.color}"></span>
              <span>${t.name}</span>
              <span style="font-size:11px;opacity:0.82;font-weight:500;">(${t.count}只)</span>
            </button>
          `
          )
          .join("");

        tabsEl.querySelectorAll(".style-tab").forEach((btn) => {
          btn.addEventListener("click", () => {
            activeBroadSeriesId = btn.getAttribute("data-series-id");
            renderBroadMarket();
          });
        });

        const totalSeries = B.total_series || [];
        const selectedGroup = groups.find((g) => g.id === activeBroadSeriesId);
        const activeSeries = selectedGroup ? selectedGroup.series || [] : totalSeries;
        const activeDiff = computeDiffFromPrevValid(activeSeries);
        const activeName = selectedGroup ? selectedGroup.name : "七大系列合计";
        const activeColor = selectedGroup ? selectedGroup.color || "#1d4ed8" : "#1d4ed8";

        let latestVal = null;
        let prevVal = null;
        let latestDate = B.end_date || "—";
        for (let i = activeSeries.length - 1; i >= 0; i--) {
          if (activeSeries[i] != null) {
            if (latestVal == null) {
              latestVal = activeSeries[i];
              latestDate = bDates[i] || latestDate;
            } else if (prevVal == null) {
              prevVal = activeSeries[i];
              break;
            }
          }
        }
        const latestChange = latestVal != null && prevVal != null ? +(latestVal - prevVal).toFixed(4) : null;
        const activeCount = selectedGroup
          ? selectedGroup.count || 0
          : groups.reduce((a, g) => a + (g.count || 0), 0);

        const kpis = document.getElementById("broadMarketKpis");
        kpis.innerHTML = `
          <div class="broad-kpi">
            <div class="k-label">${activeName} · 最新总规模</div>
            <div class="k-value">${fmt(latestVal)} <small>亿元</small></div>
          </div>
          <div class="broad-kpi">
            <div class="k-label">${activeName} · 较前一披露日变化</div>
            <div class="k-value" style="color:${latestChange > 0 ? "var(--up-red)" : latestChange < 0 ? "var(--down-green)" : "inherit"}">${sign(latestChange)} <small>亿元</small></div>
          </div>
          <div class="broad-kpi">
            <div class="k-label">${activeName} · 纳入ETF数量</div>
            <div class="k-value">${activeCount} <small>只</small></div>
          </div>
          <div class="broad-kpi">
            <div class="k-label">规模数据区间（最新披露：${latestDate}）</div>
            <div class="k-value" style="font-size:14px">${B.start_date || "—"} 至 ${B.end_date || "—"}</div>
          </div>
        `;

        document.getElementById("broadChartTitle").textContent = `${activeName}总规模走势 & 每日规模变化`;

        const chartDom = document.getElementById("broadMarketChart");
        if (window.echarts && chartDom && bDates.length) {
          if (!broadChartInstance) {
            broadChartInstance = echarts.init(chartDom);
            broadChartInstance.on("datazoom", function () {
              const opt = broadChartInstance.getOption();
              if (opt.dataZoom && opt.dataZoom[0]) {
                broadZoomStart = opt.dataZoom[0].start;
                broadZoomEnd = opt.dataZoom[0].end;
              }
            });
          }
          const tc = getThemeColors();
          const option = {
            backgroundColor: "transparent",
            tooltip: {
              trigger: "axis",
              axisPointer: { type: "cross", link: [{ xAxisIndex: "all" }] },
              backgroundColor: tc.tooltipBg,
              borderColor: tc.axisLine,
              textStyle: { color: tc.text, fontSize: 12 },
              formatter: function (params) {
                if (!params || !params.length) return "";
                const idx = params[0].dataIndex;
                const dt = bDates[idx];
                const sh = activeSeries[idx];
                const chg = activeDiff[idx];
                const chgColor = chg > 0 ? "#dc2626" : chg < 0 ? "#16a34a" : tc.text;
                return `
                  <div style="font-weight:700;margin-bottom:6px;border-bottom:1px solid ${tc.axisLine};padding-bottom:4px;">
                    📅 ${dt} · ${activeName}
                  </div>
                  <div style="display:flex;justify-content:space-between;gap:18px;line-height:1.7;">
                    <span>🏦 系列总规模：</span>
                    <strong style="font-family:monospace;">${sh != null ? fmt(sh) : "—"} 亿元</strong>
                  </div>
                  <div style="display:flex;justify-content:space-between;gap:18px;line-height:1.7;">
                    <span>📊 较前日增减：</span>
                    <strong style="font-family:monospace;color:${chgColor};">${chg != null ? sign(chg) : "—"} 亿元</strong>
                  </div>
                `;
              },
            },
            legend: { top: 2, left: "center", textStyle: { color: tc.text, fontSize: 12 } },
            axisPointer: { link: [{ xAxisIndex: "all" }] },
            grid: [
              { left: 68, right: 28, top: 38, height: "44%" },
              { left: 68, right: 28, top: "58%", height: "27%" },
            ],
            xAxis: [
              {
                type: "category",
                data: bDates,
                gridIndex: 0,
                boundaryGap: false,
                axisLine: { lineStyle: { color: tc.axisLine } },
                axisLabel: { show: false },
                axisTick: { show: false },
                splitLine: { show: true, lineStyle: { color: tc.gridLine } },
              },
              {
                type: "category",
                data: bDates,
                gridIndex: 1,
                boundaryGap: true,
                axisLine: { lineStyle: { color: tc.axisLine } },
                axisLabel: { color: tc.subText, fontSize: 11, hideOverlap: true },
                splitLine: { show: true, lineStyle: { color: tc.gridLine } },
              },
            ],
            yAxis: [
              {
                type: "value",
                name: "总规模(亿元)",
                gridIndex: 0,
                scale: true,
                nameTextStyle: { color: tc.subText, fontSize: 11.5 },
                axisLabel: { color: tc.subText, fontSize: 11 },
                splitLine: { lineStyle: { color: tc.gridLine } },
              },
              {
                type: "value",
                name: "日变化(亿元)",
                gridIndex: 1,
                nameTextStyle: { color: tc.subText, fontSize: 11 },
                axisLabel: { color: tc.subText, fontSize: 10.5 },
                splitLine: { lineStyle: { color: tc.gridLine } },
              },
            ],
            dataZoom: [
              { type: "inside", xAxisIndex: [0, 1], start: broadZoomStart, end: broadZoomEnd },
              {
                type: "slider",
                xAxisIndex: [0, 1],
                bottom: 4,
                height: 22,
                start: broadZoomStart,
                end: broadZoomEnd,
                borderColor: tc.axisLine,
                textStyle: { color: tc.subText },
              },
            ],
            series: [
              {
                name: `${activeName}总规模`,
                type: "line",
                xAxisIndex: 0,
                yAxisIndex: 0,
                showSymbol: false,
                smooth: false,
                connectNulls: false,
                data: activeSeries,
                lineStyle: { width: 2.4, color: activeColor },
                itemStyle: { color: activeColor },
                areaStyle: { color: activeColor, opacity: 0.08 },
              },
              {
                name: `${activeName}每日规模变化`,
                type: "bar",
                xAxisIndex: 1,
                yAxisIndex: 1,
                data: activeDiff,
                itemStyle: {
                  color: function (p) {
                    if (p.value == null) return "transparent";
                    return p.value >= 0 ? "#ef4444" : "#22c55e";
                  },
                },
              },
            ],
          };
          broadChartInstance.setOption(option, true);
        } else if (chartDom) {
          chartDom.innerHTML =
            '<div style="padding:40px;text-align:center;color:var(--text-muted)">等待首次全市场数据采集</div>';
        }

        const tbody = document.getElementById("broadMarketRows");
        const totalLatest = totalSeries.filter((v) => v != null).slice(-1)[0] ?? B.total_latest;
        const totalPrev = totalSeries.filter((v) => v != null).slice(-2, -1)[0];
        const totalChange =
          totalLatest != null && totalPrev != null ? +(totalLatest - totalPrev).toFixed(4) : B.total_change1;

        tbody.innerHTML =
          groups
            .map((g, i) => {
              const gSeries = g.series || [];
              const gLatest = gSeries.filter((v) => v != null).slice(-1)[0] ?? g.latest;
              const gPrev = gSeries.filter((v) => v != null).slice(-2, -1)[0];
              const gChg = gLatest != null && gPrev != null ? +(gLatest - gPrev).toFixed(4) : g.change1;

              const detail =
                (g.details || [])
                  .map(
                    (x) =>
                      `<span class="broad-chip">${x.code} ${x.name} · ${fmt(x.amount)}亿元 · ${x.exchange}</span>`
                  )
                  .join("") || "暂无可用明细";

              const isSelected = activeBroadSeriesId === g.id;
              return `
                <tr class="${isSelected ? "active-series-row" : ""}" data-select-series="${g.id}" style="cursor:pointer;">
                  <td>
                    <span class="color-dot" style="background:${g.color || "#1d4ed8"};margin-right:6px;"></span>
                    <strong>${g.name}</strong>
                  </td>
                  <td class="mono-cell">${g.count || 0}</td>
                  <td class="mono-cell">${fmt(gLatest)}</td>
                  <td class="mono-cell" style="color:${gChg > 0 ? "var(--up-red)" : gChg < 0 ? "var(--down-green)" : "inherit"}">${sign(gChg)}</td>
                  <td><button class="broad-expand" data-expand="${i}">展开 ${g.count || 0} 只ETF ▾</button></td>
                </tr>
                <tr class="broad-detail" id="broadDetail${i}">
                  <td colspan="5">${detail}</td>
                </tr>
              `;
            })
            .join("") +
          `
            <tr class="${activeBroadSeriesId === "TOTAL" ? "active-series-row" : ""}" data-select-series="TOTAL" style="cursor:pointer;">
              <td><strong>七大系列合计</strong></td>
              <td class="mono-cell">${groups.reduce((a, g) => a + (g.count || 0), 0)}</td>
              <td class="mono-cell"><strong>${fmt(totalLatest)}</strong></td>
              <td class="mono-cell" style="color:${totalChange > 0 ? "var(--up-red)" : totalChange < 0 ? "var(--down-green)" : "inherit"}">${sign(totalChange)}</td>
              <td>按代码去重汇总</td>
            </tr>
          `;

        tbody.querySelectorAll("[data-expand]").forEach((btn) =>
          btn.addEventListener("click", (e) => {
            e.stopPropagation();
            const row = document.getElementById("broadDetail" + btn.dataset.expand);
            row.classList.toggle("open");
            btn.textContent = row.classList.contains("open")
              ? "收起明细 ▴"
              : `展开 ${groups[Number(btn.dataset.expand)].count || 0} 只ETF ▾`;
          })
        );

        tbody.querySelectorAll("tr[data-select-series]").forEach((tr) =>
          tr.addEventListener("click", () => {
            activeBroadSeriesId = tr.getAttribute("data-select-series");
            renderBroadMarket();
          })
        );

        const status = document.getElementById("broadMarketStatus");
        const partial = B.partial_dates || [];
        status.textContent =
          `数据更新时间：${B.updated_at || "暂无"}；数据源：${B.source || "沪深交易所"}。${B.note || ""}` +
          (partial.length
            ? ` ⚠️ ${partial.length} 个自然日存在交易所接口采集失败/缺口：${partial.slice(-8).join("、")}${partial.length > 8 ? "…" : ""}`
            : " 当前采集未记录接口失败日期。") +
          ((B.errors || []).length ? ` 采集异常 ${B.errors.length} 项；缺失数据不以前值填充。` : "");
      }

      document.querySelectorAll("#broadRangeGroup .pill-btn").forEach((btn) => {
        btn.addEventListener("click", () => {
          document.querySelectorAll("#broadRangeGroup .pill-btn").forEach((b) => b.classList.remove("active"));
          btn.classList.add("active");
          const range = btn.getAttribute("data-range");
          const bDates = (DATA.broad_market && DATA.broad_market.dates) || [];
          const totalLen = bDates.length;
          let startIdx = 0;
          if (range === "1M") startIdx = Math.max(0, totalLen - 22);
          else if (range === "3M") startIdx = Math.max(0, totalLen - 65);
          else if (range === "6M") startIdx = Math.max(0, totalLen - 130);
          else if (range === "YTD") {
            const ytdIdx = bDates.findIndex((d) => d >= "2026-01-01");
            startIdx = ytdIdx >= 0 ? ytdIdx : 0;
          }
          broadZoomStart = +((startIdx / Math.max(1, totalLen - 1)) * 100).toFixed(2);
          broadZoomEnd = 100;
          renderBroadMarket();
        });
      });

      renderBroadMarket();

      const derived = {};
      function computeDerived() {
        DATA.etfs_meta.forEach((m) => {
          const code = m.code;
          const arr = DATA.etf_series[code] || [];
          const d1 = new Array(N).fill(0);
          const d5 = new Array(N).fill(0);
          const pct = new Array(N).fill(0);

          for (let i = 0; i < N; i++) {
            d1[i] = i >= 1 && arr[i] != null && arr[i - 1] != null ? +(arr[i] - arr[i - 1]).toFixed(2) : 0;
            d5[i] = i >= 5 && arr[i] != null && arr[i - 5] != null ? +(arr[i] - arr[i - 5]).toFixed(2) : 0;
          }

          const validVals = arr.filter((v) => v != null);
          const sortedVals = [...validVals].sort((a, b) => a - b);
          const totalValid = sortedVals.length || 1;
          for (let i = 0; i < N; i++) {
            const v = arr[i];
            if (v == null) {
              pct[i] = 0;
              continue;
            }
            let firstIdx = -1;
            let lastIdx = -1;
            for (let j = 0; j < totalValid; j++) {
              if (sortedVals[j] === v) {
                if (firstIdx === -1) firstIdx = j;
                lastIdx = j;
              }
            }
            const avgRank = (firstIdx + lastIdx) / 2 + 1;
            pct[i] = +((avgRank / totalValid) * 100).toFixed(1);
          }

          derived[code] = { share: arr, d1, d5, pct };
        });
      }
      computeDerived();

      document.getElementById("metaSubtitle").textContent =
        `数据区间：${DATA.start_date} 至 ${DATA.end_date}（共 ${DATA.total_days} 个交易日） · 最近同步：${DATA.updated_at} · 统一口径：亿元`;

      function renderSnapshotCards() {
        const container = document.getElementById("etfSnapshotGrid");
        const lastIdx = N - 1;

        container.innerHTML = DATA.etfs_meta
          .map((m) => {
            const c = m.code;
            const share = derived[c].share[lastIdx] || 0;
            const d1 = derived[c].d1[lastIdx] || 0;
            const d5 = derived[c].d5[lastIdx] || 0;
            const pct = derived[c].pct[lastIdx] || 0;
            const d1Str = (d1 >= 0 ? "+" : "") + d1.toFixed(2);
            const d5Str = (d5 >= 0 ? "+" : "") + d5.toFixed(2);
            const d1Cls = d1 > 0 ? "val-pos" : d1 < 0 ? "val-neg" : "";
            const d5Cls = d5 > 0 ? "val-pos" : d5 < 0 ? "val-neg" : "";

            return `
              <div class="etf-card" data-group="${m.group}" data-code="${c}" title="点击查看 ${m.name} (${c}) 独立资金共振图">
                <div class="etf-card-top">
                  <div class="etf-card-name">
                    <span class="color-dot" style="background:${m.color}"></span>
                    ${m.name}
                  </div>
                  <span class="etf-card-code">${c}</span>
                </div>
                <div class="etf-card-share">
                  ${share.toFixed(2)}<span>亿元</span>
                </div>
                <div class="etf-card-metrics">
                  <span>当日 <strong class="${d1Cls}">${d1Str}</strong></span>
                  <span>5日 <strong class="${d5Cls}">${d5Str}</strong></span>
                </div>
                <div class="pct-bar-wrap" title="历史规模分位数：${pct.toFixed(1)}%">
                  <div class="pct-bar-fill" style="width:${Math.min(100, Math.max(4, pct))}%;background:${m.color};"></div>
                </div>
                <div class="etf-card-foot">
                  <span>历史分位 <strong>${pct.toFixed(1)}%</strong></span>
                  <span>${m.group}</span>
                </div>
              </div>
            `;
          })
          .join("");

        container.querySelectorAll(".etf-card").forEach((card) => {
          card.addEventListener("click", () => {
            const grp = card.getAttribute("data-group");
            const code = card.getAttribute("data-code");
            selectSingleGroup(grp, code);
            document.getElementById("sec-single").scrollIntoView({ behavior: "smooth", block: "start" });
          });
        });
      }
      renderSnapshotCards();

      const chartOverview = echarts.init(document.getElementById("chartOverview"));
      let overviewZoomStart = 0;
      let overviewZoomEnd = 100;

      function buildNormalizedSeries(baseIdx = 0) {
        const series = [];
        DATA.indices_meta.forEach((m) => {
          const raw = DATA.indices_series[m.code] || [];
          const baseVal = raw[baseIdx] || raw.find((v) => v != null) || 1;
          const normData = raw.map((v) => (v != null ? +((v / baseVal) * 100).toFixed(2) : null));
          series.push({
            name: m.name,
            type: "line",
            data: normData,
            rawValues: raw,
            unit: "点",
            isIndex: true,
            group: m.group,
            symbol: "none",
            smooth: false,
            lineStyle: { width: 1.8, type: "dashed", color: m.color },
            itemStyle: { color: m.color },
            emphasis: { focus: "series", lineStyle: { width: 3 } },
          });
        });

        DATA.etfs_meta.forEach((m) => {
          const raw = DATA.etf_series[m.code] || [];
          const baseVal = raw[baseIdx] || raw.find((v) => v != null) || 1;
          const normData = raw.map((v) => (v != null ? +((v / baseVal) * 100).toFixed(2) : null));
          series.push({
            name: m.name,
            type: "line",
            data: normData,
            rawValues: raw,
            unit: "亿元",
            isIndex: false,
            group: m.group,
            symbol: "none",
            smooth: false,
            lineStyle: { width: 2.2, type: "solid", color: m.color },
            itemStyle: { color: m.color },
            emphasis: { focus: "series", lineStyle: { width: 3.5 } },
          });
        });

        return series;
      }

      let currentLegendSelected = {};
      function initLegendSelected(preset = "ALL") {
        const sel = {};
        DATA.indices_meta.forEach((m) => {
          if (preset === "ALL" || preset === "ONLY_IDX") sel[m.name] = true;
          else if (preset === "STAR50") sel[m.name] = m.group === "科创50";
          else if (preset === "LARGE_CAP") sel[m.name] = m.group === "沪深300" || m.group === "上证50";
          else if (preset === "SMID_CAP") sel[m.name] = m.group === "中证500" || m.group === "中证1000";
          else sel[m.name] = false;
        });
        DATA.etfs_meta.forEach((m) => {
          if (preset === "ALL" || preset === "ONLY_ETF") sel[m.name] = true;
          else if (preset === "STAR50") sel[m.name] = m.group === "科创50";
          else if (preset === "LARGE_CAP") sel[m.name] = m.group === "沪深300" || m.group === "上证50";
          else if (preset === "SMID_CAP") sel[m.name] = m.group === "中证500" || m.group === "中证1000";
          else sel[m.name] = false;
        });
        currentLegendSelected = sel;
      }
      initLegendSelected("ALL");

      function renderOverviewChart(baseIdx = 0) {
        const tc = getThemeColors();
        const series = buildNormalizedSeries(baseIdx);

        const option = {
          backgroundColor: "transparent",
          title: {
            text: "主力资金动向：全市场宽基 & ETF规模对比",
            subtext: `线型区分：虚线=指数走势，实线=ETF规模（亿元）；基准=100 起点：${dates[baseIdx]}`,
            left: "center",
            top: 4,
            textStyle: { color: tc.text, fontSize: 16, fontWeight: 700 },
            subtextStyle: { color: tc.subText, fontSize: 12 },
          },
          tooltip: {
            trigger: "axis",
            axisPointer: { type: "cross", label: { backgroundColor: "#3b5bdb" } },
            backgroundColor: tc.tooltipBg,
            borderColor: tc.axisLine,
            textStyle: { color: tc.text, fontSize: 12 },
            formatter: function (params) {
              if (!params || !params.length) return "";
              const dt = params[0].axisValue;
              const idx = params[0].dataIndex;
              let html = `<div style="font-weight:700;margin-bottom:6px;border-bottom:1px solid ${tc.axisLine};padding-bottom:4px;">📅 ${dt}（基准起点 ${dates[baseIdx]}=100）</div>`;
              params.forEach((p) => {
                if (p.value == null) return;
                const sObj = series[p.seriesIndex];
                const rawVal = sObj && sObj.rawValues ? sObj.rawValues[idx] : "";
                const rawStr = rawVal != null ? ` <span style="opacity:0.75">(${rawVal.toFixed(2)}${sObj.unit})</span>` : "";
                const lineTag = sObj && sObj.isIndex ? "[指数·虚]" : "[规模·实]";
                html += `<div style="display:flex;justify-content:space-between;gap:16px;line-height:1.65;">
                  <span>${p.marker} <span style="opacity:0.7;font-size:11px;">${lineTag}</span> ${p.seriesName}</span>
                  <span style="font-family:monospace;font-weight:700;">${p.value.toFixed(2)}${rawStr}</span>
                </div>`;
              });
              return html;
            },
          },
          legend: {
            top: 52,
            left: "center",
            type: "scroll",
            selected: currentLegendSelected,
            textStyle: { color: tc.text, fontSize: 12 },
            pageTextStyle: { color: tc.subText },
          },
          grid: { left: 58, right: 32, top: 112, bottom: 75 },
          xAxis: {
            type: "category",
            data: dates,
            boundaryGap: false,
            axisLine: { lineStyle: { color: tc.axisLine } },
            axisLabel: { color: tc.subText, fontSize: 11, rotate: 30 },
            splitLine: { show: true, lineStyle: { color: tc.gridLine } },
          },
          yAxis: {
            type: "value",
            name: "相对起点涨跌幅 (基准=100)",
            nameTextStyle: { color: tc.subText, fontSize: 12, padding: [0, 0, 0, 24] },
            axisLine: { show: false },
            axisLabel: { color: tc.subText, fontSize: 11.5 },
            splitLine: { lineStyle: { color: tc.gridLine } },
          },
          dataZoom: [
            { type: "inside", start: overviewZoomStart, end: overviewZoomEnd },
            {
              type: "slider",
              bottom: 10,
              height: 26,
              start: overviewZoomStart,
              end: overviewZoomEnd,
              borderColor: tc.axisLine,
              textStyle: { color: tc.subText },
            },
          ],
          series,
        };

        chartOverview.setOption(option, true);
      }
      renderOverviewChart(0);

      chartOverview.on("legendselectchanged", function (params) {
        currentLegendSelected = params.selected;
      });

      chartOverview.on("datazoom", function () {
        const opt = chartOverview.getOption();
        if (opt.dataZoom && opt.dataZoom[0]) {
          overviewZoomStart = opt.dataZoom[0].start;
          overviewZoomEnd = opt.dataZoom[0].end;
          if (document.getElementById("chkDynamicRebase").checked) {
            const startIdx = Math.max(0, Math.min(N - 1, Math.floor((overviewZoomStart / 100) * (N - 1))));
            renderOverviewChart(startIdx);
          }
        }
      });

      document.getElementById("chkDynamicRebase").addEventListener("change", function () {
        const startIdx = this.checked
          ? Math.max(0, Math.min(N - 1, Math.floor((overviewZoomStart / 100) * (N - 1))))
          : 0;
        renderOverviewChart(startIdx);
      });

      document.querySelectorAll("#overviewPresetGroup .pill-btn").forEach((btn) => {
        btn.addEventListener("click", () => {
          document.querySelectorAll("#overviewPresetGroup .pill-btn").forEach((b) => b.classList.remove("active"));
          btn.classList.add("active");
          const preset = btn.getAttribute("data-preset");
          initLegendSelected(preset);
          const startIdx = document.getElementById("chkDynamicRebase").checked
            ? Math.max(0, Math.min(N - 1, Math.floor((overviewZoomStart / 100) * (N - 1))))
            : 0;
          renderOverviewChart(startIdx);
        });
      });

      document.querySelectorAll("#overviewPeriodGroup .pill-btn").forEach((btn) => {
        btn.addEventListener("click", () => {
          document.querySelectorAll("#overviewPeriodGroup .pill-btn").forEach((b) => b.classList.remove("active"));
          btn.classList.add("active");
          const range = btn.getAttribute("data-range");
          let startIdx = 0;
          if (range === "1M") startIdx = Math.max(0, N - 22);
          else if (range === "3M") startIdx = Math.max(0, N - 65);
          else if (range === "6M") startIdx = Math.max(0, N - 130);
          else if (range === "YTD") {
            const ytdIdx = dates.findIndex((d) => d >= "2026-01-01");
            startIdx = ytdIdx >= 0 ? ytdIdx : 0;
          }
          overviewZoomStart = +((startIdx / Math.max(1, N - 1)) * 100).toFixed(2);
          overviewZoomEnd = 100;
          const baseIdx = document.getElementById("chkDynamicRebase").checked ? startIdx : 0;
          renderOverviewChart(baseIdx);
        });
      });

      const GROUPS_RESONANCE = [
        { group: "科创50", index_code: "sh000688", index_name: "科创50指数" },
        { group: "沪深300", index_code: "sh000300", index_name: "沪深300指数" },
        { group: "中证1000", index_code: "sh000852", index_name: "中证1000指数" },
        { group: "中证500", index_code: "sh000905", index_name: "中证500指数" },
        { group: "上证50", index_code: "sh000016", index_name: "上证50指数" },
      ];

      let activeGroup = "科创50";
      let activeEtfCode = "588000";
      let isAllExpanded = false;
      const chartSingle = echarts.init(document.getElementById("chartSingle"));
      let stackedChartInstances = [];

      function renderStyleTabs() {
        const tabContainer = document.getElementById("styleTabsContainer");
        tabContainer.innerHTML = GROUPS_RESONANCE.map((g) => {
          const etfs = DATA.etfs_meta.filter((e) => e.group === g.group);
          const names = etfs.map((e) => e.name).join(" / ");
          return `
            <button class="style-tab ${g.group === activeGroup ? "active" : ""}" data-group="${g.group}">
              <span>${g.group} 资金共振</span>
              <span style="font-size:11px;opacity:0.8;font-weight:500;">(${names})</span>
            </button>
          `;
        }).join("");

        tabContainer.querySelectorAll(".style-tab").forEach((btn) => {
          btn.addEventListener("click", () => {
            const grp = btn.getAttribute("data-group");
            const etfs = DATA.etfs_meta.filter((e) => e.group === grp);
            selectSingleGroup(grp, etfs[0].code);
          });
        });
      }

      function renderSubEtfPills() {
        const subContainer = document.getElementById("singleEtfSubSelect");
        const etfs = DATA.etfs_meta.filter((e) => e.group === activeGroup);
        if (etfs.length <= 1) {
          subContainer.innerHTML = `<span class="pill-label">当前观测标的：<strong>${etfs[0].name} (${etfs[0].code})</strong></span>`;
          return;
        }
        let html = `<span class="pill-label">标的选择：</span>`;
        etfs.forEach((e) => {
          html += `<button class="pill-btn ${activeEtfCode === e.code ? "active" : ""}" data-code="${e.code}">${e.name} (${e.code})</button>`;
        });
        html += `<button class="pill-btn ${activeEtfCode === "SUM" ? "active" : ""}" data-code="SUM">🔗 ${activeGroup}两只ETF合计规模</button>`;
        subContainer.innerHTML = html;

        subContainer.querySelectorAll(".pill-btn").forEach((btn) => {
          btn.addEventListener("click", () => {
            activeEtfCode = btn.getAttribute("data-code");
            renderSubEtfPills();
            renderSingleInsightAndChart();
          });
        });
      }

      function getResonanceData(groupName, etfCodeOrSum) {
        const gMeta = GROUPS_RESONANCE.find((g) => g.group === groupName);
        const idxSeries = DATA.indices_series[gMeta.index_code] || [];
        const etfs = DATA.etfs_meta.filter((e) => e.group === groupName);

        let shareSeries = [];
        let d1Series = [];
        let d5Series = [];
        let pctSeries = [];
        let titleLabel = "";
        let threshSingle = 10;

        if (etfCodeOrSum === "SUM" && etfs.length > 1) {
          titleLabel = `${groupName}合计 (${etfs.map((e) => e.name).join("+")})`;
          threshSingle = etfs.reduce((acc, e) => acc + e.single_thresh, 0);
          for (let i = 0; i < N; i++) {
            const sumVal = etfs.reduce((acc, e) => acc + (derived[e.code].share[i] || 0), 0);
            shareSeries.push(+sumVal.toFixed(2));
          }
          for (let i = 0; i < N; i++) {
            d1Series.push(i >= 1 ? +(shareSeries[i] - shareSeries[i - 1]).toFixed(2) : 0);
            d5Series.push(i >= 5 ? +(shareSeries[i] - shareSeries[i - 5]).toFixed(2) : 0);
          }
          const sorted = [...shareSeries].sort((a, b) => a - b);
          pctSeries = shareSeries.map((v) => +(((sorted.indexOf(v) + 1) / N) * 100).toFixed(1));
        } else {
          const targetEtf = etfs.find((e) => e.code === etfCodeOrSum) || etfs[0];
          titleLabel = `${targetEtf.name} (${targetEtf.code})`;
          threshSingle = targetEtf.single_thresh;
          shareSeries = derived[targetEtf.code].share;
          d1Series = derived[targetEtf.code].d1;
          d5Series = derived[targetEtf.code].d5;
          pctSeries = derived[targetEtf.code].pct;
        }

        return { gMeta, etfs, idxSeries, shareSeries, d1Series, d5Series, pctSeries, titleLabel, threshSingle };
      }

      function buildResonanceOption(groupName, etfCodeOrSum) {
        const tc = getThemeColors();
        const { gMeta, idxSeries, shareSeries, d1Series, d5Series, pctSeries, titleLabel } =
          getResonanceData(groupName, etfCodeOrSum);

        return {
          backgroundColor: "transparent",
          title: {
            text: `${groupName} 资金共振总览`,
            subtext: `${gMeta.index_name}走势 vs ${titleLabel} 规模 & 每日规模变化（单位：亿元）`,
            left: "center",
            top: 2,
            textStyle: { color: tc.text, fontSize: 15.5, fontWeight: 700 },
            subtextStyle: { color: tc.subText, fontSize: 12 },
          },
          tooltip: {
            trigger: "axis",
            axisPointer: { type: "cross", link: [{ xAxisIndex: "all" }] },
            backgroundColor: tc.tooltipBg,
            borderColor: tc.axisLine,
            textStyle: { color: tc.text, fontSize: 12 },
            formatter: function (params) {
              if (!params || !params.length) return "";
              const idx = params[0].dataIndex;
              const dt = dates[idx];
              const idxVal = idxSeries[idx];
              const shVal = shareSeries[idx];
              const d1 = d1Series[idx];
              const d5 = d5Series[idx];
              const pct = pctSeries[idx];
              const d1Color = d1 > 0 ? "#dc2626" : d1 < 0 ? "#16a34a" : tc.text;
              const d5Color = d5 > 0 ? "#dc2626" : d5 < 0 ? "#16a34a" : tc.text;
              return `
                <div style="font-weight:700;margin-bottom:6px;border-bottom:1px solid ${tc.axisLine};padding-bottom:4px;">
                  📅 ${dt} · ${groupName} (${titleLabel})
                </div>
                <div style="display:flex;justify-content:space-between;gap:18px;line-height:1.7;">
                  <span>📉 ${gMeta.index_name}：</span>
                  <strong style="font-family:monospace;">${idxVal != null ? idxVal.toFixed(2) : "-"} 点</strong>
                </div>
                <div style="display:flex;justify-content:space-between;gap:18px;line-height:1.7;">
                  <span>🏦 ETF规模：</span>
                  <strong style="font-family:monospace;">${shVal != null ? shVal.toFixed(2) : "-"} 亿元 (分位 ${pct}%)</strong>
                </div>
                <div style="display:flex;justify-content:space-between;gap:18px;line-height:1.7;">
                  <span>📊 当日规模变化：</span>
                  <strong style="font-family:monospace;color:${d1Color};">${d1 >= 0 ? "+" : ""}${d1.toFixed(2)} 亿元</strong>
                </div>
                <div style="display:flex;justify-content:space-between;gap:18px;line-height:1.7;">
                  <span>🔥 5日累计变化：</span>
                  <strong style="font-family:monospace;color:${d5Color};">${d5 >= 0 ? "+" : ""}${d5.toFixed(2)} 亿元</strong>
                </div>
              `;
            },
          },
          legend: { top: 48, left: "center", textStyle: { color: tc.text, fontSize: 12 } },
          axisPointer: { link: [{ xAxisIndex: "all" }] },
          grid: [
            { left: 64, right: 72, top: 86, height: "50%" },
            { left: 64, right: 72, top: "64%", height: "22%" },
          ],
          xAxis: [
            {
              type: "category",
              data: dates,
              gridIndex: 0,
              boundaryGap: false,
              axisLine: { lineStyle: { color: tc.axisLine } },
              axisLabel: { show: false },
              axisTick: { show: false },
              splitLine: { show: true, lineStyle: { color: tc.gridLine } },
            },
            {
              type: "category",
              data: dates,
              gridIndex: 1,
              boundaryGap: true,
              axisLine: { lineStyle: { color: tc.axisLine } },
              axisLabel: { color: tc.subText, fontSize: 11 },
              axisTick: { show: true },
              splitLine: { show: true, lineStyle: { color: tc.gridLine } },
            },
          ],
          yAxis: [
            {
              type: "value",
              name: "指数点位",
              gridIndex: 0,
              scale: true,
              nameTextStyle: { color: tc.subText, fontSize: 11.5 },
              axisLabel: { color: tc.subText, fontSize: 11 },
              splitLine: { lineStyle: { color: tc.gridLine } },
            },
            {
              type: "value",
              name: "ETF规模(亿元)",
              gridIndex: 0,
              scale: true,
              nameTextStyle: { color: tc.subText, fontSize: 11.5 },
              axisLabel: { color: tc.subText, fontSize: 11, formatter: "{value} 亿" },
              splitLine: { show: false },
            },
            {
              type: "value",
              name: "每日变化(亿元)",
              gridIndex: 1,
              nameTextStyle: { color: tc.subText, fontSize: 11 },
              axisLabel: { color: tc.subText, fontSize: 10.5 },
              splitLine: { lineStyle: { color: tc.gridLine } },
            },
          ],
          dataZoom: [
            { type: "inside", xAxisIndex: [0, 1], start: 0, end: 100 },
            {
              type: "slider",
              xAxisIndex: [0, 1],
              bottom: 6,
              height: 22,
              start: 0,
              end: 100,
              borderColor: tc.axisLine,
              textStyle: { color: tc.subText },
            },
          ],
          series: [
            {
              name: gMeta.index_name,
              type: "line",
              xAxisIndex: 0,
              yAxisIndex: 0,
              data: idxSeries,
              symbol: "none",
              lineStyle: { width: 2, color: "#94a3b8" },
              itemStyle: { color: "#94a3b8" },
            },
            {
              name: `${titleLabel}规模`,
              type: "line",
              xAxisIndex: 0,
              yAxisIndex: 1,
              data: shareSeries,
              symbol: "none",
              lineStyle: { width: 2.3, color: tc.isDark ? "#60a5fa" : "#1e293b" },
              itemStyle: { color: tc.isDark ? "#60a5fa" : "#1e293b" },
            },
            {
              name: `${titleLabel}每日变化`,
              type: "bar",
              xAxisIndex: 1,
              yAxisIndex: 2,
              data: d1Series,
              itemStyle: {
                color: function (p) {
                  return p.value >= 0 ? "#ef4444" : "#22c55e";
                },
              },
            },
          ],
        };
      }

      function renderSingleInsightAndChart() {
        const { idxSeries, shareSeries, d1Series, d5Series, pctSeries, titleLabel, threshSingle } =
          getResonanceData(activeGroup, activeEtfCode);

        const last = N - 1;
        const lookback = Math.min(20, last);
        const idxChg20 =
          idxSeries[last - lookback] > 0
            ? (((idxSeries[last] - idxSeries[last - lookback]) / idxSeries[last - lookback]) * 100).toFixed(2)
            : "0.00";
        const shareDiff20 = (shareSeries[last] - shareSeries[last - lookback]).toFixed(2);

        let patternText = "📊 震荡观察期";
        if (+idxChg20 < -1.5 && +shareDiff20 > threshSingle * 0.6) {
          patternText = "🟢 指数回落 + 规模逆势扩张（典型资金底背离/低位承接）";
        } else if (+idxChg20 > 1.5 && +shareDiff20 > threshSingle * 0.6) {
          patternText = "🔥 量价齐升（指数与ETF规模同步走强共振）";
        } else if (+idxChg20 < -1.5 && +shareDiff20 < -threshSingle * 0.6) {
          patternText = "⚠️ 指数与规模同步回落（资金暂未大规模承接）";
        } else if (+idxChg20 > 2.0 && +shareDiff20 < -threshSingle * 0.6) {
          patternText = "🔴 指数反弹 + 规模净流出（逢高止盈赎回）";
        }

        const strip = document.getElementById("singleInsightStrip");
        strip.innerHTML = `
          <div class="insight-item">
            <span class="insight-label">当前观测标的</span>
            <span class="insight-val">${titleLabel}</span>
          </div>
          <div class="insight-item">
            <span class="insight-label">最新ETF规模 / 历史分位</span>
            <span class="insight-val">${shareSeries[last].toFixed(2)} 亿元 (${pctSeries[last]}%)</span>
          </div>
          <div class="insight-item">
            <span class="insight-label">最新单日 / 5日累计变化</span>
            <span class="insight-val">
              <span class="${d1Series[last] >= 0 ? "val-pos" : "val-neg"}">${d1Series[last] >= 0 ? "+" : ""}${d1Series[last].toFixed(2)}亿</span>
              /
              <span class="${d5Series[last] >= 0 ? "val-pos" : "val-neg"}">${d5Series[last] >= 0 ? "+" : ""}${d5Series[last].toFixed(2)}亿</span>
            </span>
          </div>
          <div class="insight-item">
            <span class="insight-label">近20交易日指数涨跌 / 规模增减</span>
            <span class="insight-val">${idxChg20 >= 0 ? "+" : ""}${idxChg20}% / ${shareDiff20 >= 0 ? "+" : ""}${shareDiff20}亿</span>
          </div>
          <div class="insight-item" style="grid-column: span 2;">
            <span class="insight-label">近20日资金-价格结构判定</span>
            <span class="insight-val" style="font-family:var(--font-sans);font-size:13.5px;">${patternText}</span>
          </div>
        `;

        const opt = buildResonanceOption(activeGroup, activeEtfCode);
        chartSingle.setOption(opt, true);
      }

      function renderAllStackedCharts() {
        const container = document.getElementById("stackedChartsContainer");
        stackedChartInstances.forEach((inst) => inst.dispose());
        stackedChartInstances = [];

        container.innerHTML = GROUPS_RESONANCE.map((g, idx) => {
          return `<div class="stacked-chart-item"><div id="stackedChart_${idx}" class="chart-box-single"></div></div>`;
        }).join("");

        GROUPS_RESONANCE.forEach((g, idx) => {
          const dom = document.getElementById(`stackedChart_${idx}`);
          const inst = echarts.init(dom);
          const etfs = DATA.etfs_meta.filter((e) => e.group === g.group);
          const opt = buildResonanceOption(g.group, etfs[0].code);
          inst.setOption(opt, true);
          stackedChartInstances.push(inst);
        });
      }

      function selectSingleGroup(groupName, etfCode) {
        activeGroup = groupName;
        const etfs = DATA.etfs_meta.filter((e) => e.group === groupName);
        activeEtfCode = etfCode || etfs[0].code;
        if (isAllExpanded) {
          toggleExpandMode(false);
        }
        renderStyleTabs();
        renderSubEtfPills();
        renderSingleInsightAndChart();
      }

      function toggleExpandMode(forceState) {
        isAllExpanded = typeof forceState === "boolean" ? forceState : !isAllExpanded;
        const btn = document.getElementById("btnToggleExpandMode");
        const tabToolbar = document.getElementById("singleTabToolbar");
        const insightStrip = document.getElementById("singleInsightStrip");
        const singleDom = document.getElementById("chartSingle");
        const stackedDom = document.getElementById("stackedChartsContainer");

        if (isAllExpanded) {
          btn.textContent = "🗂️ 切换为：单宽基标签页视图";
          tabToolbar.style.display = "none";
          insightStrip.style.display = "none";
          singleDom.style.display = "none";
          stackedDom.style.display = "flex";
          renderAllStackedCharts();
        } else {
          btn.textContent = "📜 切换为：五大宽基全部展开视图";
          tabToolbar.style.display = "flex";
          insightStrip.style.display = "grid";
          singleDom.style.display = "block";
          stackedDom.style.display = "none";
          chartSingle.resize();
          renderSingleInsightAndChart();
        }
      }

      document.getElementById("btnToggleExpandMode").addEventListener("click", () => toggleExpandMode());

      renderStyleTabs();
      renderSubEtfPills();
      renderSingleInsightAndChart();

      function showToast(msg) {
        const el = document.getElementById("toastMsg");
        el.textContent = msg;
        el.style.display = "block";
        clearTimeout(el._timer);
        el._timer = setTimeout(() => {
          el.style.display = "none";
        }, 3200);
      }

      document.getElementById("btnThemeToggle").addEventListener("click", () => {
        const cur = document.documentElement.getAttribute("data-theme");
        const next = cur === "dark" ? "light" : "dark";
        document.documentElement.setAttribute("data-theme", next);
        renderBroadMarket();
        const startIdx = document.getElementById("chkDynamicRebase").checked
          ? Math.max(0, Math.min(N - 1, Math.floor((overviewZoomStart / 100) * (N - 1))))
          : 0;
        renderOverviewChart(startIdx);
        if (isAllExpanded) renderAllStackedCharts();
        else renderSingleInsightAndChart();
      });

      document.getElementById("btnSyncLatest").addEventListener("click", () => {
        showToast("本网页由 GitHub Actions 每个交易日自动更新。如需立即更新，请到 GitHub 仓库的 Actions 页面手动运行工作流。");
      });

      window.addEventListener("resize", () => {
        if (broadChartInstance) broadChartInstance.resize();
        chartOverview.resize();
        chartSingle.resize();
        stackedChartInstances.forEach((inst) => inst.resize());
      });
    })();
  </script>
</body>
</html>
