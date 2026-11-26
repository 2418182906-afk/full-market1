#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
宽基 ETF 规模追踪系统（单位统一换算为：亿元 = 份额(亿份) × 当日收盘价(元)）
数据源：
1. 五大宽基指数日线：腾讯财经日K接口 (sh000688, sh000852, sh000905, sh000016, sh000300)
2. 八大核心宽基ETF每日份额 + 每日收盘价：上交所份额接口 + 腾讯财经日K接口
3. 沪深两市七大宽基系列35只ETF每日份额汇总：broad_market.py
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

from broad_market import ALL_CODES, build_broad_payload, sync_broad_market

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
    },
    {
        "code": "510330",
        "name": "沪深300华夏",
        "full_name": "华夏沪深300ETF",
        "group": "沪深300",
        "index_code": "sh000300",
        "index_name": "沪深300指数",
        "color": "#e64980",
    },
    {
        "code": "510500",
        "name": "中证500南方",
        "full_name": "南方中证500ETF",
        "group": "中证500",
        "index_code": "sh000905",
        "index_name": "中证500指数",
        "color": "#7950f2",
    },
    {
        "code": "512500",
        "name": "中证500华夏",
        "full_name": "华夏中证500ETF",
        "group": "中证500",
        "index_code": "sh000905",
        "index_name": "中证500指数",
        "color": "#20c997",
    },
    {
        "code": "512100",
        "name": "中证1000南方",
        "full_name": "南方中证1000ETF",
        "group": "中证1000",
        "index_code": "sh000852",
        "index_name": "中证1000指数",
        "color": "#4c6ef5",
    },
    {
        "code": "560010",
        "name": "中证1000广发",
        "full_name": "广发中证1000ETF",
        "group": "中证1000",
        "index_code": "sh000852",
        "index_name": "中证1000指数",
        "color": "#94d82d",
    },
    {
        "code": "510050",
        "name": "上证50华夏",
        "full_name": "华夏上证50ETF",
        "group": "上证50",
        "index_code": "sh000016",
        "index_name": "上证50指数",
        "color": "#495057",
    },
    {
        "code": "588000",
        "name": "科创50华夏",
        "full_name": "华夏上证科创板50ETF",
        "group": "科创50",
        "index_code": "sh000688",
        "index_name": "科创50指数",
        "color": "#f76707",
    },
]


def _to_tx_symbol(code_6: str) -> str:
    """将6位A股/ETF代码转换为腾讯日K代码前缀"""
    c = str(code_6).strip().zfill(6)
    if c.startswith(("5", "6", "9")):
        return f"sh{c}"
    return f"sz{c}"


def _fetch_tx_close_series(session: requests.Session, tx_symbol: str, start_date: str) -> dict:
    """从腾讯日K接口抓取 {日期: 收盘价}"""
    url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={tx_symbol},day,{start_date},2029-12-31,1500,"
    out = {}
    for attempt in range(3):
        try:
            resp = session.get(url, timeout=15).json()
            node = resp.get("data", {}).get(tx_symbol, {})
            klines = node.get("day") or node.get("qfqday") or []
            for row in klines:
                if len(row) >= 3 and row[0] >= start_date:
                    out[row[0]] = round(float(row[2]), 4)
            return out
        except Exception:
            time.sleep(0.25 * (attempt + 1))
    return out


def sync_data(force_full: bool = False) -> dict:
    """增量同步指数日线、ETF份额(亿份)及ETF收盘价(元)，并换算为规模(亿元)"""
    os.makedirs(DATA_DIR, exist_ok=True)
    cache = {}
    if os.path.exists(CACHE_PATH) and not force_full:
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                cache = json.load(f)
        except Exception:
            cache = {}

    etf_shares_by_date = cache.get("etf_shares_by_date") or cache.get("etf_by_date") or {}
    etf_prices_data = cache.get("etf_prices_data", {})
    indices_data = cache.get("indices_data", {})

    session = requests.Session()
    all_dates_set = set()

    # 1. 指数日线
    for item in INDICES_META:
        code = item["code"]
        series = indices_data.get(code, {})
        fetched = _fetch_tx_close_series(session, code, START_DATE)
        for d, val in fetched.items():
            series[d] = round(val, 2)
            all_dates_set.add(d)
        indices_data[code] = series

    # 2. 8只核心ETF每日收盘价（用于 亿份 * 元 = 亿元）
    core_codes = [e["code"] for e in ETFS_META]
    for code in core_codes:
        sym = _to_tx_symbol(code)
        p_map = etf_prices_data.get(code, {})
        fetched_p = _fetch_tx_close_series(session, sym, START_DATE)
        for d, px in fetched_p.items():
            p_map[d] = px
        etf_prices_data[code] = p_map

    # 3. 8只核心ETF每日份额（上交所 TOT_VOL 单位为万份 -> 除以10000得亿份）
    all_trading_dates = sorted(list(all_dates_set))
    target_codes = set(core_codes)
    recent_recheck = set(all_trading_dates[-3:]) if all_trading_dates else set()
    missing_dates = [
        d for d in all_trading_dates
        if (d not in etf_shares_by_date)
        or (len(etf_shares_by_date.get(d, {})) < len(target_codes))
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
                        out[sc] = round(float(x["TOT_VOL"]) / 10000.0, 4)
                return d, out
            except Exception:
                time.sleep(0.25 * (attempt + 1))
        return d, {}

    if missing_dates:
        print(f"[Sync] 正在从上交所同步 {len(missing_dates)} 个交易日的ETF份额数据...")
        with ThreadPoolExecutor(max_workers=16) as ex:
            for d, res in ex.map(fetch_sse_date, missing_dates):
                if res:
                    etf_shares_by_date[d] = res

    valid_dates = sorted([d for d in etf_shares_by_date.keys() if etf_shares_by_date[d]])

    # 4. 全市场35只白名单ETF份额 + 对应收盘价换算（亿元）
    broad_cache = sync_broad_market(force_full=force_full)
    broad_payload = build_broad_payload(broad_cache)

    broad_prices_map = cache.get("broad_prices_map", {})
    whitelist_codes = sorted(list(ALL_CODES))
    for w_code in whitelist_codes:
        if w_code in etf_prices_data and etf_prices_data[w_code]:
            broad_prices_map[w_code] = etf_prices_data[w_code]
        else:
            sym = _to_tx_symbol(w_code)
            p_map = broad_prices_map.get(w_code, {})
            fetched = _fetch_tx_close_series(session, sym, START_DATE)
            p_map.update(fetched)
            broad_prices_map[w_code] = p_map

    # 将 broad_payload 中的各系列份额(亿份)按对应日期成分股 份额(亿份)*收盘价(元) 换算为 规模(亿元)
    b_dates = broad_payload.get("dates", [])
    by_date_raw = broad_cache.get("by_date", {})
    total_amount_series = []
    for day in b_dates:
        day_rows = by_date_raw.get(day, {})
        day_amt = 0.0
        has_any = False
        for c, rec in day_rows.items():
            sh_yi = float(rec.get("shares_10k", 0.0)) / 10000.0
            px = broad_prices_map.get(c, {}).get(day)
            if px is not None:
                day_amt += sh_yi * float(px)
                has_any = True
        total_amount_series.append(round(day_amt, 2) if has_any else None)

    for grp in broad_payload.get("groups", []):
        gid = grp.get("id")
        g_amt_series = []
        for day in b_dates:
            day_rows = by_date_raw.get(day, {})
            g_amt = 0.0
            has_any = False
            for c, rec in day_rows.items():
                if rec.get("group") == gid:
                    sh_yi = float(rec.get("shares_10k", 0.0)) / 10000.0
                    px = broad_prices_map.get(c, {}).get(day)
                    if px is not None:
                        g_amt += sh_yi * float(px)
                        has_any = True
            g_amt_series.append(round(g_amt, 2) if has_any else None)

        grp["shares_series_yi"] = grp.get("series")
        grp["series"] = g_amt_series
        valid_g = [(idx, v) for idx, v in enumerate(g_amt_series) if v is not None]
        if valid_g:
            last_i, last_v = valid_g[-1]
            prev_v = valid_g[-2][1] if len(valid_g) >= 2 else None
            grp["latest"] = last_v
            grp["change1"] = round(last_v - prev_v, 2) if prev_v is not None else None
            latest_day = b_dates[last_i]
            new_details = []
            for d_item in grp.get("details", []):
                dc = d_item["code"]
                d_sh_yi = float(d_item.get("shares", 0.0))
                d_px = broad_prices_map.get(dc, {}).get(latest_day)
                new_details.append({
                    **d_item,
                    "shares_yi": round(d_sh_yi, 2),
                    "price": d_px,
                    "amount_yi": round(d_sh_yi * float(d_px), 2) if d_px is not None else None,
                })
            new_details.sort(key=lambda x: (x["amount_yi"] if x["amount_yi"] is not None else -1), reverse=True)
            grp["details"] = new_details
        else:
            grp["latest"] = None
            grp["change1"] = None

    broad_payload["total_shares_series_yi"] = broad_payload.get("total_series")
    broad_payload["total_series"] = total_amount_series
    valid_tot = [v for v in total_amount_series if v is not None]
    if valid_tot:
        broad_payload["total_latest"] = valid_tot[-1]
        broad_payload["total_change1"] = round(valid_tot[-1] - valid_tot[-2], 2) if len(valid_tot) >= 2 else None
    else:
        broad_payload["total_latest"] = None
        broad_payload["total_change1"] = None
    broad_payload["unit"] = "亿元"
    broad_payload["note"] = (
        "单位：亿元（由各ETF披露份额[亿份] × 对应交易日场内收盘价[元]换算得出）。"
        "本页仅统计固定代码白名单中的35只指定宽基ETF，按代码去重汇总，缺失价格日不以前值填充。"
    )

    new_cache = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "start_date": valid_dates[0] if valid_dates else START_DATE,
        "end_date": valid_dates[-1] if valid_dates else "",
        "total_days": len(valid_dates),
        "indices_meta": INDICES_META,
        "etfs_meta": ETFS_META,
        "dates": valid_dates,
        "indices_data": indices_data,
        "etf_shares_by_date": {d: etf_shares_by_date[d] for d in valid_dates},
        "etf_by_date": {d: etf_shares_by_date[d] for d in valid_dates},
        "etf_prices_data": etf_prices_data,
        "broad_prices_map": broad_prices_map,
        "broad_market": broad_payload,
    }

    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(new_cache, f, ensure_ascii=False, indent=2)

    print(
        f"[Sync] 规模(亿元)同步完成: {new_cache['start_date']} ~ {new_cache['end_date']} "
        f"(共 {new_cache['total_days']} 个交易日)"
    )
    return new_cache


def build_frontend_payload(cache: dict) -> dict:
    """将缓存数据整理为前端结构，统一输出规模(亿元)及份额(亿份)"""
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
    etf_shares_series = {}
    etf_prices_series = {}
    shares_src = cache.get("etf_shares_by_date") or cache.get("etf_by_date") or {}
    prices_src = cache.get("etf_prices_data") or {}

    for etf_meta in ETFS_META:
        c = etf_meta["code"]
        p_map = prices_src.get(c, {})
        amt_vals = []
        sh_vals = []
        px_vals = []
        last_amt = None
        last_sh = None
        last_px = None
        for d in dates:
            sh = shares_src.get(d, {}).get(c)
            if sh is not None:
                last_sh = float(sh)
            px = p_map.get(d)
            if px is not None:
                last_px = float(px)

            cur_sh = last_sh
            cur_px = last_px
            cur_amt = round(cur_sh * cur_px, 2) if (cur_sh is not None and cur_px is not None) else None
            if cur_amt is not None:
                last_amt = cur_amt

            amt_vals.append(cur_amt)
            sh_vals.append(round(cur_sh, 2) if cur_sh is not None else None)
            px_vals.append(round(cur_px, 4) if cur_px is not None else None)

        etf_series[c] = amt_vals
        etf_shares_series[c] = sh_vals
        etf_prices_series[c] = px_vals

    return {
        "unit": "亿元",
        "updated_at": cache["updated_at"],
        "start_date": cache["start_date"],
        "end_date": cache["end_date"],
        "total_days": cache["total_days"],
        "dates": dates,
        "indices_meta": INDICES_META,
        "etfs_meta": ETFS_META,
        "indices_series": indices_series,
        "etf_series": etf_series,
        "etf_shares_series": etf_shares_series,
        "etf_prices_series": etf_prices_series,
        "broad_market": cache.get("broad_market", {}),
    }


HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>宽基ETF规模追踪 · 市场底背离监测系统 (单位：亿元)</title>
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
      --down-green: #16a34a;
      --warn-amber: #d97706;
      --warn-amber-bg: rgba(217, 119, 6, 0.11);
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
      --down-green: #4ade80;
      --warn-amber: #fbbf24;
      --warn-amber-bg: rgba(251, 191, 36, 0.14);
      --shadow-sm: 0 2px 6px rgba(0, 0, 0, 0.3);
      --shadow-md: 0 10px 30px rgba(0, 0, 0, 0.45);
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
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
    .brand-group { display: flex; align-items: center; gap: 14px; }
    .brand-badge {
      background: linear-gradient(135deg, #1e40af, #2563eb);
      color: #fff;
      font-weight: 700;
      font-size: 13px;
      padding: 6px 11px;
      border-radius: 8px;
    }
    .brand-title h1 { font-size: 19px; font-weight: 700; }
    .brand-subtitle { font-size: 12.5px; color: var(--text-muted); margin-top: 2px; }
    .nav-actions { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; }
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
    }
    .nav-jump a:hover { color: var(--accent); background: var(--bg-card); }
    .btn {
      display: inline-flex; align-items: center; gap: 6px;
      font-size: 12.5px; font-weight: 600; padding: 7px 13px;
      border-radius: 8px; border: 1px solid var(--border-color);
      background: var(--bg-card); color: var(--text-primary); cursor: pointer;
    }
    .btn:hover { background: var(--bg-hover); border-color: var(--border-strong); }
    .btn-primary { background: var(--accent); color: #fff; border-color: var(--accent); }
    .container { max-width: 1560px; margin: 0 auto; padding: 18px 24px 0; }
    .guide-banner {
      background: var(--bg-card); border: 1px solid var(--border-color);
      border-left: 4px solid var(--accent); border-radius: var(--radius-md);
      padding: 12px 18px; margin-bottom: 18px;
      display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 12px;
    }
    .guide-rules { display: flex; flex-wrap: wrap; align-items: center; gap: 14px; font-size: 13px; color: var(--text-secondary); }
    .rule-pill {
      display: inline-flex; align-items: center; gap: 6px;
      background: var(--bg-subtle); padding: 4px 10px; border-radius: 6px;
      border: 1px solid var(--border-color); font-weight: 500;
    }
    .guide-warn {
      font-size: 12px; color: var(--warn-amber); background: var(--warn-amber-bg);
      padding: 4px 10px; border-radius: 6px; font-weight: 600;
    }
    .broad-market-panel, .section-card {
      background: var(--bg-card); border: 1px solid var(--border-color);
      border-radius: var(--radius-lg); padding: 20px 22px; margin-bottom: 22px;
      box-shadow: var(--shadow-sm);
    }
    .broad-market-head, .section-header {
      display: flex; align-items: flex-start; justify-content: space-between;
      flex-wrap: wrap; gap: 12px; margin-bottom: 14px;
    }
    .section-header { padding-bottom: 14px; border-bottom: 1px solid var(--border-color); }
    .broad-market-head h2 { font-size: 18px; font-weight: 750; }
    .broad-market-note, .section-desc { color: var(--text-muted); font-size: 12.5px; margin-top: 4px; }
    .broad-kpis { display: grid; grid-template-columns: repeat(4, minmax(140px, 1fr)); gap: 10px; margin: 14px 0 16px; }
    .broad-kpi { border: 1px solid var(--border-color); border-radius: 10px; padding: 12px 14px; background: var(--bg-subtle); }
    .broad-kpi .k-label { color: var(--text-muted); font-size: 12px; }
    .broad-kpi .k-value { font: 700 20px var(--font-mono); margin-top: 5px; }
    .broad-tabs-bar, .toolbar-row {
      display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 10px; margin-bottom: 12px;
    }
    .broad-tabs-bar {
      padding: 10px 12px; background: var(--bg-subtle);
      border: 1px solid var(--border-color); border-radius: var(--radius-md);
    }
    .style-tabs, .pill-group { display: flex; flex-wrap: wrap; align-items: center; gap: 7px; }
    .style-tab {
      display: inline-flex; align-items: center; gap: 7px; padding: 7px 14px;
      border-radius: 8px; font-size: 13px; font-weight: 700;
      border: 1px solid var(--border-color); background: var(--bg-card);
      color: var(--text-secondary); cursor: pointer;
    }
    .style-tab.active { background: var(--accent); color: #fff; border-color: var(--accent); }
    .pill-label { font-size: 12px; font-weight: 600; color: var(--text-muted); margin-right: 4px; }
    .pill-btn {
      font-size: 12px; font-weight: 600; padding: 5px 11px; border-radius: 7px;
      border: 1px solid var(--border-color); background: var(--bg-subtle);
      color: var(--text-secondary); cursor: pointer;
    }
    .pill-btn.active { background: var(--accent); color: #fff; border-color: var(--accent); }
    .color-dot { width: 9px; height: 9px; border-radius: 50%; display: inline-block; flex-shrink: 0; }
    .broad-table-wrap { overflow: auto; margin-top: 14px; border: 1px solid var(--border-color); border-radius: var(--radius-md); }
    .broad-table { border-collapse: collapse; width: 100%; min-width: 720px; font-size: 13px; }
    .broad-table th, .broad-table td { padding: 10px 12px; border-bottom: 1px solid var(--border-color); text-align: right; white-space: nowrap; }
    .broad-table th:first-child, .broad-table td:first-child { text-align: left; }
    .broad-table th { color: var(--text-muted); font-weight: 650; background: var(--bg-subtle); }
    .broad-table tr.active-series-row td { background: var(--accent-soft); }
    .broad-expand { border: 0; background: none; color: var(--accent); cursor: pointer; font: inherit; font-weight: 700; }
    .broad-detail { display: none; background: var(--bg-subtle); }
    .broad-detail.open { display: table-row; }
    .broad-detail td { text-align: left; white-space: normal; padding: 10px 14px; }
    .broad-chip {
      display: inline-block; border: 1px solid var(--border-color); background: var(--bg-card);
      border-radius: 999px; padding: 2px 9px; margin: 2px 6px 2px 0; font-size: 11.5px; color: var(--text-secondary);
    }
    .broad-status { font-size: 12px; color: var(--text-muted); margin-top: 10px; line-height: 1.6; }
    .etf-grid { display: grid; grid-template-columns: repeat(8, 1fr); gap: 12px; margin-bottom: 20px; }
    @media (max-width: 1400px) { .etf-grid { grid-template-columns: repeat(4, 1fr); } }
    @media (max-width: 768px) { .etf-grid { grid-template-columns: repeat(2, 1fr); } .broad-kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
    .etf-card {
      background: var(--bg-card); border: 1px solid var(--border-color);
      border-radius: var(--radius-md); padding: 12px 13px; cursor: pointer;
    }
    .etf-card:hover { transform: translateY(-2px); box-shadow: var(--shadow-md); border-color: var(--accent); }
    .etf-card-top { display: flex; align-items: center; justify-content: space-between; margin-bottom: 6px; }
    .etf-card-name { font-weight: 700; font-size: 13px; display: flex; align-items: center; gap: 5px; }
    .etf-card-code { font-family: var(--font-mono); font-size: 11.5px; color: var(--text-muted); background: var(--bg-subtle); padding: 1px 6px; border-radius: 4px; }
    .etf-card-share { font-family: var(--font-mono); font-size: 19px; font-weight: 700; margin: 4px 0 6px; }
    .etf-card-share span { font-size: 11.5px; font-weight: 500; color: var(--text-muted); margin-left: 3px; }
    .etf-card-metrics { display: flex; justify-content: space-between; font-size: 11.5px; color: var(--text-secondary); margin-bottom: 7px; font-family: var(--font-mono); }
    .pct-bar-wrap { height: 5px; background: var(--bg-subtle); border-radius: 999px; overflow: hidden; margin-bottom: 5px; }
    .pct-bar-fill { height: 100%; border-radius: 999px; }
    .etf-card-foot { display: flex; align-items: center; justify-content: space-between; font-size: 11px; color: var(--text-muted); }
    .section-title { font-size: 17px; font-weight: 700; display: flex; align-items: center; gap: 8px; }
    .section-num { background: var(--accent-soft); color: var(--accent); font-size: 12px; font-weight: 700; padding: 2px 8px; border-radius: 6px; }
    .chart-box-overview { width: 100%; height: 560px; }
    .chart-box-single { width: 100%; height: 520px; }
    .insight-strip {
      display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px;
      margin-bottom: 14px; padding: 11px 14px; background: var(--bg-subtle);
      border-radius: var(--radius-md); border: 1px solid var(--border-color);
    }
    .insight-item { display: flex; flex-direction: column; gap: 2px; }
    .insight-label { font-size: 11.5px; color: var(--text-muted); }
    .insight-val { font-size: 14px; font-weight: 700; font-family: var(--font-mono); }
    .mono-cell { font-family: var(--font-mono); }
    .val-pos { color: var(--up-red); font-weight: 700; }
    .val-neg { color: var(--down-green); font-weight: 700; }
    .toast {
      position: fixed; bottom: 24px; right: 24px; background: #1e293b; color: #fff;
      padding: 10px 16px; border-radius: 9px; font-size: 13px; font-weight: 600;
      z-index: 1200; display: none;
    }
  </style>
</head>
<body>
  <header class="top-header">
    <div class="header-inner">
      <div class="brand-group">
        <span class="brand-badge">QTYX V3.6.6</span>
        <div class="brand-title">
          <h1>宽基 ETF 规模追踪 · 市场底背离监测系统（单位：亿元）</h1>
          <div class="brand-subtitle" id="metaSubtitle">加载中...</div>
        </div>
      </div>
      <div class="nav-actions">
        <nav class="nav-jump">
          <a href="#sec-broad-market">七大宽基系列汇总</a>
          <a href="#sec-overview">1. 全市场总览图</a>
          <a href="#sec-single">2. 单ETF资金共振</a>
        </nav>
        <button class="btn" id="btnThemeToggle">🌓 切换主题</button>
        <button class="btn btn-primary" id="btnSyncLatest">⏱ 每日自动更新</button>
      </div>
    </div>
  </header>

  <main class="container">
    <div class="guide-banner">
      <div class="guide-rules">
        <span class="rule-pill">📏 <strong>看图规则</strong>：虚线 = 宽基指数走势，实线 = ETF 规模（亿元，起点归一到 100）</span>
        <span class="rule-pill">📉 <strong>同步回落</strong>：指数跌 + 规模跌 → 资金净流出</span>
        <span class="rule-pill">📈 <strong>资金底背离</strong>：指数回落 + 规模逆势抬升 → 资金低位承接</span>
      </div>
      <div class="guide-warn">⚠️ 规模(亿元) = 披露份额(亿份) × 当日收盘价(元)，缺失日不以前值填充</div>
    </div>

    <section class="broad-market-panel" id="sec-broad-market">
      <div class="broad-market-head">
        <div>
          <h2>沪深两市宽基 ETF 全系列规模汇总</h2>
          <div class="broad-market-note">上证50 · 沪深300 · 中证500 · 中证1000 · 中证A500 · 科创宽基 · 创业板</div>
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
      <div id="broadMarketChart" style="height:430px;width:100%;"></div>

      <div class="broad-table-wrap">
        <table class="broad-table">
          <thead>
            <tr>
              <th>系列（点击行切换上方图表）</th>
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
        <div>
          <div class="section-title"><span class="section-num">板块一</span>全市场宽基指数 &amp; ETF 规模归一对比（亿元）</div>
          <div class="section-desc">五大宽基指数（虚线）+ 八大核心宽基 ETF 规模（实线，单位：亿元）同框对比</div>
        </div>
        <div class="pill-group" id="overviewPeriodGroup">
          <button class="pill-btn active" data-range="ALL">全部区间</button>
          <button class="pill-btn" data-range="YTD">2026年以来</button>
          <button class="pill-btn" data-range="6M">近6个月</button>
          <button class="pill-btn" data-range="3M">近3个月</button>
          <button class="pill-btn" data-range="1M">近1个月</button>
        </div>
      </div>
      <div id="chartOverview" class="chart-box-overview"></div>
    </section>

    <section class="section-card" id="sec-single">
      <div class="section-header">
        <div>
          <div class="section-title"><span class="section-num">板块二</span>单宽基 / 单 ETF 资金共振独立观测图（亿元）</div>
          <div class="section-desc">上图：左轴指数点位 + 右轴 ETF 规模（亿元）；下图：每日规模变化柱状图（亿元）</div>
        </div>
      </div>
      <div class="toolbar-row">
        <div class="style-tabs" id="styleTabsContainer"></div>
        <div class="pill-group" id="singleEtfSubSelect"></div>
      </div>
      <div class="insight-strip" id="singleInsightStrip"></div>
      <div id="chartSingle" class="chart-box-single"></div>
    </section>
  </main>

  <div class="toast" id="toastMsg"></div>

  <script>
    window.ETF_TRACKER_DATA = __PAYLOAD_JSON__;
  </script>
  <script>
    (function () {
      const DATA = window.ETF_TRACKER_DATA;
      const dates = DATA.dates || [];
      const N = dates.length;
      const fmt2 = (v) => (v == null || isNaN(v) ? "—" : Number(v).toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 }));
      const sign2 = (v) => (v == null || isNaN(v) ? "—" : (Number(v) > 0 ? "+" : "") + fmt2(v));

      function getThemeColors() {
        const isDark = document.documentElement.getAttribute("data-theme") === "dark";
        return {
          text: isDark ? "#e2e8f0" : "#1e293b",
          subText: isDark ? "#94a3b8" : "#64748b",
          gridLine: isDark ? "rgba(148, 163, 184, 0.13)" : "#f1f5f9",
          axisLine: isDark ? "#334155" : "#cbd5e1",
          tooltipBg: isDark ? "rgba(15, 23, 42, 0.94)" : "rgba(255, 255, 255, 0.96)",
        };
      }

      document.getElementById("metaSubtitle").textContent =
        `数据区间：${DATA.start_date} 至 ${DATA.end_date}（共 ${DATA.total_days} 个交易日） · 最近同步：${DATA.updated_at} · 单位：亿元`;

      // Section 0: Broad market series in 亿元
      let activeBroadSeriesId = "TOTAL";
      let broadZoomStart = 0;
      let broadZoomEnd = 100;
      let broadChartInstance = null;

      function computeDiff(series) {
        const out = new Array(series.length).fill(null);
        let prev = null;
        for (let i = 0; i < series.length; i++) {
          const cur = series[i];
          if (cur == null) out[i] = null;
          else if (prev == null) { out[i] = 0; prev = cur; }
          else { out[i] = +(cur - prev).toFixed(2); prev = cur; }
        }
        return out;
      }

      function renderBroadMarket() {
        const B = DATA.broad_market || {};
        const groups = B.groups || [];
        const bDates = B.dates || [];

        const tabsEl = document.getElementById("broadSeriesTabs");
        const tabItems = [
          { id: "TOTAL", name: "七大系列合计", color: "#1d4ed8", count: groups.reduce((a, g) => a + (g.count || 0), 0) },
          ...groups.map((g) => ({ id: g.id, name: g.name, color: g.color || "#1d4ed8", count: g.count || 0 })),
        ];
        tabsEl.innerHTML = tabItems.map((t) =>
          `<button class="style-tab ${activeBroadSeriesId === t.id ? "active" : ""}" data-series-id="${t.id}">
            <span class="color-dot" style="background:${activeBroadSeriesId === t.id ? "#fff" : t.color}"></span>
            <span>${t.name}</span><span style="font-size:11px;opacity:0.82;">(${t.count}只)</span>
          </button>`
        ).join("");
        tabsEl.querySelectorAll(".style-tab").forEach((btn) => {
          btn.addEventListener("click", () => {
            activeBroadSeriesId = btn.dataset.seriesId;
            renderBroadMarket();
          });
        });

        const selGroup = groups.find((g) => g.id === activeBroadSeriesId);
        const activeSeries = selGroup ? (selGroup.series || []) : (B.total_series || []);
        const activeDiff = computeDiff(activeSeries);
        const activeName = selGroup ? selGroup.name : "七大系列合计";
        const activeColor = selGroup ? (selGroup.color || "#1d4ed8") : "#1d4ed8";

        let latestVal = null, prevVal = null, latestDate = B.end_date || "—";
        for (let i = activeSeries.length - 1; i >= 0; i--) {
          if (activeSeries[i] != null) {
            if (latestVal == null) { latestVal = activeSeries[i]; latestDate = bDates[i] || latestDate; }
            else if (prevVal == null) { prevVal = activeSeries[i]; break; }
          }
        }
        const latestChg = latestVal != null && prevVal != null ? +(latestVal - prevVal).toFixed(2) : null;
        const activeCount = selGroup ? (selGroup.count || 0) : groups.reduce((a, g) => a + (g.count || 0), 0);

        document.getElementById("broadMarketKpis").innerHTML = `
          <div class="broad-kpi"><div class="k-label">${activeName} · 最新总规模</div><div class="k-value">${fmt2(latestVal)} <small>亿元</small></div></div>
          <div class="broad-kpi"><div class="k-label">${activeName} · 较前一披露日变化</div><div class="k-value" style="color:${latestChg > 0 ? "var(--up-red)" : latestChg < 0 ? "var(--down-green)" : "inherit"}">${sign2(latestChg)} <small>亿元</small></div></div>
          <div class="broad-kpi"><div class="k-label">${activeName} · 纳入ETF数量</div><div class="k-value">${activeCount} <small>只</small></div></div>
          <div class="broad-kpi"><div class="k-label">数据区间（最新：${latestDate}）</div><div class="k-value" style="font-size:14px">${B.start_date || "—"} 至 ${B.end_date || "—"}</div></div>
        `;

        const chartDom = document.getElementById("broadMarketChart");
        if (window.echarts && chartDom && bDates.length) {
          if (!broadChartInstance) {
            broadChartInstance = echarts.init(chartDom);
            broadChartInstance.on("datazoom", () => {
              const dz = broadChartInstance.getOption().dataZoom?.[0];
              if (dz) { broadZoomStart = dz.start; broadZoomEnd = dz.end; }
            });
          }
          const tc = getThemeColors();
          broadChartInstance.setOption({
            backgroundColor: "transparent",
            tooltip: {
              trigger: "axis",
              axisPointer: { type: "cross", link: [{ xAxisIndex: "all" }] },
              backgroundColor: tc.tooltipBg, borderColor: tc.axisLine, textStyle: { color: tc.text, fontSize: 12 },
              formatter: (params) => {
                if (!params?.length) return "";
                const idx = params[0].dataIndex;
                const sh = activeSeries[idx];
                const chg = activeDiff[idx];
                return `<div style="font-weight:700;margin-bottom:4px;">📅 ${bDates[idx]} · ${activeName}</div>
                  <div>💰 系列总规模：<strong>${fmt2(sh)} 亿元</strong></div>
                  <div>📊 较前日变化：<strong style="color:${chg > 0 ? "#dc2626" : chg < 0 ? "#16a34a" : tc.text}">${sign2(chg)} 亿元</strong></div>`;
              }
            },
            legend: { top: 2, textStyle: { color: tc.text } },
            axisPointer: { link: [{ xAxisIndex: "all" }] },
            grid: [
              { left: 72, right: 28, top: 38, height: "44%" },
              { left: 72, right: 28, top: "58%", height: "27%" },
            ],
            xAxis: [
              { type: "category", data: bDates, gridIndex: 0, boundaryGap: false, axisLabel: { show: false }, axisLine: { lineStyle: { color: tc.axisLine } } },
              { type: "category", data: bDates, gridIndex: 1, boundaryGap: true, axisLabel: { color: tc.subText, fontSize: 11, hideOverlap: true }, axisLine: { lineStyle: { color: tc.axisLine } } },
            ],
            yAxis: [
              { type: "value", name: "总规模(亿元)", gridIndex: 0, scale: true, nameTextStyle: { color: tc.subText }, axisLabel: { color: tc.subText }, splitLine: { lineStyle: { color: tc.gridLine } } },
              { type: "value", name: "日变化(亿元)", gridIndex: 1, nameTextStyle: { color: tc.subText }, axisLabel: { color: tc.subText }, splitLine: { lineStyle: { color: tc.gridLine } } },
            ],
            dataZoom: [
              { type: "inside", xAxisIndex: [0, 1], start: broadZoomStart, end: broadZoomEnd },
              { type: "slider", xAxisIndex: [0, 1], bottom: 4, height: 22, start: broadZoomStart, end: broadZoomEnd },
            ],
            series: [
              { name: `${activeName}总规模(亿元)`, type: "line", xAxisIndex: 0, yAxisIndex: 0, showSymbol: false, connectNulls: false, data: activeSeries, lineStyle: { width: 2.4, color: activeColor }, itemStyle: { color: activeColor }, areaStyle: { color: activeColor, opacity: 0.08 } },
              { name: `${activeName}每日规模变化(亿元)`, type: "bar", xAxisIndex: 1, yAxisIndex: 1, data: activeDiff, itemStyle: { color: (p) => (p.value == null ? "transparent" : p.value >= 0 ? "#ef4444" : "#22c55e") } },
            ],
          }, true);
        }

        const tbody = document.getElementById("broadMarketRows");
        tbody.innerHTML = groups.map((g, i) => {
          const gSeries = g.series || [];
          const valid = gSeries.filter((v) => v != null);
          const gLatest = valid.length ? valid[valid.length - 1] : g.latest;
          const gChg = valid.length >= 2 ? +(valid[valid.length - 1] - valid[valid.length - 2]).toFixed(2) : g.change1;
          const detail = (g.details || []).map((x) => {
            const amtText = x.amount_yi != null ? `${fmt2(x.amount_yi)}亿元` : "价格缺失";
            const pxText = x.price != null ? `价${x.price}` : "";
            return `<span class="broad-chip">${x.code} ${x.name} · ${amtText} (${fmt2(x.shares)}亿份 ${pxText})</span>`;
          }).join("") || "暂无明细";
          return `
            <tr class="${activeBroadSeriesId === g.id ? "active-series-row" : ""}" data-select-series="${g.id}" style="cursor:pointer;">
              <td><span class="color-dot" style="background:${g.color || "#1d4ed8"};margin-right:6px;"></span><strong>${g.name}</strong></td>
              <td class="mono-cell">${g.count || 0}</td>
              <td class="mono-cell">${fmt2(gLatest)}</td>
              <td class="mono-cell" style="color:${gChg > 0 ? "var(--up-red)" : gChg < 0 ? "var(--down-green)" : "inherit"}">${sign2(gChg)}</td>
              <td><button class="broad-expand" data-expand="${i}">展开 ${g.count || 0} 只ETF ▾</button></td>
            </tr>
            <tr class="broad-detail" id="broadDetail${i}"><td colspan="5">${detail}</td></tr>
          `;
        }).join("") + `
          <tr class="${activeBroadSeriesId === "TOTAL" ? "active-series-row" : ""}" data-select-series="TOTAL" style="cursor:pointer;">
            <td><strong>七大系列合计</strong></td>
            <td class="mono-cell">${groups.reduce((a, g) => a + (g.count || 0), 0)}</td>
            <td class="mono-cell"><strong>${fmt2(B.total_latest)}</strong></td>
            <td class="mono-cell" style="color:${B.total_change1 > 0 ? "var(--up-red)" : B.total_change1 < 0 ? "var(--down-green)" : "inherit"}">${sign2(B.total_change1)}</td>
            <td>按代码去重汇总</td>
          </tr>
        `;
        tbody.querySelectorAll("[data-expand]").forEach((btn) => btn.addEventListener("click", (e) => {
          e.stopPropagation();
          const r = document.getElementById("broadDetail" + btn.dataset.expand);
          r.classList.toggle("open");
          btn.textContent = r.classList.contains("open") ? "收起明细 ▴" : `展开 ${groups[Number(btn.dataset.expand)].count || 0} 只ETF ▾`;
        }));
        tbody.querySelectorAll("tr[data-select-series]").forEach((tr) => tr.addEventListener("click", () => {
          activeBroadSeriesId = tr.dataset.selectSeries;
          renderBroadMarket();
        }));
        document.getElementById("broadMarketStatus").textContent =
          `更新时间：${B.updated_at || "暂无"} · 单位：亿元 · ${B.note || ""}`;
      }

      document.querySelectorAll("#broadRangeGroup .pill-btn").forEach((btn) => btn.addEventListener("click", () => {
        document.querySelectorAll("#broadRangeGroup .pill-btn").forEach((b) => b.classList.remove("active"));
        btn.classList.add("active");
        const bDates = (DATA.broad_market && DATA.broad_market.dates) || [];
        const len = bDates.length;
        let startIdx = 0;
        if (btn.dataset.range === "1M") startIdx = Math.max(0, len - 22);
        else if (btn.dataset.range === "3M") startIdx = Math.max(0, len - 65);
        else if (btn.dataset.range === "6M") startIdx = Math.max(0, len - 130);
        else if (btn.dataset.range === "YTD") startIdx = Math.max(0, bDates.findIndex((d) => d >= "2026-01-01"));
        broadZoomStart = +((startIdx / Math.max(1, len - 1)) * 100).toFixed(2);
        broadZoomEnd = 100;
        renderBroadMarket();
      }));
      renderBroadMarket();

      // Cards & Section 1 & Section 2 in 亿元
      const lastIdx = N - 1;
      document.getElementById("etfSnapshotGrid").innerHTML = (DATA.etfs_meta || []).map((m) => {
        const arr = DATA.etf_series[m.code] || [];
        const cur = arr[lastIdx] ?? 0;
        const prev = lastIdx >= 1 ? (arr[lastIdx - 1] ?? cur) : cur;
        const chg1 = +(cur - prev).toFixed(2);
        const chg5 = lastIdx >= 5 ? +(cur - (arr[lastIdx - 5] ?? cur)).toFixed(2) : 0;
        return `
          <div class="etf-card" data-group="${m.group}" data-code="${m.code}">
            <div class="etf-card-top">
              <div class="etf-card-name"><span class="color-dot" style="background:${m.color}"></span>${m.name}</div>
              <span class="etf-card-code">${m.code}</span>
            </div>
            <div class="etf-card-share">${fmt2(cur)}<span>亿元</span></div>
            <div class="etf-card-metrics">
              <span>当日 <strong class="${chg1 >= 0 ? "val-pos" : "val-neg"}">${sign2(chg1)}</strong></span>
              <span>5日 <strong class="${chg5 >= 0 ? "val-pos" : "val-neg"}">${sign2(chg5)}</strong></span>
            </div>
            <div class="pct-bar-wrap"><div class="pct-bar-fill" style="width:65%;background:${m.color};"></div></div>
            <div class="etf-card-foot"><span>规模(亿元)</span><span>${m.group}</span></div>
          </div>
        `;
      }).join("");

      const chartOverview = echarts.init(document.getElementById("chartOverview"));
      let overviewZoomStart = 0, overviewZoomEnd = 100;
      function renderOverviewChart() {
        const tc = getThemeColors();
        const series = [];
        (DATA.indices_meta || []).forEach((m) => {
          const raw = DATA.indices_series[m.code] || [];
          const base = raw.find((v) => v != null) || 1;
          series.push({
            name: m.name, type: "line", symbol: "none", rawValues: raw, unit: "点",
            lineStyle: { width: 1.8, type: "dashed", color: m.color }, itemStyle: { color: m.color },
            data: raw.map((v) => (v != null ? +((v / base) * 100).toFixed(2) : null)),
          });
        });
        (DATA.etfs_meta || []).forEach((m) => {
          const raw = DATA.etf_series[m.code] || [];
          const base = raw.find((v) => v != null) || 1;
          series.push({
            name: m.name, type: "line", symbol: "none", rawValues: raw, unit: "亿元",
            lineStyle: { width: 2.2, type: "solid", color: m.color }, itemStyle: { color: m.color },
            data: raw.map((v) => (v != null ? +((v / base) * 100).toFixed(2) : null)),
          });
        });
        chartOverview.setOption({
          backgroundColor: "transparent",
          tooltip: {
            trigger: "axis", backgroundColor: tc.tooltipBg, borderColor: tc.axisLine, textStyle: { color: tc.text, fontSize: 12 },
            formatter: (params) => {
              if (!params?.length) return "";
              const idx = params[0].dataIndex;
              let html = `<div style="font-weight:700;margin-bottom:4px;">📅 ${dates[idx]}（起点=100）</div>`;
              params.forEach((p) => {
                if (p.value == null) return;
                const sObj = series[p.seriesIndex];
                const rv = sObj?.rawValues?.[idx];
                html += `<div style="display:flex;justify-content:space-between;gap:14px;"><span>${p.marker} ${p.seriesName}</span><span style="font-family:monospace;font-weight:700;">${p.value.toFixed(2)} (${rv != null ? fmt2(rv) : "—"}${sObj?.unit || ""})</span></div>`;
              });
              return html;
            }
          },
          legend: { top: 6, type: "scroll", textStyle: { color: tc.text } },
          grid: { left: 58, right: 28, top: 64, bottom: 72 },
          xAxis: { type: "category", data: dates, boundaryGap: false, axisLabel: { color: tc.subText } },
          yAxis: { type: "value", name: "相对起点(基准=100)", nameTextStyle: { color: tc.subText }, axisLabel: { color: tc.subText }, splitLine: { lineStyle: { color: tc.gridLine } } },
          dataZoom: [
            { type: "inside", start: overviewZoomStart, end: overviewZoomEnd },
            { type: "slider", bottom: 8, height: 24, start: overviewZoomStart, end: overviewZoomEnd },
          ],
          series,
        }, true);
      }
      renderOverviewChart();

      document.querySelectorAll("#overviewPeriodGroup .pill-btn").forEach((btn) => btn.addEventListener("click", () => {
        document.querySelectorAll("#overviewPeriodGroup .pill-btn").forEach((b) => b.classList.remove("active"));
        btn.classList.add("active");
        let startIdx = 0;
        if (btn.dataset.range === "1M") startIdx = Math.max(0, N - 22);
        else if (btn.dataset.range === "3M") startIdx = Math.max(0, N - 65);
        else if (btn.dataset.range === "6M") startIdx = Math.max(0, N - 130);
        else if (btn.dataset.range === "YTD") startIdx = Math.max(0, dates.findIndex((d) => d >= "2026-01-01"));
        overviewZoomStart = +((startIdx / Math.max(1, N - 1)) * 100).toFixed(2);
        overviewZoomEnd = 100;
        renderOverviewChart();
      }));

      // Section 2: Single ETF resonance in 亿元
      const GROUPS = [
        { group: "科创50", index_code: "sh000688", index_name: "科创50指数" },
        { group: "沪深300", index_code: "sh000300", index_name: "沪深300指数" },
        { group: "中证1000", index_code: "sh000852", index_name: "中证1000指数" },
        { group: "中证500", index_code: "sh000905", index_name: "中证500指数" },
        { group: "上证50", index_code: "sh000016", index_name: "上证50指数" },
      ];
      let activeGroup = "沪深300";
      let activeEtfCode = "510300";
      const chartSingle = echarts.init(document.getElementById("chartSingle"));

      function renderSingleSection() {
        const tc = getThemeColors();
        const tabs = document.getElementById("styleTabsContainer");
        tabs.innerHTML = GROUPS.map((g) => `<button class="style-tab ${g.group === activeGroup ? "active" : ""}" data-group="${g.group}">${g.group}</button>`).join("");
        tabs.querySelectorAll(".style-tab").forEach((btn) => btn.addEventListener("click", () => {
          activeGroup = btn.dataset.group;
          const first = (DATA.etfs_meta || []).find((e) => e.group === activeGroup);
          if (first) activeEtfCode = first.code;
          renderSingleSection();
        }));

        const etfs = (DATA.etfs_meta || []).filter((e) => e.group === activeGroup);
        const sub = document.getElementById("singleEtfSubSelect");
        sub.innerHTML = etfs.map((e) => `<button class="pill-btn ${activeEtfCode === e.code ? "active" : ""}" data-code="${e.code}">${e.name} (${e.code})</button>`).join("");
        sub.querySelectorAll(".pill-btn").forEach((btn) => btn.addEventListener("click", () => {
          activeEtfCode = btn.dataset.code;
          renderSingleSection();
        }));

        const gMeta = GROUPS.find((g) => g.group === activeGroup);
        const target = etfs.find((e) => e.code === activeEtfCode) || etfs[0];
        const idxSeries = DATA.indices_series[gMeta.index_code] || [];
        const amtSeries = DATA.etf_series[target.code] || [];
        const shSeries = (DATA.etf_shares_series && DATA.etf_shares_series[target.code]) || [];
        const pxSeries = (DATA.etf_prices_series && DATA.etf_prices_series[target.code]) || [];
        const diffSeries = computeDiff(amtSeries);

        document.getElementById("singleInsightStrip").innerHTML = `
          <div class="insight-item"><span class="insight-label">当前标的</span><span class="insight-val">${target.name} (${target.code})</span></div>
          <div class="insight-item"><span class="insight-label">最新规模(亿元)</span><span class="insight-val">${fmt2(amtSeries[lastIdx])} 亿元</span></div>
          <div class="insight-item"><span class="insight-label">最新份额 / 收盘价</span><span class="insight-val">${fmt2(shSeries[lastIdx])}亿份 × ${pxSeries[lastIdx] ?? "—"}元</span></div>
          <div class="insight-item"><span class="insight-label">较前日规模变化</span><span class="insight-val ${diffSeries[lastIdx] >= 0 ? "val-pos" : "val-neg"}">${sign2(diffSeries[lastIdx])} 亿元</span></div>
        `;

        chartSingle.setOption({
          backgroundColor: "transparent",
          tooltip: {
            trigger: "axis", axisPointer: { type: "cross", link: [{ xAxisIndex: "all" }] },
            backgroundColor: tc.tooltipBg, borderColor: tc.axisLine, textStyle: { color: tc.text, fontSize: 12 },
            formatter: (params) => {
              if (!params?.length) return "";
              const i = params[0].dataIndex;
              return `<div style="font-weight:700;margin-bottom:4px;">📅 ${dates[i]} · ${target.name}</div>
                <div>📉 ${gMeta.index_name}：<strong>${idxSeries[i] ?? "—"}</strong></div>
                <div>💰 ETF规模：<strong>${fmt2(amtSeries[i])} 亿元</strong>（${fmt2(shSeries[i])}亿份 × ${pxSeries[i] ?? "—"}元）</div>
                <div>📊 规模日变化：<strong>${sign2(diffSeries[i])} 亿元</strong></div>`;
            }
          },
          legend: { top: 4, textStyle: { color: tc.text } },
          axisPointer: { link: [{ xAxisIndex: "all" }] },
          grid: [
            { left: 68, right: 72, top: 44, height: "48%" },
            { left: 68, right: 72, top: "62%", height: "24%" },
          ],
          xAxis: [
            { type: "category", data: dates, gridIndex: 0, boundaryGap: false, axisLabel: { show: false } },
            { type: "category", data: dates, gridIndex: 1, boundaryGap: true, axisLabel: { color: tc.subText, fontSize: 11 } },
          ],
          yAxis: [
            { type: "value", name: "指数点位", gridIndex: 0, scale: true, nameTextStyle: { color: tc.subText }, axisLabel: { color: tc.subText }, splitLine: { lineStyle: { color: tc.gridLine } } },
            { type: "value", name: "规模(亿元)", gridIndex: 0, scale: true, nameTextStyle: { color: tc.subText }, axisLabel: { color: tc.subText }, splitLine: { show: false } },
            { type: "value", name: "日变化(亿元)", gridIndex: 1, nameTextStyle: { color: tc.subText }, axisLabel: { color: tc.subText }, splitLine: { lineStyle: { color: tc.gridLine } } },
          ],
          dataZoom: [
            { type: "inside", xAxisIndex: [0, 1], start: 0, end: 100 },
            { type: "slider", xAxisIndex: [0, 1], bottom: 6, height: 22, start: 0, end: 100 },
          ],
          series: [
            { name: gMeta.index_name, type: "line", xAxisIndex: 0, yAxisIndex: 0, symbol: "none", data: idxSeries, lineStyle: { width: 2, color: "#94a3b8" }, itemStyle: { color: "#94a3b8" } },
            { name: `${target.name}规模(亿元)`, type: "line", xAxisIndex: 0, yAxisIndex: 1, symbol: "none", data: amtSeries, lineStyle: { width: 2.3, color: target.color }, itemStyle: { color: target.color } },
            { name: `${target.name}规模日变化(亿元)`, type: "bar", xAxisIndex: 1, yAxisIndex: 2, data: diffSeries, itemStyle: { color: (p) => (p.value == null ? "transparent" : p.value >= 0 ? "#ef4444" : "#22c55e") } },
          ],
        }, true);
      }
      renderSingleSection();

      document.querySelectorAll(".etf-card").forEach((card) => card.addEventListener("click", () => {
        activeGroup = card.dataset.group;
        activeEtfCode = card.dataset.code;
        renderSingleSection();
        document.getElementById("sec-single").scrollIntoView({ behavior: "smooth" });
      }));

      document.getElementById("btnThemeToggle").addEventListener("click", () => {
        const cur = document.documentElement.getAttribute("data-theme");
        document.documentElement.setAttribute("data-theme", cur === "dark" ? "light" : "dark");
        renderBroadMarket();
        renderOverviewChart();
        renderSingleSection();
      });

      document.getElementById("btnSyncLatest").addEventListener("click", () => {
        const el = document.getElementById("toastMsg");
        el.textContent = "数据由工作流每日自动更新；如需立即刷新，可在本地运行 python update_etf_tracker.py --force-full";
        el.style.display = "block";
        setTimeout(() => { el.style.display = "none"; }, 3200);
      });

      window.addEventListener("resize", () => {
        if (broadChartInstance) broadChartInstance.resize();
        chartOverview.resize();
        chartSingle.resize();
      });
    })();
  </script>
</body>
</html>
