#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
宽基 ETF 份额追踪 & 资金底背离监测系统 (QTYX V3.6.6 网页复刻增强版)
"""

import argparse
import csv
import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import requests

from broad_market import sync_broad_market, build_broad_payload

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
CACHE_PATH = os.path.join(DATA_DIR, "etf_share_cache.json")
HTML_OUT_PATH = os.path.join(BASE_DIR, "etf_share_tracker.html")

START_DATE = "2024-01-02"

INDICES_META = [
    {"code": "sh000688", "name": "科创50指数", "group": "科创50", "color": "#2b6cb0"},
    {"code": "sh000852", "name": "中证1000指数", "group": "中证1000", "color": "#82c91e"},
    {"code": "sh000905", "name": "中证500指数", "group": "中证500", "color": "#495057"},
    {"code": "sh000016", "name": "上证50指数", "group": "上证50", "color": "#fd7e14"},
    {"code": "sh000300", "name": "沪深300指数", "group": "沪深300", "color": "#15aabf"},
]

ETFS_META = [
    {"code": "510300", "name": "沪深300华泰", "full_name": "华泰柏瑞沪深300ETF", "group": "沪深300", "index_code": "sh000300", "index_name": "沪深300指数", "color": "#e6b800", "single_thresh": 20.0, "five_day_thresh": 50.0, "low_pct": 20.0, "high_pct": 80.0},
    {"code": "510330", "name": "沪深300华夏", "full_name": "华夏沪深300ETF", "group": "沪深300", "index_code": "sh000300", "index_name": "沪深300指数", "color": "#e64980", "single_thresh": 14.0, "five_day_thresh": 35.0, "low_pct": 20.0, "high_pct": 80.0},
    {"code": "510500", "name": "中证500南方", "full_name": "南方中证500ETF", "group": "中证500", "index_code": "sh000905", "index_name": "中证500指数", "color": "#7950f2", "single_thresh": 10.0, "five_day_thresh": 25.0, "low_pct": 20.0, "high_pct": 80.0},
    {"code": "512500", "name": "中证500华夏", "full_name": "华夏中证500ETF", "group": "中证500", "index_code": "sh000905", "index_name": "中证500指数", "color": "#20c997", "single_thresh": 4.0, "five_day_thresh": 10.0, "low_pct": 20.0, "high_pct": 80.0},
    {"code": "512100", "name": "中证1000南方", "full_name": "南方中证1000ETF", "group": "中证1000", "index_code": "sh000852", "index_name": "中证1000指数", "color": "#4c6ef5", "single_thresh": 7.0, "five_day_thresh": 30.0, "low_pct": 20.0, "high_pct": 80.0},
    {"code": "560010", "name": "中证1000广发", "full_name": "广发中证1000ETF", "group": "中证1000", "index_code": "sh000852", "index_name": "中证1000指数", "color": "#94d82d", "single_thresh": 5.0, "five_day_thresh": 10.0, "low_pct": 20.0, "high_pct": 80.0},
    {"code": "510050", "name": "上证50华夏", "full_name": "华夏上证50ETF", "group": "上证50", "index_code": "sh000016", "index_name": "上证50指数", "color": "#495057", "single_thresh": 15.0, "five_day_thresh": 35.0, "low_pct": 20.0, "high_pct": 80.0},
    {"code": "588000", "name": "科创50华夏", "full_name": "华夏上证科创板50ETF", "group": "科创50", "index_code": "sh000688", "index_name": "科创50指数", "color": "#f76707", "single_thresh": 6.0, "five_day_thresh": 30.0, "low_pct": 20.0, "high_pct": 80.0},
]


# =============================================================================
# 轻量份额复权、向后对齐、申赎汇总与异常排查（纯标准库，无 numpy/pandas 依赖）
# =============================================================================
SHARE_EVENT_TYPES = {"split", "merge", "dividend_reinvest"}
ALL_EVENT_TYPES = SHARE_EVENT_TYPES | {"dividend_cash"}


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return math.inf
    s = sorted(values)
    idx = (len(s) - 1) * q
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return s[lo]
    return s[lo] + (s[hi] - s[lo]) * (idx - lo)


def compute_etf_flows_and_audit(
    share_rows: list[dict],
    nav_rows: list[dict],
    event_rows: list[dict] | None = None,
    trading_dates: list[str] | None = None,
    share_unit: str = "yi",
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    计算 ETF 日度净申赎金额（亿元）并输出明细、汇总与异常日志。
    - share_rows: [{"trade_date", "fund_code", "total_share"}]
    - nav_rows:   [{"trade_date", "fund_code", "unit_nav"}]
    - event_rows: [{"fund_code", "ex_date", "event_type", "ratio"}]
    """
    unit_scale = {"fen": 1.0, "wan": 1e4, "yi": 1e8}
    assert share_unit in unit_scale, f"不支持的 share_unit: {share_unit}"
    scale_to_yi = unit_scale[share_unit] / 1e8

    share_map: dict[tuple[str, str], float] = {}
    nav_map: dict[tuple[str, str], float] = {}
    funds_set: set[str] = set()
    dates_set: set[str] = set(trading_dates or [])

    for r in share_rows:
        dt = str(r["trade_date"])
        fc = str(r["fund_code"]).strip()
        key = (dt, fc)
        assert key not in share_map, f"share 存在重复键: {key}"
        share_map[key] = float(r["total_share"])
        funds_set.add(fc)
        dates_set.add(dt)

    for r in nav_rows:
        dt = str(r["trade_date"])
        fc = str(r["fund_code"]).strip()
        key = (dt, fc)
        assert key not in nav_map, f"nav 存在重复键: {key}"
        nav_map[key] = float(r["unit_nav"])
        funds_set.add(fc)
        dates_set.add(dt)

    # 整理事件：仅 split / merge / dividend_reinvest 累乘 adj_factor；dividend_cash 仅打标
    cash_set: set[tuple[str, str]] = set()
    share_ev_by_fund: dict[str, list[tuple[str, float]]] = {}
    if event_rows:
        for ev in event_rows:
            fc = str(ev["fund_code"]).strip()
            ex_dt = str(ev["ex_date"])
            et = str(ev["event_type"]).strip()
            assert et in ALL_EVENT_TYPES, f"非法 event_type: {et}"
            if et == "dividend_cash":
                cash_set.add((ex_dt, fc))
            else:
                ratio = float(ev["ratio"])
                assert ratio > 0, f"份额事件 ratio 必须大于 0: {ev}"
                share_ev_by_fund.setdefault(fc, []).append((ex_dt, ratio))

    cum_factor_by_fund_date: dict[str, list[tuple[str, float]]] = {}
    for fc, ev_list in share_ev_by_fund.items():
        ev_list.sort(key=lambda x: x[0])
        by_date: dict[str, float] = {}
        acc = 1.0
        for ex_dt, ratio in ev_list:
            acc *= ratio
            by_date[ex_dt] = acc
        cum_factor_by_fund_date[fc] = sorted(by_date.items(), key=lambda x: x[0])

    cal_dates = sorted(dates_set)
    funds = sorted(funds_set)

    # 各自按交易日历向后对齐（取 <= trade_date 的最近有效记录）
    detail: list[dict] = []
    seen_out_keys: set[tuple[str, str]] = set()

    for fc in funds:
        s_hist: list[tuple[str, float]] = []
        n_hist: list[tuple[str, float]] = []
        ev_hist = cum_factor_by_fund_date.get(fc, [])
        ev_ptr = 0
        cur_factor = 1.0

        s_idx = 0
        n_idx = 0
        prev_adj_share: float | None = None

        # 预先收集该基金有记录的日期列表
        fc_s_dates = sorted(dt for (dt, c), v in share_map.items() if c == fc and v is not None)
        fc_n_dates = sorted(dt for (dt, c), v in nav_map.items() if c == fc and v is not None)

        for dt in cal_dates:
            while s_idx < len(fc_s_dates) and fc_s_dates[s_idx] <= dt:
                d_s = fc_s_dates[s_idx]
                s_hist.append((d_s, share_map[(d_s, fc)]))
                s_idx += 1

            while n_idx < len(fc_n_dates) and fc_n_dates[n_idx] <= dt:
                d_n = fc_n_dates[n_idx]
                n_hist.append((d_n, nav_map[(d_n, fc)]))
                n_idx += 1

            while ev_ptr < len(ev_hist) and ev_hist[ev_ptr][0] <= dt:
                cur_factor = ev_hist[ev_ptr][1]
                ev_ptr += 1

            if not s_hist or not n_hist:
                continue

            share_date, total_share = s_hist[-1]
            nav_date, unit_nav = n_hist[-1]
            assert share_date == nav_date, f"{fc} 在 {dt} 对齐后 share_date({share_date}) != nav_date({nav_date})"

            adj_share = total_share * cur_factor
            assert adj_share >= 0, f"{fc} 在 {dt} 还原份额为负: {adj_share}"

            out_key = (dt, fc)
            assert out_key not in seen_out_keys, f"输出存在重复键: {out_key}"
            seen_out_keys.add(out_key)

            share_t = adj_share
            share_t1 = prev_adj_share
            delta_share = (share_t - share_t1) if share_t1 is not None else None
            amount_yi = (delta_share * unit_nav * scale_to_yi) if delta_share is not None else None

            detail.append({
                "trade_date": dt,
                "fund_code": fc,
                "share_date": share_date,
                "nav_date": nav_date,
                "total_share": round(total_share, 6),
                "adj_factor": round(cur_factor, 8),
                "adj_share": round(adj_share, 6),
                "share_T": round(share_t, 6),
                "share_T1": round(share_t1, 6) if share_t1 is not None else None,
                "delta_share": round(delta_share, 6) if delta_share is not None else None,
                "unit_nav": round(unit_nav, 6),
                "amount_yi": round(amount_yi, 6) if amount_yi is not None else None,
                "is_cash_div": (dt, fc) in cash_set,
            })
            prev_adj_share = adj_share

    # 逐只金额相加汇总（禁止用 Σ(delta_share) × 平均净值）
    sum_by_date: dict[str, float] = {}
    for row in detail:
        if row["amount_yi"] is not None:
            dt = row["trade_date"]
            sum_by_date[dt] = sum_by_date.get(dt, 0.0) + row["amount_yi"]

    summary = [
        {"trade_date": dt, "sum_amount_yi": round(sum_by_date[dt], 6)}
        for dt in sorted(sum_by_date.keys())
    ]

    # 异常日志：|delta_share| 超过该基金 99 分位
    abs_by_fund: dict[str, list[float]] = {}
    for row in detail:
        if row["delta_share"] is not None:
            abs_by_fund.setdefault(row["fund_code"], []).append(abs(row["delta_share"]))
    p99_by_fund = {fc: _percentile(vals, 0.99) for fc, vals in abs_by_fund.items()}

    anomalies: list[dict] = []
    for row in detail:
        ds = row["delta_share"]
        if ds is not None and abs(ds) > p99_by_fund.get(row["fund_code"], math.inf):
            anomalies.append({
                **row,
                "p99_thresh": round(p99_by_fund[row["fund_code"]], 6),
                "anomaly_reason": "abs(delta_share) > fund_99th_percentile",
            })

    return detail, summary, anomalies


def _write_csv(path: str, rows: list[dict], fieldnames: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})


def _to_tx_symbol(code: str, exchange: str = "") -> str:
    c = str(code).strip()
    ex = str(exchange).upper()
    if c.startswith(("sh", "sz")):
        return c
    if "SH" in ex or "沪" in ex or c.startswith(("5", "6")):
        return f"sh{c}"
    return f"sz{c}"


def fetch_etf_daily_navs(codes_with_ex: list, existing_navs: dict, session: requests.Session) -> dict:
    nav_map = {k: dict(v) for k, v in (existing_navs or {}).items()}
    for code, ex in codes_with_ex:
        sym = _to_tx_symbol(code, ex)
        series = nav_map.get(code, {})
        try:
            url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={sym},day,{START_DATE},2029-12-31,1500,qfq"
            resp = session.get(url, timeout=12).json()
            days = resp.get("data", {}).get(sym, {}).get("qfqday") or resp.get("data", {}).get(sym, {}).get("day") or []
            for d in days:
                if len(d) >= 3 and d[0] >= START_DATE:
                    val = float(d[2])
                    if val > 0:
                        series[d[0]] = round(val, 4)
        except Exception:
            pass
        nav_map[code] = series
    return nav_map


def attach_series_nav(broad_payload: dict, etf_navs: dict) -> dict:
    if not broad_payload or not broad_payload.get("groups"):
        return broad_payload

    b_dates = broad_payload.get("dates") or []
    groups = broad_payload.get("groups") or []

    if broad_payload.get("total_nav_series") and all(g.get("nav_series") for g in groups):
        return broad_payload

    default_group_nav_codes = {
        "上证50": ("510050", "SH"),
        "沪深300": ("510300", "SH"),
        "中证500": ("510500", "SH"),
        "中证1000": ("512100", "SH"),
        "中证A500": ("512050", "SH"),
        "科创宽基": ("588000", "SH"),
        "创业板": ("159915", "SZ"),
    }

    def normalize_arr(arr):
        if not arr:
            return []
        valid = [v for v in arr if v is not None]
        mx = max(valid) if valid else 0
        scale = 10000.0 if mx > 50000 else 1.0
        return [None if v is None else float(v) / scale for v in arr]

    for g in groups:
        g_name = g.get("name", "")
        g_details = g.get("details") or []
        has_daily_detail_series = any(
            isinstance(x.get("series"), list) and len(x.get("series")) == len(b_dates)
            for x in g_details
        )
        g_nav_vals = []
        fallback_code, _ = default_group_nav_codes.get(g_name, ("510300", "SH"))
        fallback_map = etf_navs.get(fallback_code, {})

        for idx, dt in enumerate(b_dates):
            weighted_numerator = 0.0
            weighted_denom = 0.0
            if has_daily_detail_series:
                for det in g_details:
                    c = str(det.get("code") or "").strip()
                    d_series = det.get("series") or []
                    sh_val = d_series[idx] if idx < len(d_series) else None
                    if sh_val is not None and float(sh_val) > 0:
                        sh_yi = float(sh_val) / 10000.0 if float(sh_val) > 50000 else float(sh_val)
                        nv = etf_navs.get(c, {}).get(dt)
                        if nv is not None and float(nv) > 0:
                            weighted_numerator += sh_yi * float(nv)
                            weighted_denom += sh_yi
            if weighted_denom > 0:
                g_nav_vals.append(round(weighted_numerator / weighted_denom, 4))
            else:
                nv = fallback_map.get(dt)
                if nv is None and fallback_map:
                    prev_dates = [k for k in fallback_map.keys() if k <= dt]
                    if prev_dates:
                        nv = fallback_map[max(prev_dates)]
                g_nav_vals.append(round(float(nv), 4) if nv is not None else None)
        g["nav_series"] = g_nav_vals

    total_nav_vals = []
    for idx, dt in enumerate(b_dates):
        num = 0.0
        den = 0.0
        for g in groups:
            g_sh = normalize_arr(g.get("series") or [])
            sh_v = g_sh[idx] if idx < len(g_sh) else None
            nv_v = (g.get("nav_series") or [])[idx] if idx < len(g.get("nav_series") or []) else None
            if sh_v is not None and sh_v > 0 and nv_v is not None and nv_v > 0:
                num += sh_v * nv_v
                den += sh_v
        if den > 0:
            total_nav_vals.append(round(num / den, 4))
        else:
            nv_300 = etf_navs.get("510300", {}).get(dt)
            total_nav_vals.append(round(float(nv_300), 4) if nv_300 is not None else None)

    broad_payload["total_nav_series"] = total_nav_vals
    return broad_payload


def sync_data(force_full: bool = False) -> dict:
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
    etf_navs = cache.get("etf_navs", {})

    session = requests.Session()
    all_dates_set = set()

    for item in INDICES_META:
        code = item["code"]
        url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={code},day,{START_DATE},2029-12-31,1200,"
        resp = session.get(url, timeout=15).json()
        days = resp.get("data", {}).get(code, {}).get("day", [])
        series = indices_data.get(code, {})
        for d in days:
            if d[0] >= START_DATE:
                series[d[0]] = round(float(d[2]), 2)
                all_dates_set.add(d[0])
        indices_data[code] = {k: v for k, v in series.items() if k >= START_DATE}

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
                        out[sc] = round(float(x["TOT_VOL"]) / 10000.0, 4)
                return d, out
            except Exception:
                time.sleep(0.25 * (attempt + 1))
        return d, {}

    if missing_dates:
        print(f"[Sync] 正在从上交所同步 {len(missing_dates)} 个交易日的ETF规模份额数据...")
        with ThreadPoolExecutor(max_workers=16) as ex:
            for d, res in ex.map(fetch_sse_date, missing_dates):
                if res:
                    etf_by_date[d] = res

    valid_dates = sorted([d for d in etf_by_date.keys() if etf_by_date[d] and d >= START_DATE])

    broad_cache = sync_broad_market(force_full=force_full)
    broad_payload = build_broad_payload(broad_cache)

    nav_targets = [
        ("510050", "SH"),
        ("510300", "SH"),
        ("510500", "SH"),
        ("512100", "SH"),
        ("512050", "SH"),
        ("588000", "SH"),
        ("159915", "SZ"),
    ]
    for g in (broad_payload.get("groups") or []):
        for det in (g.get("details") or []):
            c = str(det.get("code") or "").strip()
            if c:
                nav_targets.append((c, str(det.get("exchange") or "")))
    etf_navs = fetch_etf_daily_navs(nav_targets, etf_navs, session)
    broad_payload = attach_series_nav(broad_payload, etf_navs)

    share_rows, nav_rows = [], []
    for d in valid_dates:
        for c, sh in etf_by_date.get(d, {}).items():
            if sh is not None:
                share_rows.append({"trade_date": d, "fund_code": c, "total_share": sh})
                nv = etf_navs.get(c, {}).get(d)
                if nv is not None:
                    nav_rows.append({"trade_date": d, "fund_code": c, "unit_nav": nv})
    if share_rows and nav_rows:
        detail_rows, summary_rows, anomaly_rows = compute_etf_flows_and_audit(
            share_rows=share_rows,
            nav_rows=nav_rows,
            event_rows=None,
            trading_dates=valid_dates,
            share_unit="yi",
        )
        detail_cols = [
            "trade_date", "fund_code", "share_date", "nav_date",
            "total_share", "adj_factor", "adj_share", "share_T", "share_T1",
            "delta_share", "unit_nav", "amount_yi", "is_cash_div",
        ]
        _write_csv(os.path.join(DATA_DIR, "etf_flow_detail.csv"), detail_rows, detail_cols)
        _write_csv(os.path.join(DATA_DIR, "etf_flow_summary.csv"), summary_rows, ["trade_date", "sum_amount_yi"])
        _write_csv(os.path.join(DATA_DIR, "etf_flow_anomalies.csv"), anomaly_rows, detail_cols + ["p99_thresh", "anomaly_reason"])

    new_cache = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "start_date": valid_dates[0] if valid_dates else START_DATE,
        "end_date": valid_dates[-1] if valid_dates else "",
        "total_days": len(valid_dates),
        "indices_meta": INDICES_META,
        "etfs_meta": ETFS_META,
        "dates": valid_dates,
        "indices_data": indices_data,
        "etf_by_date": {d: etf_by_date[d] for d in valid_dates},
        "etf_navs": etf_navs,
        "broad_market": broad_payload,
    }

    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(new_cache, f, ensure_ascii=False, indent=2)

    print(
        f"[Sync] 数据同步完成: {new_cache['start_date']} ~ {new_cache['end_date']} "
        f"(共 {new_cache['total_days']} 个交易日)"
    )
    return new_cache


def build_frontend_payload(cache: dict) -> dict:
    dates = cache["dates"]
    indices_series = {}
    for idx_meta in INDICES_META:
        c = idx_meta["code"]
        raw_map = cache["indices_data"].get(c, {})
        vals, last_val = [], None
        for d in dates:
            v = raw_map.get(d, last_val)
            if v is not None:
                last_val = v
            vals.append(v)
        indices_series[c] = vals

    etf_series = {}
    for etf_meta in ETFS_META:
        c = etf_meta["code"]
        vals, last_val = [], None
        for d in dates:
            v = cache["etf_by_date"].get(d, {}).get(c, last_val)
            if v is not None:
                last_val = v
            vals.append(round(v, 4) if v is not None else None)
        etf_series[c] = vals

    broad_payload = cache.get("broad_market", {})
    if broad_payload and not broad_payload.get("total_nav_series"):
        broad_payload = attach_series_nav(broad_payload, cache.get("etf_navs", {}))

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
        "broad_market": broad_payload,
    }


def generate_html(cache: dict) -> str:
    raise NotImplementedError("请保留你原有的 HTML_TEMPLATE 与 generate_html")


class TrackerRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=BASE_DIR, **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/index.html"):
            self.path = "/etf_share_tracker.html"
        return super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/sync":
            try:
                cache = sync_data(force_full=False)
                generate_html(cache)
                body = json.dumps(
                    {
                        "ok": True,
                        "start_date": cache["start_date"],
                        "end_date": cache["end_date"],
                        "total_days": cache["total_days"],
                        "updated_at": cache["updated_at"],
                    },
                    ensure_ascii=False,
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception as e:
                body = json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()


def main():
    parser = argparse.ArgumentParser(description="宽基ETF份额追踪与资金底背离监测系统")
    parser.add_argument("--serve", action="store_true", help="启动本地Web服务器")
    parser.add_argument("--port", type=int, default=8765, help="本地Web服务器端口 (默认: 8765)")
    parser.add_argument("--force-full", action="store_true", help="强制重新拉取全量历史数据")
    args = parser.parse_args()

    cache = sync_data(force_full=args.force_full)
    generate_html(cache)

    if args.serve:
        addr = ("127.0.0.1", args.port)
        httpd = ThreadingHTTPServer(addr, TrackerRequestHandler)
        print(f"[Server] 本地交互服务已启动: http://127.0.0.1:{args.port}")
        httpd.serve_forever()


if __name__ == "__main__":
    main()
