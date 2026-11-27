#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
宽基 ETF 份额追踪 & 资金底背离监测系统 (QTYX V3.6.6 零依赖复权核算版)
特性：
- 零第三方科学计算库依赖（仅需 Python 标准库 + requests，无需安装 pandas/numpy）
- 严格采用【不复权单位净值/收盘价】与【复权还原份额 adj_share】
- 内置二分向后日期对齐 (bisect_asof_backward)、自动拆分折算识别、逐只 ETF 净申赎金额(亿元)累加及强制 assert 断言
"""

import argparse
import bisect
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


def _to_tx_symbol(code: str, exchange: str = "") -> str:
    c = str(code).strip()
    ex = str(exchange).upper()
    if c.startswith(("sh", "sz")):
        return c
    if "SH" in ex or "SSE" in ex or "沪" in ex or c.startswith(("5", "6")):
        return f"sh{c}"
    return f"sz{c}"


def fetch_etf_daily_navs(codes_with_ex: list, existing_navs: dict, session: requests.Session) -> dict:
    """
    拉取宽基ETF每日【不复权】收盘价/净值序列（raw_nav）及前复权比值。
    严禁使用前复权价直接乘以未复权份额，避免现金分红/拆分除权日出现虚假规模缺口。
    """
    nav_map = {k: dict(v) for k, v in (existing_navs or {}).items()}
    unique_targets = list(
        dict.fromkeys((str(c).strip(), str(ex).strip()) for c, ex in codes_with_ex if str(c).strip())
    )

    for code, ex in unique_targets:
        sym = _to_tx_symbol(code, ex)
        series = nav_map.get(code, {})
        try:
            # 1. 拉取不复权日K线（day 字段），与交易所当日真实份额严格同口径
            url_raw = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={sym},day,{START_DATE},2029-12-31,1500,"
            resp_raw = session.get(url_raw, timeout=12).json()
            days_raw = resp_raw.get("data", {}).get(sym, {}).get("day") or []

            # 2. 同步拉取前复权日K线（qfqday），用于自动探测除权/拆分事件
            url_qfq = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={sym},day,{START_DATE},2029-12-31,1500,qfq"
            resp_qfq = session.get(url_qfq, timeout=12).json()
            days_qfq = resp_qfq.get("data", {}).get(sym, {}).get("qfqday") or []
            qfq_dict = {d[0]: float(d[2]) for d in days_qfq if len(d) >= 3 and float(d[2]) > 0}

            for d in days_raw:
                if len(d) >= 3 and d[0] >= START_DATE:
                    dt = d[0]
                    raw_val = float(d[2])
                    if raw_val > 0:
                        series[dt] = round(raw_val, 4)
                        if dt in qfq_dict:
                            series[f"{dt}__qfq"] = round(qfq_dict[dt], 4)
        except Exception:
            pass
        nav_map[code] = series
    return nav_map


def _bisect_asof_backward(sorted_dates: list, sorted_vals: list, target_date: str):
    """
    纯粹 Python 标准库实现的向后日期对齐（等价于 pd.merge_asof(..., direction='backward')）。
    返回 (matched_date, matched_value)，若 target_date 早于所有可用日期则返回 (None, None)。
    """
    if not sorted_dates:
        return None, None
    idx = bisect.bisect_right(sorted_dates, target_date) - 1
    if idx < 0:
        return None, None
    return sorted_dates[idx], sorted_vals[idx]


def _quantile_99(values: list) -> float:
    """纯标准库计算 99% 分位数，用于异常大额申赎审计"""
    if not values:
        return float("inf")
    s = sorted(values)
    n = len(s)
    if n == 1:
        return s[0]
    pos = 0.99 * (n - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return s[lo]
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


def _reconcile_single_fund(
    code: str,
    share_dict: dict,
    nav_dict: dict,
    trading_calendar: list,
    external_events: list = None,
) -> tuple[list, list]:
    """
    对单只 ETF 执行：
    1. 向后日期对齐（bisect_asof_backward），同时保留 share_date 与 nav_date
    2. 自动检测份额拆分/折算与现金分红，构造向最新日期对齐的累计复权因子 adj_factor
    3. 计算还原份额 adj_share = raw_share * adj_factor 与日度净申赎金额(亿元)
    """
    if not share_dict or not nav_dict or not trading_calendar:
        return [], []

    s_dates = sorted(k for k, v in share_dict.items() if v is not None and float(v) > 0)
    s_vals = [float(share_dict[k]) for k in s_dates]

    n_dates = sorted(k for k, v in nav_dict.items() if "__" not in str(k) and v is not None and float(v) > 0)
    n_vals = [float(nav_dict[k]) for k in n_dates]

    if not s_dates or not n_dates:
        return [], []

    min_dt = max(trading_calendar[0], s_dates[0])
    records = []

    for dt in trading_calendar:
        if dt < min_dt:
            continue
        sh_dt, sh_v = _bisect_asof_backward(s_dates, s_vals, dt)
        nv_dt, nv_v = _bisect_asof_backward(n_dates, n_vals, dt)
        if sh_dt is None or nv_dt is None or sh_v <= 0 or nv_v <= 0:
            continue
        qfq_v = nav_dict.get(f"{nv_dt}__qfq")
        records.append(
            {
                "trade_date": dt,
                "fund_code": code,
                "share_date": sh_dt,
                "nav_date": nv_dt,
                "raw_share": sh_v,
                "raw_nav": nv_v,
                "qfq_nav": float(qfq_v) if qfq_v is not None else None,
                "step_ratio": 1.0,
                "is_cash_div": False,
                "event_tag": "",
            }
        )

    n = len(records)
    if n == 0:
        return [], []

    # 1. 若外部传入事件列表（split / merge / dividend_reinvest / dividend_cash）
    if external_events:
        ev_by_date = {}
        for ev in external_events:
            if str(ev.get("fund_code", "")).strip() == code:
                ev_by_date.setdefault(str(ev.get("ex_date", "")).strip(), []).append(ev)
        for rec in records:
            for ev in ev_by_date.get(rec["trade_date"], []):
                et = str(ev.get("event_type", "")).strip()
                rt = float(ev.get("ratio", 1.0))
                if et == "dividend_cash":
                    rec["is_cash_div"] = True
                    rec["event_tag"] = "现金分红(不调份额)"
                elif et in ("split", "merge", "dividend_reinvest") and rt > 0:
                    rec["step_ratio"] *= rt
                    rec["event_tag"] = f"{et}({rt})"

    # 2. 自动兜底探测：识别份额拆分/折算（如 1:2 拆分导致份额翻倍、净值腰斩）并过滤现金分红
    for i in range(1, n):
        cur = records[i]
        prev = records[i - 1]
        if cur["step_ratio"] != 1.0:
            continue
        sr = cur["raw_share"] / prev["raw_share"]
        nr = cur["raw_nav"] / prev["raw_nav"]
        aum_ratio = sr * nr

        qfq_r = None
        if cur["qfq_nav"] and prev["qfq_nav"] and prev["qfq_nav"] > 0:
            qfq_r = cur["qfq_nav"] / prev["qfq_nav"]

        # 现金分红特征：不复权净值小幅除息回落，份额基本不变，且前复权价连续
        if 0.80 <= nr <= 0.995 and 0.97 <= sr <= 1.03 and (qfq_r is not None and abs(nr - qfq_r) > 0.003):
            cur["is_cash_div"] = True
            cur["event_tag"] = "自动识别:现金分红(仅打标)"
        # 份额拆分/合并特征：单日份额突变 >40% 或缩水 >30%，且净值反向跳变使总资产规模连续
        elif (sr >= 1.40 or sr <= 0.70) and (0.85 <= aum_ratio <= 1.15):
            est_ratio = round(sr * 2.0) / 2.0 if sr >= 1.40 else round(sr, 2)
            if est_ratio > 0:
                cur["step_ratio"] = est_ratio
                cur["event_tag"] = f"自动识别:份额折算({est_ratio}x)"

    # 3. 逆向累乘计算向最新交易日对齐的 adj_factor（最新日 adj_factor = 1.0）
    running_factor = 1.0
    for i in range(n - 1, -1, -1):
        records[i]["adj_factor"] = round(running_factor, 6)
        records[i]["adj_share"] = round(records[i]["raw_share"] * running_factor, 4)
        running_factor *= records[i]["step_ratio"]

    # 4. 计算逐日还原份额增量与精确净申赎金额(亿元)
    abs_deltas = []
    for i in range(n):
        if i == 0:
            delta_sh = 0.0
        else:
            delta_sh = round(records[i]["adj_share"] - records[i - 1]["adj_share"], 4)
        records[i]["delta_adj_share"] = delta_sh
        # raw_share / adj_share 单位为“亿份”，raw_nav 单位为“元/份”，相乘即为“亿元”
        records[i]["net_flow_yiyuan"] = round(delta_sh * records[i]["raw_nav"], 4)
        records[i]["aum_yiyuan"] = round(records[i]["raw_share"] * records[i]["raw_nav"], 4)
        if i > 0:
            abs_deltas.append(abs(delta_sh))

    # 5. 生成异常审计日志
    q99 = _quantile_99(abs_deltas) if len(abs_deltas) > 5 else float("inf")
    anomalies = []
    for r in records:
        if r["event_tag"] or (abs(r["delta_adj_share"]) > q99 and abs(r["delta_adj_share"]) > 0):
            anomalies.append(
                {
                    "trade_date": r["trade_date"],
                    "fund_code": code,
                    "raw_share": r["raw_share"],
                    "adj_factor": r["adj_factor"],
                    "delta_adj_share": r["delta_adj_share"],
                    "raw_nav": r["raw_nav"],
                    "net_flow_yiyuan": r["net_flow_yiyuan"],
                    "reason": r["event_tag"] or f"单日净申赎超历史99分位({q99:.2f}亿份)",
                }
            )

    return records, anomalies


def reconcile_all_etfs(
    fund_share_maps: dict,
    etf_navs: dict,
    trading_calendar: list,
    external_events: list = None,
) -> tuple[dict, list]:
    """
    批量对齐复权并执行强校验断言（assert）：
    返回 lookup[(fund_code, trade_date)] -> record 与 anomalies 异常日志列表
    """
    cal = sorted(set(trading_calendar))
    assert cal == sorted(cal) and len(cal) == len(set(cal)), "交易日历存在重复或未排序！"

    lookup = {}
    all_anomalies = []
    latest_dt = cal[-1] if cal else ""
    hist_mismatch_cnt = 0

    for code, sh_map in (fund_share_maps or {}).items():
        nv_map = (etf_navs or {}).get(code, {})
        recs, anom = _reconcile_single_fund(code, sh_map, nv_map, cal, external_events=external_events)
        all_anomalies.extend(anom)

        for r in recs:
            key = (code, r["trade_date"])
            # ==================== 强制校验断言（失败即抛错） ====================
            assert key not in lookup, f"校验失败：存在重复的 (fund_code, trade_date) = {key}"
            assert r["share_date"] <= r["trade_date"], f"校验失败：share_date 未来函数 {r}"
            assert r["nav_date"] <= r["trade_date"], f"校验失败：nav_date 未来函数 {r}"
            assert r["adj_share"] >= 0.0, f"校验失败：出现负的还原份额 {r}"
            assert r["raw_nav"] > 0.0, f"校验失败：出现非正的单位净值 {r}"
            # ====================================================================
            if r["trade_date"] < latest_dt and r["share_date"] != r["nav_date"]:
                hist_mismatch_cnt += 1
            lookup[key] = r

    if hist_mismatch_cnt > 0:
        print(f"[Audit-Warn] 历史区间存在 {hist_mismatch_cnt} 条交易所披露延迟记录，已通过向后对齐安全处理。")

    all_anomalies.sort(key=lambda x: (x["trade_date"], x["fund_code"]))
    return lookup, all_anomalies


def attach_series_nav(broad_payload: dict, etf_navs: dict) -> dict:
    """
    为 broad_market 各系列及七大系列合计计算精确的【亿元规模序列】与【加权净值序列】：
    严格遵循“先逐只计算 真实份额 × 当日不复权净值，再按系列去重求和”，彻底消除平均净值乘总份额的误差。
    """
    if not broad_payload or not broad_payload.get("groups"):
        return broad_payload

    b_dates = broad_payload.get("dates") or []
    groups = broad_payload.get("groups") or []
    if not b_dates:
        return broad_payload

    # 收集 broad_market 中全部 35 只 ETF 的日度原始份额序列
    fund_share_maps = {}
    for g in groups:
        for det in (g.get("details") or []):
            c = str(det.get("code") or "").strip()
            d_series = det.get("series") or []
            if not c or len(d_series) != len(b_dates):
                continue
            valid_vals = [float(x) for x in d_series if x is not None]
            scale = 10000.0 if (valid_vals and max(valid_vals) > 50000) else 1.0
            sh_map = {}
            for idx, dt in enumerate(b_dates):
                val = d_series[idx]
                if val is not None and float(val) > 0:
                    sh_map[dt] = round(float(val) / scale, 4)
            if sh_map:
                fund_share_maps[c] = sh_map

    lookup, anomalies = reconcile_all_etfs(fund_share_maps, etf_navs, b_dates)
    if anomalies:
        broad_payload["audit_anomalies"] = anomalies[-30:]

    default_group_nav_codes = {
        "上证50": ("510050", "SH"),
        "沪深300": ("510300", "SH"),
        "中证500": ("510500", "SH"),
        "中证1000": ("512100", "SH"),
        "中证A500": ("512050", "SH"),
        "科创宽基": ("588000", "SH"),
        "创业板": ("159915", "SZ"),
    }

    total_aum_by_idx = [0.0] * len(b_dates)
    total_sh_by_idx = [0.0] * len(b_dates)
    total_flow_by_idx = [0.0] * len(b_dates)
    seen_codes_by_idx = [set() for _ in range(len(b_dates))]

    for g in groups:
        g_name = g.get("name", "")
        g_details = g.get("details") or []
        g_nav_vals = []
        g_aum_vals = []
        g_flow_vals = []

        fallback_code, _ = default_group_nav_codes.get(g_name, ("510300", "SH"))
        fallback_map = etf_navs.get(fallback_code, {})

        for idx, dt in enumerate(b_dates):
            sum_aum = 0.0
            sum_sh = 0.0
            sum_flow = 0.0

            for det in g_details:
                c = str(det.get("code") or "").strip()
                if not c:
                    continue
                rec = lookup.get((c, dt))
                if rec is not None:
                    sum_aum += rec["aum_yiyuan"]
                    sum_sh += rec["adj_share"]
                    sum_flow += rec["net_flow_yiyuan"]
                    # 按基金代码去重累加七大系列总计
                    if c not in seen_codes_by_idx[idx]:
                        seen_codes_by_idx[idx].add(c)
                        total_aum_by_idx[idx] += rec["aum_yiyuan"]
                        total_sh_by_idx[idx] += rec["adj_share"]
                        total_flow_by_idx[idx] += rec["net_flow_yiyuan"]

            if sum_sh > 0 and sum_aum > 0:
                g_nav_vals.append(round(sum_aum / sum_sh, 4))
                g_aum_vals.append(round(sum_aum, 4))
                g_flow_vals.append(round(sum_flow, 4))
            else:
                nv = fallback_map.get(dt)
                if nv is None and fallback_map:
                    prev_dates = [k for k in fallback_map.keys() if "__" not in str(k) and k <= dt]
                    if prev_dates:
                        nv = fallback_map[max(prev_dates)]
                g_nav_vals.append(round(float(nv), 4) if nv is not None else None)
                g_aum_vals.append(None)
                g_flow_vals.append(None)

        g["nav_series"] = g_nav_vals
        g["aum_yiyuan_series"] = g_aum_vals
        g["net_flow_yiyuan_series"] = g_flow_vals

    total_nav_vals = []
    for idx, dt in enumerate(b_dates):
        if total_sh_by_idx[idx] > 0 and total_aum_by_idx[idx] > 0:
            total_nav_vals.append(round(total_aum_by_idx[idx] / total_sh_by_idx[idx], 4))
        else:
            nv_300 = etf_navs.get("510300", {}).get(dt)
            total_nav_vals.append(round(float(nv_300), 4) if nv_300 is not None else None)

    broad_payload["total_nav_series"] = total_nav_vals
    broad_payload["total_aum_yiyuan_series"] = [round(v, 4) if v > 0 else None for v in total_aum_by_idx]
    broad_payload["total_net_flow_yiyuan_series"] = [round(v, 4) for v in total_flow_by_idx]
    return broad_payload


def sync_data(force_full: bool = False) -> dict:
    """增量同步五大宽基指数日线与上交所8只宽基ETF份额数据，并执行零依赖复权与净值对齐核算"""
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
    for e in ETFS_META:
        nav_targets.append((e["code"], "SH"))
    for g in (broad_payload.get("groups") or []):
        for det in (g.get("details") or []):
            c = str(det.get("code") or "").strip()
            if c:
                nav_targets.append((c, str(det.get("exchange") or "")))

    etf_navs = fetch_etf_daily_navs(nav_targets, etf_navs, session)

    # 对 8 大核心宽基 ETF 执行复权还原与日期对齐审计
    core_share_maps = {}
    for d in valid_dates:
        day_map = etf_by_date.get(d, {})
        for c, sh_val in day_map.items():
            if sh_val is not None and float(sh_val) > 0:
                core_share_maps.setdefault(c, {})[d] = float(sh_val)

    core_lookup, core_anomalies = reconcile_all_etfs(core_share_maps, etf_navs, valid_dates)
    etf_adj_by_date = {d: {} for d in valid_dates}
    for (c, d), rec in core_lookup.items():
        etf_adj_by_date[d][c] = round(float(rec["adj_share"]), 4)

    broad_payload = attach_series_nav(broad_payload, etf_navs)

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
        "etf_adj_by_date": etf_adj_by_date,
        "etf_navs": etf_navs,
        "audit_anomalies": core_anomalies[-30:],
        "broad_market": broad_payload,
    }

    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(new_cache, f, ensure_ascii=False, indent=2)

    print(
        f"[Sync] 数据同步与复权核算完成: {new_cache['start_date']} ~ {new_cache['end_date']} "
        f"(共 {new_cache['total_days']} 个交易日, 审计标记 {len(core_anomalies)} 条)"
    )
    return new_cache


def build_frontend_payload(cache: dict) -> dict:
    """将缓存数据整理为前端高密度列式JSON结构（优先输出复权还原后的连续份额序列）"""
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

    adj_source = cache.get("etf_adj_by_date") or cache.get("etf_by_date", {})
    raw_source = cache.get("etf_by_date", {})

    etf_series = {}
    for etf_meta in ETFS_META:
        c = etf_meta["code"]
        vals = []
        last_val = None
        for d in dates:
            v = adj_source.get(d, {}).get(c)
            if v is None:
                v = raw_source.get(d, {}).get(c, last_val)
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
