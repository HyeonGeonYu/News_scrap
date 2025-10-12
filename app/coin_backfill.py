#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bybit Kline Backfill (personal server)
- REST로 과거 캔들 시드/백필
- Redis LIST (최신이 앞: LPUSH) + LTRIM keep
- 인터벌 × 1000 범위로 한 번에 가져와 호출 수 최소화
- 다심볼(BTCUSDT, ETHUSDT 등) 순차 처리
"""

import os
import json
import time
import logging
from typing import List, Dict, Optional

import requests
from tenacity import retry, wait_exponential_jitter, stop_after_attempt, retry_if_exception_type
from dotenv import load_dotenv

# 네가 만든 redis 인스턴스 객체(함수 아님!)
from redis_client import redis_client

# ───────────────────────────────────────────────────────────
# 설정
# ───────────────────────────────────────────────────────────
load_dotenv()

BYBIT_BASE = os.getenv("BYBIT_BASE", "https://api.bybit.com")
CATEGORY   = os.getenv("CATEGORY", "linear")
KEEP       = int(os.getenv("KEEP", "300"))

# 심볼 목록: ENV 우선, 없으면 기본값
# 예: SYMBOLS="BTCUSDT,ETHUSDT,SOLUSDT"
SYMBOLS_ENV = os.getenv("SYMBOLS", "")
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]

LIMIT_PER_CALL = 1000  # Bybit v5 kline 최대

# ───────────────────────────────────────────────────────────
# 유틸
# ───────────────────────────────────────────────────────────

def key_kline(symbol: str, interval: str) -> str:
    """LIST 키 (최신이 앞쪽)"""
    return f"kline:{symbol}:{interval}"

def bar_from_bybit_row(row: List[str]) -> Dict:
    # Bybit REST row: [start, open, high, low, close, volume, turnover]
    return {
        "time": int(int(row[0]) / 1000),  # seconds
        "open": float(row[1]),
        "high": float(row[2]),
        "low":  float(row[3]),
        "close":float(row[4]),
    }

def latest_ts_sec(rds, symbol: str, interval: str) -> Optional[int]:
    """LIST의 0번(가장 최신) 원소의 time 반환. 없으면 None."""
    last = rds.lindex(key_kline(symbol, interval), 0)
    if not last:
        return None
    try:
        return json.loads(last)["time"]
    except Exception:
        return None

def push_bars(rds, symbol: str, interval: str, bars_old_to_new: List[Dict], keep: int = KEEP):
    """
    bars_old_to_new: 오래→최신 순서 리스트
    Redis에는 최신이 앞쪽이 되도록 '역순'으로 LPUSH
    """
    if not bars_old_to_new:
        return
    k = key_kline(symbol, interval)
    pipe = rds.pipeline()
    for b in reversed(bars_old_to_new):
        pipe.lpush(k, json.dumps(b))
    pipe.ltrim(k, 0, keep - 1)
    pipe.set(f"{k}:last_ts", bars_old_to_new[-1]["time"])
    pipe.execute()
    # 1분봉은 요약 로그, 일봉은 디버그로
    if interval == "1":
        logging.info("push %s %s +%d (keep=%d)", symbol, interval, len(bars_old_to_new), keep)
    else:
        logging.debug("push %s %s +%d", symbol, interval, len(bars_old_to_new))

def interval_step_ms(interval: str) -> int:
    if interval == "1":
        return 60_000
    elif interval == "D":
        return 86_400_000
    else:
        raise ValueError("interval must be '1' or 'D'")

class UpstreamRetryError(Exception):
    pass

@retry(
    wait=wait_exponential_jitter(initial=1, max=15),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((requests.RequestException, UpstreamRetryError)),
    reraise=True,
)
def fetch_bybit_klines(
    symbol: str,
    interval: str,
    start_ms: Optional[int],
    end_ms: Optional[int],
    limit: int = LIMIT_PER_CALL,
) -> List[Dict]:
    """
    Bybit v5 kline: 요청당 최대 1000봉.
    start/end 범위와 limit을 함께 주면, 보통 최신→과거 순으로 최대 limit개 반환.
    """
    params = {
        "category": CATEGORY,
        "symbol": symbol,
        "interval": interval,
        "limit": str(limit),
    }
    if start_ms is not None:
        params["start"] = str(start_ms)
    if end_ms is not None:
        params["end"]   = str(end_ms)

    headers = {"accept": "application/json"}
    url = f"{BYBIT_BASE}/v5/market/kline"
    resp = requests.get(url, params=params, headers=headers, timeout=15)

    # 상태 코드별 분기
    if resp.status_code == 403:
        raise requests.HTTPError(f"403 Forbidden from upstream: {resp.text[:200]}")
    if resp.status_code in (429, 500, 502, 503, 504):
        raise UpstreamRetryError(f"{resp.status_code} retryable: {resp.text[:120]}")

    resp.raise_for_status()

    j = resp.json()
    rows = (j.get("result") or {}).get("list") or []
    bars = [bar_from_bybit_row(row) for row in rows]
    bars.sort(key=lambda b: b["time"])  # 오래→최신
    logging.info("fetch %s %s -> %d bars", symbol, interval, len(bars))
    return bars

# ───────────────────────────────────────────────────────────
# 백필
# ───────────────────────────────────────────────────────────

def backfill_interval(
    rds,
    symbol: str,
    interval: str,
    seed_if_empty: bool = True,
    keep: int = KEEP,
):
    """
    - 비어 있으면 최신 keep개 시드
    - 있으면 마지막 저장 시각 이후만 백필
    - 루프당 범위: (인터벌 길이 × LIMIT_PER_CALL)
    """
    step_ms = interval_step_ms(interval)
    now_ms  = int(time.time() * 1000)
    last_ts = latest_ts_sec(rds, symbol, interval)

    # 초기 시드
    if last_ts is None:
        if not seed_if_empty:
            logging.info("[%s][%s] empty and no-seed", symbol, interval)
            return
        logging.info("[%s][%s] seeding latest %d", symbol, interval, keep)
        bars = fetch_bybit_klines(symbol, interval, start_ms=None, end_ms=now_ms, limit=keep)
        # 안전 보정: 오래→최신 + 중복 제거(의미상 last_ts 없음)
        bars = uniq_sorted_after(bars, last_ts=None)
        # 혹시 API가 limit보다 더 줬다면 마지막 keep개만

        if len(bars) > keep:
           bars = bars[-keep:]
        push_bars(rds, symbol, interval, bars, keep=keep)
        return

    # 누락 구간 백필
    cursor = (last_ts * 1000) + step_ms  # 마지막 저장 이후부터
    appended = 0
    while cursor < now_ms:
        range_ms = step_ms * LIMIT_PER_CALL
        end = min(cursor + range_ms, now_ms)
        logging.info("[%s][%s] backfill %d ~ %d", symbol, interval, cursor, end)

        try:
            bars = fetch_bybit_klines(symbol, interval, start_ms=cursor, end_ms=end, limit=LIMIT_PER_CALL)
        except Exception as e:
            logging.warning("[%s][%s] fetch error: %s", symbol, interval, e)
            cursor = end
            continue

        bars = uniq_sorted_after(bars, last_ts=last_ts)
        if not bars:
            cursor = end
            continue

        push_bars(rds, symbol, interval, bars, keep=keep)
        appended += len(bars)
        last_ts = bars[-1]["time"]  # 최신 기준 업데이트
        cursor = (bars[-1]["time"] * 1000) + step_ms

    logging.info("[%s][%s] backfill done. +~%d bars", symbol, interval, appended)

def coin_backfill_symbols(rds, symbols: List[str], intervals: List[str]):
    """다심볼 순차 처리(과도한 병렬 없이 레이트리밋 여유 확보)"""
    for sym in symbols:
        for iv in intervals:
            if iv not in ("1", "D"):
                logging.warning("skip invalid interval %s for %s", iv, sym)
                continue
            backfill_interval(
                rds=rds,
                symbol=sym,
                interval=iv,
                seed_if_empty=True,
                keep=KEEP,
            )
        # 심볼 사이 약간의 간격(필요시)
        time.sleep(0.2)
def uniq_sorted_after(bars_old_to_new: List[Dict], last_ts: Optional[int]) -> List[Dict]:
    """
    - 입력: 오래→최신 bars (time: seconds)
    - 1) time 오름차순 재정렬(안전)
    - 2) last_ts 이하 제거(= 이미 저장된 것들 제거)
    - 3) 같은 time 중복 제거(마지막 값 보존)
    - 반환: 오래→최신
    """
    if not bars_old_to_new:
        return []

    # 1) 정렬(이미 정렬돼 있어도 idempotent)
    bars = sorted(bars_old_to_new, key=lambda b: b["time"])

    # 2) last_ts 기준 필터
    if last_ts is not None:
        bars = [b for b in bars if b["time"] > last_ts]

    # 3) 같은 time 중복 제거(마지막 것 유지)
    dedup = {}
    for b in bars:
        dedup[b["time"]] = b
    # 오래→최신으로 다시 정렬
    return [dedup[t] for t in sorted(dedup.keys())]


# ───────────────────────────────────────────────────────────
# 엔트리포인트
# ───────────────────────────────────────────────────────────

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    rds = redis_client  # 네 모듈의 인스턴스(호출 X)

    # 연결 확인(선택)
    try:
        logging.info("PING: %s", rds.ping())
    except Exception as e:
        logging.error("Redis ping failed: %s", e)
        return

    # 심볼 목록
    if SYMBOLS_ENV.strip():
        symbols = [s.strip() for s in SYMBOLS_ENV.split(",") if s.strip()]
    else:
        symbols = DEFAULT_SYMBOLS[:]

    intervals = ["1", "D"]

    logging.info("symbols=%s intervals=%s keep=%d", symbols, intervals, KEEP)
    backfill_symbols(rds, symbols, intervals)

if __name__ == "__main__":
    main()
