#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
coin_backfill.py
- Bybit v5 /market/kline로 1분봉("1")과 1일봉("D")를 수집
- Redis LIST에 저장(최신이 앞쪽: index 0)
- '현재 바' 기준 정확히 KEEP개(=300)만 유지하는 '시간 창(Window)' 방식
  -> 매 실행 때 해당 창에 맞는 캔들만 한 번에 가져와 임시 키에 쓰고 RENAME으로 스왑(원자적)
- 다심볼(BTCUSDT, ETHUSDT 등) 지원
"""

import os
import json
import time
import logging
from typing import List, Dict, Optional

import requests
from tenacity import retry, wait_exponential_jitter, stop_after_attempt, retry_if_exception_type
from dotenv import load_dotenv

# 네가 만든 redis 인스턴스(모듈에서 import)
from redis_client import redis_client as redis

# ───────────────────────────────────────────────────────────
# 설정
# ───────────────────────────────────────────────────────────
load_dotenv()

BYBIT_BASE = os.getenv("BYBIT_BASE", "https://api.bybit.com")
CATEGORY   = os.getenv("CATEGORY", "linear")            # 선물 kline은 보통 'linear'
KEEP       = int(os.getenv("KEEP", "300"))              # 유지 봉 수
LIMIT_PER_CALL = 1000                                   # Bybit v5 kline 최대

# 심볼 목록: ENV가 있으면 우선
# 예: SYMBOLS="BTCUSDT,ETHUSDT,SOLUSDT"
SYMBOLS_ENV = os.getenv("SYMBOLS", "")
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]

# ───────────────────────────────────────────────────────────
# 유틸
# ───────────────────────────────────────────────────────────

def key_list(symbol: str, interval: str) -> str:
    """메인 LIST 키 (최신이 0번)"""
    return f"kline:{symbol}:{interval}"

def key_last_ts(symbol: str, interval: str) -> str:
    """마지막(최신) 바의 time(초) 보관"""
    return f"{key_list(symbol, interval)}:last_ts"

def step_ms(interval: str) -> int:
    if interval == "1":
        return 60_000
    if interval == "D":
        return 86_400_000
    raise ValueError("interval must be '1' or 'D'")

def floor_cur_bar_start_ms(now_ms: int, interval: str) -> int:
    """현재 시각이 속한 바의 시작 시각(ms)"""
    s = step_ms(interval)
    return (now_ms // s) * s

def window_start_ms(now_ms: int, interval: str, keep: int) -> int:
    """
    유지할 시간 창의 시작 시각(ms).
    현재 바 시작에서 (keep-1)*step 만큼 과거로.
    """
    cur_ms = floor_cur_bar_start_ms(now_ms, interval)
    return cur_ms - (keep - 1) * step_ms(interval)

def bar_from_bybit_row(row: List[str]) -> Dict:
    # Bybit REST row: [start, open, high, low, close, volume, turnover]
    return {
        "time": int(int(row[0]) / 1000),  # seconds
        "open": float(row[1]),
        "high": float(row[2]),
        "low":  float(row[3]),
        "close":float(row[4]),
    }

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
    Bybit v5 /market/kline:
      - 요청당 최대 1000봉
      - start/end/limit을 같이 주면 구간 내 최대 limit봉 반환(일반적으로 최신→과거이지만
        안전하게 파싱 후 정렬해서 사용)
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

    if resp.status_code == 403:
        raise requests.HTTPError(f"403 Forbidden from upstream: {resp.text[:200]}")
    if resp.status_code in (429, 500, 502, 503, 504):
        raise UpstreamRetryError(f"{resp.status_code} retryable: {resp.text[:120]}")

    resp.raise_for_status()

    j = resp.json()
    rows = (j.get("result") or {}).get("list") or []
    bars = [bar_from_bybit_row(r) for r in rows]
    # 안전하게 오래→최신 정렬
    bars.sort(key=lambda b: b["time"])
    logging.info("fetch %s %s -> %d bars", symbol, interval, len(bars))
    return bars

def uniq_and_clip_to_window(
    bars_old_to_new: List[Dict],
    start_ms: int,
    end_ms: int,
) -> List[Dict]:
    """
    - 입력: 오래→최신 bars
    - 1) window [start_ms, end_ms] 경계 밖은 제거
    - 2) 같은 time(초) 중복은 마지막 값 보존
    - 반환: 오래→최신
    """
    if not bars_old_to_new:
        return []

    start_sec = start_ms // 1000
    end_sec   = end_ms // 1000

    dedup = {}
    for b in bars_old_to_new:
        t = int(b["time"])
        if t < start_sec or t > end_sec:
            continue
        dedup[t] = b
    return [dedup[t] for t in sorted(dedup.keys())]

def write_list_replace_atomically(symbol: str, interval: str, bars_old_to_new: List[Dict]) -> int:
    """
    - 임시 키에 전체 재작성 후 RENAME으로 원자적 교체
    - LIST는 최신이 0번이므로 LPUSH 시 역순으로 넣는다
    - last_ts도 세팅
    - 반환: 실제 저장된 개수
    """
    main_k = key_list(symbol, interval)
    tmp_k  = f"{main_k}:tmp"

    p = redis.pipeline()
    p.delete(tmp_k)
    p.execute()

    if bars_old_to_new:
        p = redis.pipeline()
        # 최신이 0번이 되도록 역순으로 LPUSH
        for b in reversed(bars_old_to_new):
            p.lpush(tmp_k, json.dumps(b))
        p.execute()

        # last_ts 저장(가장 최신 바의 time)
        last_ts = bars_old_to_new[-1]["time"]
        redis.set(key_last_ts(symbol, interval), int(last_ts))

    # 원자적 교체
    # (기존 main_k 없을 수도 있으니 RENAME NX 대신: 먼저 삭제 후 RENAME or use renamenx 조건부
    #  여기서는 그냥 기존 키 삭제 후 RENAME이 간단)
    p = redis.pipeline()
    p.delete(main_k)
    p.rename(tmp_k, main_k)
    res = p.execute()

    # 저장된 개수 반환
    return redis.llen(main_k)

# ───────────────────────────────────────────────────────────
# 메인 동기화 루틴 (시간 창 스냅샷)
# ───────────────────────────────────────────────────────────

def rebuild_time_window(symbol: str, interval: str, keep: int = KEEP):
    """
    현재 시각(now)을 기준으로 '현재 바 포함' keep개 만큼의 시간 창을 계산하고,
    그 창에 해당하는 캔들을 Bybit에서 한 방에 받아서(<=1000) Redis에 통째로 재작성한다.
    - 1분봉: 300분 → 300개
    - 1일봉: 300일 → 300개
    """
    now_ms = int(time.time() * 1000)
    start_ms = window_start_ms(now_ms, interval, keep)
    end_ms   = now_ms

    bars = fetch_bybit_klines(
        symbol=symbol,
        interval=interval,
        start_ms=start_ms,
        end_ms=end_ms,
        limit=LIMIT_PER_CALL,
    )
    bars = uniq_and_clip_to_window(bars, start_ms, end_ms)

    saved = write_list_replace_atomically(symbol, interval, bars)
    logging.info("[%s][%s] window %d ~ %d ⇒ saved %d bars",
                 symbol, interval, start_ms, end_ms, saved)

# ───────────────────────────────────────────────────────────
# 오케스트레이션
# ───────────────────────────────────────────────────────────

def update_symbols(symbols: List[str], intervals: List[str], keep: int = KEEP):
    for sym in symbols:
        sym = sym.upper()
        for iv in intervals:
            if iv not in ("1", "D"):
                logging.warning("skip invalid interval %s for %s", iv, sym)
                continue
            rebuild_time_window(sym, iv, keep=keep)
        # 심볼 간 짧은 텀(선택)
        time.sleep(0.2)

# ───────────────────────────────────────────────────────────
# 엔트리 포인트
# ───────────────────────────────────────────────────────────

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    # 연결 확인
    try:
        logging.info("PING: %s", redis.ping())
    except Exception as e:
        logging.error("Redis ping failed: %s", e)
        return

    # 심볼 목록
    if SYMBOLS_ENV.strip():
        symbols = [s.strip().upper() for s in SYMBOLS_ENV.split(",") if s.strip()]
    else:
        symbols = DEFAULT_SYMBOLS[:]

    intervals = ["1", "D"]

    logging.info("symbols=%s intervals=%s keep=%d", symbols, intervals, KEEP)
    update_symbols(symbols, intervals, keep=KEEP)

if __name__ == "__main__":
    main()
