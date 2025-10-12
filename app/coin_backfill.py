#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
coin_backfill.py (B 방식 전용)
- 심볼 여러 개의 '현재 시각 기준 최신 창(KEEP개)'을 Bybit에서 받아
  Redis HASH 한 키에 JSON으로 일괄 저장(HSET 1회).
- 분봉(1) / 일봉(D) 완전 분리 운용을 권장: 분봉은 매분, 일봉은 하루 1회.

실행 테스트(단발성):
    python coin_backfill.py --intervals 1,D --symbols BTCUSDT,ETHUSDT --keep 300
"""

import os
import json
import time
import logging
import argparse
from typing import List, Dict, Optional

import requests
from tenacity import retry, wait_exponential_jitter, stop_after_attempt, retry_if_exception_type
from dotenv import load_dotenv

# Redis 클라이언트
from redis_client import redis_client as redis_client

load_dotenv()

BYBIT_BASE = os.getenv("BYBIT_BASE", "https://api.bybit.com")
CATEGORY   = os.getenv("CATEGORY", "linear")      # 보통 'linear'
KEEP       = int(os.getenv("KEEP", "300"))        # 유지 봉 수
LIMIT_PER_CALL = 1000

# 압축 저장 옵션(선택): 1이면 zlib+base64로 압축 저장 (메모리/네트워크 절감)
COMPRESS_JSON = os.getenv("COMPRESS_JSON", "0") == "1"
if COMPRESS_JSON:
    import zlib, base64

def dumps_compact(obj) -> str:
    s = json.dumps(obj, separators=(',', ':')).encode('utf-8')
    if not COMPRESS_JSON:
        return s.decode('utf-8')
    comp = zlib.compress(s, level=6)
    return base64.b64encode(comp).decode('ascii')

def loads_compact(s: bytes) -> List[Dict]:
    if s is None:
        return []
    if isinstance(s, bytes):
        s = s.decode('utf-8')
    if not COMPRESS_JSON:
        return json.loads(s)
    comp = base64.b64decode(s.encode('ascii'))
    raw = zlib.decompress(comp)
    return json.loads(raw.decode('utf-8'))

# ───────────────────────────────────────────────────────────
# Bybit HTTP
# ───────────────────────────────────────────────────────────

http = requests.Session()
http.headers.update({"accept": "application/json"})

class UpstreamRetryError(Exception):
    pass

def step_ms(interval: str) -> int:
    if interval == "1":
        return 60_000
    if interval == "D":
        return 86_400_000
    raise ValueError("interval must be '1' or 'D'")

def floor_cur_bar_start_ms(now_ms: int, interval: str) -> int:
    s = step_ms(interval)
    return (now_ms // s) * s

def window_start_ms(now_ms: int, interval: str, keep: int) -> int:
    cur_ms = floor_cur_bar_start_ms(now_ms, interval)
    return cur_ms - (keep - 1) * step_ms(interval)

def bar_from_bybit_row(row: List[str]) -> Dict:
    # [start, open, high, low, close, volume, turnover]
    return {
        "time":  int(int(row[0]) / 1000),  # sec
        "open":  float(row[1]),
        "high":  float(row[2]),
        "low":   float(row[3]),
        "close": float(row[4]),
    }

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
    params = {
        "category": CATEGORY,
        "symbol": symbol,
        "interval": interval,
        "limit": str(limit),
    }
    if start_ms is not None:
        params["start"] = str(start_ms)
    if end_ms is not None:
        params["end"] = str(end_ms)
    url = f"{BYBIT_BASE}/v5/market/kline"
    resp = http.get(url, params=params, timeout=15)
    if resp.status_code == 403:
        raise requests.HTTPError(f"403 Forbidden: {resp.text[:200]}")
    if resp.status_code in (429, 500, 502, 503, 504):
        raise UpstreamRetryError(f"{resp.status_code} retryable: {resp.text[:120]}")
    resp.raise_for_status()
    j = resp.json()
    rows = (j.get("result") or {}).get("list") or []
    bars = [bar_from_bybit_row(r) for r in rows]
    bars.sort(key=lambda b: b["time"])  # 오래→최신
    return bars

# ───────────────────────────────────────────────────────────
# B 방식: 배치 JSON을 HASH에 HSET 1회로 저장
# ───────────────────────────────────────────────────────────

def _hash_key(interval: str) -> str:
    # 분봉: kline:1:json / 일봉: kline:D:json
    return f"kline:{interval}:json"

def replace_windows_batch_json(redis_client, symbols: List[str], interval: str, keep: int = KEEP):
    """
    모든 심볼에 대해 '현재 창(KEEP개)'을 Bybit에서 받아
    하나의 HASH에 HSET 1회로 기록.
    - 키: kline:{interval}:json
    - 필드: <SYMBOL>, last_ts:<SYMBOL>, __schema_version, __updated_at
    - 내부 Redis write: HSET 1회
    """
    now_ms = int(time.time() * 1000)
    start_ms = window_start_ms(now_ms, interval, keep)

    mapping = {
        "__schema_version": "1",
        "__updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

    for sym in symbols:
        bars = fetch_bybit_klines(sym, interval, start_ms, now_ms, limit=keep)
        bars.sort(key=lambda b: b["time"])
        mapping[sym] = dumps_compact(bars)
        mapping[f"last_ts:{sym}"] = str(bars[-1]["time"] if bars else 0)

    # HSET 1회 (mapping 전체 일괄)
    redis_client.hset(_hash_key(interval), mapping=mapping)

# ───────────────────────────────────────────────────────────
# 소비자 헬퍼 (읽기)
# ───────────────────────────────────────────────────────────

def load_window(redis_client, symbol: str, interval: str):
    key = _hash_key(interval)
    raw = redis_client.hget(key, symbol)   # 1 call
    bars = loads_compact(raw) if raw else []
    last_ts = bars[-1]["time"] if bars else 0
    return bars, last_ts

def load_windows(redis_client, symbols, interval):
    key = _hash_key(interval)
    raws = redis_client.hmget(key, symbols)   # 1 call
    out = {}
    for sym, raw in zip(symbols, raws):
        bars = loads_compact(raw) if raw else []
        out[sym] = (bars, bars[-1]["time"] if bars else 0)
    return out

# ───────────────────────────────────────────────────────────
# 단발성 실행 테스트용 main
# ───────────────────────────────────────────────────────────


def main():
    symbols = ["BTCUSDT","ETHUSDT"]
    intervals = ["1","D"]
    keep = 300
    logging.basicConfig(
        level=getattr(logging, "INFO", logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    log = logging.getLogger("coin_backfill_test")

    # Redis 연결 확인
    try:
        pong = redis_client.ping()
        log.info("Redis PING: %s", pong)
    except Exception as e:
        log.exception("Redis ping failed: %s", e)
        return

    log.info("START single-run test | symbols=%s intervals=%s keep=%d COMPRESS_JSON=%s",
             symbols, intervals, keep, COMPRESS_JSON)

    for iv in intervals:
        if iv not in ("1", "D"):
            log.warning("Skip invalid interval: %s (only '1' or 'D')", iv)
            continue
        try:
            replace_windows_batch_json(redis_client, symbols, interval=iv, keep=keep)
            log.info("✅ HSET batch done for interval=%s (write=1)", iv)
        except Exception as e:
            log.exception("❌ batch update failed for interval=%s: %s", iv, e)


    log.info("DONE single-run test.")

if __name__ == "__main__":
    main()
