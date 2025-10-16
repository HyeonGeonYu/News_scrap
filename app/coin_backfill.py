#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Bybit Kline 증분 폴링 유틸 (WS 없음)
- 1분봉/1일봉 "마감 직후" 증분 수집
- 메모리/Redis에 인터벌별 KEEP 개수 유지 (예: 1분봉 10,000 / 1일봉 1,500)
- 전량 재수집 없이 last_ts 이후만 가져와 병합
"""

import os
import json
import time
import logging
from typing import List, Dict, Deque, Tuple, Optional
from collections import deque

import requests
from tenacity import retry, wait_exponential_jitter, stop_after_attempt, retry_if_exception_type
from dotenv import load_dotenv

# 프로젝트의 Redis 클라이언트 사용 (모듈 제공)
from redis_client import redis_client as redis_client

# ───────────────────────────────────────────────────────────
# 환경 변수 & 상수
# ───────────────────────────────────────────────────────────

load_dotenv()

BYBIT_BASE      = os.getenv("BYBIT_BASE", "https://api.bybit.com")
CATEGORY        = os.getenv("CATEGORY", "linear")       # 보통 'linear'
LIMIT_PER_CALL  = int(os.getenv("LIMIT_PER_CALL", "1000"))

# 정확한 캔들 마감 반영을 위해 아주 짧게 지연
SKEW_MS_1M      = int(os.getenv("SKEW_MS_1M", "1500"))  # 1분 마감 후 1.5초 대기
SKEW_MS_1D      = int(os.getenv("SKEW_MS_1D", "2000"))  # 1일 마감 후 2초 대기

# 인터벌별 KEEP (기본값: KEEP → 없으면 300)
KEEP_DEFAULT    = int(os.getenv("KEEP", "300"))
KEEP_1M         = int(os.getenv("KEEP_1M", str(KEEP_DEFAULT)))
KEEP_1D         = int(os.getenv("KEEP_1D", str(KEEP_DEFAULT)))

# 압축 저장 옵션: 1이면 zlib+base64로 압축 저장
COMPRESS_JSON = os.getenv("COMPRESS_JSON", "0") == "1"
if COMPRESS_JSON:
    import zlib, base64

# ───────────────────────────────────────────────────────────
# 직렬화/역직렬화
# ───────────────────────────────────────────────────────────

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

def _hash_key(interval: str) -> str:
    return f"kline:{interval}:json"

# ───────────────────────────────────────────────────────────
# 증분 폴링용 스토어 (인터벌별 KEEP 지원)
# ───────────────────────────────────────────────────────────

Bar = Dict[str, float]

class IncrementalStore:
    """
    interval별 keep_map 예: {"1": 10000, "D": 1500}
    """
    def __init__(self, keep_map: Dict[str, int]):
        self.keep_map = keep_map
        self.buf: Dict[Tuple[str, str], Deque[Bar]] = {}
        self.log = logging.getLogger("IncrementalStore")

    def keep_for(self, interval: str) -> int:
        # 값이 없으면 첫 번째 값 사용(보수적 fallback)
        if interval in self.keep_map:
            return self.keep_map[interval]
        return next(iter(self.keep_map.values()))

    def _k(self, interval: str, sym: str) -> Tuple[str, str]:
        return (interval, sym)

    def ensure(self, interval: str, sym: str) -> Deque[Bar]:
        k = self._k(interval, sym)
        need = self.keep_for(interval)
        dq = self.buf.get(k)
        if dq is None or dq.maxlen != need:
            # maxlen 변경 시 tail만 유지해서 새 deque로 교체
            newdq: Deque[Bar] = deque(maxlen=need)
            if dq:
                tail = list(dq)[-need:]
                newdq.extend(tail)
            self.buf[k] = newdq
            dq = newdq
        return dq

    def load_or_backfill(self, symbols: List[str], interval: str):
        """시작 시 Redis 스냅샷 우선 로드, 없으면 한 번만 백필."""
        hkey = _hash_key(interval)
        pipe = redis_client.pipeline()
        for s in symbols:
            pipe.hget(hkey, s)
        raw_list = pipe.execute()

        now_ms = int(time.time() * 1000)
        keep = self.keep_for(interval)
        start_ms = window_start_ms(now_ms, interval, keep)

        for s, raw in zip(symbols, raw_list):
            dq = self.ensure(interval, s)
            dq.clear()
            if raw:
                arr = loads_compact(raw)
                for b in arr[-keep:]:
                    dq.append(b)
                self.log.info("Loaded from Redis: %s/%s (%d bars)", interval, s, len(dq))
            else:
                bars = fetch_bybit_klines(s, interval, start_ms, now_ms, limit=keep)
                for b in bars[-keep:]:
                    dq.append(b)
                self.log.info("Backfilled via REST: %s/%s (%d bars)", interval, s, len(dq))

    def last_ts(self, interval: str, sym: str) -> Optional[int]:
        dq = self.ensure(interval, sym)
        return int(dq[-1]["time"]) if dq else None

    def merge_increment(self, interval: str, sym: str, new_bars: List[Bar]):
        """증분 데이터 병합(동일 time은 덮어써 확정치 반영)."""
        if not new_bars:
            return
        dq = self.ensure(interval, sym)
        keep = self.keep_for(interval)
        by_time = {b["time"]: b for b in dq}
        for nb in new_bars:
            by_time[int(nb["time"])] = nb
        merged = sorted(by_time.values(), key=lambda x: x["time"])[-keep:]
        dq.clear()
        dq.extend(merged)

    def flush_interval(self, interval: str, symbols: List[str]):
        """인터벌별로 Redis HSET 1회(스냅샷 저장)."""
        mapping = {
            "__schema_version": "1",
            "__updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        for s in symbols:
            dq = self.ensure(interval, s)
            arr = list(dq)
            mapping[s] = dumps_compact(arr)
            mapping[f"last_ts:{s}"] = str(arr[-1]["time"] if arr else 0)
        redis_client.hset(_hash_key(interval), mapping=mapping)

# ───────────────────────────────────────────────────────────
# 증분 수집 윈도우 계산
# ───────────────────────────────────────────────────────────

def compute_fetch_window(
    last_ts_sec: Optional[int],
    interval: str,
    now_ms: int,
    keep_for_interval: int,
) -> Tuple[Optional[int], int]:
    """
    - last_ts_sec가 있으면 그 다음 봉 시작부터 now_ms까지 증분 요청
    - 없으면 keep 윈도우 만큼 백필
    """
    if last_ts_sec is None:
        start_ms = window_start_ms(now_ms, interval, keep_for_interval)
        return start_ms, now_ms
    next_start_sec = last_ts_sec + (step_ms(interval) // 1000)
    return next_start_sec * 1000, now_ms
