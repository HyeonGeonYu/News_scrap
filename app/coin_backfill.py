#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Bybit Kline 증분 폴링 유틸 (WS 없음)
- 1분봉/1일봉 "마감 직후" 증분 수집
- 인터벌별 KEEP 개수 유지 (예: 1분봉 10,080 / 1일봉 1,500)
- 최초 실행 시 full_initialize()로 닫힌 봉 기준 KEEP만큼 전량 수집 후 HSET 1회
- 그 이후엔 last_ts 이후 '닫힌 봉'까지만 증분 수집
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

# 프로젝트의 Redis 클라이언트 사용
from redis_client import redis_client as redis_client

# ───────────────────────────────────────────────────────────
# 환경 변수 & 상수
# ───────────────────────────────────────────────────────────

load_dotenv()

BYBIT_BASE      = os.getenv("BYBIT_BASE", "https://api.bybit.com")
CATEGORY        = os.getenv("CATEGORY", "linear")             # 보통 'linear'
LIMIT_PER_CALL  = int(os.getenv("LIMIT_PER_CALL", "1000"))

# 정확한 캔들 마감 반영을 위한 소폭 지연(테스트/스케줄에서 사용)
SKEW_MS_1M      = int(os.getenv("SKEW_MS_1M", "1500"))        # 1분 마감 후 1.5초 대기
SKEW_MS_1D      = int(os.getenv("SKEW_MS_1D", "2000"))        # 1일 마감 후 2초 대기

# 인터벌별 KEEP (기본값: KEEP → 없으면 300)
KEEP_DEFAULT    = int(os.getenv("KEEP", "300"))
KEEP_1M         = int(os.getenv("KEEP_1M", str(10080)))
KEEP_1D         = int(os.getenv("KEEP_1D", str(KEEP_DEFAULT)))

# 압축 저장 옵션: 1이면 zlib+base64로 압축 저장
COMPRESS_JSON = os.getenv("COMPRESS_JSON", "0") == "1"
if COMPRESS_JSON:
    import zlib, base64

# ───────────────────────────────────────────────────────────
# 직렬화/역직렬화
# ───────────────────────────────────────────────────────────

def dumps_compact(obj) -> str:
    s = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    if not COMPRESS_JSON:
        return s.decode("utf-8")
    comp = zlib.compress(s, level=6)
    return base64.b64encode(comp).decode("ascii")

def loads_compact(s: bytes) -> List[Dict]:
    if s is None:
        return []
    if isinstance(s, bytes):
        s = s.decode("utf-8")
    if not COMPRESS_JSON:
        return json.loads(s)
    comp = base64.b64decode(s.encode("ascii"))
    raw = zlib.decompress(comp)
    return json.loads(raw.decode("utf-8"))

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
        "time":  int(int(row[0]) / 1000),  # seconds
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
# 범위 수집(페이지네이션 대용)
# ───────────────────────────────────────────────────────────

def _advance_ms(interval: str, start_ms: int) -> int:
    return start_ms + step_ms(interval)

def fetch_bybit_klines_range(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    want: int,
) -> List[Dict]:
    """
    [start_ms, end_ms] 구간에서 '닫힌 봉' 기준으로 최대 want개 수집.
    Bybit 단일 호출 limit(기본 1000)를 초과할 경우 여러 번 반복 호출.
    - 최근(끝)에서 과거(앞)로 역방향 페이지네이션
    - 각 chunk는 fetch_bybit_klines()에서 오름차순 정렬 보장
    """
    step = step_ms(interval)
    out: List[Dict] = []

    # 역방향 페이지네이션 포인터(끝→앞)
    cur_end = end_ms
    min_start = start_ms

    # 안전장치: 과도한 루프 방지(원하는 개수/limit + 여유)
    max_pages = max(1, (want // LIMIT_PER_CALL) + 5)
    pages = 0

    while len(out) < want and cur_end >= min_start and pages < max_pages:
        # 이번 페이지의 시작 시각(끝에서 limit만큼 뒤로)
        approx_span = (LIMIT_PER_CALL - 1) * step
        cur_start = max(min_start, cur_end - approx_span)

        chunk = fetch_bybit_klines(symbol, interval, cur_start, cur_end, limit=LIMIT_PER_CALL)
        pages += 1

        if not chunk:
            # 더 이상 받을 게 없음
            break

        # chunk는 오름차순(time↑). 범위를 넘어온 항목이 있다면 필터
        # (보통 필요 없지만 방어적으로 유지)
        chunk = [b for b in chunk if (int(b["time"]) * 1000) >= min_start and (int(b["time"]) * 1000) <= cur_end]
        if not chunk:
            # 범위 내 유효 결과 없음 → 더 뒤로 한 페이지 이동
            cur_end = cur_start - 1
            continue

        out.extend(chunk)

        # 다음 페이지의 끝은 "이번 chunk의 가장 오래된 바 시작 - step"
        oldest_ms = int(chunk[0]["time"]) * 1000
        next_end = oldest_ms - step
        if next_end < min_start:
            break
        cur_end = next_end

    # 정렬 + 중복 제거(혹시 일부 겹치는 경우) 후 끝에서 want개
    if out:
        out.sort(key=lambda b: b["time"])
        dedup: List[Dict] = []
        seen = set()
        for b in out:
            t = int(b["time"])
            if t in seen:
                continue
            seen.add(t)
            dedup.append(b)
        return dedup[-want:]

    return []


# ───────────────────────────────────────────────────────────
# 증분 폴링용 스토어 (인터벌별 KEEP 지원)
# ───────────────────────────────────────────────────────────

Bar = Dict[str, float]

class IncrementalStore:
    """
    interval별 keep_map 예: {"1": 10080, "D": 1500}
    """
    def __init__(self, keep_map: Dict[str, int]):
        self.keep_map = keep_map
        self.buf: Dict[Tuple[str, str], Deque[Bar]] = {}
        self.log = logging.getLogger("IncrementalStore")

    def keep_for(self, interval: str) -> int:
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
            newdq: Deque[Bar] = deque(maxlen=need)
            if dq:
                newdq.extend(list(dq)[-need:])
            self.buf[k] = newdq
            dq = newdq
        return dq

    # ── 최초 실행: 설정 KEEP으로 '닫힌 봉' 기준 전량 수집 후 즉시 플러시
    def full_initialize(self, symbols: List[str], interval: str, exclude_open: bool = True):
        now_ms = int(time.time() * 1000)
        end_ms = (floor_cur_bar_start_ms(now_ms, interval) - 1) if exclude_open else now_ms
        keep = self.keep_for(interval)
        start_ms = window_start_ms(end_ms, interval, keep)

        for sym in symbols:
            bars = fetch_bybit_klines_range(sym, interval, start_ms, end_ms, want=keep)
            dq = self.ensure(interval, sym)
            dq.clear()
            for b in bars[-keep:]:
                dq.append(b)
            self.log.info("Full-initialized %s/%s -> len=%d (keep=%d)", interval, sym, len(dq), keep)

        self.flush_interval(interval, symbols)

    # ── 기존 스냅샷 기반 로드(옵션): 길이 다르면 강제 백필해서 맞춤
    def load_or_backfill(self, symbols: List[str], interval: str):
        hkey = _hash_key(interval)
        pipe = redis_client.pipeline()
        for s in symbols:
            pipe.hget(hkey, s)
        raw_list = pipe.execute()

        now_ms = int(time.time() * 1000)
        keep = self.keep_for(interval)
        end_ms = floor_cur_bar_start_ms(now_ms, interval) - 1
        start_ms = window_start_ms(end_ms, interval, keep)

        for s, raw in zip(symbols, raw_list):
            dq = self.ensure(interval, s)
            dq.clear()

            needs_full = True
            if raw:
                arr = loads_compact(raw)
                if len(arr) >= keep:
                    trimmed = arr[-keep:]
                    if len(trimmed) == keep:
                        for b in trimmed:
                            dq.append(b)
                        needs_full = False

            if needs_full:
                bars = fetch_bybit_klines_range(s, interval, start_ms, end_ms, want=keep)
                for b in bars[-keep:]:
                    dq.append(b)

            self.log.info(
                "Initialized %s/%s => len=%d (keep=%d, source=%s)",
                interval, s, len(dq), keep,
                "redis" if not needs_full else "full_backfill",
            )

    def last_ts(self, interval: str, sym: str) -> Optional[int]:
        dq = self.ensure(interval, sym)
        return int(dq[-1]["time"]) if dq else None

    def merge_increment(self, interval: str, sym: str, new_bars: List[Bar]):
        """증분 데이터 병합(동일 time은 덮어써 확정치 반영)."""
        if not new_bars:
            return
        dq = self.ensure(interval, sym)
        keep = self.keep_for(interval)
        by_time = {int(b["time"]): b for b in dq}
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
# 증분 수집 윈도우(열린 봉 제외)
# ───────────────────────────────────────────────────────────

def compute_fetch_window(
    last_ts_sec: Optional[int],
    interval: str,
    now_ms: int,
    keep_for_interval: int,
    exclude_open: bool = True,
) -> Tuple[Optional[int], Optional[int]]:
    """
    - exclude_open=True: 현재 진행 중인 최신 봉 제외 → '닫힌 봉'까지만 수집.
    - last_ts_sec가 없으면 keep 윈도우(닫힌 봉 기준)로 백필.
    - 가져올 것 없으면 (None, None) 반환.
    """
    end_ms = (floor_cur_bar_start_ms(now_ms, interval) - 1) if exclude_open else now_ms

    if last_ts_sec is None:
        start_ms = window_start_ms(end_ms, interval, keep_for_interval)
    else:
        start_ms = (last_ts_sec + (step_ms(interval) // 1000)) * 1000

    if start_ms > end_ms:
        return None, None
    return start_ms, end_ms

# ───────────────────────────────────────────────────────────
# 테스트 실행 (__main__) - argparse 없이 ENV만 사용
# ───────────────────────────────────────────────────────────
def run_klines_minutely(SYMBOLS):
    if not SYMBOLS:
        log.warning("⏭️ SYMBOLS 비어 있음. 1m kline 작업 스킵")
        return
    t0 = time.perf_counter()
    try:
        now_ms = int(time.time() * 1000) + SKEW_MS_1M
        keep_for = store.keep_for("1")

        for sym in SYMBOLS:
            last_ts = store.last_ts("1", sym)
            start_ms, end_ms = compute_fetch_window(
                last_ts, "1", now_ms, keep_for, exclude_open=True
            )
            if start_ms is None:  # 가져올 것 없음
                continue
            bars = fetch_bybit_klines(sym, "1", start_ms, end_ms, limit=LIMIT_PER_CALL)
            store.merge_increment("1", sym, bars)

        store.flush_interval("1", SYMBOLS)
        dt_ms = (time.perf_counter() - t0) * 1000
        log.info("✅ 1m closed-only incremental (symbols=%d, keep=%d) %.1f ms",
                 len(SYMBOLS), keep_for, dt_ms)
    except Exception:
        log.exception("❌ 1m kline incremental error")


def run_klines_daily(SYMBOLS):
    if not SYMBOLS:
        log.warning("⏭️ SYMBOLS 비어 있음. 1D kline 작업 스킵")
        return
    t0 = time.perf_counter()
    try:
        now_ms = int(time.time() * 1000) + SKEW_MS_1D
        keep_for = store.keep_for("D")

        for sym in SYMBOLS:
            last_ts = store.last_ts("D", sym)
            start_ms, end_ms = compute_fetch_window(
                last_ts, "D", now_ms, keep_for, exclude_open=True
            )
            if start_ms is None:
                continue
            bars = fetch_bybit_klines(sym, "D", start_ms, end_ms, limit=LIMIT_PER_CALL)
            store.merge_increment("D", sym, bars)

        store.flush_interval("D", SYMBOLS)
        dt_ms = (time.perf_counter() - t0) * 1000
        log.info("✅ 1D closed-only incremental (symbols=%d, keep=%d) %.1f ms",
                 len(SYMBOLS), keep_for, dt_ms)
    except Exception:
        log.exception("❌ 1D kline incremental error")


if __name__ == "__main__":
    # 로깅
    logging.basicConfig(
        level=getattr(logging, "INFO", logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    log = logging.getLogger("testmain")

    # ENV 파라미터
    SYMBOLS_ENV   = os.getenv("TEST_SYMBOLS", os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT"))
    INTERVALS_ENV = os.getenv("TEST_INTERVALS", "1,D")
    MODE          = os.getenv("TEST_MODE", "full_init").lower()   # full_init | step | loop
    STEPS         = int(os.getenv("TEST_STEPS", "1"))
    SLEEP_SEC     = float(os.getenv("TEST_SLEEP", "60"))

    # LIMIT 오버라이드(옵션)
    LIMIT_OVERRIDE = os.getenv("TEST_LIMIT")
    if LIMIT_OVERRIDE:
        try:
            LIMIT_PER_CALL = int(LIMIT_OVERRIDE)
        except Exception:
            log.warning("TEST_LIMIT 파싱 실패, 기본 LIMIT_PER_CALL=%s 사용", LIMIT_PER_CALL)

    # KEEP 적용
    keep_map = {"1": KEEP_1M, "D": KEEP_1D}

    # 심볼/인터벌 파싱
    SYMBOLS_ARG: List[str] = [s.strip().upper() for s in SYMBOLS_ENV.split(",") if s.strip()]
    INTERVALS_ARG: List[str] = [iv.strip() for iv in INTERVALS_ENV.split(",") if iv.strip() in ("1", "D")]
    if not INTERVALS_ARG:
        INTERVALS_ARG = ["1", "D"]

    # Redis 핑
    try:
        pong = redis_client.ping()
        log.info("Redis PING: %s", pong)
    except Exception as e:
        log.exception("Redis ping failed: %s", e)
        raise SystemExit(2)

    store = IncrementalStore(keep_map=keep_map)

    def do_full_init(iv: str):
        store.full_initialize(SYMBOLS_ARG, iv, exclude_open=True)
        for sym in SYMBOLS_ARG:
            ts = store.last_ts(iv, sym)
            log.info("[FULL_INIT] %s/%s len=%d last_ts=%s",
                     iv, sym, len(store.ensure(iv, sym)), ts)

    def do_step(iv: str):
        now_ms = int(time.time() * 1000) + (SKEW_MS_1M if iv == "1" else SKEW_MS_1D)
        keep_for = store.keep_for(iv)
        for sym in SYMBOLS_ARG:
            last_ts = store.last_ts(iv, sym)
            start_ms, end_ms = compute_fetch_window(last_ts, iv, now_ms, keep_for, exclude_open=True)
            if start_ms is None:
                continue
            bars = fetch_bybit_klines(sym, iv, start_ms, end_ms, limit=LIMIT_PER_CALL)
            store.merge_increment(iv, sym, bars)
        store.flush_interval(iv, SYMBOLS_ARG)
        for sym in SYMBOLS_ARG:
            ts = store.last_ts(iv, sym)
            log.info("[STEP] %s/%s len=%d last_ts=%s",
                     iv, sym, len(store.ensure(iv, sym)), ts)

    def do_loop(iv: str, steps: int, sleep_sec: float):
        for i in range(steps):
            log.info("[LOOP] %s step %d/%d", iv, i + 1, steps)
            do_step(iv)
            if i < steps - 1:
                time.sleep(sleep_sec)

    log.info(
        "TEST_MODE=%s | SYMBOLS=%s | INTERVALS=%s | KEEP(1m)=%d KEEP(1d)=%d | LIMIT=%d",
        MODE, ",".join(SYMBOLS_ARG), ",".join(INTERVALS_ARG), keep_map["1"], keep_map["D"], LIMIT_PER_CALL
    )

    try:
        if MODE == "full_init":
            for iv in INTERVALS_ARG:
                do_full_init(iv)
        elif MODE == "step":
            for iv in INTERVALS_ARG:
                do_step(iv)
        elif MODE == "loop":
            for iv in INTERVALS_ARG:
                do_loop(iv, STEPS, SLEEP_SEC)
        else:
            log.error("알 수 없는 TEST_MODE: %s (full_init|step|loop 중 하나)", MODE)
    except KeyboardInterrupt:
        log.info("Interrupted by user.")
