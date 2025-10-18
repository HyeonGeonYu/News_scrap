# main.py
import sys
import time
import signal
import logging
import os
from datetime import datetime
from typing import Optional, List

from pytz import timezone, utc
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor

from storage import (
    fetch_and_store_chart_data,
    fetch_and_store_youtube_data,
    fetch_and_store_holiday_data,
    save_daily_data,
)
from redis_client import redis_client

# ── coin_backfill(증분 폴링 유틸)에서 필요한 것들 가져오기
from coin_backfill import (
    IncrementalStore,
    fetch_bybit_klines,
    compute_fetch_window,
    LIMIT_PER_CALL as CB_LIMIT_PER_CALL,
    SKEW_MS_1M as CB_SKEW_MS_1M,
    SKEW_MS_1D as CB_SKEW_MS_1D,
)

# ───────────────────────────────────────────────────────────
# 설정
# ───────────────────────────────────────────────────────────
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT, ETHUSDT, XAUTUSDT").split(",") if s.strip()]
KEEP_1M = int(os.getenv("KEEP_1M", os.getenv("KEEP", "10080")))
KEEP_1D = int(os.getenv("KEEP_1D", os.getenv("KEEP", "300")))

LIMIT_PER_CALL = CB_LIMIT_PER_CALL
SKEW_MS_1M = CB_SKEW_MS_1M
SKEW_MS_1D = CB_SKEW_MS_1D

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
SEOUL = timezone("Asia/Seoul")

# 전역 스토어(프로세스 생명주기 동안 유지 → 증분 운영)
store = IncrementalStore(keep_map={"1": KEEP_1M, "D": KEEP_1D})

# Redis 클라이언트 이름(운영 트레이싱 편의)
try:
    redis_client.client_setname("svc:main")
    log.info("Redis client name set to 'svc:main'")
except Exception:
    log.warning("client_setname failed", exc_info=True)

# ───────────────────────────────────────────────────────────
# 기존 저장 루틴
# ───────────────────────────────────────────────────────────
def scheduled_store(run_all: bool = False):
    """기존에 돌리던 저장 작업들."""
    try:
        now = datetime.now(SEOUL)

        # 유튜브: 11~15시
        if run_all or (11 <= now.hour < 15):
            log.info("⏰ YouTube 데이터 저장 (%s)", now.strftime("%Y-%m-%d %H:%M"))
            youtube_result = fetch_and_store_youtube_data()
            log.info(str(youtube_result))
        else:
            log.info("⏭️ YouTube 저장 시간대 아님 (run_all=False)")

        log.info("📈 chart data 저장 시작...")
        stored_result = fetch_and_store_chart_data()
        log.info(stored_result)

        # 휴일: 월요일
        if run_all or now.weekday() == 0:
            log.info("📅 휴일 데이터 저장 체크...")
            try:
                timestamp_b = redis_client.hget("market_holidays", "all_holidays_timestamp")
                if timestamp_b:
                    timestamp_str = timestamp_b.decode() if isinstance(timestamp_b, (bytes, bytearray)) else str(timestamp_b)
                    ts_utc = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=utc)
                    ts_kst = ts_utc.astimezone(SEOUL)
                    if ts_kst.date() == now.date():
                        log.info("⏭️ 오늘 이미 휴일 데이터 저장됨. 생략")
                    else:
                        holiday_result = fetch_and_store_holiday_data()
                        log.info(str(holiday_result))
                else:
                    holiday_result = fetch_and_store_holiday_data()
                    log.info(str(holiday_result))
            except Exception as e:
                log.exception("❌ 휴일 timestamp 확인 중 오류: %s", e)
        else:
            log.info("⏭️ 휴일 데이터 요일 아님 (run_all=False)")

        # 데일리: 23시 이후
        if run_all or (now.hour > 22):
            log.info("🕚 데일리 데이터 저장 실행")
            save_daily_data()
        else:
            log.info("⏭️ 데일리 저장 시간대 아님 (run_all=False)")

    except Exception as e:
        log.exception("❌ scheduled_store 실행 중 예외: %s", e)

# ───────────────────────────────────────────────────────────
# 증분 kline 저장 (매 분 / 매일)
# ───────────────────────────────────────────────────────────
def run_klines_minutely():
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


def run_klines_daily():
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


# ───────────────────────────────────────────────────────────
# 스타트업 중복 실행 가드 + 초기 로드/백필(조건부 플러시)
# ───────────────────────────────────────────────────────────
# main.py 의 startup_runs() 전체를 다음으로 교체

def startup_runs():
    """
    기동 직후 1회 실행:
      - YouTube/차트/휴일 저장은 그대로
      - K라인: 무조건 풀 초기화(full_initialize)로 '닫힌 봉' 기준 KEEP개를 채워서 즉시 플러시
      - 일봉은 스케줄 임박/직후(±5분)면 초기화 스킵
    """
    now = datetime.now(SEOUL)
    scheduled_daily_min = 9 * 60 + 1  # 09:01 KST
    cur_min = now.hour * 60 + now.minute
    run_daily_now = abs(cur_min - scheduled_daily_min) > 5  # ±5분 이내면 스킵

    log.info("🚀 Startup run: scheduled_store(run_all=True) + FULL kline initialize (closed-only)")
    try:
        scheduled_store(run_all=True)

        # 1분봉: 항상 풀 초기화 (닫힌 봉만)
        store.full_initialize(SYMBOLS, "1", exclude_open=True)
        log.info("🔄 Startup full-initialized 1m snapshot")

        # 1일봉: 스케줄 임박/직후면 스킵, 아니면 풀 초기화
        if run_daily_now:
            store.full_initialize(SYMBOLS, "D", exclude_open=True)
            log.info("🔄 Startup full-initialized 1D snapshot")
        else:
            log.info("⏭️ Startup에서 1D full init 스킵(스케줄 임박/직후)")
    except Exception:
        log.exception("❌ Startup run 실패")


def _load_or_backfill_with_dirty_flush(interval: str) -> bool:
    """
    store.load_or_backfill 호출 전후 last_ts를 비교해 변화가 있으면 플러시.
    (최초 백필/KEEP 변경/데이터 불일치 등)
    """
    before = {s: store.last_ts(interval, s) for s in SYMBOLS}
    store.load_or_backfill(SYMBOLS, interval)
    after = {s: store.last_ts(interval, s) for s in SYMBOLS}
    changed = any(before.get(s) != after.get(s) for s in SYMBOLS)
    if changed:
        store.flush_interval(interval, SYMBOLS)
    return changed

# ───────────────────────────────────────────────────────────
# 엔트리 포인트
# ───────────────────────────────────────────────────────────
def main():
    executors = {"default": ThreadPoolExecutor(5)}
    job_defaults = {"coalesce": True, "max_instances": 1, "misfire_grace_time": 300}
    scheduler = BackgroundScheduler(timezone=SEOUL, executors=executors, job_defaults=job_defaults)

    # 매시 정각
    scheduler.add_job(
        scheduled_store,
        CronTrigger(minute="0", timezone=SEOUL),
        id="scheduled_store",
        replace_existing=True,
    )

    # 1분봉: 매분 6초 (거래소 반영 지연 대비)
    scheduler.add_job(
        run_klines_minutely,
        CronTrigger(second="6", minute="*", timezone=SEOUL),
        id="kline_minutely",
        replace_existing=True,
    )

    # 1일봉: KST 09:01 (UTC 00:01 ≈ 일봉 경계 직후)
    scheduler.add_job(
        run_klines_daily,
        CronTrigger(hour="9", minute="1", timezone=SEOUL),
        id="kline_daily",
        replace_existing=True,
    )

    # ── 기동 직후 1회(중복 가드 포함)
    startup_runs()

    scheduler.start()
    log.info("✅ Scheduler started. (Asia/Seoul)")

    def shutdown(*_):
        log.info("🛑 Shutting down scheduler...")
        try:
            scheduler.shutdown(wait=False)
        finally:
            sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        shutdown()

if __name__ == "__main__":
    main()
