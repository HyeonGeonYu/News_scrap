import sys
import time
import signal
import logging
from datetime import datetime

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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
SEOUL = timezone("Asia/Seoul")


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
        if run_all or (11 <= now.hour < 22):
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



def startup_runs():
    now = datetime.now(SEOUL)
    scheduled_daily_min = 9 * 60 + 1  # 09:01 KST
    cur_min = now.hour * 60 + now.minute
    run_daily_now = abs(cur_min - scheduled_daily_min) > 5  # ±5분 이내면 스킵

    log.info("🚀 Startup run: scheduled_store(run_all=True) + FULL kline initialize (closed-only)")
    try:
        scheduled_store(run_all=True)
        if run_daily_now:
            log.info("🔄 Startup full-initialized 1D snapshot")
        else:
            log.info("⏭️ Startup에서 1D full init 스킵(스케줄 임박/직후)")
    except Exception:
        log.exception("❌ Startup run 실패")


def main():
    executors = {"default": ThreadPoolExecutor(5)}
    job_defaults = {"coalesce": True, "max_instances": 1, "misfire_grace_time": 300}
    scheduler = BackgroundScheduler(timezone=SEOUL, executors=executors, job_defaults=job_defaults)

    scheduler.add_job(
        scheduled_store,
        CronTrigger(minute="0", timezone=SEOUL),
        id="scheduled_store",
        replace_existing=True,
    )

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