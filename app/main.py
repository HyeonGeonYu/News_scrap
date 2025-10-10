# main.py
import sys, time, signal, logging
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
    redis_client,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SEOUL = timezone("Asia/Seoul")

def scheduled_store(run_all: bool = False):
    """
    run_all=True 이면 시간/요일 조건을 무시하고 가능한 작업을 모두 수행.
    """
    try:

        now = datetime.now(SEOUL)

        # 유튜브 데이터: 평소엔 11~15시, run_all이면 즉시 수행
        if run_all or (11 <= now.hour < 15):
            log.info("⏰ YouTube 데이터 저장 실행 (%s)", now.strftime("%Y-%m-%d %H:%M"))
            youtube_result = fetch_and_store_youtube_data()
            log.info(str(youtube_result))
        else:
            log.info("⏭️ YouTube 저장 시간대 아님 (run_all=False)")

        log.info("📈 chart data 저장 시작...")
        stored_result = fetch_and_store_chart_data()
        log.info(stored_result)


        # 휴일 데이터: 평소엔 월요일만, run_all이면 즉시 수행
        if run_all or now.weekday() == 0:
            log.info("📅 휴일 데이터 저장 체크 중...")
            try:
                timestamp_b = redis_client.hget("market_holidays", "all_holidays_timestamp")
                if timestamp_b:
                    timestamp_str = timestamp_b.decode() if isinstance(timestamp_b, (bytes, bytearray)) else str(timestamp_b)
                    ts_utc = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=utc)
                    ts_kst = ts_utc.astimezone(SEOUL)
                    if ts_kst.date() == now.date():
                        log.info("⏭️ 오늘 이미 휴일 데이터가 저장됨. 생략합니다.")
                        # 만약 run_all에서도 강제 갱신하고 싶다면 위 두 줄 대신 아래 두 줄 사용:
                        # log.info("⚠️ run_all=True: 강제 휴일 데이터 갱신")
                        # holiday_result = fetch_and_store_holiday_data(); log.info(str(holiday_result))
                    else:
                        holiday_result = fetch_and_store_holiday_data()
                        log.info(str(holiday_result))
                else:
                    holiday_result = fetch_and_store_holiday_data()
                    log.info(str(holiday_result))
            except Exception as e:
                log.exception("❌ Redis timestamp 확인 중 오류: %s", e)
        else:
            log.info("⏭️ 휴일 데이터 요일 아님 (run_all=False)")

        # 데일리 저장: 평소엔 23시 이후, run_all이면 즉시 수행
        if run_all or (now.hour > 22):
            log.info("🕚 데일리 데이터 저장 실행")
            save_daily_data()
        else:
            log.info("⏭️ 데일리 저장 시간대 아님 (run_all=False)")

    except Exception as e:
        log.exception("❌ scheduled_store 실행 중 예외: %s", e)

def main():
    executors = {"default": ThreadPoolExecutor(5)}
    job_defaults = {
        "coalesce": True,
        "max_instances": 1,
        "misfire_grace_time": 300,
    }
    scheduler = BackgroundScheduler(
        timezone=SEOUL,
        executors=executors,
        job_defaults=job_defaults,
    )

    trigger = CronTrigger(minute="0", timezone=SEOUL)  # 매시 정각
    scheduler.add_job(scheduled_store, trigger=trigger, id="scheduled_store", replace_existing=True)

    # ✅ 기동 직후 1회 동기 실행: 시간 조건 무시하고 전부 수행
    log.info("🚀 Startup run: scheduled_store(run_all=True)")
    try:
        scheduled_store(run_all=True)
    except Exception:
        log.exception("❌ Startup run 실패")

    scheduler.start()
    log.info("✅ Scheduler started. (매시 정각 실행, Asia/Seoul)")

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
