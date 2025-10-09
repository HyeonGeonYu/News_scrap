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

def scheduled_store():
    # 예외가 여기서 터지면 다음 실행이 막히지 않도록 전체 try/except
    try:
        now = datetime.now(SEOUL)
        log.info("📈 chart data 저장 시작...")
        stored_result = fetch_and_store_chart_data()
        log.info(stored_result)

        if 11 <= now.hour < 15:
            log.info("⏰ Scheduled store running at %s", now.strftime("%Y-%m-%d %H:%M"))
            youtube_result = fetch_and_store_youtube_data()
            log.info(str(youtube_result))

        # 월요일에만 휴일 데이터 갱신
        if now.weekday() == 0:
            log.info("📅 월요일: 휴일 데이터 저장 체크 중...")
            try:
                timestamp_b = redis_client.hget("market_holidays", "all_holidays_timestamp")
                if timestamp_b:
                    timestamp_str = timestamp_b.decode() if isinstance(timestamp_b, (bytes, bytearray)) else str(timestamp_b)
                    ts_utc = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=utc)
                    ts_kst = ts_utc.astimezone(SEOUL)
                    if ts_kst.date() == now.date():
                        log.info("⏭️ 오늘 이미 휴일 데이터가 저장됨. 생략합니다.")
                    else:
                        holiday_result = fetch_and_store_holiday_data()
                        log.info(str(holiday_result))
                else:
                    holiday_result = fetch_and_store_holiday_data()
                    log.info(str(holiday_result))
            except Exception as e:
                log.exception("❌ Redis timestamp 확인 중 오류: %s", e)

        if now.hour == 23:
            log.info("🕚 23시까지 스크랩된 데이터 저장 시작")
            save_daily_data()

    except Exception as e:
        # 잡 내부 예외는 로깅만 하고 끝냄(다음 라운드에 다시 시도)
        log.exception("❌ scheduled_store 실행 중 예외: %s", e)

def main():
    # executors 설정(기본 10개 스레드 → 5개로 줄이고, 잡은 1개만 동시에 실행)
    executors = {"default": ThreadPoolExecutor(5)}
    job_defaults = {
        "coalesce": True,           # 밀린 실행은 1번으로 합치기
        "max_instances": 1,         # 겹치기 방지
        "misfire_grace_time": 300,  # 5분 이내 밀림은 허용
    }
    scheduler = BackgroundScheduler(
        timezone=SEOUL,
        executors=executors,
        job_defaults=job_defaults,
    )

    trigger = CronTrigger(minute="0", timezone=SEOUL)  # 매시 정각
    scheduler.add_job(scheduled_store, trigger=trigger, id="scheduled_store", replace_existing=True)

    # ✅ 기동 직후 1회 동기 실행
    log.info("🚀 Startup run: scheduled_store()")
    try:
        scheduled_store()
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
