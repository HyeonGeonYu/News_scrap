# main.py
import signal, sys, time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone
from storage import *

def scheduled_store():
    now = datetime.now(timezone('Asia/Seoul'))
    print("📈 chart data 저장 시작...")
    stored_result = fetch_and_store_chart_data()
    print(stored_result)

    if 11 <= now.hour < 15:
        print("⏰ Scheduled store running at", now.strftime("%Y-%m-%d %H:%M"))
        youtube_result = fetch_and_store_youtube_data()
        print(youtube_result)

    if now.weekday() == 0:
        print("📅 월요일: 휴일 데이터 저장 체크 중...")
        try:
            timestamp_str = redis_client.hget("market_holidays", "all_holidays_timestamp")
            if timestamp_str:
                timestamp = datetime.strptime(timestamp_str.decode(), "%Y-%m-%dT%H:%M:%SZ")
                timestamp_kst = timestamp.replace(tzinfo=timezone('UTC')).astimezone(timezone('Asia/Seoul'))
                if timestamp_kst.date() == now.date():
                    print("⏭️ 오늘 이미 휴일 데이터가 저장됨. 생략합니다.")
                    return
            holiday_result = fetch_and_store_holiday_data()
            print(holiday_result)
        except Exception as e:
            print(f"❌ Redis에서  timestamp 확인 중 오류 발생: {str(e)}")

    if now.hour == 23:
        print("🕚 23시까지 스크랩된 데이터 저장 시작")
        save_daily_data()

def main():
    seoul = timezone('Asia/Seoul')
    scheduler = BackgroundScheduler(timezone=seoul)  # ⚠️ 타임존 지정
    trigger = CronTrigger(minute='0', timezone=seoul)  # 매시 정각마다
    scheduler.add_job(scheduled_store, trigger=trigger, id="scheduled_store", replace_existing=True)
    scheduler.start()
    print("✅ Scheduler started. (매시 정각 실행)")

    # 프로세스 유지 + 안전 종료
    def shutdown(*_):
        print("\n🛑 Shutting down scheduler...")
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        shutdown()

if __name__ == "__main__":  # ⚠️ 재임포트로 중복 시작 방지
    main()
