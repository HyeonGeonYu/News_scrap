# main.py
import sys
import time
import signal
import logging
from datetime import datetime
from pytz import timezone, utc
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor
from persist import persist_today_data

from storage import (
    fetch_and_store_chart_data,
    fetch_and_store_youtube_data,
    fetch_and_store_holiday_data,
    save_daily_data,
)
from 세계정세분석 import analyze_and_store_world_state
from 전일브리핑 import generate_and_store_daily_briefing, generate_and_store_rolling_briefing
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

def run_world_state_analysis(startup: bool = False):
    """
    daily_collections 최근 한 달(롤링 30일)을 입력으로 세계 정세(나라별 상태 + 양자 관계)를
    구조화 JSON으로 추출해 Supabase world_state(주차별 행)에 저장.
    persist 직후(일일 데이터 확정 후) 매일 실행 → 상태가 매일 조금씩 변동, 그 주 행은 주말에 freeze.
    실패해도 persist 흐름에 영향 없게 격리.
    startup=True(컨테이너 기동)면 오늘 이미 저장된 경우 LLM 9회를 다시 태우지 않는다.
    """
    try:
        log.info("🌍 world_state 분석 시작 (최근 30일, startup=%s)", startup)
        analyze_and_store_world_state(days=30, skip_if_done_today=startup)
        log.info("🌍 world_state 분석 완료")
    except Exception as e:
        log.exception("❌ world_state 분석 중 예외: %s", e)


def run_daily_briefing():
    """전날(확정된 06:50 윈도우) 각국 요약을 종합해 핵심 뉴스 5개 브리핑 생성.
    youtube_data['global_briefing']으로 발행 → 홈 자동 전파. 같은 날 중복 생성은 내부에서 스킵."""
    try:
        log.info("🗞️ 전일 글로벌 브리핑 생성 시작")
        generate_and_store_daily_briefing()
        log.info("🗞️ 전일 글로벌 브리핑 완료")
    except Exception as e:
        log.exception("❌ 전일 브리핑 생성 중 예외: %s", e)


def run_rolling_briefing():
    """오늘(KST 달력일) 요약이 4개국 이상 모이면 낮에도 홈 브리핑을 갱신(2026-09-19).
    새 입력 없음·직전 갱신 2h 이내면 내부에서 스킵 → 하루 3~5회. 실패해도 수집 흐름에 영향 없게 격리."""
    try:
        generate_and_store_rolling_briefing()
    except Exception as e:
        log.exception("❌ 오늘 브리핑(롤링) 중 예외: %s", e)


def startup_persist_supabase():
    """
    서버 시작 시 1회 실행.
    현재 진행 중인 06:50 day를 저장.
    예: 5/14 낮 실행 → 5/14 06:50 ~ 5/15 06:50 window 기준 현재까지 저장
    """
    try:
        log.info("📦 Supabase persist 시작 실행 current_day 포함")
        persist_today_data(dry_run=False, include_current_day=True)
    except Exception as e:
        log.exception("❌ Supabase startup persist 실행 중 예외: %s", e)

    # 일일 데이터 확정 후 세계 정세 분석 + 전일 브리핑 (기동 시엔 당일 완료분 스킵)
    run_world_state_analysis(startup=True)
    run_daily_briefing()

# ───────────────────────────────────────────────────────────
# Supabase 장기 저장 루틴
# ───────────────────────────────────────────────────────────
def scheduled_persist_supabase():
    """
    Redis에 쌓인 직전 완료된 하루 데이터를 Supabase에 저장.
    예: 5/14 06:55 실행 → 5/13 06:50 ~ 5/14 06:50 저장
    """
    try:
        log.info("📦 Supabase persist 스케줄 실행")
        persist_today_data(dry_run=False, include_current_day=False)
    except Exception as e:
        log.exception("❌ Supabase persist 실행 중 예외: %s", e)

    # 일일 데이터 확정 후 세계 정세 분석 + 전일 브리핑
    run_world_state_analysis()
    run_daily_briefing()

# ───────────────────────────────────────────────────────────
# 트레이딩봇 월간 보고서 (2026-09-02 도입) — 매월 1일 07:30, 지난달 심볼×전략 평가를
# 생성해 파일봇 텔레그램으로 전송. 파라미터 조정 판단은 세션에서 사용자와 진행.
# ───────────────────────────────────────────────────────────
def scheduled_monthly_report():
    import os, subprocess, requests as _rq
    try:
        log.info("📊 월간 보고서 생성 실행")
        r = subprocess.run(["python", "monthly_report.py"], capture_output=True,
                           encoding="utf-8", timeout=300, cwd="/app")
        md = (r.stdout or "").strip()
        if not md.startswith("#"):
            raise RuntimeError(f"보고서 생성 실패: {(r.stderr or md)[:300]}")
        label = md.splitlines()[0].split("—")[-1].strip()
        path = f"/tmp/monthly_{label}.md"
        with open(path, "w", encoding="utf-8") as f:
            f.write(md)
        tok = os.getenv("TELEGRAM_FILEBOT_TOKEN", "").strip()
        if tok:
            with open(path, "rb") as f:
                _rq.post(f"https://api.telegram.org/bot{tok}/sendDocument",
                         data={"chat_id": "7762304100",
                               "caption": f"📊 트레이딩봇 월간 보고서 {label} — 세션에서 '월간 보고서 리뷰'로 조정 판단 진행"},
                         files={"document": (f"tradingbot_{label}.md", f)}, timeout=60)
            log.info("📊 월간 보고서 전송 완료 (%s)", label)
        else:
            log.warning("📊 TELEGRAM_FILEBOT_TOKEN 미설정 — 보고서 전송 생략(%s)", path)
    except Exception as e:
        log.exception("❌ 월간 보고서 실행 중 예외: %s", e)


# ───────────────────────────────────────────────────────────
# 기존 저장 루틴
# ───────────────────────────────────────────────────────────
def scheduled_store(run_all: bool = False):
    """기존에 돌리던 저장 작업들.
    ⚠️ 단계별 try 격리 — 한 단계 실패(예: 한투 API 접속불가)가 뒤 단계(특히 23시
    데일리 스냅샷)를 막으면 다음날 persist가 빈 데이터가 됨(2026-07-04 실제 발생)."""
    now = datetime.now(SEOUL)

    # 유튜브: 24시간 매시 (2026-09-19). 종전 11~22시 창은 새벽·아침 업로드(홍콩 00시·인도 01시·한국 08시·미국 09시)를
    # 11시까지 묶어 두어 3~11시간 지연을 만들었다. 쿼터는 재생목록 우선 + search 예산(storage.YT_SEARCH_MAX_PER_DAY)으로 보호.
    try:
        log.info("⏰ YouTube 데이터 저장 (%s)", now.strftime("%Y-%m-%d %H:%M"))
        youtube_result = fetch_and_store_youtube_data()
        log.info(str(youtube_result))
    except Exception as e:
        log.exception("❌ YouTube 저장 중 예외(다음 단계 계속): %s", e)

    # 오늘 브리핑(롤링) — 오늘 요약이 충분히 모이면 홈 브리핑을 낮에도 갱신
    run_rolling_briefing()

    try:
        log.info("📈 chart data 저장 시작...")
        stored_result = fetch_and_store_chart_data()
        log.info(stored_result)
    except Exception as e:
        log.exception("❌ chart data 저장 중 예외(다음 단계 계속): %s", e)

    # 휴일: 월요일
    try:
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
    except Exception as e:
        log.exception("❌ 휴일 데이터 저장 중 예외(다음 단계 계속): %s", e)

    # 데일리: 23시 이후 — persist(익일 06:55)의 입력이라 반드시 실행돼야 함
    try:
        if run_all or (now.hour > 22):
            log.info("🕚 데일리 데이터 저장 실행")
            save_daily_data()
        else:
            log.info("⏭️ 데일리 저장 시간대 아님 (run_all=False)")
    except Exception as e:
        log.exception("❌ 데일리 저장 중 예외: %s", e)

def startup_runs():
    now = datetime.now(SEOUL)
    scheduled_daily_min = 9 * 60 + 1
    cur_min = now.hour * 60 + now.minute
    run_daily_now = abs(cur_min - scheduled_daily_min) > 5

    log.info("🚀 Startup run: scheduled_store(run_all=True) + FULL kline initialize (closed-only)")
    try:
        scheduled_store(run_all=True)

        # ✅ 서버 시작 시에는 오늘 진행 중인 day 저장
        startup_persist_supabase()

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

    scheduler.add_job(
        scheduled_persist_supabase,
        CronTrigger(hour=6, minute=55, timezone=SEOUL),
        id="persist_supabase",
        replace_existing=True,
    )

    # 매월 1일 07:30 — 지난달 트레이딩 월간 보고서(persist 완료 후)
    scheduler.add_job(
        scheduled_monthly_report,
        CronTrigger(day=1, hour=7, minute=30, timezone=SEOUL),
        id="monthly_report",
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