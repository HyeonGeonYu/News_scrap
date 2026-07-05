# 전일브리핑.py
# 확정된 전날(06:50 윈도우) 각국 뉴스 요약(summary_result)을 모두 모아
# "핵심적으로 봐야 할 뉴스 5개"를 선별한다 — 여러 나라가 공통으로 주목한 사안 우선.
#
# 실행 시점: 06:55 scheduled_persist_supabase(전날 데이터 확정) 직후 매일.
#   예) 7/8 06:55 실행 → 7/7 06:50~7/8 06:50 윈도우(day="2026-07-07") 데이터로 브리핑.
#
# 입력:
#   - Supabase daily_collections day=전날 (raw_json.youtube_data[country].summary_result)
#   - (폴백) Redis news:daily_saved_data:YYYYMMDD 스냅샷
# 출력:
#   - Redis hset youtube_data["global_briefing"] = {date, generated_at, items[5]}
#     → 기존 /youtube 패스스루로 프론트 홈에 자동 전파 (새 API 불필요)
#   - Redis set news:daily_briefing:YYYYMMDD (TTL 30일, 히스토리)
import os
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta, time as dtime

from pytz import timezone
from dotenv import load_dotenv
from openai import OpenAI

from redis_client import redis_client

env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SEOUL = timezone("Asia/Seoul")

# 세계정세분석.py / 프론트 newsParams와 동일 키
COUNTRIES = ["Korea", "USA", "Japan", "China", "Germany", "UK", "India", "HongKong"]

BRIEFING_PROMPT = (
    "너는 글로벌 뉴스 데스크의 수석 에디터다. 아래는 어제 하루 동안 각국 대표 뉴스 채널이 보도한 내용의 요약이다.\n"
    "이걸 종합해 '어제 세계에서 핵심적으로 봐야 할 뉴스 5개'를 선별하라.\n\n"
    "선별 기준(중요):\n"
    "1. 여러 나라 채널이 공통으로 다룬 사안을 최우선으로 뽑아라 — 그것이 글로벌 이슈다.\n"
    "2. 금융시장·지정학·거시경제에 파급이 큰 사안을 우선하라.\n"
    "3. 한 나라만 다뤘어도 파급이 명백히 크면 포함 가능.\n"
    "4. 같은 사안은 하나로 묶어라(중복 금지). 각 항목의 summary는 2~3문장으로: 무슨 일이 있었고 왜 중요한지.\n"
    "5. countries에는 그 사안을 보도한 나라(입력에 실제 등장한 나라만)를 나열하라.\n"
    "반드시 한국어로, 정확히 5개를 rank 1(가장 중요)부터 순서대로 출력하라."
)


def _briefing_schema() -> dict:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "rank": {"type": "integer"},
                        "title": {"type": "string"},
                        "summary": {"type": "string"},
                        "countries": {"type": "array", "items": {"type": "string"}},
                        "category": {
                            "type": "string",
                            "enum": ["지정학", "경제·시장", "정치", "산업·기술", "사회·기타"],
                        },
                    },
                    "required": ["rank", "title", "summary", "countries", "category"],
                },
            }
        },
        "required": ["items"],
    }


# ───────────────────────────────────────────────────────────
# 1) 전날 하루치 수집
# ───────────────────────────────────────────────────────────
def _target_day(now=None) -> str:
    """직전 완료된 06:50 윈도우의 day(YYYY-MM-DD). persist_today_data와 동일 규칙."""
    now = now or datetime.now(SEOUL)
    today_0650 = SEOUL.localize(datetime.combine(now.date(), dtime(6, 50)))
    day_start = (today_0650 - timedelta(days=1)) if now >= today_0650 else (today_0650 - timedelta(days=2))
    return day_start.strftime("%Y-%m-%d")


def _extract(yd: dict) -> dict:
    out = {}
    for country, info in (yd or {}).items():
        if country in COUNTRIES and isinstance(info, dict) and info.get("summary_result"):
            out[country] = str(info["summary_result"])
    return out


def _collect_day_summaries(day: str) -> dict:
    """{country: summary_text} — Supabase 우선, Redis 스냅샷 폴백."""
    per_country = {}
    try:
        from persist import get_supabase
        supabase = get_supabase()
        resp = (supabase.table("daily_collections")
                .select("day, raw_json")
                .eq("day", day)
                .limit(1)
                .execute())
        rows = resp.data or []
        if rows:
            per_country = _extract((rows[0].get("raw_json") or {}).get("youtube_data", {}))
    except Exception as e:
        log.warning("⚠️ daily_collections(day=%s) 조회 실패, Redis 폴백: %s", day, e)

    if not per_country:
        raw = redis_client.get(f"news:daily_saved_data:{day.replace('-', '')}")
        if raw:
            try:
                per_country = _extract(json.loads(raw).get("youtube_data", {}))
            except Exception:
                pass

    # 3차 폴백: 라이브 youtube_data 해시에서 processed_time이 그 날 06:50 윈도우에 든 것만.
    # (23시 스냅샷/persist가 누락된 날에도 그날 처리된 나라만큼은 브리핑 가능)
    if not per_country:
        try:
            day_start = SEOUL.localize(datetime.strptime(day, "%Y-%m-%d").replace(hour=6, minute=50))
            day_end = day_start + timedelta(days=1)
            from pytz import utc as _utc
            live = redis_client.hgetall("youtube_data") or {}
            picked = {}
            for cb, jb in live.items():
                c = cb.decode() if isinstance(cb, (bytes, bytearray)) else str(cb)
                if c not in COUNTRIES:
                    continue
                try:
                    info = json.loads(jb.decode() if isinstance(jb, (bytes, bytearray)) else jb)
                    pt = _utc.localize(datetime.strptime(info["processed_time"], "%Y-%m-%dT%H:%M:%SZ")).astimezone(SEOUL)
                    if day_start <= pt < day_end and info.get("summary_result"):
                        picked[c] = str(info["summary_result"])
                except Exception:
                    continue
            if picked:
                log.info("ℹ️ 라이브 해시 폴백 사용 (day=%s): %s", day, list(picked.keys()))
                per_country = picked
        except Exception as e:
            log.warning("⚠️ 라이브 해시 폴백 실패: %s", e)
    return per_country


# ───────────────────────────────────────────────────────────
# 2) LLM 선별 + 저장
# ───────────────────────────────────────────────────────────
def generate_daily_briefing(day: str | None = None) -> dict:
    day = day or _target_day()
    per_country = _collect_day_summaries(day)
    if not per_country:
        raise RuntimeError(f"브리핑 입력 없음 (day={day})")

    blocks = [f"===== {c} 뉴스 채널 ({day}) =====\n{txt}" for c, txt in per_country.items()]
    log.info("🗞️ 브리핑 입력: day=%s countries=%s", day, list(per_country.keys()))

    completion = openai_client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": BRIEFING_PROMPT},
            {"role": "user", "content": "\n\n".join(blocks)},
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {"name": "daily_briefing", "strict": True, "schema": _briefing_schema()},
        },
    )
    items = json.loads(completion.choices[0].message.content).get("items", [])
    items = sorted(items, key=lambda x: x.get("rank", 99))[:5]

    return {
        "date": day,
        "generated_at": datetime.now(SEOUL).isoformat(),
        "items": items,
    }


def store_daily_briefing(briefing: dict):
    payload = json.dumps(briefing, ensure_ascii=False)
    # 홈 전파용 (기존 /youtube 패스스루) — save_daily_data는 processed_time 없는 필드를 걸러서 안전
    redis_client.hset("youtube_data", "global_briefing", payload)
    # 히스토리 (30일)
    redis_client.set(f"news:daily_briefing:{briefing['date'].replace('-', '')}", payload, ex=30 * 86400)
    log.info("✅ 전일 브리핑 저장 완료 date=%s items=%d", briefing["date"], len(briefing["items"]))


def briefing_already_done(day: str | None = None) -> bool:
    """해당 day 브리핑이 이미 있으면 True (재시작 시 LLM 재호출 방지)."""
    day = day or _target_day()
    try:
        raw = redis_client.hget("youtube_data", "global_briefing")
        if raw:
            cur = json.loads(raw.decode() if isinstance(raw, (bytes, bytearray)) else raw)
            return cur.get("date") == day
    except Exception:
        pass
    return False


def generate_and_store_daily_briefing(force: bool = False) -> dict | None:
    """스케줄러/수동 실행 진입점. 같은 날 중복 생성은 스킵(force=True로 강제)."""
    day = _target_day()
    if not force and briefing_already_done(day):
        log.info("⏭️ 전일 브리핑 이미 생성됨 (day=%s) — 스킵", day)
        return None
    briefing = generate_daily_briefing(day)
    store_daily_briefing(briefing)
    return briefing


if __name__ == "__main__":
    out = generate_and_store_daily_briefing(force=True)
    print(json.dumps(out, ensure_ascii=False, indent=2))
