# 세계정세분석.py
# 최근 한 달치 나라별 뉴스 요약(summary_result)을 모아서
# 각국 상태 + 양자 관계를 게임 대시보드용 구조화 JSON으로 추출한다.
#
# 06:55 scheduled_persist_supabase(전날 06:50~당일 06:50 윈도우 확정) 직후 매일 실행.
# 입력이 롤링 한 달 윈도우라 매일 돌리면 상태가 조금씩만 변동(급변 X).
#
# 입력:
#   - Supabase daily_collections 최근 N일(기본 30) (raw_json.youtube_data[country].summary_result)
#   - (폴백) Redis news:daily_saved_data:YYYYMMDD 스냅샷
# 출력:
#   - Supabase world_state (week_start PK, raw_json, updated_at) ← 그 주 행을 매일 upsert
#     · 같은 주에는 행을 갱신 → 주가 끝나면 그 주 최종 상태로 freeze
#     · 아카이브 사이트에서 week_start 기준 주차별 조회
#     ※ 테이블 사전 생성 필요
import os
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta

from pytz import timezone
from dotenv import load_dotenv
from openai import OpenAI

from redis_client import redis_client

env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai_client = OpenAI(api_key=OPENAI_API_KEY)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SEOUL = timezone("Asia/Seoul")

# 프론트 newsParams.order / channels[].country 와 동일한 키
COUNTRIES = ["Korea", "USA", "Japan", "China", "Germany", "UK", "India"]


# ───────────────────────────────────────────────────────────
# 1) 입력 수집: 최근 N일 국가별 요약
# ───────────────────────────────────────────────────────────
def _extract_summaries_from_youtube_data(yd: dict, day, per_country: dict):
    for country, info in (yd or {}).items():
        if country not in per_country or not isinstance(info, dict):
            continue
        summ = info.get("summary_result")
        if summ and not any(it["date"] == str(day) for it in per_country[country]):
            per_country[country].append({
                "date": str(day),
                "summary": summ,
                "url": info.get("url"),
                "title": info.get("title"),
            })


def _collect_from_supabase(days: int, per_country: dict) -> bool:
    """Supabase daily_collections 최근 N일에서 summary_result 수집. 성공 여부 반환."""
    try:
        from persist import get_supabase
        supabase = get_supabase()
        resp = (supabase.table("daily_collections")
                .select("day, raw_json")
                .order("day", desc=True)
                .limit(days)
                .execute())
        rows = resp.data or []
    except Exception as e:
        log.warning("⚠️ daily_collections 조회 실패, Redis 폴백 사용: %s", e)
        return False

    for row in rows:
        raw = row.get("raw_json") or {}
        _extract_summaries_from_youtube_data(raw.get("youtube_data", {}), row.get("day"), per_country)
    return True


def _collect_from_redis(days: int, per_country: dict):
    """폴백: Redis news:daily_saved_data:YYYYMMDD 스냅샷에서 수집."""
    now = datetime.now(SEOUL)
    for i in range(days, 0, -1):
        date_str = (now - timedelta(days=i)).strftime("%Y%m%d")
        raw = redis_client.get(f"news:daily_saved_data:{date_str}")
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        _extract_summaries_from_youtube_data(payload.get("youtube_data", {}), date_str, per_country)


def _collect_recent_summaries(days: int = 30):
    """국가별 최근 N일 summary_result 를 (날짜, 텍스트) 리스트로 모은다.
    Supabase daily_collections 우선, 실패 시 Redis 스냅샷 폴백."""
    per_country = {c: [] for c in COUNTRIES}

    if not _collect_from_supabase(days, per_country):
        _collect_from_redis(days, per_country)

    # Supabase 결과가 비면 Redis 폴백 보강
    if all(len(v) == 0 for v in per_country.values()):
        _collect_from_redis(days, per_country)

    counts = {c: len(v) for c, v in per_country.items()}
    log.info("📚 수집된 요약 개수: %s", counts)
    return per_country


def _build_input_text(per_country: dict) -> str:
    blocks = []
    for country in COUNTRIES:
        items = sorted(per_country.get(country, []), key=lambda x: x["date"])
        if not items:
            body = "(최근 뉴스 요약 없음)"
        else:
            body = "\n\n".join(f"[{it['date']}]\n{it['summary']}" for it in items)
        blocks.append(
            f"========== {country} 뉴스 채널 보도 시작 (출처국 = {country}) ==========\n"
            f"{body}\n"
            f"========== {country} 뉴스 채널 보도 끝 =========="
        )
    return "\n\n".join(blocks)


def _build_sources(per_country: dict) -> dict:
    """{country: {date: {url, title}}} — 프론트가 근거 날짜를 원문 링크로 변환할 때 사용."""
    out = {}
    for country, items in per_country.items():
        m = {}
        for it in items:
            if it.get("url") or it.get("title"):
                m[it["date"]] = {"url": it.get("url"), "title": it.get("title")}
        if m:
            out[country] = m
    return out


# ───────────────────────────────────────────────────────────
# 2) LLM 구조화 분석
# ───────────────────────────────────────────────────────────
def _claim_schema():
    """근거(text + 출처 날짜들)를 가진 주장 단위. dates 는 입력에 등장한 날짜 문자열."""
    return {
        "type": "object",
        "properties": {
            "text":  {"type": "string"},
            "dates": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["text", "dates"],
        "additionalProperties": False,
    }


def _country_schema():
    props = {
        "mood":         {"type": "string"},
        "mood_score":   {"type": "integer"},
        "icon":         {"type": "string"},
        "eco":          {"type": "integer"},
        "pol":          {"type": "integer"},
        "dip":          {"type": "integer"},
        "score_basis":  _claim_schema(),               # eco/pol/dip 점수 산정 근거 + 날짜
        "issues":       {"type": "array", "items": _claim_schema()},  # 정확히 3개
        "worry":        _claim_schema(),
        "hope":         _claim_schema(),
        "special_note": {"type": "string"},            # 공휴일 목록과 분리된 '특이사항'
    }
    return {
        "type": "object",
        "properties": props,
        "required": list(props.keys()),
        "additionalProperties": False,
    }


def _relations_schema():
    """reduce 단계: 관계 배열만."""
    return {
        "type": "object",
        "properties": {
            "relations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "a":     {"type": "string", "enum": COUNTRIES},
                        "b":     {"type": "string", "enum": COUNTRIES},
                        "score": {"type": "integer"},
                        "label": {"type": "string"},
                        "dates": {"type": "array", "items": {"type": "string"}},  # 근거 날짜
                    },
                    "required": ["a", "b", "score", "label", "dates"],
                    "additionalProperties": False,
                },
            },
        },
        "required": ["relations"],
        "additionalProperties": False,
    }


# map 단계: 나라 1개만 입력 → 그 나라 텍스트만 보므로 타국 내용 오귀속이 구조적으로 불가능.
# __C__ 는 호출 시 나라명으로 치환. ({text,dates} 리터럴 때문에 f-string 안 씀)
_COUNTRY_PROMPT_TMPL = (
    "당신은 국제 정세 분석가다. 아래는 __C__ 뉴스 채널의 최근 약 한 달 보도 요약이다.\n"
    "오직 __C__ 자신의 현재 상태를 게임 대시보드용 구조화 데이터로 분석한다.\n"
    "최근 흐름일수록 더 비중을 두되, 한 달 추세 위에서 현재 상태를 판단한다.\n\n"
    "규칙:\n"
    "- 모든 텍스트는 한국어.\n"
    "- 이 채널이 다른 나라를 보도하더라도 그건 __C__의 현안이 아니다. __C__ 자신의 상황·이슈·입장만 뽑아라.\n"
    "- mood: 현재 분위기 2~4글자 형용사(예: 긴장된, 강경한, 안정적, 도약중, 불안한).\n"
    "- mood_score: 종합 안정도 0~10 정수(0=위기, 10=매우 안정).\n"
    "- icon: __C__를 상징하는 이모지 1개.\n"
    "- eco / pol / dip: 경제 / 정치안정 / 외교 점수 각 0~10 정수.\n"
    "- score_basis: {text, dates} — eco/pol/dip 점수를 그렇게 매긴 근거 한두 문장 + 근거 날짜.\n"
    "- issues: {text, dates} 정확히 3개. text 는 25자 이내 현안 한 줄.\n"
    "- worry: {text, dates} — 가장 걱정하는 것 한 문장(30자 이내).\n"
    "- hope: {text, dates} — 기대하는 것 한 문장(30자 이내).\n"
    "- special_note: 이번 주 특이사항(휴장·큰 행사 등) 한 줄. 공휴일 목록은 제외. 없으면 빈 문자열.\n"
    "- dates: 입력에 실제 등장한 날짜 문자열 그대로(예: \"2026-06-15\"). 지어내지 말 것. 근거 없으면 [].\n"
    "- 정보가 부족하면 합리적으로 추론하되 과장하지 않는다.\n"
)

# reduce 단계: 7개국 전체를 보고 관계만. 관계는 양쪽 블록 종합이 핵심.
RELATIONS_PROMPT = (
    "당신은 국제 정세 분석가다. 아래는 7개국(Korea, USA, Japan, China, Germany, UK, India)\n"
    "뉴스 채널의 최근 약 한 달 보도다. 각 나라는 '========== {나라} 뉴스 채널 보도 시작 ...' 블록으로 구분된다.\n"
    "7개국 사이의 의미있는 양자 관계만 분석한다(보통 8~14개).\n\n"
    "규칙:\n"
    "- 각 관계는 두 나라 a, b '양쪽 블록을 종합'해서 판단한다"
    "(a 채널이 b를 어떻게 보도/대하는지 + b 채널이 a를 어떻게 보도/대하는지를 함께 본다. 한쪽만 보지 마라).\n"
    "- score: -5(적대) ~ +5(동맹) 정수. 양쪽 관점이 다르면 종합해 한 값으로.\n"
    "- label: 관계 핵심 10자 이내(예: 무역전쟁 확전, 동맹 강화, 국경 분쟁).\n"
    "- dates: 근거 날짜. a 또는 b 둘 중 어느 블록에 등장한 날짜든 쓸 수 있다. 입력에 실제 있는 날짜만.\n"
    "- 같은 나라 쌍 중복 금지((a, b) 순서만 다른 것도).\n"
    "- 모든 텍스트는 한국어.\n"
)


def _analyze_one_country(country: str, items: list) -> dict:
    """map: 그 나라 요약만 입력 → 타국 내용이 물리적으로 안 섞임."""
    items = sorted(items, key=lambda x: x["date"])
    if items:
        text = "\n\n".join(f"[{it['date']}]\n{it['summary']}" for it in items)
    else:
        text = "(최근 뉴스 요약 없음)"

    completion = openai_client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": _COUNTRY_PROMPT_TMPL.replace("__C__", country)},
            {"role": "user", "content": f"[{country} 뉴스 채널 보도]\n\n{text}"},
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {"name": "country_state", "strict": True, "schema": _country_schema()},
        },
    )
    return json.loads(completion.choices[0].message.content)


def _analyze_relations(per_country: dict) -> list:
    """reduce: 7개국 전체를 보고 양자 관계만 종합."""
    input_text = _build_input_text(per_country)
    completion = openai_client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": RELATIONS_PROMPT},
            {"role": "user", "content": input_text},
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {"name": "relations", "strict": True, "schema": _relations_schema()},
        },
    )
    return json.loads(completion.choices[0].message.content).get("relations", [])


def analyze_world_state(days: int = 30) -> dict:
    per_country = _collect_recent_summaries(days)

    if all(len(v) == 0 for v in per_country.values()):
        raise RuntimeError("분석할 뉴스 요약이 없습니다.")

    # MAP: 나라별 독립 분석 (다른 나라 텍스트가 섞이지 않아 오귀속 불가)
    countries = {}
    for c in COUNTRIES:
        log.info("🤖 [map] %s 분석", c)
        countries[c] = _analyze_one_country(c, per_country.get(c, []))

    # REDUCE: 관계는 전체 블록 종합
    log.info("🤖 [reduce] 관계 분석")
    relations = _analyze_relations(per_country)

    result = {
        "countries": countries,
        "relations": relations,
        # 근거 날짜 → 원문 링크 변환용 출처 맵 (LLM이 아니라 백엔드가 부착)
        "sources": _build_sources(per_country),
    }
    log.info("✅ 분석 완료: countries=%d relations=%d sources=%d",
             len(countries), len(relations),
             sum(len(v) for v in result["sources"].values()))
    return result


# ───────────────────────────────────────────────────────────
# 3) 저장
# ───────────────────────────────────────────────────────────
def _week_start(dt) -> str:
    """해당 날짜가 속한 주의 월요일(KST) 날짜 문자열 YYYY-MM-DD."""
    monday = dt - timedelta(days=dt.weekday())  # weekday: 월=0
    return monday.strftime("%Y-%m-%d")


def store_world_state(result: dict):
    now = datetime.now(SEOUL)
    week_start = _week_start(now)

    # Supabase (archive) — 그 주 행을 매일 upsert (주가 끝나면 그 주 최종 상태로 freeze)
    try:
        from persist import get_supabase
        supabase = get_supabase()
        supabase.table("world_state").upsert({
            "week_start": week_start,
            "raw_json": result,
            "updated_at": now.isoformat(),
        }, on_conflict="week_start").execute()
        log.info("✅ world_state Supabase 저장 완료 week_start=%s", week_start)
    except Exception as e:
        log.warning("⚠️ world_state Supabase 저장 실패(테이블 존재 확인): %s", e)


def analyze_and_store_world_state(days: int = 30) -> dict:
    """스케줄러/수동 실행 진입점."""
    result = analyze_world_state(days)
    store_world_state(result)
    return result


if __name__ == "__main__":
    out = analyze_and_store_world_state(days=30)
    print(json.dumps(out, ensure_ascii=False, indent=2))
