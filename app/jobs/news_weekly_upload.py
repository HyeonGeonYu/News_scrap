# jobs/news_weekly_upload.py
import os
import json
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import Iterable, Optional, List, Dict, Any, Tuple

import psycopg
import redis
from dotenv import load_dotenv


KST = timezone(timedelta(hours=9))
REDIS_KEY_PREFIX = "news:daily_saved_data:"  # + YYYYMMDD


# -------------------------
# date helpers
# -------------------------
def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def parse_yyyymmdd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def iter_last_n_days(end_day: date, n: int) -> Iterable[date]:
    start = end_day - timedelta(days=n - 1)
    d = start
    while d <= end_day:
        yield d
        d += timedelta(days=1)


def week_id_sunday(today: date) -> str:
    """
    week_id = 이번 주 일요일(YYYYMMDD)
    - 일요일이면 자기 자신
    - 월~토면 직전 일요일
    """
    days_since_sun = (today.weekday() + 1) % 7  # 일(6)->0, 월(0)->1 ...
    sunday = today - timedelta(days=days_since_sun)
    return yyyymmdd(sunday)


# -------------------------
# env / clients
# -------------------------
def load_env():
    """
    우선순위:
    1) 프로젝트 루트(.env) (jobs/ 상위 폴더 기준)
    2) 현재 파일 같은 폴더의 .env
    """
    here = Path(__file__).resolve()
    root_env = here.parents[1] / ".env"  # jobs/.. == project root 가정
    local_env = here.parent / ".env"

    if root_env.exists():
        load_dotenv(dotenv_path=root_env)
    elif local_env.exists():
        load_dotenv(dotenv_path=local_env)
    else:
        # 그래도 OS env로 들어올 수 있으니 무조건 에러내진 않음
        pass


def get_redis_client() -> redis.Redis:
    host = os.getenv("REDIS_HOST")
    port = os.getenv("REDIS_PORT")
    password = os.getenv("REDIS_PASSWORD")

    if not host or not port:
        raise RuntimeError("REDIS_HOST / REDIS_PORT missing in .env")
    if not password:
        raise RuntimeError("REDIS_PASSWORD missing in .env")

    return redis.Redis(
        host=host,
        port=int(port),
        password=password,
        ssl=True,  # Upstash는 보통 TLS
        decode_responses=True,
        socket_timeout=10,
        socket_connect_timeout=10,
    )


def get_pg_dsn() -> str:
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN missing in .env")
    return dsn


# -------------------------
# core upload logic
# -------------------------
def collect_rows_from_redis(r: redis.Redis, *, end_day: date, n_days: int) -> Tuple[str, date, date, List[Dict[str, Any]]]:
    """
    Redis에서 최근 n_days를 훑어서 존재하는 키만 rows로 만든다.
    반환:
      week_id, week_start, week_end, rows
    """
    week_id = week_id_sunday(end_day)
    week_start = parse_yyyymmdd(week_id)

    days = list(iter_last_n_days(end_day, n_days))
    keys = [f"{REDIS_KEY_PREFIX}{yyyymmdd(d)}" for d in days]
    vals = r.mget(keys)

    rows: List[Dict[str, Any]] = []
    last_present_day: Optional[date] = None

    for d, k, v in zip(days, keys, vals):
        if not v:
            continue
        try:
            payload_obj = json.loads(v)
        except Exception:
            print("⚠️ JSON parse failed:", k)
            continue

        rows.append(
            {
                "day": d,
                "week_id": week_id,
                "payload": json.dumps(payload_obj, ensure_ascii=False),
                "source": "redis:news:daily_saved_data",
            }
        )
        last_present_day = d

    week_end = last_present_day or end_day
    return week_id, week_start, week_end, rows


def upsert_to_supabase(*, pg_dsn: str, week_id: str, week_start: date, week_end: date, rows: List[Dict[str, Any]]) -> None:
    with psycopg.connect(pg_dsn) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                insert into public.news_daily_raw(day, week_id, payload, source)
                values (%(day)s, %(week_id)s, %(payload)s::jsonb, %(source)s)
                on conflict (day) do update set
                  week_id = excluded.week_id,
                  payload = excluded.payload,
                  ingested_at = now(),
                  source = excluded.source
                """,
                rows,
            )

            cur.execute(
                """
                insert into public.news_weekly_runs(week_id, week_start, week_end, days_uploaded, last_uploaded_at)
                values (%s, %s, %s, %s, now())
                on conflict (week_id) do update set
                  week_end = excluded.week_end,
                  days_uploaded = excluded.days_uploaded,
                  last_uploaded_at = now()
                """,
                (week_id, week_start, week_end, len(rows)),
            )

        conn.commit()


# -------------------------
# entrypoint
# -------------------------
def run_weekly_news_upload(n_days: int = 7) -> None:
    """
    스케줄러에서 호출할 함수(엔트리).
    - 최근 n_days 중 존재하는 일자만 업로드
    """
    load_env()

    end_day = datetime.now(KST).date()
    r = get_redis_client()
    pg_dsn = get_pg_dsn()

    week_id, week_start, week_end, rows = collect_rows_from_redis(r, end_day=end_day, n_days=n_days)

    if not rows:
        print(f"✅ 업로드할 데이터가 없음 (최근 {n_days}일) / week_id={week_id}")
        return

    upsert_to_supabase(pg_dsn=pg_dsn, week_id=week_id, week_start=week_start, week_end=week_end, rows=rows)
    print(f"✅ 업로드 완료: {len(rows)}일치 / week_id={week_id} (week_start={week_start}, week_end={week_end})")


if __name__ == "__main__":
    run_weekly_news_upload(n_days=7)