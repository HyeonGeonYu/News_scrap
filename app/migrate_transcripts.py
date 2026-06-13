"""
daily_collections.raw_json.youtube_data.[country].summary_content
→ youtube_transcripts 테이블로 마이그레이션 (일회성)
"""
import os
import json
import logging
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SECRET_KEY") or os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_SECRET_KEY 환경변수가 필요함")
    return create_client(url, key)

def run():
    supabase = get_supabase()

    page_size = 50
    offset = 0
    total_inserted = 0
    total_skipped = 0

    while True:
        resp = (
            supabase.table("daily_collections")
            .select("day, raw_json")
            .order("day", desc=True)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            break

        for row in rows:
            day = row["day"]
            raw = row.get("raw_json") or {}
            youtube_data = raw.get("youtube_data") or {}

            transcript_rows = []
            for country, info in youtube_data.items():
                if not isinstance(info, dict):
                    continue
                sc = info.get("summary_content")
                if sc:
                    transcript_rows.append({"day": day, "country": country, "summary_content": sc})

            if not transcript_rows:
                total_skipped += 1
                continue

            supabase.table("youtube_transcripts").upsert(
                transcript_rows, on_conflict="day,country"
            ).execute()
            log.info("day=%s → %d개 삽입", day, len(transcript_rows))
            total_inserted += len(transcript_rows)

        offset += page_size
        if len(rows) < page_size:
            break

    log.info("완료: 총 %d개 삽입, %d일 스킵(transcript 없음)", total_inserted, total_skipped)

if __name__ == "__main__":
    run()
