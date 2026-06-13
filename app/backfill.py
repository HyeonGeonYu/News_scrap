"""
과거 Supabase daily_collections 데이터에 Whisper 텍스트 + GPT 요약을 채우는 백필 스크립트.

실행:
    python backfill.py            # 실제 저장
    python backfill.py --dry-run  # 저장 없이 로그만 확인
"""

import sys
import json
import argparse
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

from persist import get_supabase
from URL과요약문만들기 import get_transcript_text, summarize_content


def extract_video_id(url):
    if not url:
        return None
    try:
        parsed = urlparse(url)
        return parse_qs(parsed.query).get("v", [None])[0]
    except Exception:
        return None


def needs_backfill(video_data):
    if not video_data:
        return False
    summary_result = video_data.get("summary_result")
    summary_content = video_data.get("summary_content")
    has_summary = bool(summary_result)
    has_text = bool(summary_content) and len(summary_content) > 50
    return not has_summary  # 요약이 없으면 백필 대상


def backfill(dry_run=False, limit=30):
    supabase = get_supabase()

    rows = supabase.table("daily_collections")\
        .select("day, raw_json")\
        .order("day", desc=True)\
        .limit(limit)\
        .execute()

    total_updated = 0

    for row in rows.data:
        day = row["day"]
        raw_json = row.get("raw_json") or {}
        youtube_data = raw_json.get("youtube_data") or {}

        if not youtube_data:
            print(f"[{day}] youtube_data 없음, 스킵")
            continue

        row_updated = False

        for country, video_data in youtube_data.items():
            if not needs_backfill(video_data):
                print(f"[{day}] {country} — 이미 요약 있음, 스킵")
                continue

            url = video_data.get("url")
            video_id = extract_video_id(url)
            summary_content = video_data.get("summary_content")
            has_text = bool(summary_content) and len(summary_content) > 50

            print(f"[{day}] {country} — 백필 시작 (텍스트: {'있음' if has_text else '없음'}, video_id: {video_id})")

            # 텍스트가 없으면 Whisper 시도
            if not has_text:
                if not video_id:
                    print(f"[{day}] {country} — video_id 추출 실패, 스킵")
                    continue

                transcript = get_transcript_text(video_id)
                if transcript:
                    print(f"[{day}] {country} — Whisper 완료 ({len(transcript)}자)")
                    video_data["summary_content"] = transcript
                elif summary_content and len(summary_content) > 50:
                    print(f"[{day}] {country} — Whisper 실패, 기존 description 사용 ({len(summary_content)}자)")
                else:
                    print(f"[{day}] {country} — Whisper 실패 + 유효한 텍스트 없음, 스킵")
                    continue

            # GPT 요약 생성
            content_for_summary = video_data.get("summary_content")
            summary_result = summarize_content(content_for_summary)

            if summary_result:
                print(f"[{day}] {country} — GPT 요약 완료")
                video_data["summary_result"] = summary_result
                row_updated = True
            else:
                print(f"[{day}] {country} — GPT 요약 실패")

        if row_updated:
            # youtube_transcripts에 transcript 별도 저장
            transcript_rows = []
            for country, video_data in youtube_data.items():
                sc = video_data.get("summary_content")
                if sc:
                    transcript_rows.append({"day": day, "country": country, "summary_content": sc})

            # daily_collections에는 summary_content 제거하고 저장
            youtube_data_stripped = {
                country: {k: v for k, v in info.items() if k != "summary_content"}
                for country, info in youtube_data.items()
            }
            raw_json["youtube_data"] = youtube_data_stripped

            if not dry_run:
                if transcript_rows:
                    supabase.table("youtube_transcripts").upsert(
                        transcript_rows, on_conflict="day,country"
                    ).execute()
                supabase.table("daily_collections").update({
                    "raw_json": raw_json,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }).eq("day", day).execute()
                print(f"[{day}] ✅ Supabase 업데이트 완료 (transcripts: {len(transcript_rows)}개)")
            else:
                print(f"[{day}] [DRY-RUN] 저장 스킵")
            total_updated += 1

    print(f"\n완료: {total_updated}일 업데이트됨")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="저장 없이 로그만 확인")
    parser.add_argument("--limit", type=int, default=30, help="조회할 최대 일수 (기본: 30)")
    args = parser.parse_args()

    backfill(dry_run=args.dry_run, limit=args.limit)
