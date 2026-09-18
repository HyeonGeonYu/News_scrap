$env:DOCKER_BUILDKIT=1
docker rm -f news-scrap
docker build -f app/Dockerfile -t news-scrap .
docker run -d --name news-scrap `
  --network tradingbot_default `
  --env-file app/.env `
  -e TRADING_REDIS_URL=redis://redis:6379/0 `
  -e TZ=Asia/Seoul `
  --shm-size=1g `
  --memory 4g --memory-swap 4g `
  --restart unless-stopped `
  --gpus all `
  news-scrap

# 실제 운영 기동은 infra/wsl/news-scrap.sh (WSL docker, host net, --memory 4g). 위는 참고용.
# ⚠️ Whisper 는 20분 조각 순차 변환(URL과요약문만들기._transcribe_chunked). 통째 변환은 1~2시간 방송에서
#    RSS 3GB+ → OOM-kill → restart 루프(2026-09-16 631회)로 타국 수집까지 막았음. storage.get_transcript_guarded 가
#    같은 영상 STT 를 2회까지만 허용(Redis news:stt_attempt:<video_id>).

# ⚠️ --network tradingbot_default + TRADING_REDIS_URL=redis://redis:6379/0 필수:
#    트레이딩 데이터는 tradingBot 로컬 Redis(2026-07-16 이전)에서 읽는다.
#    빠지면 persist가 stale Upstash를 읽어 아카이브/equity가 옛날값이 됨.

# ── LLM 요약 = 구독 Claude (2026-09-12 전환, app/llm.py) ─────────────────────
# 요약 3곳(나라별 자막 요약·세계정세 map/reduce·전일 브리핑)이 `claude -p --json-schema`로 돈다.
# 실패·한도 소진·토큰 없음이면 자동으로 OpenAI gpt-4.1-mini 폴백(종전 동작). 로그로 구분:
#   "🤖 CLAUDE OK role=… model=… in=… out=…"  /  "⚠️ CLAUDE FAIL … → OPENAI FALLBACK: …"
# app/.env 키:
#   CLAUDE_CODE_OAUTH_TOKEN   `claude setup-token`(Windows 터미널, 브라우저 승인) 결과. 1년 만료 → 2027-09 갱신
#   CLAUDE_MODEL=opus         기본 모델. 역할별 덮어쓰기: CLAUDE_MODEL_SUMMARY / _WORLD_MAP / _WORLD_REDUCE / _BRIEFING
#                             (opus|sonnet|haiku). 바꾸면 컨테이너 재생성(docker rm -f + run) 필요(--env-file은 생성 시 읽음)
#   CLAUDE_DISABLE=1          Claude 끄고 OpenAI만
# 소모량(구독 한도 추이) 확인 — Redis news:llm:usage:YYYYMMDD 누적:
#   docker exec news-scrap python -c "import llm,json; print(json.dumps(llm.usage_report(7), ensure_ascii=False, indent=1))"
# 연결 점검(작은 호출 1회): docker exec -e CLAUDE_MODEL=haiku news-scrap python llm.py
# 재시작 시 세계정세 분석은 당일 저장분이 있으면 스킵(한도 보호). 강제: python 세계정세분석.py

# ── 신선도 개선 (2026-09-19) ────────────────────────────────────────────────
# 1) 유튜브 수집 창 11~22시 → 24시간 매시. 새벽·아침 업로드(홍콩 00시·인도 01시·한국 08시·미국 09시)가
#    11시까지 대기하던 3~11시간 지연 제거. 쿼터는 재생목록(1유닛) 우선(후보가 24h 이내면 search 생략) +
#    search.list(100유닛, order=date)는 짝수 시각에만·나라별 하루 YT_SEARCH_MAX_PER_DAY(기본 6)회
#    (Redis news:yt_search:<국가>:<날짜>). 최악 ≈7,000유닛 < 10,000. BBC·PBS 재생목록은 오래된 순이라 search 로 잡힘.
# 2) 오늘 브리핑(롤링): 오늘(KST 달력일) 요약이 ROLLING_MIN_COUNTRIES(기본 4)개국 이상이면 매시 수집 직후
#    홈 브리핑(youtube_data.global_briefing)을 다시 생성(rolling=true, countries_in, generated_at). 새 입력 없음·
#    직전 갱신 ROLLING_MIN_INTERVAL_MIN(기본 120)분 이내면 스킵 → 하루 3~5회. 06:55 최종본(전일)은 종전대로
#    히스토리(news:daily_briefing:*)까지 저장하며 롤링을 대체. 모델: CLAUDE_MODEL_BRIEFING_ROLLING.
#    프론트(hyeongeonnoil GlobalBriefingCard)는 rolling 여부로 "오늘/전일 글로벌 브리핑" 제목·갱신 시각 표시.

