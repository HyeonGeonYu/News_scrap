$env:DOCKER_BUILDKIT=1
docker rm -f news-scrap
docker build -f app/Dockerfile -t news-scrap .
docker run -d --name news-scrap `
  --network tradingbot_default `
  --env-file app/.env `
  -e TRADING_REDIS_URL=redis://redis:6379/0 `
  -e TZ=Asia/Seoul `
  --shm-size=1g `
  --restart unless-stopped `
  --gpus all `
  news-scrap

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

