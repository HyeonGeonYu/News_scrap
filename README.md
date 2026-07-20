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

