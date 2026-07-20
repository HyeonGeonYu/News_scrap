import os
from dotenv import load_dotenv
from pathlib import Path
import redis

env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

if REDIS_PORT is None:
    raise ValueError("REDIS_PORT 환경 변수가 설정되지 않았습니다.")

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=int(REDIS_PORT),
    password=REDIS_PASSWORD,
    ssl=True
)

# 트레이딩 데이터(config/asset/signals/trade_records/lots/OpenPctLog)는 tradingBot이
# 2026-07-16 로컬 Docker Redis로 이전 → Upstash 복사본은 stale. persist가 이걸로 읽는다.
# TRADING_REDIS_URL 예: redis://host.docker.internal:6379/0 (또는 redis://127.0.0.1:6379/0)
# 미설정 시 기존 Upstash(redis_client)로 폴백 → 동작 불변.
TRADING_REDIS_URL = os.getenv("TRADING_REDIS_URL")
trading_redis_client = redis.from_url(TRADING_REDIS_URL) if TRADING_REDIS_URL else redis_client