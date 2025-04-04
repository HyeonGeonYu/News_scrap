"""
from celery import Celery
import os

from pathlib import Path
from dotenv import load_dotenv
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

if REDIS_PORT is None:
    raise ValueError("REDIS_PORT 환경 변수가 설정되지 않았습니다.")

celery_app = Celery(
  'worker',
  broker=f'redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/0',
  backend=f'redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/0'
)
print("Redis 연결 테스트")
print(celery_app.control.inspect().ping())
print("Redis 연결 테스트")
"""