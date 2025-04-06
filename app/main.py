from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import redis
import os
import json
from apscheduler.schedulers.background import BackgroundScheduler
import time

# from .tasks import fetch_and_store_youtube_data
from app.storage import fetch_and_store_youtube_data, scheduled_store

from pathlib import Path
from dotenv import load_dotenv
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

if REDIS_PORT is None:
    raise ValueError("REDIS_PORT 환경 변수가 설정되지 않았습니다.")

redis_client = redis.Redis(
      host=REDIS_HOST,
      port=REDIS_PORT,
      password=REDIS_PASSWORD,
      ssl=True
  )

app = FastAPI()

# 스케줄러 시작
scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_store, 'interval', minutes=20)
scheduler.start()

# CORS 설정 추가
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 모든 도메인 허용 (보안 문제 있으면 특정 도메인만 허용)
    allow_credentials=True,
    allow_methods=["*"],  # 모든 HTTP 메서드 허용 (GET, POST 등)
    allow_headers=["*"],  # 모든 헤더 허용
)

@app.head("/youtube")
def head_video():
    return {}  # HEAD 요청은 본문 없이 응답 가능

@app.get("/youtube")
def get_video():
    try:
        data = redis_client.get('youtube_data')
        return data
    except Exception as e:
        return f"데이터 조회 중 오류 발생: {str(e)}"
    return {"message": "Fetching latest data, please retry in a few seconds."}

@app.get("/youtube/timestamp")
def get_youtube_data_timestamp():
    timestamp = redis_client.get("youtube_data_timestamp")
    if timestamp:
        return timestamp.decode("utf-8")
    return "No timestamp found"

@app.get("/store")
def store_url():
    result = fetch_and_store_youtube_data()
    return {"message": "저장 완료", "result": result}


