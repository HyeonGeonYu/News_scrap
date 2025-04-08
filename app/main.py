from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from app.redis_client import redis_client
from app.storage import fetch_and_store_youtube_data, scheduled_store


app = FastAPI()

# 스케줄러 시작
from apscheduler.triggers.cron import CronTrigger
scheduler = BackgroundScheduler()
trigger = CronTrigger(minute='0,10,20,30,40,50')  # 매 10분마다
scheduler.add_job(scheduled_store, trigger=trigger)
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
        return int(timestamp.decode("utf-8"))
    return "No timestamp found"

@app.get("/store")
def store_url():
    result = fetch_and_store_youtube_data()
    return {"message": "저장 완료", "result": result}


