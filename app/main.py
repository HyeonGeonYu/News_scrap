from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from app.redis_client import redis_client
from app.storage import scheduled_store
import json
from datetime import datetime
from app.test_config import channels
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
def youtube_data():

    countries = [item['country'] for item in channels]
    result = {}

    for country in countries:
        raw_data = redis_client.get(f"youtube_data:{country}")
        ts = redis_client.get(f"youtube_data_timestamp:{country}")

        try:
            data = json.loads(raw_data)
            data["processedAt"] = int(ts.decode("utf-8"))
            result[country] = data

        except Exception as e:
            result[country] = {"error": f"{country} 처리 중 오류: {str(e)}"}
    return result

# 나스닥 데이터 반환 API 추가
@app.get("/index_data/{index_name}")
def get_index_data(index_name: str):
    """
    나스닥 100 등 다른 인덱스의 데이터를 반환하는 API
    :param index_name: 인덱스 이름 (예: nasdaq100, hshares, kospi 등)
    :return: 지정된 인덱스의 데이터
    """
    try:
        # Redis에서 인덱스 데이터 가져오기
        redis_key = f"index_name:{index_name.lower()}"
        raw_data = redis_client.get(redis_key)

        if raw_data:
            data = json.loads(raw_data)
            return {"data": data}
        else:
            return {"error": f"{index_name} 데이터가 존재하지 않습니다."}

    except Exception as e:
        return {"error": f"데이터를 가져오는 중 오류 발생: {str(e)}"}
