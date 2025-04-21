from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from app.redis_client import redis_client
from app.storage import scheduled_store
import json
from app.test_config import channels
app = FastAPI()

# 스케줄러 시작
from apscheduler.triggers.cron import CronTrigger
scheduler = BackgroundScheduler()
trigger = CronTrigger(minute='0,15,30,45')  # 매 15분마다
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

@app.get("/youtube")
def youtube_data():
    result = {}
    all_data = redis_client.hgetall("youtube_data")

    for country_bytes, raw_data_bytes in all_data.items():
        country = country_bytes.decode()
        try:
            raw_data = raw_data_bytes.decode()
            data = json.loads(raw_data)
            result[country] = data

        except Exception as e:
            result[country] = {"error": f"{country} 처리 중 오류: {str(e)}"}
    return result


@app.get("/chartdata/{category}")
def get_chart_data(category: str):
    """
    통합 차트 데이터 API
    :param category: 'index' 또는 'currency'
    :return: 해당 category의 모든 항목 데이터
    """
    try:
        redis_key = f"chart_data:{category}"  # 저장 시에도 이렇게 되어 있음
        result = redis_client.get(redis_key)
        if result:
            return json.loads(result)  # 반드시 파싱해서 dict 형태로 리턴
        else:
            return {"error": f"'{category}'에 해당하는 데이터가 없습니다."}

    except Exception as e:
        return {"error": f"데이터 가져오기 실패: {str(e)}"}