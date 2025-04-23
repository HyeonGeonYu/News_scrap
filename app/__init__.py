# app/__init__.py 또는 app/app_factory.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.redis_client import redis_client
from app.storage import scheduled_store
from app.test_config import channels
import json
def create_app():
    app = FastAPI()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/")
    def root():
        return {"message": "Hello, World!"}

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
        :param category: 'index', 'currency', 'commodity'
        :return: 해당 category의 모든 항목 데이터 (ex: 전체 kospi, nasdaq 등)
        """
        try:
            redis_key = "chart_data"  # HSET으로 저장된 hash key
            result = redis_client.hget(redis_key, category)

            if result:
                return json.loads(result)  # JSON 파싱해서 dict 반환
            else:
                return {"error": f"'{category}'에 해당하는 데이터가 없습니다."}

        except Exception as e:
            return {"error": f"데이터 가져오기 실패: {str(e)}"}

    @app.get("/market-holidays")
    def get_market_holidays_api():
        result = {}
        try:
            all_data_raw = redis_client.hget("market_holidays", "all_holidays")
            timestamp_raw = redis_client.hget("market_holidays", "all_holidays_timestamp")

            # 데이터가 없으면 오류 메시지 반환
            if not all_data_raw or not timestamp_raw:
                result["error"] = "공휴일 데이터가 존재하지 않거나, 시간 정보가 없습니다."
                return result

                # 공휴일 데이터 및 저장 시간 디코딩
            all_data = json.loads(all_data_raw.decode())
            timestamp = timestamp_raw.decode()

            # 결과에 공휴일 데이터와 저장 시간을 함께 추가
            result["holidays"] = all_data
            result["timestamp"] = timestamp

            return result
        except Exception as e:
            result["error"] = f"공휴일 데이터를 가져오는 중 오류가 발생했습니다: {str(e)}"
            return result

    return app
