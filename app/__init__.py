from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.redis_client import redis_client
from . import storage
from . import 임시코드

import json
from pytz import timezone, utc
from datetime import datetime
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

    @app.head("/")
    def head_root():
        return {}

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

    @app.get("/test-save")
    def test_save_endpoint():
        now = datetime.now(timezone('Asia/Seoul'))
        print("📈 chart data 저장 시작...")
        stored_result = storage.fetch_and_store_chart_data()
        print(stored_result)

        print("⏰ Scheduled store running at", now.strftime("%Y-%m-%d %H:%M"))
        youtube_result = storage.fetch_and_store_youtube_data()
        print(youtube_result)
        try:
            timestamp_str = redis_client.hget("market_holidays", "all_holidays_timestamp")
            if timestamp_str:
                timestamp = datetime.strptime(timestamp_str.decode(), "%Y-%m-%dT%H:%M:%SZ")
                timestamp_kst = timestamp.replace(tzinfo=timezone('UTC')).astimezone(timezone('Asia/Seoul'))

                if timestamp_kst.date() == now.date():
                    print("⏭️ 오늘 이미 휴일 데이터가 저장됨. 생략합니다.")
                    return

            # 저장 안 되어 있거나 날짜가 오늘이 아니면 실행
            holiday_result = storage.fetch_and_store_holiday_data()
            print(holiday_result)

        except Exception as e:
            print(f"❌ Redis에서  timestamp 확인 중 오류 발생: {str(e)}")

    @app.get("/test-code")
    def test_code():
        result = {}
        video_url = "https://www.youtube.com/watch?v=WxnFW-yn3YM"
        transcript = 임시코드.get_youtube_transcript_text(video_url)
        print("\n=== 최종 자막 ===")
        print(transcript)
        result["transcript"] = transcript
        return result

    return app
