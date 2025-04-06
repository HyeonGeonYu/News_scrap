from pathlib import Path
from dotenv import load_dotenv
import os
import redis
import time
import json
from app.URL찾기 import get_latest_video_url
from pytz import timezone
from datetime import datetime
from dateutil import parser
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    ssl=True  # 로컬 개발이면 False 해도 됨
)

def fetch_and_store_youtube_data():
    try:
        channels = [
            {"country": "USA", "channel_handle": "@NBCNews", "keyword": "Nightly News Full Episode",
             "content_type": "video"},
            {"country": "Japan", "channel_handle": "@tbsnewsdig",
             "keyword": "【LIVE】朝のニュース（Japan News Digest Live）", "content_type": "video"},
            {"country": "China", "channel_handle": "@CCTV", "keyword": "CCTV「新闻联播」", "content_type": "playlist"}
        ]

        results = {}
        for channel in channels:
            video_data = get_latest_video_url(channel["channel_handle"], channel["keyword"], channel["content_type"])
            dt = parser.parse(video_data["publishedAt"])
            video_data["publishedAtFormatted"] = dt.astimezone(timezone("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")
            results[channel["country"]] = video_data

        redis_client.set('youtube_data', json.dumps(results))
        redis_client.set("youtube_data_timestamp", str(time.time()))  # 저장 시간도 같이

        return f"데이터 저장 완료"
    except Exception as e:
        return f"저장 중 오류 발생: {str(e)}"

def scheduled_store():
    now = datetime.now(timezone('Asia/Seoul'))
    if 11 <= now.hour < 15:  # 11시 ~ 14:59 사이 (15시 이전)
        print("Running scheduled store at", now)
        fetch_and_store_youtube_data()

"""
if __name__ == "__main__":
    result = fetch_and_store_youtube_data()
    print(result)

    # 저장된 데이터 확인
    data = redis_client.get("youtube_data")
    print("📦 저장된 유튜브 데이터:")
    print(json.loads(data))
"""