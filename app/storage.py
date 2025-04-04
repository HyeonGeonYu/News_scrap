from pathlib import Path
from dotenv import load_dotenv
import os
import redis
import time
import json
from .URL찾기 import get_latest_video_url

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
            {"country": "USA", "channel_id": "UC16niRr50-MSBwiO3YDb3RA", "keyword": "Nightly News Full Episode",
             "content_type": "videos"},
            {"country": "Japan", "channel_id": "UC6AG81pAkf5gf0Hz0UeV0kA",
             "keyword": "【LIVE】朝のニュース（Japan News Digest Live）最新情報など｜TBS NEWS DIG", "content_type": "streams"},
            {"country": "China", "channel_id": "UCi6O0HzkZbL47h3zdsqIJMQ", "keyword": "CCTV「新闻联播」",
             "content_type": "videos"}
        ]

        results = {}
        for channel in channels:
            video_url = get_latest_video_url(channel["channel_id"], channel["keyword"], channel["content_type"])
            results[channel["country"]] = video_url

        redis_client.set('youtube_data', json.dumps(results))
        redis_client.set("youtube_data_timestamp", str(time.time()))  # 저장 시간도 같이

        return f"데이터 저장 완료"
    except Exception as e:
        return f"저장 중 오류 발생: {str(e)}"

