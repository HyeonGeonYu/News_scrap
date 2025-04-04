"""
from .celery_app import celery
import redis
import os
from .URL찾기 import get_latest_video_url
import json
import time

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


@celery.task
def fetch_and_store_youtube_data():
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
        #video_url = get_latest_video_url(channel["channel_id"], channel["keyword"], channel["content_type"])
        results[channel["country"]] = "video_url"

    redis_client.set('youtube_data', json.dumps(results))
    redis_client.set("youtube_data_timestamp", str(time.time()))  # 저장 시간도 같이
"""
