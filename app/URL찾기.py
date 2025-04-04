import time
from difflib import SequenceMatcher

def is_similar(a, b, threshold=0.7):
    a, b = a.lower(), b.lower()
    if b in a:  # b(검색 키워드)가 a(영상 제목) 안에 포함되면 true
        return True
    return SequenceMatcher(None, a, b).ratio() >= threshold

def contains_date_or_scheduled(text):
    scheduled_keywords = ["예정일", "대기 중","Unknown"]  # 일정 관련 키워드

    if any(keyword in text for keyword in scheduled_keywords):
        return True
    return False


from pathlib import Path
from dotenv import load_dotenv
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

import requests
import os

# 🔑 YouTube Data API 키 (보안을 위해 환경변수 사용 추천)
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")  # .env에서 불러오기

def get_latest_video_url(channel_handle, keyword, content_type="video"):
    base_url = "https://www.googleapis.com/youtube/v3/search"
    channel_id = get_channel_id(channel_handle)
    params = {
        "part": "snippet",
        "channelId": channel_id,  # 변환된 채널 ID 사용
        "q": keyword,
        "type": content_type,
        "order": "date",  # 최신순 정렬
        "maxResults": 3,
        "key": YOUTUBE_API_KEY,
    }

    response = requests.get(base_url, params=params)
    data = response.json()

    if "items" in data and len(data["items"]) > 0:
        video_id = data["items"][0]["id"]["videoId"]
        return f"https://www.youtube.com/watch?v={video_id}"

    return None  # 영상이 없을 경우


# 📌 1. 채널 핸들 (@NBCNews) → 채널 ID 변환
def get_channel_id(channel_handle):
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet",
        "q": channel_handle,  # 핸들 검색
        "type": "channel",
        "key": YOUTUBE_API_KEY
    }

    response = requests.get(url, params=params)
    data = response.json()

    if "items" in data and len(data["items"]) > 0:
        return data["items"][0]["id"]["channelId"]  # 채널 ID 반환

    return None  # 채널 ID를 찾을 수 없을 때

"""
# ✅ 테스트 실행
if __name__ == "__main__":
    channels = [
        {"country": "USA", "channel_handle": "@NBCNews", "keyword": "Nightly News Full Episode"},
        {"country": "Japan", "channel_handle": "@tbsnewsdig",
         "keyword": "【LIVE】朝のニュース（Japan News Digest Live）"},
        {"country": "China", "channel_handle": "@CCTV", "keyword": "CCTV「新闻联播」"}
    ]

    results = {}
    for channel in channels:
        channel_id = get_channel_id(channel["channel_handle"])
        video_url = get_latest_video_url(channel_id, channel["keyword"])
        results[channel["country"]] = video_url

    print(results)  # 최신 영상 링크 출력
"""