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
    if content_type=="video":
        channel_url = "https://www.googleapis.com/youtube/v3/channels"
        params = {
            "part": "id",
            "forHandle": channel_handle,
            "key": YOUTUBE_API_KEY
        }

        response = requests.get(channel_url, params=params)
        data = response.json()
        channel_id = data["items"][0]['id']

        search_url = "https://www.googleapis.com/youtube/v3/search"
        params = {
            "part": "snippet",
            "channelId": channel_id,  # 변환된 채널 ID 사용
            "q": keyword,
            "order": "date",  # 최신순 정렬
            "maxResults": 1,
            "key": YOUTUBE_API_KEY,
        }

        response = requests.get(search_url, params=params)
        data = response.json()
        data = data["items"][0]
        video_id = data["id"]["videoId"]

    elif content_type=="playlist":
        search_playlist_url = "https://www.googleapis.com/youtube/v3/playlistItems"
        playlist_id = "PL0eGJygpmOH5xQuy8fpaOvKrenoCsWrKh"
        params = {
            "part": "snippet",
            "playlistId": playlist_id,
            "maxResults": 5,
            "key": YOUTUBE_API_KEY
        }
        response = requests.get(search_playlist_url, params=params)
        data = response.json()

        for item in data["items"]:
            snippet = item["snippet"]
            # Skip if it's a private video
            if snippet.get("title", "").lower() == "private video":
                continue
            # Found the first public video
            video_id = snippet["resourceId"]["videoId"]
            data = item
            break
        video_id = data["snippet"]["resourceId"]["videoId"]

    if len(data) > 0:
        videos_check_url = "https://www.googleapis.com/youtube/v3/videos"
        params = {
            "part": "snippet",
            "id": video_id,
            "key": YOUTUBE_API_KEY
        }
        response = requests.get(videos_check_url, params=params)
        data = response.json()

        video_title = data["items"][0]["snippet"]["title"]
        video_pbtime = data["items"][0]["snippet"]["publishedAt"]
        video_description = data["items"][0]["snippet"]["description"]

        video_title
        video_pbtime
        video_description
        return {
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "title": video_title,
            "publishedAt": video_pbtime,
            "description": video_description
        }
    return None  # 영상이 없을 경우

"""
# ✅ 테스트 실행
if __name__ == "__main__":
    channels = [
        {"country": "USA", "channel_handle": "@NBCNews", "keyword": "Nightly News Full Episode", "content_type":"video"},
        {"country": "Japan", "channel_handle": "@tbsnewsdig",
         "keyword": "【LIVE】朝のニュース（Japan News Digest Live）", "content_type":"video"},
        {"country": "China", "channel_handle": "@CCTV", "keyword": "CCTV「新闻联播」", "content_type":"playlist"}
    ]

    results = {}
    for channel in channels:
        video_url = get_latest_video_url(channel["channel_handle"], channel["keyword"],channel["content_type"])
        results[channel["country"]] = video_url

    print(results)  # 최신 영상 링크 출력
"""