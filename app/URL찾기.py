import time
from difflib import SequenceMatcher
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
import difflib
# 🔑 YouTube Data API 키 (보안을 위해 환경변수 사용 추천)
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")  # .env에서 불러오기

import isodate

def find_similar_video_id(data, keyword, similarity_threshold=0.7,from_playlist=False):
    keyword = keyword.lower()
    k_len = len(keyword)

    for item in data["items"]:
        if isinstance(item["id"], dict):
            video_id = item["id"].get("videoId")
        else:
            video_id = item["snippet"]["resourceId"]["videoId"]

        videos_check_url = "https://www.googleapis.com/youtube/v3/videos"
        params = {
            "part": "contentDetails",
            "id": video_id,
            "key": YOUTUBE_API_KEY
        }
        response = requests.get(videos_check_url, params=params)
        video_data = response.json()
        duration = isodate.parse_duration(video_data["items"][0]["contentDetails"]['duration'])
        # 1시간 이하 영상만
        if duration.total_seconds() >= 3600:
            continue

        title = item["snippet"]["title"]
        text = title.lower()
        max_sim = 0.0

        # 슬라이딩 윈도우
        for i in range(len(text) - k_len + 1):
            window = text[i:i + k_len]
            sim = SequenceMatcher(None, keyword, window).ratio()
            if sim > max_sim:
                max_sim = sim

        if max_sim > similarity_threshold:
            return video_id
    # 매칭 없을 경우 None 반환
    return None

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
            "maxResults": 10,
            "key": YOUTUBE_API_KEY,
        }

        response = requests.get(search_url, params=params)
        data = response.json()

        video_id = find_similar_video_id(data, keyword, similarity_threshold=0.7)

    elif content_type=="playlist":
        search_playlist_url = "https://www.googleapis.com/youtube/v3/playlistItems"
        playlist_id = channel_handle
        params = {
            "part": "snippet",
            "playlistId": playlist_id,
            "maxResults": 5,
            "key": YOUTUBE_API_KEY
        }
        response = requests.get(search_playlist_url, params=params)
        data = response.json()

        video_id = find_similar_video_id(data, keyword, similarity_threshold=0.7,from_playlist=True)

    if video_id:
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

        return {
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "title": video_title,
            "publishedAt": video_pbtime,
            "description": video_description
        }
    return None  # 영상이 없을 경우


# ✅ 테스트 실행
if __name__ == "__main__":
    channels = [
        {"country": "USA", "channel_handle": "PL0tDb4jw6kPymVj5xNNha5PezudD5Qw9L", "keyword": "Nightly News Full Episode", "content_type":"playlist"},
        {"country": "Japan", "channel_handle": "@tbsnewsdig",
         "keyword": "【LIVE】朝のニュース（Japan News Digest Live）", "content_type":"video"},
        {"country": "China", "channel_handle": "PL0eGJygpmOH5xQuy8fpaOvKrenoCsWrKh", "keyword": "CCTV「新闻联播」", "content_type":"playlist"} # @CCTV 중 특정 재생목록
    ]

    results = {}
    for channel in channels:
        video_url = get_latest_video_url(channel["channel_handle"], channel["keyword"],channel["content_type"])
        results[channel["country"]] = video_url

    print(results)  # 최신 영상 링크 출력
