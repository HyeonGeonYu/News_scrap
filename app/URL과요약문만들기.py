import time
from difflib import SequenceMatcher
from youtube_transcript_api import YouTubeTranscriptApi
from pathlib import Path
from dotenv import load_dotenv
import requests
import os
import difflib
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)
# 🔑 YouTube Data API 키 (보안을 위해 환경변수 사용 추천)
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")  # .env에서 불러오기
OPENAI_API_KEY = os.getenv("OPENAI_API_KE")  # .env에서 불러오기
import isodate
from datetime import datetime
import re
import json

from openai import OpenAI
openai_client = OpenAI(api_key=OPENAI_API_KEY)
from app.test_config import channels

from datetime import datetime
import pytz

def utc_to_kst(utc_time_str):
    utc_time = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%SZ")
    utc_zone = pytz.timezone("UTC")
    kst_zone = pytz.timezone("Asia/Seoul")
    utc_time = utc_zone.localize(utc_time)
    kst_time = utc_time.astimezone(kst_zone)
    return kst_time.strftime("%Y-%m-%dT%H:%M:%SZ")  # 예: '2025-04-17 06:00:00'
def summarize_content(content):
    if len(content) > 30000:
        return "❌ 요약 실패: 글자 수(30000) 초과"
    if not content.strip():
        return "❌ 요약 실패: 내용 없음"

    try:

        completion = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "user", "content": content+"\n\n 주요 뉴스 한글로 설명해 한글로"}
            ]
        )
        summary = completion.choices[0].message.content
        return summary

    except Exception as e:
        print("❌ 요약 API 실패:", e)
        return "❌ 요약 실패: GPT 호출 오류"

def find_similar_video_title_id(data, keyword, similarity_threshold=0.7,from_playlist=False):
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
        try:
            duration = isodate.parse_duration(video_data["items"][0]["contentDetails"]['duration'])
            # 10분 ~ 2시간 이하 영상만
            if duration.total_seconds() >= 7200 or duration.total_seconds() <= 600:
                continue
        except (KeyError, IndexError, ValueError) as e:
            print(f"duration 정보 없는id {video_id}: {e}")
            continue  # 해당 영상을 스킵하고 다음 영상 처리

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

def get_latest_video_data(channel):
    channel_handle = channel["channel_handle"]
    keywords = channel["keyword"] if isinstance(channel["keyword"], list) else [channel["keyword"]]
    playlist_ids = channel["playlist_id"] if isinstance(channel["playlist_id"], list) else [channel["playlist_id"]]
    save_fields = channel["save_fields"]

    # 채널id기준 viedo_id
    channel_url = "https://www.googleapis.com/youtube/v3/channels"
    params = {
        "part": "id",
        "forHandle": channel_handle,
        "key": YOUTUBE_API_KEY
    }

    response = requests.get(channel_url, params=params)
    channel_id = response.json()["items"][0]["id"]

    latest_video_data = None
    latest_time = None

    for i, keyword in enumerate(keywords):
        # 해당 키워드에 맞는 플레이리스트 ID 사용
        playlist_id = playlist_ids[i] if i < len(playlist_ids) else playlist_ids[-1]
        search_url = "https://www.googleapis.com/youtube/v3/search"
        params = {
            "part": "snippet",
            "channelId": channel_id,  # 변환된 채널 ID 사용
            "q": keyword,
            "order": "date",  # 최신순 정렬
            "maxResults": 5,
            "key": YOUTUBE_API_KEY,
        }

        response = requests.get(search_url, params=params)
        video_id_cid = find_similar_video_title_id(response.json(), keyword, similarity_threshold=0.7)

        # playlistid기준 viedo_id
        search_playlist_url = "https://www.googleapis.com/youtube/v3/playlistItems"
        params = {
            "part": "snippet",
            "playlistId": playlist_id,
            "maxResults": 5,
            "key": YOUTUBE_API_KEY
        }
        response = requests.get(search_playlist_url, params=params)
        video_id_plst = find_similar_video_title_id(response.json(), keyword, similarity_threshold=0.7, from_playlist=True)

        for video_id in [video_id_cid, video_id_plst]:
            if video_id: # 찾은 video id가 있는경우
                videos_check_url = "https://www.googleapis.com/youtube/v3/videos"
                params = {
                    "part": "snippet",
                    "id": video_id,
                    "key": YOUTUBE_API_KEY
                }
                response = requests.get(videos_check_url, params=params)
                data = response.json()
                if not data.get("items"):
                    continue
                published_time = datetime.strptime(data["items"][0]["snippet"]["publishedAt"], "%Y-%m-%dT%H:%M:%SZ")
                if latest_time is None or published_time > latest_time:
                    latest_time = published_time
                    latest_video_data = data

    if latest_video_data:
        video_info = latest_video_data["items"][0]
        video_id = video_info['id']
        video_title = video_info["snippet"]["title"]
        video_pbtime = utc_to_kst(video_info["snippet"]["publishedAt"])
        if "description" == save_fields:
            summary_content = video_info["snippet"]["description"]
        elif "subtitle" == save_fields:
            country_to_lang = {
                "Korea": "ko",
                "USA": "en",
                "Japan": "ja",
                "China": "zh"
            }
            language_code = country_to_lang.get(channel['country'], "en")  # 기본은 영어
            try:
                transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
                generated_transcript = next(
                    (t for t in transcript_list if t.is_generated and t.language_code == language_code),
                    next((t for t in transcript_list if t.is_generated), None)
                )
                if generated_transcript:
                    transcript = generated_transcript.fetch()
                    full_text = "\n".join([entry.text for entry in transcript])
                    summary_content = full_text
                else:
                    summary_content = None
            except Exception as e:
                print("❌ 자막 가져오기 실패:", e)
                summary_content = None

    return {
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "title": video_title,
            "publishedAt": video_pbtime,
            "summary_target": save_fields,
            "summary_content": summary_content
        }
    return None  # 영상이 없을 경우


# ✅ 테스트 실행
if __name__ == "__main__":

    results = {}
    for channel in channels:
        country = channel["country"]
        video_data = get_latest_video_data(channel)
        summary_result = summarize_content(video_data['summary_content'])
        video_data['summary_result'] = summary_result
        results[channel["country"]] = video_data
    print(results)  # 최신 영상 링크 출력
