import time
from difflib import SequenceMatcher
from youtube_transcript_api import YouTubeTranscriptApi
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
from openai import OpenAI
def summarize_content(content):
    if len(content) > 30000:
        return "❌ 요약 실패: 글자 수(30000) 초과"
    if not content.strip():
        return "❌ 요약 실패: 내용 없음"
    client = OpenAI()
    try:

        completion = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "주요 뉴스를 반드시 한글로 설명해줘. 불필요한 도입 문구 없이, 바로 "
                        "요점 나열 형식으로 시작해."
                        "예: 1. **핵심 내용 요약**: 실제 내용..."
                        " 반드시 한글로 설명해줘"
                    )
                },
                {"role": "user", "content": content}
            ]
        )
        summary = completion.choices[0].message.content
        return summary

    except Exception as e:
        print("❌ 요약 API 실패:", e)
        return "❌ 요약 실패: GPT 호출 오류"


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
        # 10분 ~ 1시간30분 이하 영상만
        if duration.total_seconds() >= 5400 or duration.total_seconds() <= 600:
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


def get_latest_video_url(channel):
    channel_handle = channel["channel_handle"]
    keyword = channel["keyword"]
    content_type = channel["content_type"]
    save_fields = channel["save_fields"]


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

        if "description" == save_fields:
            summary_content = data["items"][0]["snippet"]["description"]
        elif "subtitle" == save_fields:
            country_to_lang = {
                "Korea": "ko",
                "USA": "en",
                "Japan": "ja",
                "China": "zh",
                # 필요한 만큼 추가 가능
            }
            language_code = country_to_lang.get(channel['country'], "en")  # 기본은 영어
            try:
                transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
                generated_transcript = None
                for transcript in transcript_list:
                    if transcript.is_generated and transcript.language_code == language_code:
                        generated_transcript = transcript
                        break
                if not generated_transcript:
                    for transcript in transcript_list:
                        if transcript.is_generated:
                            generated_transcript = transcript
                            break
                if generated_transcript:
                    transcript = generated_transcript.fetch()
                    full_text = "\n".join([entry.text for entry in transcript])
                    summary_content = full_text
                else:
                    summary_content = ""
            except Exception as e:
                print("❌ 자막 가져오기 실패:", e)
                summary_content = ""

    summary_result = summarize_content(summary_content)

    return {
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "title": video_title,
            "publishedAt": video_pbtime,
            "summary_target": save_fields,
            "summary_content": summary_content,
            "summary_result": summary_result
        }
    return None  # 영상이 없을 경우


# ✅ 테스트 실행
if __name__ == "__main__":

    channels = [
        {"country": "Korea",
         "channel_handle": "@newskbs",
         "keyword": "[풀영상] 뉴스12",
         "content_type": "video",
         "save_fields": "subtitle"},
        {
            "country": "USA",
            "channel_handle": "PL0tDb4jw6kPymVj5xNNha5PezudD5Qw9L",
            "keyword": "Nightly News Full Episode",
            "content_type": "playlist",
            "save_fields": "subtitle"
        },
        {
            "country": "Japan",
            "channel_handle": "@tbsnewsdig",
            "keyword": "【LIVE】朝のニュース（Japan News Digest Live）",
            "content_type": "video",
            "save_fields": "subtitle"
        },
        {
            "country": "China",
            "channel_handle": "PL0eGJygpmOH5xQuy8fpaOvKrenoCsWrKh",
            "keyword": "CCTV「新闻联播」",
            "content_type": "playlist",
            "save_fields": "description"
        }
    ]

    results = {}
    for channel in channels:
        video_url = get_latest_video_url(channel)
        results[channel["country"]] = video_url

    print(results)  # 최신 영상 링크 출력
