from difflib import SequenceMatcher
from pathlib import Path
from dotenv import load_dotenv
import requests
import os
from playwright.sync_api import sync_playwright
import sys
import asyncio

env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")  # .env에서 불러오기
OPENAI_API_KEY = os.getenv("OPENAI_API_KE")  # .env에서 불러오기
import isodate
from openai import OpenAI
openai_client = OpenAI(api_key=OPENAI_API_KEY)
from app.test_config import channels

from datetime import datetime


def get_channel_id(channel_handle):
    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {
        "part": "id",
        "forHandle": channel_handle,
        "key": YOUTUBE_API_KEY
    }
    response = requests.get(url, params=params)
    return response.json().get("items", [{}])[0].get("id")


def get_video_details(video_id):
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "snippet,contentDetails",
        "id": video_id,
        "key": YOUTUBE_API_KEY
    }
    response = requests.get(url, params=params)
    items = response.json().get("items")
    return items[0] if items else None
def get_transcript_text(video_id):
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        video_url = "https://www.youtube.com/watch?v="+video_id
        page = browser.new_page()
        page.goto(video_url)

        # 더보기 버튼 클릭
        try:
            page.click("tp-yt-paper-button#expand")
        except:
            print("⚠️ 더보기 버튼 클릭 실패(이미 열려있을 수도 있음)")

        try:
            page.click("button:has(span:text('스크립트 표시'))")
            print("✅ 스크립트 버튼 클릭 성공")
        except:
            print("⚠️ 스크립트 버튼 클릭 실패(스크립트 버튼이 없을 수 있음)")
            browser.close()
            return None  # 버튼이 없으면 빈 문자열 반환

        # 자막 텍스트 추출
        page.wait_for_selector("yt-formatted-string.segment-text")
        segments = page.query_selector_all("yt-formatted-string.segment-text")
        transcript_texts = [seg.inner_text().strip() for seg in segments]
        full_transcript = "\n".join(transcript_texts)

        browser.close()

        return full_transcript

def find_best_video(data, keyword, from_playlist=False):
    for item in data.get("items", []):

        vid_id = None
        if isinstance(item.get("id"), dict):
            vid_id = item["id"].get("videoId")
        elif isinstance(item.get("snippet", {}).get("resourceId"), dict):
            vid_id = item["snippet"]["resourceId"].get("videoId")
        title = item["snippet"]["title"].lower()
        if any(SequenceMatcher(None, keyword.lower(), title[i:i+len(keyword)]).ratio() > 0.9
               for i in range(len(title) - len(keyword) + 1)):
            video = get_video_details(vid_id)
            if not video:
                continue
            try:
                duration = isodate.parse_duration(video["contentDetails"]["duration"])
                if 300 <= duration.total_seconds() <= 7200:
                    return vid_id
            except Exception as e:
                print(f"⏱ duration 파싱 실패: {e}")
    return None
def search_video_ids(channel_id, playlist_id, keyword):
    results = []
    for url, id_param in [
        ("https://www.googleapis.com/youtube/v3/search", {"channelId": channel_id, "q": keyword}),
        ("https://www.googleapis.com/youtube/v3/playlistItems", {"playlistId": playlist_id}),
    ]:
        params = {
            "part": "snippet",
            "maxResults": 5,
            "key": YOUTUBE_API_KEY,
            **id_param
        }
        resp = requests.get(url, params=params)
        vid_id = find_best_video(resp.json(), keyword, from_playlist="playlistId" in id_param)
        if vid_id:
            results.append(vid_id)
    return results

def summarize_content(content):
    if content is None:
        print("contents 없음")
        return None
    if len(content) > 300000:
        print("300,000 이상 길이 요약내용")
        return None
    if not content.strip():  # 공백만 있는 경우
        print("contents 전체 공백")
        return None
    try:
        prompt = (
                content.strip()
                + "\n\n---\n\n"
                + "위 뉴스 전체 내용을 기반으로 각 뉴스 항목별로 정리해줘.\n"
                + "뉴스가 여러 개일 경우 **각 뉴스마다 아래 형식**을 반복해서 작성해줘:\n\n"
                + "(대제목으로 1,2,3...) 1. 🗞️ [뉴스 제목 혹은 주제 요약] \n"
                + "✅ 한줄 요약: (핵심 사건을 한 문장으로)\n"
                + "🔥 주요 쟁점:\n"
                + " (들여쓰기 4칸 보기편하게)1) ...\n"
                + " (들여쓰기 4칸 보기편하게)2) ...\n"
                + " (들여쓰기 4칸 보기편하게)3) ...\n\n"
                + "각 뉴스는 명확히 구분해서 작성해"
                + "정리 순서는 뉴스 등장 순서와 같게 해줘. "
                + "반드시 한글,한국어로만 작성해."
        )

        completion = openai_client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        summary = completion.choices[0].message.content
        return summary

    except Exception as e:
        print(f"오류 발생: {e}")
        return None

    except Exception as e:
        print("❌ 요약 API 실패:", e)
        return "❌ 요약 실패: GPT 호출 오류"

def find_similar_video_title_id(data, keyword, similarity_threshold=0.9,from_playlist=False):
    try:
        items = data["items"]
    except KeyError as e:
        print(f"❌ 'items' 필드가 없습니다: {str(e)}")
        return None
    keyword = keyword.lower()
    k_len = len(keyword)

    for item in data["items"]:
        if isinstance(item["id"], dict):
            video_id = item["id"].get("videoId")
        else:
            video_id = item["snippet"]["resourceId"]["videoId"]
        title = item["snippet"]["title"]
        text = title.lower()
        max_sim = 0.0

        # 슬라이딩 윈도우로 유사도 측정
        for i in range(len(text) - k_len + 1):
            window = text[i:i + k_len]
            sim = SequenceMatcher(None, keyword, window).ratio()
            if sim > max_sim:
                max_sim = sim
                # 유사도가 threshold를 넘으면 그때 duration 확인
                if max_sim > similarity_threshold:
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
                        # 10분 ~ 2시간 사이만 허용
                        if 300 <= duration.total_seconds() <= 7200:
                            return video_id
                    except (KeyError, IndexError, ValueError) as e:
                        print(f"duration 정보 없는 id {video_id}: {e}")
                        continue
    return None  # 찾는 영상이 없을 경우

def get_latest_video_data(channel):
    # 채널id기준 viedo_id
    channel_id = get_channel_id(channel["channel_handle"])
    if not channel_id:
        print("❌ 채널 ID를 찾을 수 없습니다.")
        return None
    if not channel_id:
        print("❌ 채널 ID를 찾을 수 없습니다.")
        return None

    keywords = channel["keyword"] if isinstance(channel["keyword"], list) else [channel["keyword"]]
    playlist_ids = channel["playlist_id"] if isinstance(channel["playlist_id"], list) else [channel["playlist_id"]]

    latest = {"time": None, "data": None}

    for i, keyword in enumerate(keywords):
        # 해당 키워드에 맞는 플레이리스트 ID 사용
        playlist_id = playlist_ids[i] if i < len(playlist_ids) else playlist_ids[-1]
        for video_id in search_video_ids(channel_id, playlist_id, keyword):
            video = get_video_details(video_id)
            if not video:
                continue
            pb_time = datetime.strptime(video["snippet"]["publishedAt"], "%Y-%m-%dT%H:%M:%SZ")
            if not latest["time"] or pb_time > latest["time"]:
                latest.update({
                    "time": pb_time,
                    "video_id": video_id,
                    "data": {
                        "url": f"https://www.youtube.com/watch?v={video_id}",
                        "title": video["snippet"]["title"],
                        "publishedAt": video["snippet"]["publishedAt"],
                        "summary_target": channel["save_fields"],
                        "summary_content": video["snippet"]["description"] if channel[
                                                                                  "save_fields"] == "description" else None
                    }
                })
    # 최신 영상 확정 후 자막 가져오기 (필요한 경우에만)
    if latest["data"] and channel["save_fields"] == "subtitle":
        transcript = get_transcript_text(latest["video_id"])
        latest["data"]["summary_content"] = transcript
    return latest["data"]


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
