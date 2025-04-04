
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from difflib import SequenceMatcher
import re

def is_similar(a, b, threshold=0.7):
    a, b = a.lower(), b.lower()
    if b in a:  # b(검색 키워드)가 a(영상 제목) 안에 포함되면 true
        return True
    return SequenceMatcher(None, a, b).ratio() >= threshold

def get_video_metadata(video):
    try:
        metadata_line = video.find_element(By.XPATH, ".//div[@id='metadata-line']")
        metadata_items = metadata_line.find_elements(By.XPATH,
                                                     ".//span[@class='inline-metadata-item style-scope ytd-video-meta-block']")

        views = metadata_items[0].text if len(metadata_items) > 0 else "Unknown"
        upload_time = metadata_items[1].text if len(metadata_items) > 1 else "Unknown"

        return views, upload_time
    except:
        return "Unknown", "Unknown"


def contains_date_or_scheduled(text):
    scheduled_keywords = ["예정일", "대기 중","Unknown"]  # 일정 관련 키워드

    if any(keyword in text for keyword in scheduled_keywords):
        return True
    return False

"""
def get_latest_video_url(channel_url, title_keyword, content_type="videos"):
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # 브라우저 창 띄우지 않음
    chrome_options.add_argument("--disable-gpu")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(f"{channel_url}/{content_type}")

    try:
        wait = WebDriverWait(driver, 3)

        # 최신 20개 영상 요소 가져오기
        video_elements = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//ytd-rich-grid-media")))

        for video in video_elements[:20]:
            title_element = video.find_element(By.XPATH, ".//a[@id='video-title-link']")
            video_title = title_element.text.strip()



            if is_similar(video_title, title_keyword):
                # 영상 URL 가져오기

                views, upload_time = get_video_metadata(video)
                video_url = title_element.get_attribute("href")

                if contains_date_or_scheduled(upload_time):
                    continue

                return video_url

        return None  # 해당 영상이 없는 경우

    except Exception as e:
        print("오류 발생:", e)
        return None

    finally:
        driver.quit()
"""

"""
if __name__ == "__main__":
    channel = "https://www.youtube.com/@NBCNews"
    keyword = "Nightly News Full Episode"
    content_type = "videos"  # 'videos' 또는 'streams' 선택 가능
    url = get_latest_video_url(channel, keyword, content_type)
    if url:
        print("Latest Matching Video URL:", url)
    else:
        print("No matching video found.")


    channel = "https://www.youtube.com/@tbsnewsdig"
    keyword = "【LIVE】朝のニュース（Japan News Digest Live）最新情報など｜TBS NEWS DIG"
    content_type = "streams"  # 'videos' 또는 'streams' 선택 가능
    url = get_latest_video_url(channel, keyword, content_type)
    if url:
        print("Latest Matching Video URL:", url)
    else:
        print("No matching video found.")

    channel = "https://www.youtube.com/@CCTV"
    keyword = "CCTV「新闻联播」"
    content_type = "videos"  # 'videos' 또는 'streams' 선택 가능
    url = get_latest_video_url(channel, keyword, content_type)
    if url:
        print("Latest Matching Video URL:", url)
    else:
        print("No matching video found.")
"""


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