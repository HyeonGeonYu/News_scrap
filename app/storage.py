import time
import json
from app.URL과요약문만들기 import get_latest_video_url
from pytz import timezone
from datetime import datetime
from dateutil import parser
from app.redis_client import redis_client
def fetch_and_store_youtube_data():
    try:
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
        today_key = f"processed_urls:{datetime.now().date()}"
        updated = False
        for channel in channels:
            country = channel["country"]
            # ⛔️ 오늘 이미 처리했으면 skip (API 호출 X)
            if redis_client.hexists(today_key, country):
                print(f"⏭️ {country} — 이미 오늘 처리됨. API 호출 생략")
                continue

            video_data = get_latest_video_url(channel)
            dt = parser.parse(video_data["publishedAt"])
            video_data["publishedAtFormatted"] = dt.astimezone(timezone("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")

            results[country] = video_data
            video_data["processedAt"] = datetime.now(timezone("Asia/Seoul")).strftime("%Y-%m-%d")
            redis_client.hset(today_key, country, video_data["url"])
            redis_client.expire(today_key, 86400)

            print(f"🔔 {country} 새 URL 저장됨: {video_data['url']}")

            updated = True
        if updated:
            redis_client.set("youtube_data", json.dumps(results))
            redis_client.set("youtube_data_timestamp", str(int(time.time())))
            print("✅ Redis에 새 데이터 저장 완료")

        return f"데이터 저장 완료"
    except Exception as e:
        return f"저장 중 오류 발생: {str(e)}"


def scheduled_store():
    now = datetime.now(timezone('Asia/Seoul'))
    if 11 <= now.hour < 15:  # 11시 ~ 14시 59분
        print("⏰ Scheduled store running at", now.strftime("%Y-%m-%d %H:%M"))
        fetch_and_store_youtube_data()
    else:
        print("⏳ Not within update window:", now.strftime("%H:%M"))


if __name__ == "__main__":
    result = fetch_and_store_youtube_data()
    print(result)

    # 저장된 데이터 확인
    data = redis_client.get("youtube_data")
    print("📦 저장된 유튜브 데이터:")
    print(json.loads(data))
