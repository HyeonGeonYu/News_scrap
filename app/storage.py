import time
import json
from app.URL과요약문만들기 import get_latest_video_data, summarize_content
from pytz import timezone
from datetime import datetime
from dateutil import parser
from app.redis_client import redis_client
from test_config import channels

def fetch_and_store_youtube_data():
    try:
        today_date = datetime.now(timezone("Asia/Seoul")).strftime("%Y-%m-%d")
        today_key = f"processed_urls:{today_date}"
        updated = False

        for channel in channels:
            # ⛔️ 오늘 이미 처리되었으면 stop 유튜브 API 회피
            country = channel["country"]
            existing_url = redis_client.hget(today_key, country)
            if existing_url:
                print(f"⏭️ {country} — {today_key} : {existing_url.decode()}")
                continue
            video_data = get_latest_video_data(channel)

            # ⛔️ 이미 저장된 URL과 동일하면 stop OpenAI API 회피
            existing_url_str = redis_client.hget(today_key, country).decode() if existing_url else None
            if existing_url_str==video_data['url']:
                print(f"⏭️ {country} — 이전 URL과 동일: {existing_url.decode()}")
                continue

            summary_result = summarize_content(video_data['summary_content'])
            video_data['summary_result'] = summary_result

            dt = parser.parse(video_data["publishedAt"])
            video_data["publishedAtFormatted"] = dt.astimezone(timezone("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")
            video_data["processedAt"] = datetime.now(timezone("Asia/Seoul")).strftime("%Y-%m-%d")

            # ✅ Redis에 나라별로 개별 저장
            redis_client.set(f"youtube_data:{country}", json.dumps(video_data))
            redis_client.set(f"youtube_data_timestamp:{country}", str(int(time.time())))

            redis_client.hset(today_key, country, video_data["url"])
            redis_client.expire(today_key, 86400)  # 86400초 = 1일

            print(f"🔔 {country} 새 URL 저장됨: {video_data['url']}")
            updated = True


        return "✅ 데이터 저장 완료" if updated else "✅ 모든 데이터는 이미 최신 상태입니다."
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
    for channel in channels:
        country = channel["country"]
        data = redis_client.get(f"youtube_data:{country}")
        print("📦 저장된 유튜브 데이터:")
        print(json.loads(data))
