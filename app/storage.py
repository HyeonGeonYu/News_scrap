import time
import json
from app.URL과요약문만들기 import get_latest_video_data, summarize_content
from app.지수정보가져오기 import fetch_index_info
from pytz import timezone
from datetime import datetime
from dateutil import parser
from app.redis_client import redis_client
from test_config import channels

# url, 요약 저장 코드
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

            # ⛔️ 요약할 내용이 없으면 stop OpenAI API 회피
            if not video_data['summary_content'].strip():
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


def fetch_and_store_index_data():
    try:
        new_data = fetch_index_info()  # List of dicts, 날짜 오름차순 정렬이라고 가정
        index_name = "nasdaq100"
        redis_key = f"index_data:{index_name.lower()}"

        # 기존 데이터 불러오기
        existing_raw = redis_client.get(redis_key)
        existing_data = json.loads(existing_raw) if existing_raw else []

        if existing_data:
            last_stored_date = existing_data[-1]["date"]
            # 새 데이터 중, 기존 마지막 날짜 이후만 필터링
            filtered_new = [d for d in new_data if d["date"] > last_stored_date]
            print(f"📌 기존 데이터 {len(existing_data)}개, 새로 추가된 {len(filtered_new)}개")

            updated_data = existing_data + filtered_new
        else:
            print("📌 기존 데이터 없음. 전체 새로 저장")
            updated_data = new_data
        # 최대 100개 유지
        trimmed_data = updated_data[-100:]
        redis_client.set(redis_key, json.dumps(trimmed_data))
        redis_client.set(f"{redis_key}:updatedAt", datetime.now(timezone("Asia/Seoul")).strftime("%Y-%m-%d %H:%M"))
        print(f"✅ {len(trimmed_data)}개 지수 데이터 저장 완료")

        return "✅ 데이터 저장 완료"
    except Exception as e:
        return f"저장 중 오류 발생: {str(e)}"
# 나스닥 데이터 저장코드

def scheduled_store():
    now = datetime.now(timezone('Asia/Seoul'))
    if 11 <= now.hour < 15:  # 11시 ~ 14시 59분
        if now.hour == 11 and now.minute == 0:
            print("📈 index data...")
            fetch_and_store_index_data()

        print("⏰ Scheduled store running at", now.strftime("%Y-%m-%d %H:%M"))
        fetch_and_store_youtube_data()




if __name__ == "__main__":
    result = fetch_and_store_youtube_data()
    print(result)

    # 저장된 데이터 확인
    for channel in channels:
        country = channel["country"]
        data = redis_client.get(f"youtube_data:{country}")
        print("📦 저장된 유튜브 데이터:")
        print(json.loads(data))

    fetch_and_store_index_data()