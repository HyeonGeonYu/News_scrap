import time
import json
from app.URL과요약문만들기 import get_latest_video_data, summarize_content
from app.지수정보가져오기 import fetch_index_info
from pytz import timezone
from datetime import datetime
from dateutil import parser
from app.redis_client import redis_client
import app.test_config

# url, 요약 저장 코드
def fetch_and_store_youtube_data():
    try:
        today_date = datetime.now(timezone("Asia/Seoul")).strftime("%Y-%m-%d")
        today_key = f"processed_urls:{today_date}"
        updated = False

        for channel in app.test_config.channels:
            # ⛔️ 오늘 이미 처리되었으면 stop 유튜브 API 회피
            country = channel["country"]
            existing_url = redis_client.hget(today_key, country)
            if existing_url:
                print(f"⏭️ {country} — {today_key} : {existing_url.decode()}")
                continue
            video_data = get_latest_video_data(channel)

            # ⛔️ 이미 저장된 URL과 동일하거나 오늘자 뉴스가 아니면 stop OpenAI API 회피

            video_date_str = video_data['publishedAt'].split('T')[0]
            existing_url_str = redis_client.hget(today_key, country).decode() if existing_url else None
            if existing_url_str==video_data['url']:
                print(f"⏭️ {country} — 이전 URL과 동일: {existing_url.decode()}")
                continue

            # ⛔️ 오늘 올라온 영상이 아님
            if video_date_str != today_date:
                print(f"⏭️ 업로드 날짜:{video_date_str} — 탐색날짜:{today_date}")
                continue

            # ⛔️ 요약할 내용이 없으면 stop, 3만자 넘는 경우엔 OpenAI API 회피 후 요약내용없이 저장
            if video_data['summary_content']:
                summary_result = summarize_content(video_data['summary_content'])
                video_data['summary_result'] = summary_result
            else:
                video_data['summary_result'] = "요약할 내용(자막 또는 description) 없음."
                continue

            video_data["processedAt"] = datetime.now(timezone("Asia/Seoul")).strftime('%Y-%m-%dT%H:%M:%SZ')

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
    for index_name, symbol  in app.test_config.INDEX_SYMBOLS.items():
        try:
            new_data = fetch_index_info(symbol, day_num=200)  # 심볼 전달
            redis_key = f"index_name:{index_name.lower()}"
            redis_client.set(redis_key, json.dumps(new_data))
            redis_client.set(f"{redis_key}:updatedAt", datetime.now(timezone("Asia/Seoul")).strftime('%Y-%m-%dT%H:%M:%SZ'))
            print(f"✅ {len(new_data)}개 지수 데이터(100일평균,종가,+-10%env, +-3%env) 저장 완료")

            print(f"✅ [{index_name.upper()}] {len(new_data)}개 지수 데이터 저장 완료")
        except Exception as e:
            print(f"❌ [{index_name.upper()}] 저장 중 오류 발생: {str(e)}")

def scheduled_store():
    now = datetime.now(timezone('Asia/Seoul'))
    if 11 <= now.hour < 15:  # 11시 ~ 14시 59분
        if now.hour == 11 and now.minute == 0:
            print("📈 index data...")
            fetch_and_store_index_data()

        print("⏰ Scheduled store running at", now.strftime("%Y-%m-%d %H:%M"))
        fetch_and_store_youtube_data()




if __name__ == "__main__":
    fetch_and_store_index_data()

    result = fetch_and_store_youtube_data()
    print(result)

    # 저장된 데이터 확인
    for channel in app.test_config.channels:
        country = channel["country"]
        data = redis_client.get(f"youtube_data:{country}")
        print("📦 저장된 유튜브 데이터:")
        print(json.loads(data))

