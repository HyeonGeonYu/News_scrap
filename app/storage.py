import time
import json
from app.URL과요약문만들기 import get_latest_video_data, summarize_content
from app.지수정보가져오기 import fetch_index_info
from pytz import timezone
from datetime import datetime
from dateutil import parser
from app.redis_client import redis_client
from app.test_config import channels

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

            # ⛔️ 이미 저장된 URL과 동일하거나 오늘자 뉴스가 아니면 stop OpenAI API 회피
            video_published_date = datetime.strptime(video_data['publishedAt'], "%Y-%m-%d %H:%M:%S")
            video_date_str = video_published_date.strftime("%Y-%m-%d")  # 비교를 위한 "YYYY-MM-DD" 형식으로 변환
            existing_url_str = redis_client.hget(today_key, country).decode() if existing_url else None
            if existing_url_str==video_data['url']:
                print(f"⏭️ {country} — 이전 URL과 동일: {existing_url.decode()}")
                continue

            # ⛔️ 오늘 올라온 영상이 아님
            if video_date_str != today_date:
                print(f"⏭️ 업로드 날짜:{video_date_str} — 탐색날짜:{today_date}")
                continue

            # ⛔️ 요약할 내용이 없으면 stop OpenAI API 회피 후 요약내용없이 저장
            if video_data['summary_content']:

                summary_result = summarize_content(video_data['summary_content'])
                video_data['summary_result'] = summary_result
            else:
                video_data['summary_result'] = "요약할 내용(자막 또는 description) 없음."

            dt = parser.parse(video_data["publishedAt"])
            video_data["publishedAtFormatted"] = dt.astimezone(timezone("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")
            video_data["processedAt"] = datetime.now(timezone("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")

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

def calculate_moving_average(data, period=100):
    result = []
    for i in range(len(data)):
        if i < period:
            pass
        else:
            avg = sum(d["close"] for d in data[i - period + 1:i + 1]) / period
            result.append(avg)
    return result

def calculate_envelope(moving_avg, percentage):
    upper = []
    lower = []
    for avg in moving_avg:
        upper.append(avg * (1 + percentage))
        lower.append(avg * (1 - percentage))
    return upper, lower


def fetch_and_store_index_data():
    try:
        new_data = fetch_index_info(day_num = 200)  # List of dicts, 날짜 오름차순 정렬이라고 가정
        index_name = "nasdaq100"
        redis_key = f"index_data:{index_name.lower()}"

        moving_avg = calculate_moving_average(new_data, period=100)
        upper10, lower10 = calculate_envelope(moving_avg, 0.10)
        upper3, lower3 = calculate_envelope(moving_avg, 0.03)

        trimmed_data = new_data[-100:]
        # 각 데이터에 해당 계산값 추가
        for i in range(len(trimmed_data)):
            trimmed_data[i]["ma100"] = moving_avg[i]
            trimmed_data[i]["envelope10_upper"] = upper10[i]
            trimmed_data[i]["envelope10_lower"] = lower10[i]
            trimmed_data[i]["envelope3_upper"] = upper3[i]
            trimmed_data[i]["envelope3_lower"] = lower3[i]

        # 최대 100개 유지
        redis_client.set(redis_key, json.dumps(trimmed_data))
        redis_client.set(f"{redis_key}:updatedAt", datetime.now(timezone("Asia/Seoul")).strftime("%Y-%m-%d %H:%M"))
        print(f"✅ {len(trimmed_data)}개 지수 데이터 저장 완료")

        return "✅ 데이터 저장 완료"
    except Exception as e:
        return f"저장 중 오류 발생: {str(e)}"

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