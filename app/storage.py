import time
import json
from app.URL과요약문만들기 import get_latest_video_data, summarize_content
from app.지수정보가져오기 import fetch_index_info
from pytz import timezone, utc
from app.redis_client import redis_client
from datetime import datetime
from app.test_config import ALL_SYMBOLS, channels
# url, 요약 저장 코드
def convert_to_kst(published_utc_str):
    seoul_tz = timezone("Asia/Seoul")
    published_utc = datetime.strptime(published_utc_str, "%Y-%m-%dT%H:%M:%SZ")
    published_kst = published_utc.replace(tzinfo=utc).astimezone(seoul_tz)
    return published_kst

def fetch_and_store_youtube_data():
    try:

        seoul_tz = timezone("Asia/Seoul")
        today_date = datetime.now(seoul_tz).date().strftime("%Y-%m-%d")
        updated = False

        for channel in channels:
            country = channel["country"]

            # 저장되어있는 데이터의 저장된 날짜 확인
            existing_raw = redis_client.hget("youtube_data", country)
            if existing_raw:
                existing_data = json.loads(existing_raw)
                # existing_data에서 processed_time 가져오기
                processed_time = existing_data.get('processed_time')
                if processed_time:
                    processed_date = convert_to_kst(processed_time).strftime("%Y-%m-%d")
                    if processed_date == today_date:
                        print(f"⏭️ {country} — 오늘 데이터 이미 존재")
                        continue  # 오늘 데이터는 이미 있음, 넘어감
                else:
                    print(f"⚠️ {country} — processed_time 없음, 새로 조회합니다.")
            else:
                print(f"💡 {country} — 기존 데이터 없음, 새로 조회합니다.")

                # existing_data에서 processed_time 가져오기
            # 🔍 새 영상 서치
            video_data = get_latest_video_data(channel)
            video_date_str = convert_to_kst(video_data['publishedAt']).strftime("%Y-%m-%d")

            # ✅ 오늘 영상인지 확인
            if video_date_str != today_date:
                print(f"⏭️ {country} — 오늘 영상 아님 ({video_date_str})")
                continue

            # ✅ 요약 생성
            summary_result = summarize_content(video_data['summary_content'])
            video_data['summary_result'] = summary_result

            # ✅ 저장 시간 추가 (UTC 기준)
            video_data['processed_time'] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

            # ✅ Redis에 해당 국가만 저장 (덮어쓰기)
            redis_client.hset("youtube_data", country, json.dumps(video_data))
            print(f"🔔 {country} 데이터 저장됨: {video_data['url']}")
            updated = True

        return "✅ 데이터 저장 완료" if updated else "✅ 모든 데이터는 이미 최신 상태입니다."

    except Exception as e:
        return f"❌ 저장 중 오류 발생: {str(e)}"

def fetch_and_store_chart_data():
    results = []

    for category, symbol_dict in ALL_SYMBOLS.items():
        category_data = {}

        for name, symbol in symbol_dict.items():
            try:
                new_data = fetch_index_info(symbol, day_num=200)
                category_data[name] = new_data
                results.append(f"✅ [{category.upper()} - {name.upper()}] {len(new_data)}개 데이터 수집 완료")
            except Exception as e:
                results.append(f"❌ [{category.upper()} - {name.upper()}] 수집 중 오류 발생: {str(e)}")

        try:
            redis_key = f"chart_data:{category}"
            redis_client.set(redis_key, json.dumps(category_data))
            results.append(f"✅ 전체 {category.upper()} 데이터 Redis에 저장 완료")
        except Exception as e:
            results.append(f"❌ 전체 {category.upper()} 데이터 저장 중 오류 발생: {str(e)}")

    return "\n".join(results)



def scheduled_store():
    now = datetime.now(timezone('Asia/Seoul'))
    print("📈 chart data 저장 시작...")
    stored_result = fetch_and_store_chart_data()
    print(stored_result)
    if 11 <= now.hour < 15:  # 11시 ~ 14시 59분
        print("⏰ Scheduled store running at", now.strftime("%Y-%m-%d %H:%M"))
        youtube_result = fetch_and_store_youtube_data()
        print(youtube_result)




if __name__ == "__main__":

    # result = fetch_and_store_index_data()
    # print(result)
    # result = fetch_and_store_currency_data()
    # print(result)

    result = fetch_and_store_youtube_data()
    print(result)
