import time
import json
from app.URL과요약문만들기 import get_latest_video_data, summarize_content
from app.지수정보가져오기 import fetch_index_info
from pytz import timezone
from app.redis_client import redis_client
import app.test_config
from datetime import datetime
# url, 요약 저장 코드
def fetch_and_store_youtube_data():
    try:
        seoul_tz = timezone("Asia/Seoul")
        today_date = datetime.now(seoul_tz).strftime("%Y-%m-%d")
        today_key = f"processed_urls:{today_date}" # 한국시간기준으로 바꿈
        updated = False

        for channel in app.test_config.channels:
            # ⛔️ 오늘 이미 처리되었으면 stop 유튜브 API 회피
            country = channel["country"]
            existing_url = redis_client.hget(today_key, country) #
            if existing_url:
                print(f"⏭️ {country} — {today_key} : {existing_url.decode()}")
                continue

            # 서치 시작
            utc_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")  # Z는 UTC의 표기법입니다
            redis_client.set(f"youtube_data_timestamp:{country}", utc_timestamp)
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

            # ✅ Redis에 나라별로 개별 저장

            redis_client.set(f"youtube_data:{country}", json.dumps(video_data))
            redis_client.hset(today_key, country, video_data["url"])
            redis_client.expire(today_key, 86400)  # 86400초 = 1일



            print(f"🔔 {country} 새 URL 저장됨: {video_data['url']}")
            updated = True


        return "✅ 데이터 저장 완료" if updated else "✅ 모든 데이터는 이미 최신 상태입니다."
    except Exception as e:
        return f"저장 중 오류 발생: {str(e)}"

def fetch_and_store_currency_data():
    all_currency_data = {}
    results = []

    for index_name, symbol in app.test_config.CURRENCY_SYMBOLS_KRW.items():
        try:
            new_data = fetch_index_info(symbol, day_num=200)
            all_currency_data[index_name] = new_data
            results.append(f"✅ [{index_name.upper()}] {len(new_data)}개 환율 데이터 수집 완료")
        except Exception as e:
            results.append(f"❌ [{index_name.upper()}] 수집 중 오류 발생: {str(e)}")

    try:
        redis_client.set("currency_data:all", json.dumps(all_currency_data))
        results.append("✅ 전체 환율 데이터 Redis에 저장 완료")
    except Exception as e:
        results.append(f"❌ 전체 환율 데이터 저장 중 오류 발생: {str(e)}")

    return "\n".join(results)


def fetch_and_store_index_data():
    all_index_data = {}
    results = []

    for index_name, symbol in app.test_config.INDEX_SYMBOLS.items():
        try:
            new_data = fetch_index_info(symbol, day_num=200)
            all_index_data[index_name] = new_data
            results.append(f"✅ [{index_name.upper()}] {len(new_data)}개 지수 데이터 수집 완료")
        except Exception as e:
            results.append(f"❌ [{index_name.upper()}] 수집 중 오류 발생: {str(e)}")

    try:
        redis_client.set("index_data:all", json.dumps(all_index_data))
        results.append("✅ 전체 지수 데이터 Redis에 저장 완료")
    except Exception as e:
        results.append(f"❌ 전체 데이터 저장 중 오류 발생: {str(e)}")

    return "\n".join(results)


def scheduled_store():
    now = datetime.now(timezone('Asia/Seoul'))
    if 11 <= now.hour < 15:  # 11시 ~ 14시 59분
        if now.hour == 11 and 0 <= now.minute < 10:
            print("📈 Index data 저장 시작...")
            index_result = fetch_and_store_index_data()
            print(index_result)

            print("💱 Currency data 저장 시작...")
            currency_result = fetch_and_store_currency_data()
            print(currency_result)

        print("⏰ Scheduled store running at", now.strftime("%Y-%m-%d %H:%M"))
        youtube_result = fetch_and_store_youtube_data()
        print(youtube_result)




if __name__ == "__main__":

    # result = fetch_and_store_index_data()
    # print(result)
    scheduled_store()
    # result = fetch_and_store_currency_data()
    # print(result)

    #result = fetch_and_store_youtube_data()
    #print(result)
