import json
from app.URL과요약문만들기 import get_latest_video_data, summarize_content, get_transcript_text
from app.지수정보가져오기 import fetch_stock_info, calculate_dxy_from_currency_data, get_access_token
from app.휴장일구하기 import get_market_holidays
from urllib.parse import urlparse, parse_qs
from pytz import timezone, utc
from app.redis_client import redis_client
from datetime import datetime
from app.test_config import ALL_SYMBOLS, channels
import pytz
import os
from pathlib import Path
from dotenv import load_dotenv

# url, 요약 저장 코드
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

KIS_APP_KEY = os.getenv("KIS_APP_KEY")
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET")
CACHE_PATH = Path(__file__).resolve().parent / "token_cache.json"

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
                        if existing_data.get('summary_content') is None:

                            # 요약 다시 생성
                            print(f"✏️ {country} — summary_content 없음, 요약을 생성합니다.")
                            url = existing_data.get('url')
                            parsed_url = urlparse(url)
                            query_params = parse_qs(parsed_url.query)
                            video_id_list = query_params.get('v')
                            if not video_id_list:
                                print(f"❌ {country} — video_id 추출 실패, 스킵합니다.")
                                continue
                            video_id = video_id_list[0]
                            transcript = get_transcript_text(video_id)
                            if not transcript:
                                print(f"❌ {country} — transcript 가져오기 실패, 스킵합니다.")
                                continue
                            # 기존 데이터에 추가
                            existing_data['summary_content'] = transcript
                            # Redis 덮어쓰기
                            redis_client.hset("youtube_data", country, json.dumps(existing_data))
                            print(f"🔔 {country} — 스크립트 추가 저장 완료")
                        if existing_data.get('summary_result') is None:
                            transcript = existing_data.get('summary_content')
                            summary_result = summarize_content(transcript)

                            existing_data['summary_result'] = summary_result
                            # 저장 시간 업데이트 (UTC)
                            existing_data['processed_time'] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                            # Redis 덮어쓰기
                            redis_client.hset("youtube_data", country, json.dumps(existing_data))
                            if summary_result == None:
                                print(f"🔔 {country} — 요약 결과 추가되지 않음")
                            else:
                                print(f"🔔 {country} — 요약 결과 추가 저장 완료")
                            updated = True
                        continue
                        # processed_time이 오늘 날짜가 아니면 새로 조회
                    else:
                        print(f"⚠️ {country} — processed_time이 오늘이 아니어서 새로 조회합니다.")
                else:
                    print(f"⚠️ {country} — processed_time 없음, 새로 조회합니다.")
            else:
                print(f"💡 {country} — 기존 데이터 없음, 새로 조회합니다.")

                # existing_data에서 processed_time 가져오기
            # 🔍 새 영상 서치
            video_data = get_latest_video_data(channel)
            if not video_data:
                print(f"❌ {country} — 영상 데이터를 찾을 수 없음, 스킵합니다.")
                continue

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
    token = get_access_token(KIS_APP_KEY, KIS_APP_SECRET)
    # ALL_SYMBOLS에 정의된 각각의 카테고리별로 처리
    for source, symbol_dict in ALL_SYMBOLS.items():
        for category, symbols in symbol_dict.items():
            source_data = {}
            if category =='currency':
                new_data = calculate_dxy_from_currency_data(token)
                source_data['dxy'] = new_data
                results.append(
                    f"✅ [{source.upper()} - {category.upper()} - {'dxy'.upper()}] {len(new_data['data'])}개 데이터 수집 완료")
            for name, symbol in symbols.items():
                try:
                    # fetch_stock_info 호출 시, symbol과 source 전달
                    new_data = fetch_stock_info(symbol, token, category,source=source, day_num=200)
                    source_data[name] = new_data

                    results.append(f"✅ [{source.upper()} - {category.upper()} - {name.upper()}] {len(new_data['data'])}개 데이터 수집 완료")
                except Exception as e:
                    results.append(f"❌ [{source.upper()} - {category.upper()} - {name.upper()}] 수집 중 오류 발생: {str(e)}")

            try:
                # Redis에 저장할 데이터 형식
                redis_key = "chart_data"
                 # 기존 데이터를 Redis에서 조회하여 비교
                existing_data_raw = redis_client.hget(redis_key, category)
                existing_data = json.loads(existing_data_raw.decode()) if existing_data_raw else {}

                updated_data = existing_data.copy()
                updated_data.update(source_data)  # 성공적으로 수집된 name만 덮어씀

                existing_data_str = json.dumps(existing_data, sort_keys=True)
                new_data_str = json.dumps(updated_data, sort_keys=True)
                if existing_data_str == new_data_str:
                    results.append(f"⏭️ {category.upper()} 데이터 변경 없음, 저장 생략")
                else:
                    processed_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                    redis_client.hset(redis_key, category + "_processed_time", processed_time)
                    redis_client.hset(redis_key, category, new_data_str)
                    results.append(f"✅ 전체 {category.upper()} 데이터 Redis에 저장 완료")

            except Exception as e:
                results.append(f"❌ 전체 {category.upper()} 데이터 저장 중 오류 발생: {str(e)}")
    return "\n".join(results)

def fetch_and_store_holiday_data():
    results = []
    try:

        holiday_data = get_market_holidays()
        redis_key = "market_holidays"
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        new_data_str = json.dumps(holiday_data, ensure_ascii=False, sort_keys=True)
        redis_client.hset(redis_key, "all_holidays", new_data_str)
        redis_client.hset(redis_key, "all_holidays_timestamp", timestamp)
        results.append(f"✅ 전체 공휴일 데이터 Redis에 저장 완료 (저장 시간: {timestamp})")

    except Exception as e:
        results.append(f"❌ 전체 공휴일 데이터 처리 중 오류 발생: {str(e)}")

def save_daily_data():
    youtube_data = redis_client.hgetall("youtube_data")

    kst = pytz.timezone('Asia/Seoul')
    now_kst = datetime.now(kst)

    today_kst = datetime.now(kst).date()  # 한국 기준 오늘
    date_str = now_kst.strftime("%Y%m%d")  # Redis 필드 키용 (예: 20250615)
    save_dict = {}

    filtered_data = {}
    for country_bytes, json_bytes in youtube_data.items():
        try:
            country = country_bytes.decode()
            json_str = json_bytes.decode()
            data = json.loads(json_str)

            # processed_time 가져오기 (UTC로 되어있다고 가정)
            processed_time_utc = datetime.strptime(data['processed_time'], "%Y-%m-%dT%H:%M:%SZ")
            processed_time_utc = pytz.utc.localize(processed_time_utc)

            # UTC → 한국 시간
            processed_time_kst = processed_time_utc.astimezone(kst)

            # 오늘 저장된 것만 필터링
            if processed_time_kst.date() == today_kst:
                filtered_data[country] = data

        except (KeyError, ValueError, json.JSONDecodeError):
            print(f"⚠️ {country} 데이터 처리 실패")
    save_dict = {"youtube_data": filtered_data}
    redis_client.hset("daily_saved_data", date_str, json.dumps(save_dict, ensure_ascii=False))
    print(f"✅ {len(filtered_data)}개 저장 완료 → Redis key: daily_saved_data, field: {date_str}")

def scheduled_store():
    now = datetime.now(timezone('Asia/Seoul'))
    print("📈 chart data 저장 시작...")
    stored_result = fetch_and_store_chart_data()
    print(stored_result)

    if 11 <= now.hour < 15:  # 11시 ~ 14시 59분
        print("⏰ Scheduled store running at", now.strftime("%Y-%m-%d %H:%M"))
        youtube_result = fetch_and_store_youtube_data()
        print(youtube_result)



    # ✅ 월요일일 때만 실행
    if now.weekday() == 0:  # 0 = 월요일
        print("📅 월요일: 휴일 데이터 저장 체크 중...")

        try:
            timestamp_str = redis_client.hget("market_holidays", "all_holidays_timestamp")
            if timestamp_str:
                timestamp = datetime.strptime(timestamp_str.decode(), "%Y-%m-%dT%H:%M:%SZ")
                timestamp_kst = timestamp.replace(tzinfo=timezone('UTC')).astimezone(timezone('Asia/Seoul'))

                if timestamp_kst.date() == now.date():
                    print("⏭️ 오늘 이미 휴일 데이터가 저장됨. 생략합니다.")
                    return

            # 저장 안 되어 있거나 날짜가 오늘이 아니면 실행
            holiday_result = fetch_and_store_holiday_data()
            print(holiday_result)

        except Exception as e:
            print(f"❌ Redis에서  timestamp 확인 중 오류 발생: {str(e)}")

    # ✅ 밤 11시에만 save_daily_data 실행
    if now.hour == 23:
        print("🕚 23시 → 하루 데이터 저장 시작")
        save_daily_data()


if __name__ == "__main__":

    result = fetch_and_store_youtube_data()
    print(result)
    save_daily_data()

    result = fetch_and_store_chart_data()
    print(result)

    # result = fetch_and_store_holiday_data()
    # print(result)

    # scheduled_store()