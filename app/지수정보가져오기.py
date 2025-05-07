import yfinance as yf
import app.test_config
from pykrx import stock
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import requests
import os
import json
from datetime import datetime, timedelta
# 환경변수 불러오기
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

KIS_APP_KEY = os.getenv("KIS_APP_KEY")
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET")
CACHE_PATH = Path(__file__).resolve().parent / "token_cache.json"


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

def load_cached_token():
    if CACHE_PATH.exists():
        with open(CACHE_PATH, "r") as f:
            cache = json.load(f)
            expires_at = datetime.fromisoformat(cache["expires_at"])
            if datetime.now() < expires_at:
                return cache["access_token"]
    return None

def save_token_to_cache(access_token, expires_at_str):
    # 문자열 → datetime 객체
    expires_at = datetime.strptime(expires_at_str, "%Y-%m-%d %H:%M:%S")
    with open(CACHE_PATH, "w") as f:
        json.dump({
            "access_token": access_token,
            "expires_at": expires_at.isoformat()
        }, f)

def get_access_token(app_key, app_secret):
    cached_token = load_cached_token()
    if cached_token:
        print("✅ Using cached access token.")
        return cached_token

    print("🔐 Requesting new access token...")
    url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"
    headers = {"Content-Type": "application/json; charset=UTF-8"}
    body = {
        "grant_type": "client_credentials",
        "appkey": app_key,
        "appsecret": app_secret
    }

    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()
    data = response.json()
    access_token = data["access_token"]
    expires_at_str = data.get("access_token_token_expired")
    if not expires_at_str:
        # 만료 시간 정보가 없을 경우 24시간 유효
        expires_at_str = (datetime.now() + timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")

    save_token_to_cache(access_token, expires_at_str)
    return access_token

token = get_access_token(KIS_APP_KEY, KIS_APP_SECRET)
app_key = KIS_APP_KEY
app_secret = KIS_APP_SECRET

def fetch_stock_or_index_prices(symbol, source="domestic", num_days=200):
    # 오늘 날짜와 200일 전 날짜 계산
    today = datetime.today()
    start_date = (today - timedelta(days=365))  # 1년 전
    end_date = today# 오늘

    if source == "domestic":
        # 국내 주식/지수의 경우
        url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        tr_id = "FHKST03010100"
        market_code = "J"

        current_end = end_date
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "appkey": app_key,
            "appsecret": app_secret,
            "tr_id": tr_id,
            "custtype": "P"
        }
        all_data = []

        while True:
            current_start = current_end - timedelta(days=120)  # 넉넉히 120일 간격 (휴장일 고려)
            if current_start < start_date:
                current_start = start_date

            params = {
                "FID_COND_MRKT_DIV_CODE": market_code,
                "FID_INPUT_ISCD": symbol,
                "FID_INPUT_DATE_1": current_start.strftime("%Y%m%d"),
                "FID_INPUT_DATE_2": current_end.strftime("%Y%m%d"),
                "FID_ORG_ADJ_PRC": "0",
                "FID_PERIOD_DIV_CODE": "D"
            }
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            # JSON 데이터 반환
            data = response.json()
            if 'output2' not in data or not data['output2']:
                break  # 더 이상 데이터가 없으면 종료

            all_data.extend(data['output2'])

            # 가장 오래된 날짜에서 하루 전으로 다음 루프 설정
            oldest = min(data['output2'], key=lambda x: x['stck_bsop_date'])
            current_end = datetime.strptime(oldest['stck_bsop_date'], "%Y%m%d") - timedelta(days=1)

            if current_end < start_date:
                break
        data = {'output2':all_data}

        market_cap_df = stock.get_market_cap_by_date(start_date, end_date, symbol)
        market_caps = market_cap_df["시가총액"].tolist()  # 날짜별 시가총액

        # 공매도 상태 데이터를 가져오기
        shorting_data_df = stock.get_shorting_status_by_date(start_date, end_date,
                                                             symbol)

    elif source == "overseas":
        url = "https://openapi.koreainvestment.com:9443/uapi/overseas-price/v1/quotations/inquire-daily-chartprice"
        tr_id = "FHKST03030100"
        market_code = "N"
        current_end = datetime.today()
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "appkey": app_key,
            "appsecret": app_secret,
            "tr_id": tr_id,
            "custtype": "P"
        }
        all_data = []
        while True:
            current_start = current_end - timedelta(days=120)  # 넉넉히 120일 간격 (휴장일 고려)
            if current_start < start_date:
                current_start = start_date

            params = {
                "FID_COND_MRKT_DIV_CODE": market_code,
                "FID_INPUT_ISCD": symbol,
                "FID_INPUT_DATE_1": current_start.strftime("%Y%m%d"),
                "FID_INPUT_DATE_2": current_end.strftime("%Y%m%d"),
                "FID_PERIOD_DIV_CODE": "D"
            }
        # API 호출
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
        # JSON 데이터 반환
            data = response.json()
            if 'output2' not in data or not data['output2']:
                break  # 더 이상 데이터가 없으면 종료
            all_data.extend(data['output2'])

            # 가장 오래된 날짜에서 하루 전으로 다음 루프 설정
            oldest = min(data['output2'], key=lambda x: x['stck_bsop_date'])
            current_end = datetime.strptime(oldest['stck_bsop_date'], "%Y%m%d") - timedelta(days=1)

            if current_end < start_date:
                break
        data = {'output2': all_data}
    else:
        raise ValueError("Invalid source type. Use 'domestic' or 'overseas'.")


    if 'output2' in data and isinstance(data['output2'], list):
        # 데이터를 최신 날짜 기준으로 정렬한 후, 최근 100일 치 데이터만 추출
        sorted_data = sorted(data['output2'], key=lambda x: x['stck_bsop_date'])
        data_filtered = sorted_data[-num_days:]
    else:
        data_filtered = data.get('output2', [])  # 오류나 데이터가 없을 경우 기본 빈 리스트

    result = []
    last_short_balance_amount = 0  # 첫 번째 기본값
    for i, row in enumerate(data_filtered):
        if source =="domestic":
            date_key = pd.to_datetime(row["stck_bsop_date"], format="%Y%m%d")
            match_val = shorting_data_df.loc[shorting_data_df.index == date_key, "잔고금액"].values
            if match_val.size > 0:
                last_short_balance_amount = match_val[0]
            market_cap = market_caps[i]
            short_ratio = (last_short_balance_amount / market_cap) * 100 if market_cap > 0 else 0
            result.append({
                "date": datetime.strptime(row['stck_bsop_date'], "%Y%m%d").strftime("%Y-%m-%d"),
                "open": round(float(row['stck_oprc']), 2),
                "high": round(float(row['stck_hgpr']), 2),
                "low": round(float(row['stck_lwpr']), 2),
                "close": round(float(row['stck_clpr']), 2),
                "volume": int(row.get('acml_vol', 0)),  # 실제 데이터에 해당 필드가 없다면 기본 0으로 설정됨
                "short_ratio": round(short_ratio, 2)
            })
        elif source =="overseas":
            result.append({
                "date": datetime.strptime(row['stck_bsop_date'], "%Y%m%d").strftime("%Y-%m-%d"),
                "open": round(float(row['ovrs_nmix_oprc']), 2),
                "high": round(float(row['ovrs_nmix_hgpr']), 2),
                "low": round(float(row['ovrs_nmix_lwpr']), 2),
                "close": round(float(row['ovrs_nmix_prpr']), 2),
                "volume": int(row.get('acml_vol', 0)),  # 실제 데이터에 해당 필드가 없다면 기본 0으로 설정됨

            })
    if source == "domestic":
        if len(result) >= 4:
            # Get the value of the 4th last short_ratio
            last_valid_short_ratio = result[-4]["short_ratio"]
            # Update the last 3 entries to have the same short_ratio as the 4th last one
            for i in range(1, 4):
                result[-i]["short_ratio"] = last_valid_short_ratio
    return result

def fetch_stock_info(symbol, source="krx", day_num=200, ma_period=100):
    processed_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    if source == "krx":
        source = "domestic"  # "domestic" 또는 "overseas"로 선택
        data = fetch_stock_or_index_prices(symbol, source=source)
    elif source == "yfinance":
        source = "overseas"  # "domestic" 또는 "overseas"로 선택
        data = fetch_stock_or_index_prices(symbol, source=source)
    else:
        raise ValueError("Invalid data source. Use 'krx' or 'yfinance'.")

    moving_avg = calculate_moving_average(data, period=ma_period)
    upper10, lower10 = calculate_envelope(moving_avg, 0.10)
    upper3, lower3 = calculate_envelope(moving_avg, 0.03)

    trimmed_data = data[-len(moving_avg):]
    for i in range(len(trimmed_data)):
        trimmed_data[i]["ma100"] = moving_avg[i]
        trimmed_data[i]["envelope10_upper"] = upper10[i]
        trimmed_data[i]["envelope10_lower"] = lower10[i]
        trimmed_data[i]["envelope3_upper"] = upper3[i]
        trimmed_data[i]["envelope3_lower"] = lower3[i]
    return {'processed_time': processed_time, 'data': trimmed_data}

# 한화오션 (042660) / 현대해상 (001450)

# 공매도 비율 계산 함수
def calculate_short_ratio(symbol, day_num=200):
    end_date = datetime.today()
    start_date = end_date - timedelta(days=day_num * 2)  # 주말 제외 대비 여유있게

    # 공매도 상태 데이터 가져오기 (날짜 범위와 종목 코드 지정)
    df = stock.get_shorting_status_by_date(start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d"), symbol)

    # 필요한 데이터만 선택하여 리스트로 변환
    data = []
    for _, row in df.iterrows():
        data.append({
            "date": row.name.strftime("%Y-%m-%d"),  # 날짜 (index가 날짜로 되어 있음)
            "short_sale_volume": int(row["거래량"]),
            "short_sale_balance": int(row["잔고수량"]),
            "short_sale_value": int(row["거래대금"]),
            "short_balance_amount": int(row["잔고금액"]),
        })

    shorting_data = pd.DataFrame(data)

    # 시가총액 계산
   # market_cap = get_market_cap(symbol)

    # 공매도 비율 계산 (잔고금액 / 시가총액 * 100)
    #shorting_data["short_ratio"] = (shorting_data["short_balance_amount"] / market_cap) * 100

    return shorting_data


if __name__ == "__main__":
    results = {}
    test_dict = app.test_config.ALL_SYMBOLS
    source = 'yfinance'
    category = 'index'
    for index_name, symbol  in test_dict[source][category].items():
        new_data = fetch_stock_info(symbol, source, day_num=200)  # 심볼 전달
        results[test_dict[source][category][index_name]] = new_data
    print(results)  # 지수정보