import yfinance as yf
import app.test_config
from pykrx import stock
from datetime import datetime, timedelta
import pandas as pd
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

def fetch_stock_info(symbol, source="krx", day_num=200, ma_period=100):
    data = []
    processed_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    if source == "krx":
        end_date = datetime.today()
        start_date = end_date - timedelta(days=day_num * 2)
        df = stock.get_market_ohlcv_by_date(
            start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d"), symbol
        ).tail(day_num).reset_index()

        market_cap_df = stock.get_market_cap_by_date(start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d"), symbol)
        market_caps = market_cap_df["시가총액"].tolist()  # 날짜별 시가총액

        # 공매도 상태 데이터를 가져오기
        shorting_data_df = stock.get_shorting_status_by_date(start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d"),
                                                             symbol)

        last_short_balance_amount = 0  # 첫 번째 기본값
        for i, row in df.iterrows():
            match_val = shorting_data_df.loc[shorting_data_df.index == row["날짜"], "잔고금액"].values
            if match_val.size > 0:
                last_short_balance_amount = match_val[0]
            market_cap = market_caps[i]
            short_ratio = (last_short_balance_amount / market_cap) * 100 if market_cap > 0 else 0

            data.append({
                "date": row["날짜"].strftime("%Y-%m-%d"),
                "open": round(row["시가"], 2),
                "high": round(row["고가"], 2),
                "low": round(row["저가"], 2),
                "close": round(row["종가"], 2),
                "volume": int(row["거래량"]),
                "short_ratio": round(short_ratio, 2)
            })

    elif source == "yfinance":
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="12mo").tail(day_num)

        for date, row in hist.iterrows():
            data.append({
                "date": date.strftime("%Y-%m-%d"),
                "open": round(row["Open"], 2),
                "high": round(row["High"], 2),
                "low": round(row["Low"], 2),
                "close": round(row["Close"], 2),
                "volume": int(row["Volume"]),
            })

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
    sorurce = 'krx'
    category = 'kr_stock'
    for index_name, symbol  in test_dict[sorurce][category].items():
        new_data = fetch_stock_info(symbol, sorurce, day_num=200)  # 심볼 전달
        results[test_dict[sorurce][category][index_name]] = new_data
    print(results)  # 지수정보