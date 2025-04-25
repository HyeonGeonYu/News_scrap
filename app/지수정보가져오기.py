import yfinance as yf
import app.test_config
from datetime import datetime

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
        end = datetime.today()
        start = end - timedelta(days=day_num * 2)
        df = stock.get_market_ohlcv_by_date(
            start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), symbol
        ).tail(day_num).reset_index()

        for _, row in df.iterrows():
            data.append({
                "date": row["날짜"].strftime("%Y-%m-%d"),
                "open": round(row["시가"], 2),
                "high": round(row["고가"], 2),
                "low": round(row["저가"], 2),
                "close": round(row["종가"], 2),
                "volume": int(row["거래량"]),
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

from pykrx import stock
from datetime import datetime, timedelta

def fetch_korea_stock_info(symbol, day_num=200, ma_period=100):
    end = datetime.today()
    start = end - timedelta(days=day_num * 2)  # 주말 제외 대비 여유있게

    df = stock.get_market_ohlcv_by_date(
        start.strftime("%Y%m%d"),
        end.strftime("%Y%m%d"),
        symbol
    ).tail(day_num).reset_index()

    data = []
    processed_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    for _, row in df.iterrows():
        data.append({
            "date": row["날짜"].strftime("%Y-%m-%d"),
            "open": round(row["시가"], 2),
            "high": round(row["고가"], 2),
            "low": round(row["저가"], 2),
            "close": round(row["종가"], 2),
            "volume": int(row["거래량"]),
        })

    # 이동 평균 계산
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

    print(results)

if __name__ == "__main__":
    results = {}
    for index_name, symbol  in app.test_config.INDEX_SYMBOLS.items():
        new_data = fetch_stock_info(symbol, day_num=200)  # 심볼 전달
        results[app.test_config.INDEX_SYMBOLS[index_name]] = new_data
    print(results)  # 지수정보