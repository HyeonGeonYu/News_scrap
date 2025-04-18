import yfinance as yf
import app.test_config


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
def fetch_index_info(symbol, day_num=200, ma_period=100):
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="12mo").tail(day_num)

    data = []
    for date, row in hist.iterrows():
        data.append({
            "date": date.strftime("%Y-%m-%d"),
            "open": round(row["Open"], 2),
            "high": round(row["High"], 2),
            "low": round(row["Low"], 2),
            "close": round(row["Close"], 2),
            "volume": int(row["Volume"]),
        })

    # 이동 평균 계산
    moving_avg = calculate_moving_average(data, period=100)
    upper10, lower10 = calculate_envelope(moving_avg, 0.10)
    upper3, lower3 = calculate_envelope(moving_avg, 0.03)
    # 이동평균 이후 데이터만큼 잘라서 붙임
    trimmed_data = data[-len(moving_avg):]
    for i in range(len(trimmed_data)):
        trimmed_data[i]["ma100"] = moving_avg[i]
        trimmed_data[i]["envelope10_upper"] = upper10[i]
        trimmed_data[i]["envelope10_lower"] = lower10[i]
        trimmed_data[i]["envelope3_upper"] = upper3[i]
        trimmed_data[i]["envelope3_lower"] = lower3[i]

    return trimmed_data


# ✅ 테스트 실행

if __name__ == "__main__":

    results = {}
    for index_name, symbol  in app.test_config.INDEX_SYMBOLS.items():
        new_data = fetch_index_info(symbol, day_num=200)  # 심볼 전달
        results[app.test_config.INDEX_SYMBOLS[index_name]] = new_data
    print(results)  # 지수정보

