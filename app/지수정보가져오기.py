import yfinance as yf

def fetch_index_info(day_num):
    ndx = yf.Ticker("^NDX")  # ^NDX는 나스닥100 지수
    hist = ndx.history(period="12mo")
    hist = hist.tail(day_num)  # 최근 200 거래일 가져오기

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

    return data



if __name__ == "__main__":
    fetch_index_info()
