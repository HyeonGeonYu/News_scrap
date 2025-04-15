import yfinance as yf
import json
from datetime import datetime
from pytz import timezone
from app.redis_client import redis_client

def fetch_index_info():
    ndx = yf.Ticker("^NDX")  # ^NDX는 나스닥100 지수
    hist = ndx.history(period="6mo")
    hist = hist.tail(100)  # 최근 100 거래일만 가져오기

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
