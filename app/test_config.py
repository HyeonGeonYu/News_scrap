channels = [
        {"country": "Korea",
         "channel_handle": "@newskbs",
         "playlist_id": ["PL9a4x_yPK_85sGRvAQX4LEVHY8F9v405J","PL9a4x_yPK_84yhk67LjymVv7kbI5eyOVU"],
         "keyword": ["[풀영상] 뉴스12", "[풀영상] 뉴스광장"],
         "save_fields": "subtitle"},
        {
            "country": "USA",
            "channel_handle": "@NBCNews",
            "playlist_id": "PL0tDb4jw6kPymVj5xNNha5PezudD5Qw9L",
            "keyword": "Nightly News Full",
            "save_fields": "subtitle"
        },
        {
            "country": "Japan",
            "channel_handle": "@tbsnewsdig",
            "keyword": "（Japan News Digest Live）",
            "playlist_id": "PLhoNlZaJqDLaPgn1NqC9FxMPnlkemRpyr",
            "save_fields": "subtitle"
        },
        {
            "country": "China",
            "channel_handle": "@CCTV",
            "playlist_id": "PL0eGJygpmOH5xQuy8fpaOvKrenoCsWrKh",
            "keyword": "CCTV「新闻联播」",
            "save_fields": "description"
        }
    ]


INDEX_SYMBOLS = {
    "nasdaq100": "NDX",           # 나스닥 100
    "nikkei225": "JP#NI225",          # 닛케이 225
    "hangseng": "HK#HS",            # 항셍
    "kospi200": "^KS200",          # 코스피 200
    "eurostoxx50": "SX5E",    # 유로스톡스 50
    "dax": "GR#DAX",               # 독일 DAX
}
KR_STOCK_SYMBOLS = {
        "삼성전자": "005930",
        "한화오션": "042660",
        "현대해상": "001450"
    }

US_STOCK_SYMBOLS = {
    "tesla": "TSLA",  # 테슬라
}

CURRENCY_SYMBOLS = {
    "usd_krw": "FX@KRW",     # 한국 원
    "usd_dxy": "DX-Y.NYB",     # 달러 인덱스 (참고용)
    "usd_jpy": "FX@JPY",     # 일본 엔
    "usd_cny": "FX@CNY",     # 중국 위안
    "usd_eur": "FX@EUR",     # 유로
    "usd_gbp": "FX@GBP",     # 영국 파운드
    "usd_inr": "FX@IDR",     # 인도 루피
    "usd_thb": "FX@THB",     # 태국 바트
    "usd_vnd": "FX@VND",     # 베트남 동
    "usd_sgd": "FX@SGD",     # 싱가포르 달러
}

COMMODITY_SYMBOLS = {
    "gold": "M0101",                # 금
    "crude_oil": "WTIF",           # 서부 텍사스산 원유 (WTI)
    "natural_gas": "NG=F",         # 천연가스
    "corn": "M0301",                # 옥수수
    "wheat": "ZW=F",               # 밀
    "live_cattle": "LE=F",         # 생우
}

ALL_SYMBOLS = {
    "krx": {"kr_stock":KR_STOCK_SYMBOLS,
    },
    "yfinance": {
        "index" : INDEX_SYMBOLS,
        "commodity" : COMMODITY_SYMBOLS,
        "currency" : CURRENCY_SYMBOLS,
        "us_stock" : US_STOCK_SYMBOLS,
    }
}