channels = [
        {"country": "Korea",
         "channel_handle": "@newskbs",
         "playlist_id": ["PL9a4x_yPK_85sGRvAQX4LEVHY8F9v405J","PL9a4x_yPK_84yhk67LjymVv7kbI5eyOVU"],
         "keyword": ["[풀영상] 뉴스12", "[풀영상] 뉴스광장"],
         "save_fields": "subtitle"},
        {
            "country": "USA",
            "channel_handle": "@PBSNewsHour",
            "playlist_id": "PLgawtcOBBjr9w4YGiJrPD8f6MXTCIUfK9",
            "keyword": ["PBS News Hour full episode", "PBS News Weekend full episode"],
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
        },
        {
            "country": "Germany",
            "channel_handle": "@tagesschau",
            "playlist_id": "PLkKDSXRppVa7FFdxr-wQaNltbtvOkZvx_",
            "keyword": " / ",
            "save_fields": "description"
        },
        {
            "country": "UK",
            "channel_handle": "@BBCNews",
            "playlist_id": "PLS3XGZxi7cBU_AtRadnVPJPRC6LHrexaF",
            "keyword": "BBC Newscast",
            "save_fields": "subtitle"
        },
        {
            "country": "India",
            "channel_handle": "@DDnews",
            "playlist_id": "",
            "keyword": "LIVE: News Night",
            "save_fields": "subtitle"
        },
    ]

channels = [
    {
        "country": "USA",
        "channel_handle": "@PBSNewsHour",
        "playlist_id": "PLgawtcOBBjr9w4YGiJrPD8f6MXTCIUfK9",
        "keyword": ["PBS News Hour full episode", "PBS News Weekend full episode"],
        "save_fields": "subtitle"
    },
    ]


dmi_KR_STOCK_SYMBOLS = {
         "kospi200": "2001",          # 코스피 200
    }

os_INDEX_SYMBOLS = {
    "nasdaq100": "NDX",           # 나스닥 100
    "ShanghaiA": "CH#SHA",           # 상해 A
    "hangseng": "HK#HS",            # 항셍
    "nikkei225": "JP#NI225",          # 닛케이 225
    "BSE30":"IN#BOMBAY", # 인도 센섹스30
    "eurostoxx50": "SX5E",    # 유로스톡스 50
    "dax": "GR#DAX",               # 독일 DAX 30
}

os_treasury_SYMBOLS = {
    "us-t10" : "Y0202",
    "jp-t10" : "Y0207",
    "kr-t3" : "Y0101"
}

dmc_KR_STOCK_SYMBOLS = {
        "삼성전자": "005930",
        "한화오션": "042660",
        "현대해상": "001450"
    }

os_US_STOCK_SYMBOLS = {
    "tesla": "TSLA",  # 테슬라
}

yf_CURRENCY_SYMBOLS = {
    "dxy": "DX-Y.NYB",     # 한국 원
}

os_CURRENCY_SYMBOLS = {
    "usd_krw": "FX@KRW",     # 한국 원
    "usd_jpy": "FX@JPY",     # 일본 엔
    "usd_cny": "FX@CNY",     # 중국 위안
    "usd_eur": "FX@EUR",     # 유로 유로->달러임
    "usd_gbp": "FX@GBP",     # 영국 파운드 파운드->달러
    "usd_cad": "FX@CAD",     # 캐나다 달러
    "usd_sek": "FX@SEK",     # 스웨덴 크로나
    "usd_chf": "FX@CHF",     # 스위스 프랑
    "usd_inr": "FX@IDR",     # 인도 루피
    "usd_thb": "FX@THB",     # 태국 바트
    "usd_vnd": "FX@VND",     # 베트남 동
    "usd_sgd": "FX@SGD",     # 싱가포르 달러
}

os_COMMODITY_SYMBOLS = {
    "gold": "NYGOLD",                # 금C
    "crude_oil": "WTIF",           # WTI 원유C
    "corn": "CHICORN",  # CBOT 옥수수C
    "coffee": "COFFE",  # NYBOT 커피 C
}

ALL_SYMBOLS = {
    "domestic": {"kr_stock":dmc_KR_STOCK_SYMBOLS,
    },
    "overseas": {
        "index" : os_INDEX_SYMBOLS,
        "commodity" : os_COMMODITY_SYMBOLS,
        "currency" : os_CURRENCY_SYMBOLS,
        "us_stock" : os_US_STOCK_SYMBOLS,
        "treasury" : os_treasury_SYMBOLS
    },
    "dmr": {"index":dmi_KR_STOCK_SYMBOLS,
    }
}
