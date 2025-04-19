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
            "keyword": "Nightly News Full Episode",
            "save_fields": "subtitle"
        },
        {
            "country": "Japan",
            "channel_handle": "@tbsnewsdig",
            "keyword": "【LIVE】朝のニュース（Japan News Digest Live）",
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
    "nasdaq100": "^NDX",
    "nikkei225": "^N225",
    "hangseng": "^HSI",
    "kospi200": "^KS200",
    "gold": "GC=F"
}

CURRENCY_SYMBOLS_KRW = {
    "usd_krw": "USDKRW=X",     # 미국 달러 ➝ 원화 (USD → KRW)
    "dxy": "DX-Y.NYB",         # 달러 인덱스 (Dollar Index)
    "eur_usd": "EURUSD=X",     # 유로 ➝ 달러 (EUR → USD)
    "jpy_usd": "JPYUSD=X",     # 일본 엔 ➝ 달러 (JPY → USD)
    "cny_usd": "CNYUSD=X",     # 중국 위안 ➝ 달러 (CNY → USD)
    "gbp_usd": "GBPUSD=X",     # 영국 파운드 ➝ 달러 (GBP → USD)
    "cad_usd": "CADUSD=X",     # 캐나다 달러 ➝ 달러 (CAD → USD)
    "sgd_usd": "SGDUSD=X",     # 싱가포르 달러 ➝ 달러 (SGD → USD)
}

