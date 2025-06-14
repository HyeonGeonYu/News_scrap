import time
from pykrx import stock
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from app.test_config import ALL_SYMBOLS
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


def save_token_to_cache(access_token, expires_at_str):
    # 문자열 → datetime 객체
    expires_at = datetime.strptime(expires_at_str, "%Y-%m-%d %H:%M:%S")
    with open(CACHE_PATH, "w") as f:
        json.dump({
            "access_token": access_token,
            "expires_at": expires_at.isoformat()
        }, f)

def load_cached_token():
    if CACHE_PATH.exists():
        with open(CACHE_PATH, "r") as f:
            cache = json.load(f)
            expires_at = datetime.fromisoformat(cache["expires_at"])
            if datetime.now() < expires_at:
                return cache["access_token"]
    return None

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

import requests

# === 사용자 정의 값 ===
tr_id = "OTFM1411R"

# 계좌 및 조회 정보
CANO = "12345678"            # 계좌 앞 8자리
ACNT_PRDT_CD = "01"          # 계좌 뒤 2자리
CRCY_CD = "USD"              # 예: 'USD' / 'KRW' 등
INQR_DT = "20250614"         # 조회일자 (YYYYMMDD)

# === API URL ===
base_url = "https://openapi.koreainvestment.com:9443"
endpoint = "/uapi/overseas-futureoption/v1/trading/inquire-deposit"

# === Headers ===
headers = {
    "content-type": "application/json; charset=utf-8",
    "authorization": token,
    "appkey": app_key,
    "appsecret": app_secret,
    "tr_id": tr_id
}

# === Query Parameters ===
params = {
    "CANO": CANO,
    "ACNT_PRDT_CD": ACNT_PRDT_CD,
    "CRCY_CD": CRCY_CD,
    "INQR_DT": INQR_DT
}

# === GET 요청 ===
response = requests.get(base_url + endpoint, headers=headers, params=params)

# === 결과 출력 ===
if response.status_code == 200:
    print("✅ 조회 성공:")
    print(response.json())
else:
    print("❌ 조회 실패:")
    print("Status Code:", response.status_code)
    print("Response:", response.text)


