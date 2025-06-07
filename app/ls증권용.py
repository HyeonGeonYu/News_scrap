import requests
from datetime import datetime, timedelta
import json
from pathlib import Path
from dotenv import load_dotenv
import os

# .env 파일 경로 설정 및 로드
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

LS_APPKEY = os.getenv("LS_APPKEY")
LS_SECRETKEY = os.getenv("LS_SECRETKEY")

TOKEN_CACHE_PATH = Path(__file__).resolve().parent / "ls_token_cache.json"

def load_cached_token():
    if TOKEN_CACHE_PATH.exists():
        with open(TOKEN_CACHE_PATH, "r") as f:
            token_info = json.load(f)
        expires_at = datetime.strptime(token_info["expires_at"], "%Y-%m-%d %H:%M:%S")
        if datetime.now() < expires_at:
            return token_info["access_token"]
    return None

def save_token_to_cache(access_token, expires_at_str):
    token_info = {
        "access_token": access_token,
        "expires_at": expires_at_str
    }
    with open(TOKEN_CACHE_PATH, "w") as f:
        json.dump(token_info, f)

def get_access_token(app_key, app_secret):
    cached_token = load_cached_token()
    if cached_token:
        print("✅ Using cached access token.")
        return cached_token

    print("🔐 Requesting new access token...")
    url = "https://openapi.ls-sec.co.kr:8080/oauth2/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    payload = {
        "grant_type": "client_credentials",
        "appkey": app_key,
        "appsecretkey": app_secret,
        "scope": "oob"
    }

    response = requests.post(url, headers=headers, data=payload)
    response.raise_for_status()

    data = response.json()
    access_token = data["access_token"]

    # expires_in 필드(초 단위)가 주어짐
    expires_in_seconds = int(data.get("expires_in", 86400))  # fallback: 24시간
    expires_at = datetime.now() + timedelta(seconds=expires_in_seconds)
    expires_at_str = expires_at.strftime("%Y-%m-%d %H:%M:%S")

    save_token_to_cache(access_token, expires_at_str)
    return access_token

if __name__ == "__main__":
    token = get_access_token(LS_APPKEY, LS_SECRETKEY)
    print(f"Access Token: {token}")
