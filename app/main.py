from fastapi import FastAPI
from .URL찾기 import get_latest_video_url
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# CORS 설정 추가
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 모든 도메인 허용 (보안 문제 있으면 특정 도메인만 허용)
    allow_credentials=True,
    allow_methods=["*"],  # 모든 HTTP 메서드 허용 (GET, POST 등)
    allow_headers=["*"],  # 모든 헤더 허용
)

@app.get("/youtube", methods=["GET", "HEAD"])
async def get_youtube():
    """
    미국, 중국, 일본의 최신 뉴스 영상 URL을 반환합니다.
    """
    channels = [
        {"country": "USA", "channel_id": "https://www.youtube.com/@NBCNews", "keyword": "Nightly News Full Episode","content_type" : "videos"},
        {"country": "Japan", "channel_id": "https://www.youtube.com/@tbsnewsdig", "keyword": "【LIVE】朝のニュース（Japan News Digest Live）最新情報など｜TBS NEWS DIG","content_type" : "streams"},
        {"country": "China", "channel_id": "https://www.youtube.com/@CCTV", "keyword": "CCTV「新闻联播」","content_type" : "videos"}
    ]

    results = {}
    for channel in channels:
    # video_url = get_latest_video_url(channel["channel_id"], channel["keyword"], channel["content_type"])
        results[channel["country"]] = "https://www.youtube.com"

    return results