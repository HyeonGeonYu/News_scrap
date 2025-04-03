from fastapi import FastAPI
from .URL찾기 import get_latest_video_url

app = FastAPI()

@app.get("/youtube")
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