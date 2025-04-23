# test_main.py
from fastapi import FastAPI
from app.storage import fetch_and_store_youtube_data
from fastapi.testclient import TestClient

# 테스트용 FastAPI 앱 만들기
app = FastAPI()

@app.get("/test/scheduled-store")
def test_scheduled_store():
    try:
        fetch_and_store_youtube_data()
        return {"status": "✅ fetch_and_store_youtube_data 실행 완료"}
    except Exception as e:
        return {"status": "❌ 오류 발생", "detail": str(e)}

# 테스트 클라이언트 생성
client = TestClient(app)

# 실행 시 바로 테스트 실행
if __name__ == "__main__":
    response = client.get("/test/scheduled-store")
    print("테스트 결과:", response.json())
