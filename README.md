uvicorn app.main:app --reload
uvicorn app.main:app --reload --port 8001

pip install -r requirements.txt
playwright install

# 루트에서
# 이미지 빌드 (루트에서, Dockerfile은 app/Dockerfile)
docker build -f app/Dockerfile -t news-scrap .

# 실행 (app/.env가 있다면)
docker run -d --name news-scrap `
  --env-file app/.env `
  -e TZ=Asia/Seoul `
  news-scrap

