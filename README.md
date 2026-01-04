uvicorn app.main:app --reload
uvicorn app.main:app --reload --port 8001

pip install -r requirements.txt
playwright install
playwright install msedge

# 루트에서
# 이미지 빌드 (루트에서, Dockerfile은 app/Dockerfilree)


$env:DOCKER_BUILDKIT=1
docker rm -f news-scrap
docker build -f app/Dockerfile -t news-scrap .
docker run -d --name news-scrap `
  --env-file app/.env `
  -e TZ=Asia/Seoul `
  --shm-size=1g `
  news-scrap


# 테스트용
$env:DOCKER_BUILDKIT=1
docker rm -f news-scrap
docker build -f app/Dockerfile -t news-scrap .

docker run --rm -it `
  --env-file app/.env `
  -e TZ=Asia/Seoul `
  --shm-size=1g `
  -v "${PWD}\app:/app" `
  news-scrap `
  python /app/test.py



