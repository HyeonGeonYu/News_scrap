$env:DOCKER_BUILDKIT=1
docker rm -f news-scrap
docker build -f app/Dockerfile -t news-scrap .
docker run -d --name news-scrap `
  --env-file app/.env `
  -e TZ=Asia/Seoul `
  --shm-size=1g `
  --restart unless-stopped `
  --gpus all `
  news-scrap

