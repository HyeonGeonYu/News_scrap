uvicorn app.main:app --reload
uvicorn app.main:app --reload --port 8001

pip install -r requirements.txt
playwright install

# 루트에서
docker compose down
docker compose build --no-cache
docker compose up -d
docker compose logs -f worker
