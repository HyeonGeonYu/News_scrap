# 1️⃣ 베이스 이미지
FROM python:3.11-slim

# 2️⃣ 작업 디렉토리
WORKDIR /app

# 3️⃣ 의존성 설치
COPY requirements.txt .
RUN pip install -r requirements.txt

# 4️⃣ Playwright와 필요한 라이브러리 설치
RUN apt-get update && \
    apt-get install -y \
    wget \
    libgtk-4-1 \
    libgraphene-1.0-0 \
    libgstreamer1.0-0 \
    libgstreamer-plugins-base1.0-0 \
    libavif15 \
    libenchant-2-2 \
    libsecret-1-0 \
    libmanette-0.2-0 \
    libgles2 && \
    playwright install

# 5️⃣ 프로젝트 코드 복사
COPY . .

# 6️⃣ 앱 실행 (main.py 기준)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "10000"]
