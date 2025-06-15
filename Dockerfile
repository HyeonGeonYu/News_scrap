# 1️⃣ 베이스 이미지
FROM python:3.11-slim

# 2️⃣ 작업 디렉토리
WORKDIR /app

# 3️⃣ 시스템 의존성 설치 (Playwright 필수 패키지 추가됨)
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libatspi2.0-0 \
    libxcomposite1 \
    libgbm1 \
    libasound2 \
    libx11-xcb1 \
    libxrandr2 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libdrm2 \
    libxshmfence1 \
    ca-certificates \
    fonts-liberation \
    libappindicator3-1 \
    xdg-utils \
    libgtk-4-1 \
    libgraphene-1.0-0 \
    libgstreamer1.0-0 \
    libgstreamer-plugins-base1.0-0 \
    libavif15 \
    libenchant-2-2 \
    libsecret-1-0 \
    libmanette-0.2-0 \
    libgles2 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 필요한 코드만 복사 (app 폴더만!)
COPY app/ .
# PYTHONPATH 설정
ENV PYTHONPATH=/app

# 4️⃣ Python 의존성 설치
COPY requirements.txt .
RUN pip install -r requirements.txt

# 5️⃣ Playwright 설치 (브라우저 포함)
RUN pip install playwright && playwright install --with-deps


CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]
