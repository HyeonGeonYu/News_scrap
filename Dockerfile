FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Seoul \
    LANG=C.UTF-8

# timezone, tini, CA
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata ca-certificates tini \
 && rm -rf /var/lib/apt/lists/*

# 비루트 유저
RUN useradd -m -u 10001 appuser

# ---- 의존성 설치 (캐시 최적화) ----
WORKDIR /app
COPY requirements.txt ./requirements.txt
RUN python -m pip install --upgrade pip && \
    python -m pip install -r requirements.txt

# ---- 앱 복사 ----
# repo 루트의 app/ 디렉토리 전체를 /app 로
COPY app/ /app/
RUN chown -R appuser:appuser /app
USER appuser

# PID 1 신호/좀비처리
ENTRYPOINT ["/usr/bin/tini", "--"]
# app/main.py 실행 (스케줄러 진입점)
CMD ["python", "main.py"]
