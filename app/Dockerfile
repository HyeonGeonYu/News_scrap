# app/Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Seoul \
    LANG=C.UTF-8

RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata ca-certificates tini \
 && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 10001 appuser

WORKDIR /app

# 컨텍스트는 루트(.) → 루트의 requirements.txt 복사
COPY requirements.txt ./requirements.txt
RUN python -m pip install --upgrade pip && \
    python -m pip install -r requirements.txt && \
    python -m playwright install --with-deps chromium

# 루트의 app/ 내용을 컨테이너 /app 으로 복사
COPY app/ /app/

RUN chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["/usr/bin/tini","--"]
# ★ 스크립트 실행 (모듈 -m 아님) → from storage import ... 과 일치
CMD ["python","main.py"]
