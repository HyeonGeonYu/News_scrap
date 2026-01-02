# app/Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Seoul \
    LANG=C.UTF-8 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata ca-certificates tini \
    wget gnupg \
 && rm -rf /var/lib/apt/lists/*

# ---- Microsoft Edge repo 추가 + Edge 설치
RUN wget -qO- https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg \
 && echo "deb [arch=amd64] https://packages.microsoft.com/repos/edge stable main" > /etc/apt/sources.list.d/microsoft-edge.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends microsoft-edge-stable \
 && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 10001 appuser

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN python -m pip install --upgrade pip && \
    python -m pip install -r requirements.txt && \
    python -m playwright install --with-deps msedge

RUN chown -R appuser:appuser /ms-playwright

COPY app/ /app/
RUN chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["/usr/bin/tini","--"]
CMD ["python","main.py"]
