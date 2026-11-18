FROM denoland/deno:bin AS deno

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=deno /deno /usr/local/bin/deno

WORKDIR /app

COPY requirements.txt .

RUN pip install --upgrade pip \
    && pip install --upgrade -r requirements.txt \
    && deno --version \
    && python -m yt_dlp --version \
    && ffmpeg -version

COPY . .

CMD ["python", "bot.py"]
