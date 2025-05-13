FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    curl unzip wget gnupg ca-certificates fonts-liberation \
    chromium chromium-driver \
    && rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/chromium
ENV PATH="${PATH}:/usr/lib/chromium/"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["sleep", "infinity"]
