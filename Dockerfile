# EduThreat-CTI API Dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies including Chrome/Chromium for Selenium
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libwayland-client0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

# Install Google Chrome (stable version) - using modern method without deprecated apt-key
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome-keyring.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-chrome-stable \
    && rm -rf /var/lib/apt/lists/* \
    && google-chrome --version

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY src/ ./src/

# Copy scripts for migration and utilities
COPY scripts/ ./scripts/

# Create directories for logs (data dir will be mounted as Railway volume)
RUN mkdir -p logs

# Set environment variables
# Railway persistent storage is mounted at /app/data
# DB will be created/used from persistent volume
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV EDU_CTI_DATA_DIR=/app/data
ENV EDU_CTI_DB_PATH=eduthreat.db

# Expose port
EXPOSE 8000

# Use shell form so PORT env gets read by Python
# Start Xvfb in background for non-headless Selenium support, then run the app
CMD Xvfb :99 -screen 0 1920x1080x24 -ac +extension GLX +render -noreset & \
    export DISPLAY=:99 && \
    python -m src.edu_cti.api --host 0.0.0.0
