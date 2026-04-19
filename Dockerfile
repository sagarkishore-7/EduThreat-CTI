# EduThreat-CTI API Dockerfile
# Optimized for Railway deployment
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Install system dependencies for Playwright and general operation
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    wget \
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
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers (chromium only to save space)
RUN playwright install chromium --with-deps

# Copy the source code
COPY src/ ./src/
COPY pyproject.toml ./

# Install the package itself (so imports work as `src.edu_cti.*`)
RUN pip install --no-cache-dir .

# Copy scripts for migration and utilities
COPY scripts/ ./scripts/

# Create directories for logs (data dir will be mounted as Railway volume)
RUN mkdir -p logs

# Set environment variables
# Railway persistent storage is mounted at /app/data
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV EDU_CTI_DATA_DIR=/app/data
ENV EDU_CTI_DB_PATH=eduthreat.db

# Expose port (Railway sets PORT automatically)
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8000}/api/health || exit 1

# Start the API server - Railway injects PORT env var
CMD python -m src.edu_cti.api --host 0.0.0.0
