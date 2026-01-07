# EduThreat-CTI API Dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY src/ ./src/
COPY data/ ./data/

# Create logs directory
RUN mkdir -p logs

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV EDU_CTI_DB_PATH=/app/data/eduthreat.db

# Expose port (Railway provides PORT env var)
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8000}/api/health || exit 1

# Default command (Railway overrides with railway.json)
CMD ["python", "-m", "src.edu_cti.api", "--host", "0.0.0.0", "--port", "8000"]
