# EduThreat-CTI API Server Dockerfile
# Serves the FastAPI REST API for the CTI dashboard

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY data/ ./data/

# Create directories
RUN mkdir -p /app/data /app/logs

# Environment variables
ENV PYTHONPATH=/app
ENV EDU_CTI_DB_PATH=eduthreat.db
ENV EDU_CTI_DATA_DIR=/app/data
ENV EDU_CTI_LOG_FILE=/app/logs/api.log

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the API server
CMD ["uvicorn", "src.edu_cti.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

