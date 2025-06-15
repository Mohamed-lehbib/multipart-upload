FROM python:3.9-slim-bullseye

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends gcc && \
    pip install --no-cache-dir -r requirements.txt && \
    apt-get purge -y --auto-remove gcc && \
    rm -rf /var/lib/apt/lists/*

# Copy the rest of the code
COPY . .

# Expose port
EXPOSE 8000

# Default command (can be overridden by docker-compose)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
