# Base image
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Install system dependencies for PostgreSQL, MySQL, and Kafka
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy pyproject.toml and install dependencies
COPY pyproject.toml .

RUN pip install --no-cache-dir uv && \
    uv pip install --system --no-cache -e .

# Copy the rest of the application code
COPY . .

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash app && \
    chown -R app:app /app
USER app

# Add healthcheck for the consumer
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Command to run the consumer
CMD ["python", "-m consumer.weather_consumer"]
