# Multi-stage build for CDC demo initialization and streaming
FROM python:3.11-slim as base

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    netcat-traditional \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set Java home for PySpark
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set working directory
WORKDIR /app

# Install Poetry
RUN pip install --no-cache-dir poetry==1.7.1

# Copy dependency files
COPY pyproject.toml poetry.lock* ./

# Configure Poetry to not create virtualenv (we're in a container)
RUN poetry config virtualenvs.create false

# Install dependencies
RUN poetry install --no-interaction --no-ansi --no-root --only main

# Copy application code
COPY src/ ./src/
COPY scripts/ ./scripts/

# Install the package
RUN poetry install --no-interaction --no-ansi --only-root

# Set Python path
ENV PYTHONPATH=/app

# Default command (will be overridden by docker-compose)
CMD ["bash"]
