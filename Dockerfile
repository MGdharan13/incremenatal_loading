# Base image with Python 3.11
FROM python:3.11-slim

# Avoid interactive prompts during package install
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV PORT=8000

# Install default JRE (Java runtime) and system tools for PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre-headless \
        procps \
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# default-jre-headless creates a symlink at /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY config.py main.py app.py par.py ./
COPY web/ ./web/
COPY input/ ./input/
RUN mkdir -p output/clean output/rejected

EXPOSE 8000

# Start Web UI Server
CMD ["python", "app.py"]
