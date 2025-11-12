FROM apache/spark:3.5.0-python3

USER root

# Install netcat for healthchecks and wait-for-it functionality
RUN apt-get update && \
    apt-get install -y netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# Create directory for Ivy cache and set permissions
RUN mkdir -p /opt/spark/.ivy2 /opt/spark-jobs && \
    chown -R spark:spark /opt/spark/.ivy2 /opt/spark-jobs

# Switch to spark user for downloading dependencies
USER spark

# Pre-download and cache all Maven dependencies
# This significantly reduces startup time for the streaming job
RUN echo "System.exit(0)" | /opt/spark/bin/spark-shell \
    --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.20.18" \
    2>&1 | grep -E "(Resolving|downloading|Downloaded)" || true

# Switch back to root for entrypoint setup
USER root

# Set working directory for Spark jobs
WORKDIR /opt/spark-jobs

# Copy entrypoint script
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Switch to spark user for security
USER spark

# Set entrypoint
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
