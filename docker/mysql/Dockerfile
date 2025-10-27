FROM mysql:8.0

# Enable binlog for CDC
RUN echo "[mysqld]" >> /etc/mysql/conf.d/cdc.cnf && \
    echo "server-id=1" >> /etc/mysql/conf.d/cdc.cnf && \
    echo "log_bin=mysql-bin" >> /etc/mysql/conf.d/cdc.cnf && \
    echo "binlog_format=ROW" >> /etc/mysql/conf.d/cdc.cnf && \
    echo "binlog_row_image=FULL" >> /etc/mysql/conf.d/cdc.cnf && \
    echo "expire_logs_days=7" >> /etc/mysql/conf.d/cdc.cnf

# Copy initialization script
COPY init.sql /docker-entrypoint-initdb.d/

EXPOSE 3306
