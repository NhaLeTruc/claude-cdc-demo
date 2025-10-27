FROM postgres:15-alpine

# Enable logical replication for CDC
RUN echo "wal_level = logical" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "max_wal_senders = 10" >> /usr/share/postgresql/postgresql.conf.sample && \
    echo "max_replication_slots = 10" >> /usr/share/postgresql/postgresql.conf.sample

# Copy initialization script
COPY init.sql /docker-entrypoint-initdb.d/

EXPOSE 5432
