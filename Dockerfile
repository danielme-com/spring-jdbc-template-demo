FROM mysql:8.0.22
ENV MYSQL_ROOT_HOST=% \
    MYSQL_ROOT_PASSWORD=root
COPY init.sql /docker-entrypoint-initdb.d/
