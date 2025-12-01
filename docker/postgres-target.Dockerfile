# Use an appropriate base image
FROM postgres:13

# create table at entrypoint with public schema
COPY ./docker/target.sql /docker-entrypoint-initdb.d/

# RUN chmod a+r /docker-entrypoint-initdb.d/*