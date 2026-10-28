FROM python:3.12-slim

RUN pip install --no-cache-dir dbt-core==1.8.7 dbt-postgres==1.8.2

WORKDIR /dbt
COPY . /dbt/

ENTRYPOINT ["dbt"]
