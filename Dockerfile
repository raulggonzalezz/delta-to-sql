FROM mi-data-runtime:3.5.7

USER root

WORKDIR /opt/delta-to-sql

# Dependencias adicionales: Spark y Delta ya vienen en Runtime.
RUN pip3 install --no-cache-dir \
    "psycopg[binary]>=3.2,<4" \
    "PyYAML>=6,<7"

# Aplicación y configuración.
COPY src/app/ ./app/
COPY config/config.yaml ./config/config.yaml

ENV PYTHONPATH="/opt/delta-to-sql:${PYTHONPATH}"

USER spark
ENTRYPOINT ["/opt/entrypoint.sh"]