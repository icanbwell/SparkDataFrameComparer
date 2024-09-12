FROM imranq2/helix.spark:3.5.1.11-slim
# https://github.com/icanbwell/helix.spark
USER root

ENV PYTHONPATH=/sdc
ENV CLASSPATH=/sdc/jars:$CLASSPATH

COPY Pipfile* /sdc/
WORKDIR /sdc

RUN pipenv sync --dev --system --extra-pip-args="--prefer-binary"

# override entrypoint to remove extra logging
RUN mv /opt/minimal_entrypoint.sh /opt/entrypoint.sh

USER root

COPY . /sdc

# Run as non-root user
# https://spark.apache.org/docs/latest/running-on-kubernetes.html#user-identity
USER spark
