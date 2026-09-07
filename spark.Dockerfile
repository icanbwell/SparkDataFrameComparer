FROM 856965016623.dkr.ecr.us-east-1.amazonaws.com/helix.spark:3.5.5.0-slim
# https://github.com/icanbwell/helix.spark
# Pulled from the services-account private ECR rather than Docker Hub (CIE-8032).
# Requires `make ecr-login` locally; CI logs in to the same registry first.
# The tag is unchanged on purpose: 3.5.5.0 == Spark 3.5.5, which must match the
# pyspark==3.5.5 pin in Pipfile.
USER root

ENV PYTHONPATH=/sdc
ENV CLASSPATH=/sdc/jars:$CLASSPATH

COPY Pipfile Pipfile.lock /sdc/
WORKDIR /sdc

RUN pipenv sync --system --dev --extra-pip-args="--prefer-binary"

# override entrypoint to remove extra logging
RUN mv /opt/minimal_entrypoint.sh /opt/entrypoint.sh

USER root

COPY . /sdc

# Run as non-root user
# https://spark.apache.org/docs/latest/running-on-kubernetes.html#user-identity
USER spark
