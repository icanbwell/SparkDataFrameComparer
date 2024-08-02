FROM imranq2/helix.spark:3.3.0.47-slim
# https://github.com/icanbwell/helix.spark
USER root

ENV PYTHONPATH=/sdc
ENV CLASSPATH=/sdc/jars:$CLASSPATH

COPY Pipfile* /sdc/
WORKDIR /sdc

RUN df -h # for space monitoring
RUN pipenv sync --dev --system && pipenv run pip install pyspark==3.3.0

# override entrypoint to remove extra logging
RUN mv /opt/minimal_entrypoint.sh /opt/entrypoint.sh

COPY . /sdc

RUN df -h # for space monitoring
RUN mkdir -p /fhir && chmod 777 /fhir
RUN mkdir -p /.local/share/virtualenvs && chmod 777 /.local/share/virtualenvs
# USER 1001
