FROM imranq2/helix.spark:3.3.0.48-slim
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

USER root
# install python 3.12 - it's not available in normal ubuntu repositories
# https://github.com/deadsnakes/issues/issues/53
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys F23C5A6CF475977595C89F51BA6932366A755776 && \
    echo "deb https://ppa.launchpadcontent.net/deadsnakes/ppa/ubuntu/ jammy main" | tee /etc/apt/sources.list.d/deadsnakes-ubuntu-ppa-lunar.list && \
    apt-get update && apt-get install -y python3.12 && \
    update-alternatives --install /usr/bin/python python /usr/bin/python3.12 1 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1

RUN pip install pyspark==3.3.0
RUN pip install pytest>=8.2.2

COPY . /sdc

RUN df -h # for space monitoring
RUN mkdir -p /fhir && chmod 777 /fhir
RUN mkdir -p /.local/share/virtualenvs && chmod 777 /.local/share/virtualenvs
# USER 1001
