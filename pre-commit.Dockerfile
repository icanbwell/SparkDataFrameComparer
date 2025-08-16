FROM imranq2/helix.spark:3.5.5.0-precommit-slim

RUN apt-get update && \
    apt-get install -y git && \
    pip install pipenv

COPY Pipfile Pipfile.lock ./

RUN pipenv sync --system --dev --verbose

WORKDIR /sourcecode
RUN git config --global --add safe.directory /sourcecode
CMD ["pre-commit", "run", "--all-files"]
