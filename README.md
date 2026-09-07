[![Build and Test](https://github.com/imranq2/SparkDataFrameComparer/actions/workflows/build_and_test.yml/badge.svg)](https://github.com/imranq2/SparkDataFrameComparer/actions/workflows/build_and_test.yml)

[![Upload Python Package](https://github.com/imranq2/SparkDataFrameComparer/actions/workflows/main.yml/badge.svg)](https://github.com/imranq2/SparkDataFrameComparer/actions/workflows/main.yml)

[![Known Vulnerabilities](https://snyk.io/test/github/imranq2/SparkDataFrameComparer/badge.svg?targetFile=requirements.txt)](https://snyk.io/test/github/imranq2/SparkDataFrameComparer?targetFile=requirements.txt)

# SparkDataFrameComparer
Deep compare of two data frames (recurses down into array and struct columns)

## Local development

Both docker images used by this repo now pull their base image from a **private**
ECR repository in b.well's services account (`856965016623`) instead of Docker Hub:

| file | base image |
|---|---|
| `spark.Dockerfile` | `856965016623.dkr.ecr.us-east-1.amazonaws.com/helix.spark:3.5.5.0-slim` |
| `pre-commit.Dockerfile` | `856965016623.dkr.ecr.us-east-1.amazonaws.com/helix.spark:3.5.5.0-precommit-slim` |

So before building anything you need AWS credentials for that account:

1. Run `aws sso login --profile services`.
2. `make build` / `devdocker` / `up` / `run-pre-commit` / `Pipfile.lock` log docker in
   to the registry for you. If you build by another route (e.g. `docker build -f
   pre-commit.Dockerfile .` by hand), run `make ecr-login` first.
3. Pass `AWS_SERVICES_PROFILE=<name>` if your profile is not called `services`.

Note that this means the docker-based targets are not usable by external
contributors without b.well AWS access. The Python package itself has no such
requirement — `pip install sparkdataframecomparer` and `pytest` against a local
pyspark install are unaffected.
