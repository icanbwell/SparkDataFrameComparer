FROM 856965016623.dkr.ecr.us-east-1.amazonaws.com/helix.spark:3.5.5.0-precommit-slim
# https://github.com/icanbwell/helix.spark
# Pulled from the services-account private ECR rather than Docker Hub (CIE-8032).
# Registry-only change: same image, same tag as before.  Requires `make ecr-login`
# locally; CI logs in to the same registry first.
#
# This clears CUSTOM-RULE-559 but NOT CUSTOM-RULE-2300, which accepts only
# .../root-mirror/* .  There IS a one-line flip that clears 2300 as well:
#
#     FROM 856965016623.dkr.ecr.us-east-1.amazonaws.com/root-mirror/python:3.12-slim
#
# (verified against Aikido's own engine: that FROM clears both 559 and 2300, and it
# is the only option that clears 2300 for this file).  It works because the
# -precommit-slim tag contains no Spark at all -- it is just python:3.12-slim plus
# git, pipenv and build-essential -- and every hook in .pre-commit-config.yaml is
# `language: system` (autoflake/flake8/black/mypy) or pure Python
# (end-of-file-fixer), so nothing here needs the JVM.  It would additionally need
# `git` and `build-essential` added to the apt-get line below, to replace what the
# current base supplies.
#
# NOT taken, deliberately: an org-wide search found no working root-mirror/*-slim
# usage anywhere (bwell_Platform's python:3.7-slim-bookworm aside, every working
# root-mirror example in the org is alpine), so root-mirror/python:3.12-slim may
# simply not be mirrored, and there are no AWS credentials available to check.
# Keeping the base image byte-identical to today beats swapping a proven tag for an
# unverifiable one.  Revisit once the tag is confirmed present:
#   aws ecr describe-images --repository-name root-mirror/python --region us-east-1

RUN apt-get update && \
    apt-get install -y git && \
    pip install pipenv

COPY Pipfile Pipfile.lock ./

RUN pipenv sync --system --dev --verbose

WORKDIR /sourcecode
RUN git config --global --add safe.directory /sourcecode
CMD ["pre-commit", "run", "--all-files"]

# don't default to root.  pre-commit-hook overrides this at runtime with
# --user "$(id -u):$(id -g)" so the bind-mounted /sourcecode stays writable
# whatever UID the host uses.
USER 1001
