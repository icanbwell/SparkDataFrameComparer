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

### Prerequisite: these base-image tags must exist in the services ECR

`helix.spark`'s publish workflows push **only to Docker Hub** — there is no ECR push step —
so the tags below do not reach `856965016623.dkr.ecr.us-east-1.amazonaws.com/helix.spark`
unless someone copies them. Until they do, `docker build` here fails on a missing image.

| tag to copy | expected digest |
|---|---|
| `3.5.5.0-slim` | `sha256:e2c4762e38e3f57bfa99afdd68621c0be46eb373475c5578113c0e683b193126` |
| `3.5.5.0-precommit-slim` | `sha256:0a972955724aca975fe124f6d4310c27003f9dcce839fdb5b93c9d3044d2b6b8` |

Copy with a manifest-preserving tool. These are multi-arch (`linux/amd64` + `linux/arm64`);
a `docker pull`/`tag`/`push` cycle from an Apple-silicon Mac would push arm64 only and
silently break amd64 CI runners.

```bash
aws sso login --profile services
aws ecr get-login-password --region us-east-1 --profile services \
  | crane auth login 856965016623.dkr.ecr.us-east-1.amazonaws.com --username AWS --password-stdin
DEST=856965016623.dkr.ecr.us-east-1.amazonaws.com/helix.spark
crane copy icanbwell/helix.spark:3.5.5.0-slim "$DEST:3.5.5.0-slim"
crane copy icanbwell/helix.spark:3.5.5.0-precommit-slim "$DEST:3.5.5.0-precommit-slim"
# verify:
crane digest "$DEST:3.5.5.0-slim"
crane digest "$DEST:3.5.5.0-precommit-slim"
```

Source is `icanbwell/helix.spark` (the icanbwell-owned namespace mandated by CIE-8032); it is
digest-identical to the old `imranq2/helix.spark`, so the copy is the same image bytes.

The tag itself is **derived, not chosen**: `A.B.C` in the tag is the Apache Spark version in
the image and must match this repo's `pyspark` pin. Do not change the tag as part of a
registry migration.

See `CIE-8032` for the full decision trail. Copying these tags does NOT by itself make CI
pass — this is a public repo on `ubuntu-latest` with no AWS identity, so a GitHub OIDC
trusted role scoped to `repo:icanbwell/SparkDataFrameComparer:*` is still required.
