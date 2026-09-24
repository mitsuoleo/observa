ARG PYTHON_IMAGE=python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c
FROM ${PYTHON_IMAGE} AS base
WORKDIR /app
COPY probes/python/requirements.txt probes/python/constraints.txt ./
RUN pip install --no-cache-dir -c constraints.txt -r requirements.txt
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY probes/python/observa_probe ./observa_probe

FROM base AS test
COPY probes/python/requirements-test.txt probes/python/pyproject.toml ./
RUN pip install --no-cache-dir -c constraints.txt -r requirements-test.txt
COPY probes/python/tests ./tests
COPY tests/fixtures /fixtures
CMD ["sh", "-c", "ruff check observa_probe tests && pytest"]

FROM base AS runtime
USER 10001:10001
ENTRYPOINT ["python", "-m", "observa_probe"]


