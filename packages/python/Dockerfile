FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c
RUN python -m pip install --no-cache-dir --upgrade pip==26.2.1
WORKDIR /app/packages/python
COPY packages/python/pyproject.toml ./
COPY packages/python/observa_messaging ./observa_messaging
RUN pip install --no-cache-dir '.[test]'
COPY packages/python/tests ./tests
COPY tests/contracts /app/tests/contracts
CMD ["sh", "-c", "ruff check observa_messaging tests && pytest -q"]
