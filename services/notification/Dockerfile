FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c AS base
RUN python -m pip install --no-cache-dir --upgrade pip==26.2.1
WORKDIR /app
COPY services/notification/requirements.txt ./
COPY packages/python/pyproject.toml /packages/python/pyproject.toml
COPY packages/python/observa_messaging /packages/python/observa_messaging
RUN pip install --no-cache-dir -r requirements.txt /packages/python
COPY services/notification/app ./app
COPY services/notification/schema.sql ./schema.sql
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app

FROM base AS test
COPY services/notification/requirements-test.txt ./
RUN pip install --no-cache-dir -r requirements-test.txt
COPY services/notification/tests ./tests
CMD ["sh", "-c", "ruff check app tests && pytest -q tests"]

FROM base AS runtime
USER 10001:10001
EXPOSE 8000
CMD ["python", "-m", "app.main"]
