FROM python:3.13-slim AS base

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY schemas ./schemas
COPY src ./src
COPY ui ./ui

ENV PYTHONUNBUFFERED=1


FROM base AS test

COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt
COPY pytest.ini .
COPY tests ./tests
RUN pytest -q


FROM base AS runtime

CMD ["python", "-m", "src.consumer"]
