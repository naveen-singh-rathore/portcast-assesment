# Two images from one file:
#   api       (default) the service; no load-test code or deps
#   loadtest  bench.py + loadgen.py, used by `make load` / `make bench`
FROM python:3.12-slim AS base
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY quota/ quota/
RUN useradd --create-home app

FROM base AS loadtest
COPY requirements-loadtest.txt .
RUN pip install --no-cache-dir -r requirements-loadtest.txt
COPY loadtest/ loadtest/
USER app

FROM base AS api
COPY service/ service/
COPY quotas.yaml .
USER app
EXPOSE 8000
CMD ["uvicorn", "service.app:app", "--host", "0.0.0.0", "--port", "8000"]
