# Python 3.12, NOT 3.13: Prefect 3.6.18 (pinned in requirements-prefect.txt to
# match the server image) is not compatible with Python 3.13 — it imports the
# importlib_metadata backport that 3.6.18 doesn't declare as a dep on 3.13, so
# the worker crash-loops with ModuleNotFoundError. 3.12 mirrors the server's
# prefecthq/prefect:3.6.18-python3.12 image. Bump in lockstep with Prefect.
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements-prefect.txt .
RUN pip install --no-cache-dir -r requirements-prefect.txt

# Copy application code
COPY api/ api/
COPY collectors/ collectors/
COPY config/ config/
COPY db/ db/
COPY flows/ flows/
COPY utils/ utils/
COPY scripts/ scripts/
# main_ultrafast.py and collect_metadata.py were archived to scripts/legacy_eodhd/
# at the 2026-07-28 cleanup (retired EODHD ad-hoc scripts), already covered by the
# scripts/ copy above.
COPY prefect.yaml .

# Create logs directory
RUN mkdir -p logs

ENV PYTHONUNBUFFERED=1
