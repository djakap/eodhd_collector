FROM python:3.13-slim

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
