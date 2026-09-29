# One image for the producer, the setup job and the feature API (+ its React console).

# ---- build the console ----
FROM node:20-alpine AS console
WORKDIR /web
COPY web/package.json web/package-lock.json* ./
RUN if [ -f package-lock.json ]; then npm ci --no-audit --no-fund; else npm install --no-audit --no-fund; fi
COPY web/ ./
RUN npm run build

# ---- python runtime ----
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    SCHEMA_DIR=/app/schemas/user_event

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY eddy ./eddy
COPY schemas ./schemas
COPY --from=console /web/dist ./eddy/static

RUN useradd --create-home appuser
USER appuser

CMD ["python", "-m", "eddy.producer"]
