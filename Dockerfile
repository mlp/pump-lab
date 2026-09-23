FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
ENV SPACES_BUCKET=fumppun SPACES_REGION=lon1 SPACES_PREFIX=pump-lab/prospective \
    PUMP_RUN_ID=pump-24h-20260923 PUMP_EXCLUDE_MAYHEM=1
WORKDIR /app
COPY requirements-collector.txt .
RUN pip install --no-cache-dir -r requirements-collector.txt
COPY collector.py ws_probe.py decode.py ./
COPY sources/pump.json sources/pump.json
USER 65534:65534
CMD ["python", "-u", "collector.py"]
