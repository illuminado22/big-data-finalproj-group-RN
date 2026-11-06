# Image shared by the dashboard (app.py) and the Kafka producer (python -m producer)
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    TZ=UTC \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app

COPY requirements/app.txt requirements/app.txt
RUN --mount=type=cache,target=/root/.cache/pip pip install --default-timeout=300 --retries 10 -r requirements/app.txt

COPY app.py ./
COPY dashboard ./dashboard
COPY producer ./producer

EXPOSE 8501
CMD ["streamlit", "run", "app.py"]
