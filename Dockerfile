# Image shared by the dashboard (app.py) and the Kafka producer (producer.py)
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    TZ=UTC \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py exports.py producer.py ./

EXPOSE 8501
CMD ["streamlit", "run", "app.py"]
