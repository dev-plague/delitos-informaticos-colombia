FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive PIP_NO_CACHE_DIR=1
RUN pip install --no-cache-dir \
      streamlit==1.40.1 \
      plotly==5.24.1 \
      pandas==2.2.3 \
      psycopg2-binary==2.9.10

WORKDIR /app
EXPOSE 8501
CMD ["streamlit", "run", "src/dashboard.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
