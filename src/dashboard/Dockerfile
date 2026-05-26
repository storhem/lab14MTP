FROM python:3.12-slim

WORKDIR /app

# Build context: ./src (задаётся в docker-compose.yml)
# Устанавливаем зависимости анализатора + streamlit
COPY analyzer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt streamlit

# Файлы дашборда
COPY dashboard/ .

# analysis.py нужен дашборду: app.py добавляет /analyzer в sys.path
COPY analyzer/analysis.py /analyzer/analysis.py

RUN mkdir -p /data

EXPOSE 8501
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
