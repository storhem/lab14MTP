FROM python:3.12-slim

WORKDIR /app
COPY ../analyzer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt streamlit

COPY . .
COPY ../analyzer/analysis.py /app/../analyzer/analysis.py

RUN mkdir -p /data

EXPOSE 8501
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
