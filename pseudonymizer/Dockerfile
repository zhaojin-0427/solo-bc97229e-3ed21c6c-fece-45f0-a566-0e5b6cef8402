FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PSEUDO_DATA_DIR=/data

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY wsgi.py ./

RUN mkdir -p /data/uploads /data/outputs

EXPOSE 8000

# 单 worker + 多线程: 线程池并行转换批次，编号分配由 SQLite 短事务串行保证一致
CMD ["gunicorn", "-w", "1", "--threads", "8", "-b", "0.0.0.0:8000", \
     "--timeout", "180", "--graceful-timeout", "30", "wsgi:app"]
