FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generator.py .

# Wait a bit for Kafka to be ready, then start generating
CMD ["sh", "-c", "sleep 15 && python generator.py"]
