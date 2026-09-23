FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sensor.py .

# Desligar o buffer de saída do Python para que os logs sejam exibidos em tempo real
ENV PYTHONUNBUFFERED=1

CMD ["python", "sensor.py"]
