FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Desligar o buffer de saída do Python para que os logs sejam exibidos em tempo real
ENV PYTHONUNBUFFERED=1

# Set the default command
CMD ["python", "/app/processador.py"]