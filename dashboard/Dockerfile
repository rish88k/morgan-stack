FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY index.html templates/index.html
COPY app.py app.py

EXPOSE 6969

CMD ["gunicorn", "--bind", "0.0.0.0:6969", "--workers", "2", "--timeout", "30", "app:app"]
