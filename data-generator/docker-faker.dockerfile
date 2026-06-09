FROM python:3.9

WORKDIR /app

RUN pip install --no-cache-dir psycopg2-binary faker

COPY schema.py .
COPY seed_data.py .
COPY main.py .

ENV PYTHONUNBUFFERED=1

CMD [ "python", "-u", "main.py" ]