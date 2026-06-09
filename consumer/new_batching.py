import json
import gzip
import time
from botocore import regions
from botocore.config import Config
from botocore.exceptions import ClientError
import boto3
from confluent_kafka import Consumer
from datetime import datetime, UTC
import os



KAFKA_CONFIG = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'minio-sink-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'security.protocol': 'PLAINTEXT',
}

print(f"KAFKA_CONFIG: {KAFKA_CONFIG}");

S3_CONFIG= {
    'aws_access_key_id':os.getenv('AWS_ACCESS_KEY_ID'),
    'aws_secret_access_key':os.getenv('AWS_SECRET_ACCESS_KEY')
}

print(f"S3_CONFIG: {S3_CONFIG}")

consumer= Consumer(KAFKA_CONFIG)
print("consumer is created")

s3_client = boto3.client("s3", **S3_CONFIG, region_name='ap-south-1', config=Config(connect_timeout=5, read_timeout=5))
print(f"s3_client is created")

bucket_name='banking-database-dump-dev'
print(f"bucket_name is created")








class KafkaToS3Sink:
    def __init__(self, topic, bucket, folder, batch_size, flush_interval):
        self.topic = topic
        self.bucket = bucket
        self.folder = folder
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.batch = []
        self.last_flush = time.time()

    def add_record(self, record):
        self.batch.append(record)
        if len(self.batch) >= self.batch_size:
            self.flush()

    def check_timer(self):
        if time.time() - self.last_flush >= self.flush_interval:
            self.flush()

    def flush(self):
        if not self.batch:
            return
        
        timestamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%S')
        filename = f"{self.folder}/batch_{timestamp}_{time.time_ns()}.json.gz"
        
        payload = "\n".join(json.dumps(r) for r in self.batch).encode("utf-8")
        compressed = gzip.compress(payload)
        
        s3_client.put_object(Bucket=self.bucket, Key=filename, Body=compressed)
        print(f"[{self.topic}] Flushed {len(self.batch)} records → {filename}")
        
        self.batch = []
        self.last_flush = time.time()

# ----------------------------
# Initialize Sinks
# ----------------------------
sinks = {
    'source_dev.public.acc_transactions': KafkaToS3Sink(
        'source_dev.public.acc_transactions', bucket_name, 'transactions', 100, 30
    ),
    'source_dev.public.accounts': KafkaToS3Sink(
        'source_dev.public.accounts', bucket_name, 'accounts', 20, 300 # 5 mins
    ),
    'source_dev.public.customers': KafkaToS3Sink(
        'source_dev.public.customers', bucket_name, 'customers', 10, 900 # 15 mins
    )
}

def run_consumer():
    consumer.subscribe(list(sinks.keys()))
    print(f"Listening for: {list(sinks.keys())}")

    try:
        while True:
            msg = consumer.poll(1.0)
            
            # 1. Handle message
            if msg and not msg.error():
                topic = msg.topic()
                record = json.loads(msg.value().decode("utf-8"))
                sinks[topic].add_record(record)
            
            # 2. Check time-based flushes for ALL sinks
            for sink in sinks.values():
                sink.check_timer()

    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        for sink in sinks.values():
            sink.flush()
        consumer.close()