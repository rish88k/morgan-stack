import json
import gzip
import time
from botocore import regions
from botocore.config import Config
from botocore.exceptions import ClientError
import boto3
from confluent_kafka import Consumer
from datetime import datetime, UTC




KAFKA_CONFIG = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'minio-sink-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'security.protocol': 'PLAINTEXT',
}

print(f"KAFKA_CONFIG: {KAFKA_CONFIG}");

MINIO_CONFIG= {
    'aws_access_key_id': '',
    'aws_secret_access_key': ''
}

print(f"MINIO_CONFIG: {MINIO_CONFIG}")

consumer= Consumer(KAFKA_CONFIG)
print("consumer is created, connected to kafka broker")

s3_client = boto3.client("s3", **MINIO_CONFIG, region_name='ap-southeast-2', config=Config(connect_timeout=5, read_timeout=5))
print(f"s3_client is created, connected to minio server")

bucket_name='de-project-banking-pipeline-dev-2'
print(f"bucket_name is created, connected to minio server")




# ----------------------------
# Micro-batch config
# ----------------------------
BATCH_SIZE = 20          # records
FLUSH_INTERVAL = 10        # seconds
batch = []
last_flush = time.time()

# ----------------------------
# Flush function
# ----------------------------
def flush_batch():
    global batch, last_flush

    if not batch:
        return

    # Now you can call the class directly
    timestamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%S')
    filename = f"transactions/batch_{timestamp}_{time.time_ns()}.json.gz"

    payload = "\n".join(json.dumps(r) for r in batch).encode("utf-8")
    compressed = gzip.compress(payload)

    s3_client.put_object(
        Bucket=bucket_name,
        Key=filename,
        Body=compressed,
        ContentType="application/gzip"
    )

    print(f"Flushed {len(batch)} records → {filename}")

    batch = []
    last_flush = time.time()

# ----------------------------
# Kafka consumer loop
# ----------------------------
def run_consumer():
    
    response = s3_client.list_buckets()
    print(response)

    try:
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"})
    except s3_client.exceptions.BucketAlreadyExists:
        print("bucket already exists")
        pass
    except s3_client.exceptions.ClientError as e:
        print(f"bucket client error: {e}")
        pass
    except Exception as e:
        print(f"bucket creation error: {e}")
        pass
    consumer.subscribe(['banking_dev.public.acc_transactions'])
    print("Listening for events...")

    try:
        while True:
            msg = consumer.poll(1.0)
            num=0;

            if msg is None:
                # time-based flush
                if time.time() - last_flush >= FLUSH_INTERVAL:
                    flush_batch()
                    num=0;
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            record = json.loads(msg.value().decode("utf-8"))
            batch.append(record)
            num=num+1
            print(f"records appended: {num}")

            # size-based flush
            if len(batch) >= BATCH_SIZE:
                flush_batch()

    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        flush_batch()   # flush remaining records
        consumer.close()

if __name__ == "__main__":
    run_consumer()