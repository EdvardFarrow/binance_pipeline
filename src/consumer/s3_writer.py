import uuid
import io
from datetime import datetime, timezone
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config

from src.common.config import settings

class MinIOWriter:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            endpoint_url=settings.minio_endpoint,
            aws_access_key_id=settings.minio_access_key,
            aws_secret_access_key=settings.minio_secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        self.bucket = settings.minio_raw_bucket

        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
        except self.s3_client.exceptions.ClientError:
            self.s3_client.create_bucket(Bucket=self.bucket)

    def write_batch(self, batch: list[dict]) -> str | None:
        """
        Converts the batch to Parquet in-memory and pushes it to MinIO.
        """
        if not batch:
            return None

        table = pa.Table.from_pylist(batch)

        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        file_bytes = sink.getvalue().to_pybytes()

        now = datetime.now(timezone.utc)
        partition_path = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
        filename = f"{uuid.uuid4()}.parquet"
        full_path = f"{partition_path}/{filename}"

        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=full_path,
            Body=file_bytes
        )
        
        return f"{self.bucket}/{full_path}"