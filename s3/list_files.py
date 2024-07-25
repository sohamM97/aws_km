import asyncio
import os

import aioboto3
from constants import AWS_ACCESS_KEY, AWS_SECRET_KEY, BUCKET_NAME


async def list_objects(bucket_name: str):
    print(f"Listing all objects under bucket: {bucket_name}")

    s3_session = aioboto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    )
    async with s3_session.client("s3") as s3_client:
        objects = await s3_client.list_objects_v2(Bucket=bucket_name)
        for obj in objects["Contents"]:
            print(obj["Key"])


async def list_objects_with_prefix(bucket_name: str, prefix: str):
    print(f"Listing all objects under bucket: {bucket_name} starting with {prefix}:")

    s3_session = aioboto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    )
    async with s3_session.client("s3") as s3_client:
        objects = await s3_client.list_objects_v2(Bucket=bucket_name)
        for obj in objects["Contents"]:
            name = os.path.basename(obj["Key"])
            if name.startswith(prefix):
                print(obj["Key"])


async def main():
    await list_objects(bucket_name=BUCKET_NAME)
    await list_objects_with_prefix(bucket_name=BUCKET_NAME, prefix="ms")


if __name__ == "__main__":
    asyncio.run(main())
