import asyncio
import os

import aioboto3
from constants import BUCKET_NAME


async def main():
    s3_session = aioboto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    )
    async with s3_session.client("s3") as s3_client:
        objects = await s3_client.list_objects_v2(Bucket=BUCKET_NAME)
        for obj in objects["Contents"]:
            print(obj["Key"])


if __name__ == "__main__":
    asyncio.run(main())
