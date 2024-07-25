import asyncio

import aioboto3
from constants import AWS_ACCESS_KEY, AWS_SECRET_KEY, BUCKET_NAME


async def main():
    s3_client = aioboto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY
    ).client("s3")

    async with s3_client as client:
        await client.delete_object(Bucket=BUCKET_NAME, Key="testdir/hello.txt")


if __name__ == "__main__":
    asyncio.run(main())
