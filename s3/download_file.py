import asyncio

import aioboto3
from constants import AWS_ACCESS_KEY, AWS_SECRET_KEY, BUCKET_NAME


async def get_chunks(blob, chunk_size):
    while True:
        chunk = await blob.read(chunk_size)
        if not chunk:
            break
        yield chunk


async def main():
    s3_client = aioboto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY
    ).client("s3")

    async with s3_client as client:
        response = await client.get_object(Bucket=BUCKET_NAME, Key="msdhoni.pdf")
        streaming_body = response["Body"]
        async for chunk in get_chunks(streaming_body, 1024):
            print(chunk)


if __name__ == "__main__":
    asyncio.run(main())
