import asyncio
import logging
import os
from io import BytesIO
from typing import Optional

import aioboto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = "soham-boto-s3-test"


async def upload_file(file_name: str, bucket: str, object_name: Optional[str] = None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_session = aioboto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    )
    try:
        async with s3_session.client("s3") as s3_client:
            await s3_client.upload_file(
                file_name,
                bucket,
                object_name,
                ExtraArgs={"ACL": "public-read"},
            )
    except ClientError as e:
        logging.error(e)
        return None

    return f"https://{bucket}.s3.amazonaws.com/{object_name}"


async def upload_fileobj(
    fileobj: BytesIO, bucket: str, object_name: Optional[str] = None
):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # Upload the file
    s3_session = aioboto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    )
    try:
        async with s3_session.client("s3") as s3_client:
            await s3_client.upload_fileobj(
                fileobj,
                bucket,
                object_name,
                ExtraArgs={"ACL": "public-read"},
            )
    except ClientError as e:
        logging.error(e)
        return None

    return f"https://{bucket}.s3.amazonaws.com/{object_name}"


async def main():
    file_url = await upload_file(
        file_name="msdhoni.pdf",
        bucket=BUCKET_NAME,
        object_name="testdir/msdhoni.pdf",
    )

    if file_url:
        print(f"File uploaded at {file_url}")
    else:
        print("Unable to upload file!")

    with open("msdhoni.pdf", "rb") as f:
        file_url = await upload_fileobj(
            fileobj=f,
            bucket=BUCKET_NAME,
            object_name="testdir/msdhonibytes.pdf",
        )

    if file_url:
        print(f"File uploaded using fileobj at {file_url}")
    else:
        print("Unable to upload file using fileobj!")


if __name__ == "__main__":
    asyncio.run(main())
