import asyncio
import logging
import os

import aioboto3
import aioboto3.session
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()


async def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            s3_session = aioboto3.Session(
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
                aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
            )
            async with s3_session.client("s3") as s3_client:
                await s3_client.create_bucket(Bucket=bucket_name)

        else:
            s3_client = aioboto3.Session(
                "s3",
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
                aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
                region_name=region,
            )
            location = {"LocationConstraint": region}
            async with s3_session.client("s3") as s3_client:
                await s3_client.create_bucket(
                    Bucket=bucket_name, CreateBucketConfiguration=location
                )

        async with s3_session.client("s3") as s3_client:
            await s3_client.put_public_access_block(
                Bucket=bucket_name,
                PublicAccessBlockConfiguration={
                    "BlockPublicAcls": False,
                    "IgnorePublicAcls": False,
                    "BlockPublicPolicy": False,
                    "RestrictPublicBuckets": False,
                },
            )

            await s3_client.put_bucket_ownership_controls(
                Bucket=bucket_name,
                OwnershipControls={
                    "Rules": [
                        {"ObjectOwnership": "BucketOwnerPreferred"}  # or 'ObjectWriter'
                    ]
                },
            )

    except ClientError as e:
        logging.error(e)
        return False
    return True


async def main():
    if await create_bucket("soham-boto-s3-async"):
        print("Bucket created!")
    else:
        print("Error creating bucket!")

    session = aioboto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    )
    async with session.client("s3") as s3:
        response = await s3.list_buckets()

        # Output the bucket names
        print("Buckets:")
        for bucket in response["Buckets"]:
            print(f'  {bucket["Name"]}')


if __name__ == "__main__":
    asyncio.run(main())
