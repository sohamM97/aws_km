import asyncio
import os

import aioboto3
import botocore
import botocore.exceptions
import dotenv

dotenv.load_dotenv()


async def main():
    boto3_client = aioboto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    ).client("sts")

    try:
        async with boto3_client as client:
            print(await client.get_caller_identity())
    except botocore.exceptions.ClientError as exc:
        print("Incorrect credentials!")
        raise exc


if __name__ == "__main__":
    asyncio.run(main())
