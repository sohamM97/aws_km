import asyncio
import os
from uuid import uuid4

import aioboto3
from constants import COLLECTION_NAME, INDEX_NAME
from dotenv import load_dotenv
from opensearchpy import AsyncHttpConnection, AsyncOpenSearch, AWSV4SignerAsyncAuth
from pypdf import PdfReader
from upload import BUCKET_NAME, upload_file

load_dotenv()

FILE_NAME = "msdhoni.pdf"


async def main():

    s3_url = await upload_file(file_name=FILE_NAME, bucket=BUCKET_NAME)
    session = aioboto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    )

    service = "aoss"
    region = "us-east-1"
    credentials = await session.get_credentials()
    credentials = await credentials.get_frozen_credentials()
    awsauth = AWSV4SignerAsyncAuth(
        credentials,
        region,
        service,
    )

    async with session.client("opensearchserverless") as aoss_client:
        response = await aoss_client.batch_get_collection(names=[COLLECTION_NAME])

    host = response["collectionDetails"][0]["collectionEndpoint"].replace(
        "https://", ""
    )

    # Build the OpenSearch client
    opensearch_client = AsyncOpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=AsyncHttpConnection,
        timeout=300,
    )

    if FILE_NAME.endswith(".pdf"):
        content = ""
        reader = PdfReader(FILE_NAME)
        for i in range(len(reader.pages)):
            page = reader.pages[i]
            content += page.extract_text()
    else:
        with open(FILE_NAME) as f:
            content = f.read()

    response = await opensearch_client.index(
        index=INDEX_NAME,
        body={
            "id": uuid4(),
            "project_uuid": uuid4(),
            "filename": FILE_NAME,
            "content": content,
            "sourcepage": FILE_NAME,
            "sourcefilepath": s3_url,
            "language": "english",
            "tags": [1, 2, 3],
            "embedding": [1, 2, 3],
        },
    )
    print("\nDocument added:")
    print(response)

    await opensearch_client.close()


if __name__ == "__main__":
    asyncio.run(main())
