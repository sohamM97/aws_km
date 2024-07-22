import asyncio
import os

import aioboto3
from constants import COLLECTION_NAME, INDEX_NAME
from dotenv import load_dotenv
from opensearchpy import AsyncHttpConnection, AsyncOpenSearch, AWSV4SignerAsyncAuth

load_dotenv()


async def main():

    session = aioboto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    )

    async with session.client("opensearchserverless") as aoss_client:
        response = await aoss_client.batch_get_collection(names=[COLLECTION_NAME])

    host = response["collectionDetails"][0]["collectionEndpoint"].replace(
        "https://", ""
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

    opensearch_client = AsyncOpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=AsyncHttpConnection,
        timeout=300,
    )

    # Search for the document.
    q = "dhoni"
    query = {
        "query": {
            "bool": {
                "must": [
                    {"knn": {"embedding": {"vector": [1, 2, 3], "k": 50}}},
                    {"multi_match": {"query": q, "fields": ["content"]}},
                ],
                "filter": [{"terms": {"tags": [1, 2]}}],
            }
        },
    }

    response = await opensearch_client.search(body=query, index=INDEX_NAME)
    print("\nSearch results:")
    print(response)

    await opensearch_client.close()


if __name__ == "__main__":
    asyncio.run(main())
