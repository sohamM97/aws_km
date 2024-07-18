import boto3
from constants import COLLECTION_NAME, INDEX_NAME
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

aoss_client = boto3.client("opensearchserverless")
response = aoss_client.batch_get_collection(names=[COLLECTION_NAME])
host = response["collectionDetails"][0]["collectionEndpoint"].replace("https://", "")

service = "aoss"
region = "us-east-1"
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    service,
    session_token=credentials.token,
)

opensearch_client = OpenSearch(
    hosts=[{"host": host, "port": 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    timeout=300,
)

# Search for the document.
q = "consumer disclosure"
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

response = opensearch_client.search(body=query, index=INDEX_NAME)
print("\nSearch results:")
print(response)
