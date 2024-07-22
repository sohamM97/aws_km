import json
import time

import boto3
import botocore
from constants import (
    ACCESS_POLICY_NAME,
    COLLECTION_NAME,
    ENCRYPTION_POLICY_NAME,
    INDEX_NAME,
    NETWORK_POLICY_NAME,
)
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# Build the client using the default credential configuration.
# You can use the CLI and run 'aws configure' to set access key, secret
# key, and default region.

client = boto3.client("opensearchserverless")
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


def create_encryption_policy(client):
    """Creates an encryption policy that matches all collections beginning with tv-"""
    try:
        response = client.create_security_policy(
            description="Encryption policy for TV collections",
            name=ENCRYPTION_POLICY_NAME,
            policy=json.dumps(
                {
                    "Rules": [
                        {
                            "ResourceType": "collection",
                            "Resource": [f"collection/{COLLECTION_NAME}*"],
                        }
                    ],
                    "AWSOwnedKey": True,
                }
            ),
            type="encryption",
        )
        print("\nEncryption policy created:")
        print(response)
    except botocore.exceptions.ClientError as error:
        if error.response["Error"]["Code"] == "ConflictException":
            print(
                "[ConflictException] The policy name or rules conflict with an existing policy."
            )
        else:
            raise error


def create_network_policy(client):
    """Creates a network policy that matches all collections beginning with tv-"""
    try:
        response = client.create_security_policy(
            description="Network policy for TV collections",
            name=NETWORK_POLICY_NAME,
            policy=json.dumps(
                [
                    {
                        "Description": "Public access for TV collection",
                        "Rules": [
                            {
                                "ResourceType": "dashboard",
                                "Resource": [f"collection/{COLLECTION_NAME}*"],
                            },
                            {
                                "ResourceType": "collection",
                                "Resource": [f"collection/{COLLECTION_NAME}*"],
                            },
                        ],
                        "AllowFromPublic": True,
                    }
                ]
            ),
            type="network",
        )
        print("\nNetwork policy created:")
        print(response)
    except botocore.exceptions.ClientError as error:
        if error.response["Error"]["Code"] == "ConflictException":
            print("[ConflictException] A network policy with this name already exists.")
        else:
            raise error


def create_access_policy(client):
    """Creates a data access policy that matches all collections beginning with tv-"""
    try:
        response = client.create_access_policy(
            description="Data access policy for TV collections",
            name=ACCESS_POLICY_NAME,
            # TODO: principal name is hardcoded
            policy=json.dumps(
                [
                    {
                        "Rules": [
                            {
                                "Resource": [f"index/{INDEX_NAME}*/*"],
                                "Permission": [
                                    "aoss:CreateIndex",
                                    "aoss:DeleteIndex",
                                    "aoss:UpdateIndex",
                                    "aoss:DescribeIndex",
                                    "aoss:ReadDocument",
                                    "aoss:WriteDocument",
                                ],
                                "ResourceType": "index",
                            },
                            {
                                "Resource": [f"collection/{COLLECTION_NAME}*"],
                                "Permission": ["aoss:CreateCollectionItems"],
                                "ResourceType": "collection",
                            },
                        ],
                        "Principal": ["arn:aws:iam::514857968326:user/dash"],
                    }
                ]
            ),
            type="data",
        )
        print("\nAccess policy created:")
        print(response)
    except botocore.exceptions.ClientError as error:
        if error.response["Error"]["Code"] == "ConflictException":
            print("[ConflictException] An access policy with this name already exists.")
        else:
            raise error


def create_collection(client):
    """Creates a collection"""
    try:
        response = client.create_collection(name=COLLECTION_NAME, type="VECTORSEARCH")
        return response
    except botocore.exceptions.ClientError as error:
        if error.response["Error"]["Code"] == "ConflictException":
            print(
                "[ConflictException] A collection with this name already exists. Try another name."
            )
        else:
            raise error


def wait_for_collection_creation(client):
    """Waits for the collection to become active"""
    response = client.batch_get_collection(names=[COLLECTION_NAME])
    # Periodically check collection status
    while (response["collectionDetails"][0]["status"]) == "CREATING":
        print("Creating collection...")
        time.sleep(30)
        response = client.batch_get_collection(names=[COLLECTION_NAME])
    print("\nCollection successfully created:")
    print(response["collectionDetails"])
    # Extract the collection endpoint from the response
    host = response["collectionDetails"][0]["collectionEndpoint"]
    final_host = host.replace("https://", "")
    index_data(final_host)


def index_data(host):
    """Create an index and add some sample data"""
    # Build the OpenSearch client
    client = OpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=300,
    )
    # It can take up to a minute for data access rules to be enforced
    time.sleep(45)

    # Create index
    if client.indices.exists(index=INDEX_NAME):
        print(f"Index {INDEX_NAME} already exists!")
    else:
        response = client.indices.create(
            index=INDEX_NAME,
            body={
                "settings": {"index.knn": True},
                "mappings": {
                    "properties": {
                        "embedding": {
                            "type": "knn_vector",
                            # TODO: Change to 1536
                            "dimension": 3,
                            "method": {
                                "engine": "nmslib",
                                "name": "hnsw",
                                "space_type": "cosinesimil",
                            },
                        },
                        # TODO: do we need id? AWS generates its own _id, check how to use
                        # that
                        "id": {"type": "text"},
                        "project_uuid": {"type": "text"},
                        "filename": {"type": "text"},
                        "content": {"type": "text"},
                        "sourcepage": {"type": "text"},
                        "sourcefilepath": {"type": "text"},
                        "language": {"type": "text"},
                        "tags": {"type": "long"},
                    }
                },
            },
        )
        print("\nCreating index:")
        print(response)


def main():
    create_encryption_policy(client)
    create_network_policy(client)
    create_access_policy(client)
    create_collection(client)
    wait_for_collection_creation(client)


if __name__ == "__main__":
    main()
