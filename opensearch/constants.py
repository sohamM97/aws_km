import os

import dotenv

dotenv.load_dotenv()


COLLECTION_NAME = "documents"
INDEX_NAME = "documents"
ENCRYPTION_POLICY_NAME = "documents-policy"
NETWORK_POLICY_NAME = "documents-policy"
ACCESS_POLICY_NAME = "documents-policy"

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
