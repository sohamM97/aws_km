import os

from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = "soham-boto-s3-test"

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
