import json

import boto3

brt = boto3.client(service_name="bedrock-runtime")

body = json.dumps(
    {
        "prompt": "\n\nHuman: explain black holes to 8th graders\n\nAssistant:",
        # "max_tokens_to_sample": 300,
        "temperature": 0.1,
        "top_p": 0.9,
    }
)

modelId = "meta.llama3-8b-instruct-v1:0"
accept = "application/json"
contentType = "application/json"

response = brt.invoke_model(
    body=body, modelId=modelId, accept=accept, contentType=contentType
)

response_body = json.loads(response.get("body").read())
# text
print(response_body.get("generation"))
