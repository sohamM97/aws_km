import asyncio
import html
import json
import os

import aioboto3
from dotenv import load_dotenv

load_dotenv()


def _textract_table_to_html(table_blocks: list, id_to_word_mapping: dict) -> str:
    # Organize cells by rows and columns
    cells = [block for block in table_blocks if block["BlockType"] == "CELL"]
    row_count = max(cell["RowIndex"] for cell in cells)

    table_html = "<table>"
    for row_index in range(1, row_count + 1):
        table_html += "<tr>"
        row_cells = sorted(
            [cell for cell in cells if cell["RowIndex"] == row_index],
            key=lambda cell: cell["ColumnIndex"],
        )
        for cell in row_cells:
            tag = (
                "th"
                if cell.get("EntityTypes") and "COLUMN_HEADER" in cell["EntityTypes"]
                else "td"
            )
            cell_spans = ""
            if cell.get("ColumnSpan", 1) > 1:
                cell_spans += f" colSpan={cell['ColumnSpan']}"
            if cell.get("RowSpan", 1) > 1:
                cell_spans += f" rowSpan={cell['RowSpan']}"

            cell_content = ""
            for rel in cell.get("Relationships", []):
                ids = rel["Ids"]
                words = [id_to_word_mapping[id_] for id_ in ids]
                cell_content = " ".join(words)

            table_html += f"<{tag}{cell_spans}>{html.escape(cell_content)}</{tag}>"
        table_html += "</tr>"
    table_html += "</table>"
    return table_html


def _id_to_word_mapping(response):
    mapping = {}
    for block in response["Blocks"]:
        if block["BlockType"] == "WORD":
            id_ = block["Id"]
            word = block["Text"]
            mapping[id_] = word

    return mapping


async def main():
    boto3_client = aioboto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    ).client("textract")

    async with boto3_client as client:
        response = await client.start_document_analysis(
            DocumentLocation={
                "S3Object": {
                    "Bucket": "soham-boto-s3-test",
                    "Name": "table.pdf",
                }
            },
            FeatureTypes=["TABLES"],
        )
        job_id = response["JobId"]

        print(f"Polling job id: {job_id}")
        response = await client.get_document_analysis(JobId=job_id)
        while response["JobStatus"] == "IN_PROGRESS":
            await asyncio.sleep(5)
            print(f"Polling job id: {job_id}")
            response = await client.get_document_analysis(JobId=job_id)

        with open("response.json", "w") as f:
            json.dump(response, f, indent=4)

    id_to_word_mapping = _id_to_word_mapping(response)

    table_blocks = [
        block for block in response["Blocks"] if block["BlockType"] in ["TABLE", "CELL"]
    ]

    html_table = _textract_table_to_html(table_blocks, id_to_word_mapping)

    with open("table.html", "w") as f:
        f.write(html_table)


if __name__ == "__main__":
    asyncio.run(main())
