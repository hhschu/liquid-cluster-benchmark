import os
from datetime import datetime
from pathlib import Path

import requests

host = "https://dbc-252cce92-d2bd.cloud.databricks.com"
path = "/api/2.0/sql/history/queries"
headers = {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}
params = {
    "filter_by": {
        "statuses": ["FINISHED"],
        "user_ids": ["1152846111474968"],  # Cosmos
        "query_start_time_range": {
            "start_time_ms": int(datetime(2023, 11, 20).timestamp() * 1000),
            "end_time_ms": int(datetime(2023, 11, 27).timestamp() * 1000),
        },
    },
    "max_results": 1000,
}


def is_valid(query):
    query = query.lower()
    return query.startswith("select") and "telemetry_v1" in query


queries = []
out_file = Path("static/queries.txt")
has_next_page = True
while has_next_page:
    resp = requests.get(host + path, headers=headers, json=params).json()
    for query in resp["res"]:
        query_text = query["query_text"]
        if is_valid(query_text):
            queries.append(query_text)
    has_next_page = resp.get("has_next_page", False)
    params["page_token"] = resp.get("next_page_token")

queries = set(queries)
print("Number of queries:", len(queries))
out_file.write_text("\n".join(queries))
