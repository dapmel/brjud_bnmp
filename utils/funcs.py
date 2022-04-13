"""Utility functions."""
from typing import List
import yaml

with open("utils/config.yml") as ymlfile:
    cfg = yaml.safe_load(ymlfile)


def define_payload(d: dict) -> str:
    """Define and populate a request payload format based on a maps' keys."""
    # Document type queries

    query_types: List[str] = ["doctype", "agency", "city", "state"]
    for query in query_types:
        if d.get(query):
            payload: str = cfg["payloads"][query]
            break

    # The string won't be formatted with values it doesn't support
    return payload.format(
        state=d.get("state"), city=d.get("city"), agency=d.get("agency"),
        doctype=d.get("doctype"))
