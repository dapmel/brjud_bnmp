"""Database configuration extraction.

The configuration file must be a sibling to this file.
"""
import os
import yaml


def config(filename="database.yml"):
    """Extract data from DB config file and return it if it is valid."""
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    file_with_path = f"{BASE_DIR}/{filename}"

    with open(file_with_path) as ymlfile:
        cfg = yaml.safe_load(ymlfile)

    params: dict = cfg["db_params"]
    for key in ["host", "database", "user", "password"]:
        if key not in params:
            raise Exception(f"Section '{key}' not found in '{filename}'")
    return params
