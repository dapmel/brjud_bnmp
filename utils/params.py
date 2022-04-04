"""Contains configuration parameters used by the program.

This data may be altered at free will.
"""

# ! The BNMP cookie
# Must be updated daily.
# A fresh cookie can be found on the request headers of any search done on
# "https://portalbnmp.cnj.jus.br/#/pesquisa-peca".
# Extract it via your browser's developer tooklit.
# Example: https://stackoverflow.com/a/4423097
COOKIE = "portalbnmp=eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJndWVzdF9wb3J0YWxibm1wIiwiYXV0aCI6IlJPTEVfQU5PTllNT1VTIiwiZXhwIjoxNjQ5MTY5MzAyfQ.UuekyDR7u6HNtpt4ewN3VnArra7qCxGaA7_6KREE2f--PY3AaEzGMLieygwxbPahLll9k6RloT5M6kW-e49fkA"  # noqa: E501 # flake8: ignore line size

# Multithreading request connections.
# Change this value to alter the number of active threads a requester can keep.
CONNECTIONS = 24
