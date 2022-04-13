"""Contains configuration parameters used by the program."""

# ! The BNMP cookie
# Must be updated daily.
# A fresh cookie can be found on the request headers of any search done on
# "https://portalbnmp.cnj.jus.br/#/pesquisa-peca".
# Extract it via your browser's developer tooklit.
# Example: https://stackoverflow.com/a/4423097
COOKIE = "portalbnmp=eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJndWVzdF9wb3J0YWxibm1wIiwiYXV0aCI6IlJPTEVfQU5PTllNT1VTIiwiZXhwIjoxNjQ5ODU1ODE2fQ.ytXKGRp__MVB205-8TsDEuN1BL7h_PpqpxbzTMh4sZ39kr2iRX_1iW7sdo5eIHViW5HQgBOvJZhRZFra_Eai8A"  # noqa: E501 # flake8: ignore line size

# Multithreading request connections.
# Change this value to alter the number of active threads a requester can keep.
CONNECTIONS = 24
