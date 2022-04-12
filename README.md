# BRJUD BNMP

Scraps data from Banco Nacional de Mandados de Prisão 2.0 (BNMP2) API.

## Why?
The current version of BNMPs portal is broken as it cannot return more than 10k documents on any given search. The export functionality is unable to retrieve more than 10k documents as well. Beyond that, the frontend is oftenly unstable. This inhibits the extraction of the database.

## About
Automatically extracts all the data available in the BNMP2 API through multiple queries. First it maps the API and calculates what queries will be needed to extract all data available, does the bulk data extraction and then saves detailed data from each document.

This program needs a functionining Postgresql database to work. The database configuration parameters must be included in the `db\database.ini` file.

## Usage
```
python3 -m pip install -r requirements.txt
python3 BNMP.py
```

### Note about usage and testing
This program also depends on a updated cookie extracted from the BNMP API search headers. It must be placed in the `utils\params.py` file.

### Note about server failures
The BNMP API may give error responses due to service instability. There is no workaround for now, just re-run the program and it will continue from where it stoped.
