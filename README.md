[![codecov](https://codecov.io/gh/tohwiket/brjud_bnmp/branch/main/graph/badge.svg?token=ELY4OK6V9A)](https://codecov.io/gh/tohwiket/brjud_bnmp)
![example workflow](https://github.com/tohwiket/brjud_bnmp/actions/workflows/bnmp.yml/badge.svg)

# BRJUD BNMP

Scraps data from Banco Nacional de Mandados de Prisão 2.0 (BNMP2) API.

## Why?
The current version of BNMPs portal is broken as it cannot return more than 10k documents on any given search. The export functionality is unable to retrieve more than 10k documents as well. Beyond that, the frontend is oftenly unstable. This inhibits the extraction of the database.

## About
Automatically extracts all the data available in the BNMP2 API through multiple queries. First it maps the API and calculates what queries will be needed to extract all data available, does the bulk data extraction and then saves detailed data from each document.

## Usage
### Requirements
This program needs a functionining Postgresql database to work. The database configuration parameters must be included in the `db\database.yml` file.

### Install required packages
```
python3 -m pip install -r requirements.txt
```

### Update your cookies!
This program also depends on a updated cookie extracted from the BNMP API search headers. It must be placed in the `utils\config.yml` file.

### Test your copy
```
python3 -m pytest --cov=./ --cov-config .coveragerc
```
(This will create a testing db and table.)

## Sample usage
```
python3 main.py
```

## Note about server failures
The BNMP API may give error responses due to service instability. There is no workarounds for now, just re-run the program and it will continue from where it stoped.
