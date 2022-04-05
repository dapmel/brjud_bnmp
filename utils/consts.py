"""Contains configuration constants used by the program.

Changes in this file will alter the behavior of the program.
"""

import utils.params as params

# Headers used for BNMP requests.
# The cookie must be updated daily on the params.py file.
headers = {
    "content-type": "application/json;charset=UTF-8",
    "cookie": params.COOKIE,
    "user-agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
                   "(KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36")
}

# - - - BNMP API - - -
root_url = "https://portalbnmp.cnj.jus.br/"
# - URLs -
# URL used on payload-based requests to the API
# ! Order: ASC / DESC
base_url = root_url + ("bnmpportal/api/pesquisa-pecas/filter?page={page}&"
                       "size={query_size}&sort=numeroPeca,{order}")

# URL used to retrieve the cities of a state
cities_url = root_url + "scaservice/api/municipios/por-uf/{state}"
# URL used to retrieve the legal agencies of a city
agencies_url = root_url + \
    "bnmpportal/api/pesquisa-pecas/orgaos/municipio/{city}"

# - Payloads -
# State queries and probing
state_payload = ('{{"buscaOrgaoRecursivo":false,"orgaoExpeditor":{{}},'
                 '"idEstado":{state}}}')
# City queries
city_payload = ('{{"buscaOrgaoRecursivo":false,"orgaoExpeditor":{{}},'
                '"idEstado":{state},"idMunicipio":{city}}}')
# Agency queries
agency_payload = ('{{"buscaOrgaoRecursivo":false,"orgaoExpeditor":{agency},'
                  '"idEstado":{state},"idMunicipio":{city}}}')
# Document type queries
doctype_payload = ('{{"buscaOrgaoRecursivo":false,"orgaoExpeditor":{agency},'
                   '"idEstado":{state},"idMunicipio":{city},'
                   '"idTipoDocumento":{doctype}}}')

# URL scraping
url_endpoint = root_url + "bnmpportal/api/certidaos/{id}/{type}"

# DB QUERIES
sql_bnmp_create_table = """
    CREATE TABLE IF NOT EXISTS bnmp (
        mandado_id integer PRIMARY KEY,
        mandado_type smallint NOT NULL,
        processo_number VARCHAR(100) NOT NULL,
        peca_number VARCHAR(100) NOT NULL,
        expedition_date DATE NOT NULL,
        scrap_date DATE NOT NULL,
        last_seen_date DATE NOT NULL,
        not_found_date DATE,
        jsonb jsonb
    );
"""
sql_insert_mandado = """
    INSERT INTO bnmp (
        mandado_id, mandado_type,
        processo_number, peca_number,
        expedition_date, scrap_date,
        last_seen_date
    ) VALUES(%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (mandado_id) DO UPDATE
    SET (last_seen_date) = ROW(EXCLUDED.last_seen_date);
"""
sql_update_json = """
    UPDATE bnmp
        SET jsonb = %s
        WHERE mandado_id = %s;
"""
sql_mandados_without_json = """
    SELECT mandado_id, mandado_type FROM bnmp WHERE jsonb IS NULL;
"""
