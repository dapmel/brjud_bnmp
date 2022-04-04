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
base_url = root_url + ("bnmpportal/api/pesquisa-pecas/filter?page={page}&"
                       "size={query_size}&sort=asc")
# URL used to retrieve the cities of a state
cities_url = root_url + "scaservice/api/municipios/por-uf/{state_id}"
# URL used to retrieve the legal agencies of a city
agencies_url = root_url + \
    "bnmpportal/api/pesquisa-pecas/orgaos/municipio/{city_id}"

# - Payloads -
# State queries and probing
state_payload = ('{{"buscaOrgaoRecursivo":false,"orgaoExpeditor":{{}},'
                 '"idEstado":{state_id}}}')
# City queries
city_payload = ('{{"buscaOrgaoRecursivo":false,"orgaoExpeditor":{{}},'
                '"idEstado":{state_id},"idMunicipio":{city_id}}}')
# Agency queries
agency_payload = ('{{"buscaOrgaoRecursivo":false,"orgaoExpeditor":{agency},'
                  '"idEstado":{state_id},"idMunicipio":{city_id}}}')
# Document type queries
doctype_payload = ('{{"buscaOrgaoRecursivo":false,"orgaoExpeditor":{agency},'
                   '"idEstado":{state_id},"idMunicipio":{city_id}, '
                   '"idTipoDocumento":{doctype_id}}}')
# Person name queries
letter_payload = ('{{"buscaOrgaoRecursivo":false,"orgaoExpeditor":{agency},'
                  '"idEstado":{state_id},"idMunicipio":{city_id}, '
                  '"idTipoDocumento":{doctype_id}, "nomePessoa": "{letter}"}}')

# - Queries -
# 2000 is the maximum query size supported. This must not be changed.
query_size = 2_000

# URL scraping
url_endpoint = root_url + "bnmpportal/api/certidaos/{id}/{type}"

# DB QUERIES
sql_bnmp_create_table = """
    CREATE TABLE IF NOT EXISTS bnmp_data (
        mandado_id integer PRIMARY KEY,
        mandado_type smallint NOT NULL,
        processo_number VARCHAR(100) NOT NULL,
        peca_number VARCHAR(100) NOT NULL,
        expedition_date DATE NOT NULL,
        scrap_date DATE NOT NULL,
        last_seen_date DATE NOT NULL,
        not_found_date DATE,
        state_id smallint NOT NULL,
        jsonb jsonb
    );
"""
sql_insert_mandado = """
    INSERT INTO bnmp_data (
        mandado_id, mandado_type,
        processo_number, peca_number,
        expedition_date, scrap_date,
        last_seen_date, state_id
    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (mandado_id) DO UPDATE
    SET (last_seen_date) = ROW(EXCLUDED.last_seen_date);
"""
sql_update_json = """
    UPDATE bnmp_data
        SET jsonb = %s
        WHERE mandado_id = %s;
"""
sql_mandados_without_json = """
    SELECT mandado_id, mandado_type FROM bnmp_data
        WHERE jsonb IS NULL;
"""
