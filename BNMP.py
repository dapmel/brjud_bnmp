"""BNMP2."""
from datetime import datetime
from typing import Generator, List, Set, Union
import concurrent.futures
import copy
import json
import logging
import psycopg2 as pg
import requests

import utils.consts as consts
import utils.params as params

from db.db_config import config
from db.db_testing import db_testing

logging.basicConfig(
    format="%(asctime)s: %(message)s", datefmt="%H:%M:%S", level=logging.INFO
)


def define_payload(d: dict) -> str:
    """Define a request payload format based on a dictionary key."""
    # Requests based on document type
    if d.get("doctype"):
        return consts.doctype_payload
    # Requests based on agency id
    elif d.get("agency"):
        return consts.agency_payload
    # Requests based on city id
    elif d.get("city"):
        return consts.city_payload
    # Requests based on state id
    else:
        return consts.state_payload


class Mapper:
    """Generate API map."""

    def __init__(self):
        """Initialize with logging."""
        logging.info("Initializing mapper.")

    def requester(self, d: dict) -> dict:
        """Make a probing request to the API."""
        payload = define_payload(d)
        # The string won't be formatted with values it doesn't support
        data: str = payload.format(
            state=d.get("state"), city=d.get("city"), agency=d.get("agency"),
            doctype=d.get("doctype"))
        url: str = consts.base_url.format(page=0, query_size=1, order="ASC")
        response: requests.models.Response = requests.post(
            url, headers=consts.headers, data=data, timeout=30)
        return json.loads(response.text)

    def probe(self, d):
        """Define how many documents are available under a specific query."""
        probe: str = self.requester(d)
        # Error response
        if probe.get("type"):
            raise Exception(probe)

        probe_size: int = int(probe["totalPages"]) if probe.get(
            "totalPages") else 0

        # Fill the first zero probe found
        for key, value in d.items():
            if "probe" in key and value == 0:
                d[key] = probe_size
                break
        return d

    def cities_retriever(self, state: int) -> Generator[int, None, None]:
        """Return a list of cities ids in a state."""
        url: str = consts.cities_url.format(state=state)
        response: requests.models.Response = requests.get(
            url, headers=consts.headers)
        cities_json = json.loads(response.text)
        return (i["id"] for i in cities_json)

    def agencies_retriever(self, city: int) -> Generator[str, None, None]:
        """Return a generator of the legal agencies ids of a city.

        Returns a string in the format ``{"id":number}`` because
        the API expects a "JSON object" as a orgaoExpeditor parameter.
        """
        url: str = consts.agencies_url.format(city=city)
        response: requests.models.Response = requests.get(
            url, headers=consts.headers)
        agencies_json = json.loads(response.text)
        return (f'{{"id":{agency["id"]}}}' for agency in agencies_json)

    def threads(self, maps) -> Generator:
        """Generate and run a requests threadpool and return data."""
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=params.CONNECTIONS) as executor:
            future_to_url = (executor.submit(self.probe, d) for d in maps)
            for future in concurrent.futures.as_completed(future_to_url):
                # Errors will be raised on the ``mapper`` method
                data = future.result()
                if data is not None:
                    yield data

    def gen_map(self, state_range: range) -> Generator[
        Union[dict, None], None, None
    ]:
        """Create API map."""
        def validate_probe(d, probe_name: str) -> Union[dict, None]:
            if d[probe_name] == 0:
                return None
            if d[probe_name] <= 20_000:
                d['include_desc'] = False if d[probe_name] <= 10_000 else True
                return d
            else:
                raise ValueError

        state_maps: List[dict] = [{"state": state, "state_probe": 0}
                                  for state in state_range]
        city_maps: List[dict] = []
        agency_maps: List[dict] = []
        doctype_maps: List[dict] = []

        for d in self.threads(state_maps):
            try:
                yield validate_probe(d, "state_probe")
            except ValueError:
                for city in self.cities_retriever(d['state']):
                    d = copy.deepcopy(d)
                    d['city'] = city
                    d['city_probe'] = 0
                    city_maps.append(d)

        for d in self.threads(city_maps):
            try:
                yield validate_probe(d, "city_probe")
            except ValueError:
                for agency in self.agencies_retriever(d['city']):
                    d = copy.deepcopy(d)
                    d['agency'] = agency
                    d['agency_probe'] = 0
                    agency_maps.append(d)

        for d in self.threads(agency_maps):
            try:
                yield validate_probe(d, "agency_probe")
            except ValueError:
                for doctype in range(1, 14):
                    d = copy.deepcopy(d)
                    d['doctype'] = doctype
                    d['doctype_probe'] = 0
                    doctype_maps.append(d)

        for d in self.threads(doctype_maps):
            yield validate_probe(d, "doctype_probe")


class BulkScraper:
    """Scraps the BNMP API."""

    def __init__(self, db_params: dict = None) -> None:
        """Add db params to the state, test them and set states ids range."""
        logging.info("Initializing Scraper")
        if db_params is not None:
            self.db_params = db_params
        else:
            self.db_params = config()

        db_testing("bnmp", consts.sql_bnmp_create_table, self.db_params)

    def requester(self, d) -> Generator:
        """Do requests for both ``Mapper`` and ``APIScraper`` classes."""
        def calc_range(depth: int) -> range:
            """Return the range of pages needed to reach a given depth."""
            max_range: int = (int(depth) // 2_000) + 1
            if max_range > 4:
                max_range = 4
            return range(max_range)

        payload = define_payload(d)
        # The string won't be formatted with values it doesn't support
        data: str = payload.format(
            state=d.get("state"), city=d.get("city"), agency=d.get("agency"),
            doctype=d.get("doctype"))

        # Value of the last probe. Dictionary order is assured by Python > 3.6
        depth = [*d.values()][-2]
        include_descending = [*d.values()][-1]
        for page in calc_range(depth):
            url = consts.base_url.format(
                page=page, query_size=2_000, order="ASC")
            response = requests.post(
                url, headers=consts.headers, data=data, timeout=30)
            yield json.loads(response.text)

            if include_descending:
                url = consts.base_url.format(
                    page=page, query_size=2_000, order="DESC")
                response = requests.post(
                    url, headers=consts.headers, data=data, timeout=30)
                yield json.loads(response.text)

    def threads(self, payloads):
        """Generate and run a requests threadpool and return data."""
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=params.CONNECTIONS
        ) as executor:
            future_to_url = (executor.submit(self.requester, payload)
                             for payload in payloads if payload is not None)
            for future in concurrent.futures.as_completed(future_to_url):
                # Error handling is done on the ``scraper`` method
                yield from future.result()

    def start(self) -> bool:
        """Start."""
        mapper = Mapper()
        with pg.connect(**self.db_params) as conn, conn.cursor() as curs:
            for obj in self.threads(mapper.gen_map(range(1, 28))):
                logging.info("New query")
                if obj.get('type'):
                    raise Exception(f"Error: {obj}")
                for process in obj["content"]:
                    # dates are for "scrap_date" and "last_seen" fields
                    curs.execute(consts.sql_insert_mandado, (
                        process["id"], process["idTipoPeca"],
                        process["numeroProcesso"], process["numeroPeca"],
                        process["dataExpedicao"], datetime.now().date(),
                        datetime.now().date()
                    ))
                    conn.commit()

        return True


class DetailsScraper:
    """Scraps detailed data from the BNMP API."""

    def __init__(self, db_params: dict = None) -> None:
        """Set db params and urls pending detailed scraping."""
        logging.info("Initiating Details Scraper")

        if db_params is not None:
            self.db_params = db_params
        else:
            self.db_params = config()

        with pg.connect(**self.db_params) as conn, conn.cursor() as curs:
            curs.execute(consts.sql_mandados_without_json)
            tuples = curs.fetchall()
            self.pending_urls: Set[str] = {consts.url_endpoint.format(
                id=i[0], type=i[1]) for i in tuples}

    def load_url(self, url):
        """Make a request and return the response text."""
        response: requests.models.Response = requests.get(
            url, headers=consts.headers, timeout=30)
        return response.text

    def threads(self, urls):
        """Generate and run a requests threadpool and return valid data."""
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=params.CONNECTIONS
        ) as executor:
            future_to_url = (executor.submit(self.load_url, url)
                             for url in urls)
            for future in concurrent.futures.as_completed(future_to_url):
                # Errors are ignored for now
                data = future.result()
                if "mandado" in data:
                    yield data

    def start(self) -> bool:
        """Start."""
        logging.info("Scraping details.")
        details = 0
        with pg.connect(**self.db_params) as conn, conn.cursor() as curs:
            for res in self.threads(self.pending_urls):
                json_res = json.loads(res)
                id = json_res['id']
                curs.execute(consts.sql_update_json,
                             (json.dumps(json_res), id))
                conn.commit()
                details += 1
                if details % 100 == 0:
                    logging.debug(f"Detailed queries: {details}")

        logging.info("BNMP details scrapping done.")

        return True


if __name__ == "__main__":
    bulk = BulkScraper()
    bulk.start()
    details = DetailsScraper()
    details.start()
