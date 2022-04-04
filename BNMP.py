"""Tools to "map" the queries needed to scrap all the BNMP API data.

The BNMP database is currently broken as it cannot return more than 10.000
entries on any given search. A way to circumvent this is to split large queries
(i.e. states) into smaller ones (i.e. cities, agencies).
This file contains the tools used to create the "map" of queries needed to try
to reach 100% of the BNMP data.
"""
from typing import Any, Generator, List, Set, Tuple, Union
import concurrent.futures
import dataclasses
import json
import logging
import psycopg2 as pg
import requests
import string

from utils.funcs import MapItem, requester
import utils.consts as consts
import utils.funcs as funcs
import utils.params as params

from db_utils.db_config import config
from db_utils.db_testing import db_testing

logging.basicConfig(
    format="%(asctime)s: %(message)s",
    # datefmt="%Y-%m-%d %H:%M:%S",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)


class Mapper:
    """The map creation tools.

    Calculates what queries are needed to extract data
    from a given state, returning a "map".
    """

    def __init__(self) -> None:
        """Initialize with logging."""
        logging.info("Initializing API Mapper")

    def probe(self, mapitem: MapItem) -> Union[None, MapItem]:
        """Encapsulate ``self.requester()`` method with a call and response."""
        try:
            probe: str = requester(mapitem)
        except requests.exceptions.ReadTimeout:
            logging.error("TIMEOUT")
            return None
        probe_size: int = int(probe["totalPages"]) if probe.get(  # type:ignore
            "totalPages") else 0

        # ! Set ``probe_size`` in the first ``None`` value found.
        # ! i.e. a ``MapItem`` that only contais a ``state_id``
        # ! will have the ``state_probe`` field filled
        # Iteration over keys
        for item in mapitem:
            # Retrieval of value
            value: Any = getattr(mapitem, item)
            if value is None:
                # Setting of probe_size
                setattr(mapitem, item, probe_size)
                break

        return mapitem

    def cities_retriever(self, state_id: int) -> Generator:
        """Return a list of cities ids in a state."""
        url: str = consts.cities_url.format(state_id=state_id)
        response: requests.models.Response = requests.get(
            url, headers=consts.headers)
        cities_json = json.loads(response.text)
        return (i["id"] for i in cities_json)

    def agencies_retriever(self, city_id: int) -> Generator:
        """Return a generator of the legal agencies ids of a city.

        Returns a string in the format ``{"id":number}`` because
        the API expects a "JSON object" as a orgaoExpeditor parameter.
        """
        url: str = consts.agencies_url.format(city_id=city_id)
        response: requests.models.Response = requests.get(
            url, headers=consts.headers)
        agencies_json = json.loads(response.text)
        return (f'{{"id":{agency["id"]}}}' for agency in agencies_json)

    def threads(self, mapitems: List[MapItem]) -> Generator[
        MapItem, None, None
    ]:
        """Generate and run a requests threadpool and return data."""
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=params.CONNECTIONS
        ) as executor:
            future_to_url = (
                executor.submit(self.probe, mapitem) for mapitem in mapitems
            )
            for future in concurrent.futures.as_completed(future_to_url):
                # Errors will be raised on the ``mapper`` method
                data = future.result()
                if data is not None:
                    yield data

    def valid_fields(self, mapitem: MapItem) -> list:
        """Return list of ``not None`` values of a ``MapItem``."""
        return [
            getattr(mapitem, item)
            for item in mapitem if getattr(mapitem, item) is not None]

    def mapper(self, state_id: int) -> List[MapItem]:
        """Generate requests and probes to populate ``MapItems`` to yield.

        This method is divided in 'stages'. Each stage deals with
        increasingly granular data and larger ammount of requests.
        """
        # Lists that will contain work to be done by each stage
        states_items: List[MapItem] = [MapItem(state_id)]
        cities_items: List[MapItem] = []
        agencies_items: List[MapItem] = []
        doctypes_items: List[MapItem] = []
        letters_items: List[MapItem] = []

        # ! "State" stage
        # The first stage is to map each state. If a state has more than 10.000
        # documents then it is eligible to the next maping stage.
        # 'For every ``mapitem`` returned by the threads that received the
        # list with ``mapitems`` generated with ``self.state_range`` as input'
        for mapitem in self.threads(states_items):
            # Skip states with no documents (does not happen for now)
            if mapitem.state_probe == 0:
                continue
            # States with more than 10.000 documents are added to the list
            # that will feed the city mapping state
            if mapitem.state_probe > 10_000:
                # Generates a new ``mapitem`` for each city in the state
                for city in self.cities_retriever(mapitem.state_id):
                    cities_items.append(
                        MapItem(*self.valid_fields(mapitem), city))

        # ! "City" stage
        for mapitem in self.threads(cities_items):
            if mapitem.city_probe == 0:
                continue
            if mapitem.city_probe > 10_000:
                # Generates a new ``mapitem`` for each agency in the city
                for agency in self.agencies_retriever(mapitem.city_id):
                    agencies_items.append(
                        MapItem(*self.valid_fields(mapitem), agency))

        # ! "Agency" stage
        for mapitem in self.threads(agencies_items):
            if mapitem.agency_probe == 0:
                continue
            if mapitem.agency_probe > 10_000:
                # Generates a new ``mapitem`` for each document type
                for doctype in range(1, 14):
                    doctypes_items.append(
                        MapItem(*self.valid_fields(mapitem), doctype))

        # ! "Doctype" stage
        # If an agency has more than 10.000 records then a "document type"
        # query is needed. A "doctype" query returns data divided by the type
        # of document a person has in the BNMP database.
        # There are 13 types of documents
        for mapitem in self.threads(doctypes_items):
            if mapitem.doctype_probe == 0:
                continue
            if mapitem.doctype_probe > 10_000:
                # Generates a new ``mapitem`` for each letter in the alphabet
                for letter in string.ascii_lowercase:
                    letters_items.append(
                        MapItem(*self.valid_fields(mapitem), letter))

        # ! "Letter" stage
        # If a document type has more than 10.000 records then the last resort
        # is to make a query using a letter in the "name" field. This seems to
        # return the records of people that has such letter in the first name.
        # A letter query may return more than 10.000 records but as all letter
        # queries are later combined and deduplicated, the total data
        # collection tends to be 100%.
        letters_mapitems = self.threads(letters_items)
        letters_items = []
        for mapitem in letters_mapitems:
            if dataclasses.is_dataclass(mapitem):
                letters_items.append(mapitem)

        return states_items + cities_items + \
            agencies_items + doctypes_items + letters_items


class BulkScraper:
    """Scraps the BNMP API."""

    def __init__(self, db_params: dict = None) -> None:
        """Add db params to the state, test them and set states ids range."""
        logging.info("Initializing API Scraper")
        if db_params is not None:
            self.db_params = db_params
        else:
            self.db_params = config()

        db_testing("bnmpapi", consts.sql_bnmp_create_table, self.db_params)

        self.states_range = range(1, 28)

    def threads(
        self, payloads: Set[Tuple[int, int, Any, Any, Any, Any]]
    ) -> Generator[List[dict], None, None]:
        """Generate and run a requests threadpool and return data."""
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=params.CONNECTIONS
        ) as executor:
            future_to_url = (executor.submit(requester, payload)
                             for payload in payloads)
            for future in concurrent.futures.as_completed(future_to_url):
                # Error handling is done on the ``scraper`` method
                yield future.result()

    def generate_map(
        self, mapper: Mapper, state_id: int
    ) -> Set[Tuple[int, int, Any, Any, Any, Any]]:
        """Generate API map."""
        payloads: Set[Tuple[int, int, Any, Any, Any, Any]] = set()
        for m in mapper.mapper(state_id):
            if getattr(m, "letter"):
                depth: int = getattr(m, "letter_probe")
            elif getattr(m, "doctype_id"):
                depth = getattr(m, "doctype_probe")
            elif getattr(m, "agency"):
                depth = getattr(m, "agency_probe")
            elif getattr(m, "city_id"):
                depth = getattr(m, "city_probe")
            else:
                depth = getattr(m, "state_probe")
            payloads.add((
                depth, getattr(m, 'state_id'),
                getattr(m, 'city_id'), getattr(m, 'agency'),
                getattr(m, 'doctype_id'), getattr(m, 'letter')
            ))

        return payloads

    def start(self) -> bool:
        """Trigger generation of map and save valid data to the database."""
        mapper = Mapper()
        with pg.connect(**self.db_params) as conn, conn.cursor() as curs:
            for state_id in self.states_range:
                logging.info(f"Mapping state {state_id}")
                state_map = self.generate_map(mapper, state_id)
                logging.info(f"Scraping state {state_id}")
                for obj in self.threads(state_map):
                    if not isinstance(obj, list):
                        continue
                    for row in obj:
                        if 'type' in row:
                            if "status" in row and row['status'] == 401:
                                raise Exception(
                                    "Auth error. Check your cookies.")
                            else:
                                raise Exception(
                                    f"Error. {row['detail']}. {row['status']}")

                        for process in row["content"]:
                            payload = (
                                process["id"],
                                process["idTipoPeca"],
                                process["numeroProcesso"],
                                process["numeroPeca"],
                                process["dataExpedicao"],
                                # One date for scrap_date, other for last_seen
                                funcs.date_now(),
                                funcs.date_now(),
                                state_id
                            )
                            curs.execute(consts.sql_insert_mandado, payload)
                            conn.commit()

        logging.warning("BNMP Bulk scraping done.")

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
                if details % 1000 == 0:
                    logging.debug(f"Detailed queries: {details}")

        logging.info("BNMP details scrapping done.")

        return True


if __name__ == "__main__":
    logging.info("Extracting bulk data.")
    bulk_scraper = BulkScraper()
    bulk_scraper.start()
    details_scraper = DetailsScraper()
    details_scraper.start()
