"""BNMP2 API scraping suite."""

from datetime import datetime
from typing import Generator, List, Set, Union
from concurrent.futures import as_completed, ThreadPoolExecutor
import copy
import json
import logging
import psycopg2 as pg
import requests
from db.db_config import config
from db.db_testing import DBTester
from utils.funcs import define_payload

import yaml

with open("utils/config.yml") as ymlfile:
    cfg = yaml.safe_load(ymlfile)

logging.basicConfig(
    format=cfg["log"]["format"], datefmt="%H:%M:%S", level=logging.INFO)


class Mapper:
    """Generate API maps.

    Maps are a solution to reduce the time to extract data from the API. They
    are composed with query parameters such as 'state' and 'city' and the
    ammount of documents each one contains in the API.
    """

    def __init__(self):
        """Initialize with logging."""
        logging.info("Initializing")

    def requester(self, d: dict) -> dict:
        """Make a probing request to the API and return a JSON dict."""
        data: str = define_payload(d)
        url: str = cfg["url"]["base"].format(page=0, query_size=1, order="ASC")
        response: requests.models.Response = requests.post(
            url, headers=cfg["requests"]["headers"], data=data,
            timeout=cfg["requests"]["timeout"])
        return json.loads(response.text)

    def probe(self, d):
        """Define how many documents are available under a specific query."""
        probe: str = self.requester(d)
        # Error response
        if probe.get("type"):
            raise Exception(probe)  # pragma: no cover

        probe_size: int = int(probe["totalPages"]) if probe.get(
            "totalPages") else 0

        # Fill the first probe with zero value found
        for key, value in d.items():
            if "probe" in key and value == 0:
                d[key] = probe_size
                break
        return d

    def cities_retriever(self, state: int) -> Generator[int, None, None]:
        """Return a generator of cities ids in a state."""
        url: str = cfg["url"]["cities"].format(state=state)
        response: requests.models.Response = requests.get(
            url, headers=cfg["requests"]["headers"])
        cities_json = json.loads(response.text)
        return (i["id"] for i in cities_json)

    def agencies_retriever(self, city: int) -> Generator[str, None, None]:
        """Return a generator of the legal agencies ids of a city.

        Returns strings formatted as ``{"id":number}`` because the API expects
        a "JSON object" as a ``orgaoExpeditor`` parameter.
        """
        url: str = cfg["url"]["agencies"].format(city=city)
        response: requests.models.Response = requests.get(
            url, headers=cfg["requests"]["headers"])
        agencies_json: dict = json.loads(response.text)
        return (f'{{"id":{agency["id"]}}}' for agency in agencies_json)

    def threads(self, maps) -> Generator:
        """Generate and run a requests threadpool and yield data."""
        with ThreadPoolExecutor(
                max_workers=cfg["threads"]["max_workers"]) as executor:
            futures = (executor.submit(self.probe, d) for d in maps)
            for future in as_completed(futures):
                data = future.result()
                if data is not None:
                    yield data

    def gen_map(self, state_range: range) -> Generator[
            Union[dict, None], None, None]:
        """Yield API maps."""
        logging.info("API mapping initiated")

        def validate_probe(d, probe_name: str) -> Union[dict, None]:
            # Discard empty parameters
            if d[probe_name] == 0:
                return None
            # If probe < 10_000, a single query is done to extract data
            # If 10_000 < probe < 20_000, a descending query is added to the
            # default one
            if d[probe_name] <= 20_000:
                d['include_desc'] = False if d[probe_name] <= 10_000 else True
                return d
            # If probe > 20_000, a more detailed query is needed
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

        logging.info("API mapping complete")


class BulkScraper:
    """Scraps the BNMP API.

    Will populate the database on the first run. On subsequent runs will add
    new entries or update the ones that already exist.
    """

    def __init__(self, db_params: dict = None):
        """Add db params to the state, test them and set states ids range."""
        logging.info("Initializing")
        self.db_params = db_params if db_params is not None else config()
        # self.states = range(1, 28)
        self.states = [26]
        DBTester("bnmp", cfg["sql"]["create"], self.db_params)

    def requester(self, d) -> Generator:
        """Request API data."""
        def calc_range(depth: int) -> range:
            """Return the page range needed to reach a given depth.

            The maximum value is 5 as the API can only return 10k results in a
            given query.
            """
            max_range: int = (int(depth) // 2_000) + 1
            if max_range > 5:
                max_range = 5
            return range(max_range)

        data: str = define_payload(d)
        # Value of the last probe. Dictionary order is assured by Python > 3.6
        depth = [*d.values()][-2]
        include_descending = [*d.values()][-1]
        for page in calc_range(depth):
            url = cfg["url"]["base"].format(
                page=page, query_size=2_000, order="ASC")
            response = requests.post(
                url, headers=cfg["requests"]["headers"], data=data, timeout=30)
            yield json.loads(response.text)

            if include_descending:
                url = cfg["url"]["base"].format(
                    page=page, query_size=2_000, order="DESC")
                response = requests.post(
                    url, headers=cfg["requests"]["headers"], data=data,
                    timeout=30)
                yield json.loads(response.text)

    def threads(self, maps: Generator) -> Generator:
        """Generate and run a requests threadpool and yield data."""
        with ThreadPoolExecutor(
                max_workers=cfg["threads"]["max_workers"]) as executor:
            futures = (executor.submit(self.requester, d)
                       for d in maps if d is not None)
            for future in as_completed(futures):
                # Error handling is done on the ``scraper`` method
                yield from future.result()

    def start(self) -> bool:
        """Extract bulk data available in BNMP API."""
        logging.info("Bulk data extraction initiated")
        mapper = Mapper()
        with pg.connect(**self.db_params) as conn, conn.cursor() as curs:
            for obj in self.threads(mapper.gen_map(self.states)):
                if obj.get('type'):
                    raise Exception(f"Error: {obj}")  # pragma: no cover
                for process in obj["content"]:
                    # dates are for "scrap_date" and "last_seen" fields
                    curs.execute(cfg["sql"]["insert"], (
                        process["id"], process["idTipoPeca"],
                        process["numeroProcesso"], process["numeroPeca"],
                        process["dataExpedicao"], datetime.now().date(),
                        datetime.now().date()
                    ))
                    conn.commit()
        logging.info("Bulk data extraction complete")
        return True


class DetailsScraper:
    """Scraps detailed data from the BNMP API."""

    def __init__(self, db_params: dict = None) -> None:
        """Set DB params and urls pending detailed scraping."""
        logging.info("Initiating")
        self.db_params = db_params if db_params is not None else config()
        with pg.connect(**self.db_params) as conn, conn.cursor() as curs:
            curs.execute(cfg["sql"]["select_incomplete"])
            self.pending_urls: Set[str] = {cfg["url"]["details"].format(
                id=i[0], type=i[1]) for i in curs.fetchall()}

    def load_url(self, url: str) -> str:
        """Return the response text of a request."""
        return requests.get(url, headers=cfg["requests"]["headers"],
                            timeout=cfg["requests"]["timeout"]).text

    def threads(self, urls: Set[str]) -> Generator:
        """Generate and run a requests threadpool and yield valid data."""
        with ThreadPoolExecutor(cfg["threads"]["max_workers"]) as executor:
            futures = (executor.submit(self.load_url, url) for url in urls)
            for future in as_completed(futures):
                data = future.result()
                if "mandado" in data:
                    yield data

    def start(self) -> bool:
        """Add detailed data from the BNMP API as JSON to the DB."""
        logging.info("Detailed data extraction initiated")
        with pg.connect(**self.db_params) as conn, conn.cursor() as curs:
            for res in self.threads(self.pending_urls):
                json_res = json.loads(res)
                curs.execute(cfg["sql"]["update_json"],
                             (json.dumps(json_res), json_res['id']))
                conn.commit()

        logging.info("Detailed data extraction complete")
        return True
