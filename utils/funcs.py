"""Functions used by BNMP scraping classes."""
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import List, Optional, Tuple, Union
import json
import requests

import utils.consts as consts


@dataclass
class MapItem:
    """Custom Dataclass that contains queries probing data."""

    state_id: int
    state_probe: Union[int, None] = None
    city_id: Union[int, None] = None
    city_probe: Union[int, None] = None
    agency: Union[int, None] = None
    agency_probe: Union[int, None] = None
    doctype_id: Union[int, None] = None
    doctype_probe: Union[int, None] = None
    letter: Union[str, None] = None
    letter_probe: Union[int, None] = None

    def __iter__(self):
        """Make ``dataclass`` iterable as a dictionary."""
        return iter(asdict(self))


def calc_range(depth: int) -> range:
    """Return the range of pages needed to reach a given depth."""
    max_range: int = int(depth) // consts.query_size + 1
    if max_range > 4:
        max_range = 4
    return range(max_range)


def requester(payload: Union[MapItem, Tuple]) -> Union[str, List[str]]:
    """Do requests for both ``Mapper`` and ``APIScraper`` classes."""
    # Mapper
    if isinstance(payload, MapItem):
        mode: str = "probe"
        state_id: int = payload.state_id
        city_id: Optional[int] = payload.city_id
        agency: Optional[int] = payload.agency
        doctype_id: Optional[int] = payload.doctype_id
        letter: Optional[str] = payload.letter
        page: int = 0
        query_size: int = 1
    # APIScraper
    elif isinstance(payload, tuple):
        mode = "bulk"
        (
            depth, state_id, city_id,
            agency, doctype_id, letter,
        ) = payload[::1]
        query_size = consts.query_size
    else:
        raise ValueError("Invalid payload type.")

    # Requests based on state id.
    payload_string: str = consts.state_payload
    # Requests based on city id
    if city_id:
        payload_string = consts.city_payload
    # Requests based on agency id
    if agency:
        payload_string = consts.agency_payload
    # Requests based on document type
    if doctype_id:
        payload_string = consts.doctype_payload
    # Requests based on letters
    if letter:
        payload_string = consts.letter_payload

    # The string won't be formatted with values it doesn't support
    data: str = payload_string.format(
        state_id=state_id, city_id=city_id,
        agency=agency, doctype_id=doctype_id,
        letter=letter,
    )

    if mode == "probe":
        url: str = consts.base_url.format(
            page=page, query_size=query_size)
        response: requests.models.Response = requests.post(
            url, headers=consts.headers, data=data, timeout=30)
        return json.loads(response.text)
    elif mode == "bulk":
        responses: List[str] = []
        for page in calc_range(depth):
            url = consts.base_url.format(
                page=page, query_size=query_size)
            response = requests.post(
                url, headers=consts.headers, data=data, timeout=30)
            responses.append(json.loads(response.text))
        return responses
    else:
        raise ValueError("Invalid payload type.")


def date_now():
    """Return current date."""
    return datetime.now().date()
