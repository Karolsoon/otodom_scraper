from datetime import datetime as dt
from typing import Any, Callable
from time import sleep
from random import randint

from bs4 import BeautifulSoup
import requests
from rich.progress import track

import config
from src.utils.log_util import get_logger
from src.scraper import parser


log = get_logger(__name__, 30, True, True)
log.setLevel('INFO')


class Page_Processor:
    OFFER_LINK_PREFIX = config.OFFER_LINK_STARTSWITH

    def __init__(
            self,
            run_time: str = dt.now().isoformat(),
            offer_link_startswith: str = config.OFFER_LINK_STARTSWITH):
        self.run_time = run_time
        self.offer_link_startswith = offer_link_startswith

    def make_soup(self, raw_html: str) -> BeautifulSoup:
        """
        Create soup object from raw html
        """
        return BeautifulSoup(raw_html, 'html.parser')

    def get_links(self, raw_listing_html: str) -> list[str]:
        """
        Extracts links to offer details from raw listing html string.

        Returns a list of links.
        """
        soup = self.make_soup(raw_listing_html)
        links = self.__get_links_from(soup)
        return [
            x.split('?')[0]
            for x
            in links
            if x.startswith(self.OFFER_LINK_PREFIX)
        ]

    def get_item_from(self, soup: BeautifulSoup|dict, hierarchy: dict[str, list[str|dict[str, list|Callable]]]) -> dict[str, str]|BeautifulSoup:
        transformations = {
            'soup': self.__get_item_from_soup,
            'json': self.__get_item_from_json
        }
        output = soup
        
        for stage in hierarchy['stages']:
            output = transformations.get(stage['input'])(output, stage['path'])
            if stage.get('transformation'):
                output = self.__transform(output, stage['transformation'])

        return output

    def get_pagination(self, raw_html: str) -> dict[str, int]:
        soup = self.make_soup(raw_html=raw_html)
        return self.get_item_from(soup, config.HIERARCHIES['pagination'])

    def prepare_data_for_insert(self, result: dict[str, dict[str, str|int|None]]) -> dict[str, str|int|None]:
        """
        Prepare data for insert into the database.
        The result input is the output after parsing a single detail page.
        """
        return {
            "status": 1,
            "city": result.get('city', {}).get('name', None),
            "postal_code": result.get('city', {}).get('id', None),
            "street": parser.parse_street(result.get('street', None)),
            "price": result.get('characteristics', {}).get('Cena', {}).get('value', None),
            "area": result.get('characteristics', {}).get('Powierzchnia', {}).get('value', None),
            "price_per_m2": result.get('characteristics', {}).get('cena za metr', {}).get('value', None),
            "floors": parser.parse_floor(result.get('characteristics', {}).get('Liczba pięter', {}).get('value', None)),
            "floor": parser.parse_floor(result.get('characteristics', {}).get('Piętro', {}).get('localizedValue', None)),
            "rooms": result.get('characteristics', {}).get('Liczba pokoi', {}).get('value', None),
            "build_year": result.get('characteristics', {}).get('Rok budowy', {}).get('value', None) or result.get('build_year'),
            "building_type": result.get('characteristics', {}).get('Rodzaj zabudowy', {}).get('value', None),
            "building_material": result.get('characteristics', {}).get('Materiał budynku', {}).get('value', None),
            "rent": result.get('characteristics', {}).get('Czynsz', {}).get('value', None),
            "windows": result.get('characteristics', {}).get('Okna', {}).get('value', None),
            "land_area": result.get('characteristics', {}).get('Powierzchnia działki', {}).get('value', None),
            "construction_status": result.get('characteristics', {}).get('Stan wykończenia', {}).get('value', None),
            "market": result.get('characteristics', {}).get('Rynek', {}).get('value', None),
            "posted_by": result.get('posted_by'),
            "description": result.get('description'),
            "ground_plan": result.get('characteristics', {}).get('Rzut mieszkania', {}).get('value', None),
            "coordinates_lat_lon": ','.join([str(x) for x in result.get('coordinates', {}).values()]) or None,
            "informacje_dodatkowe_json": str(result.get('other', {}).get('Informacje dodatkowe', [])).replace("'", '"'),
            "media_json": str(result.get('other', {}).get('Media', [])).replace("'", '"'),
            "ogrodzenie_json": str(result.get('other', {}).get("Ogrodzenie", [])).replace("'", '"'),
            "dojazd_json": str(result.get('other', {}).get("Dojazd", [])).replace("'", '"'),
            "ogrzewanie_json": str(result.get('other', {}).get("Ogrzewanie", [])).replace("'", '"'),
            "okolica_json": str(result.get('other', {}).get("Okolica", [])).replace("'", '"'),
            "zabezpieczenia_json": str(result.get('other', {}).get("Zabezpieczenia", [])).replace("'", '"'),
            "wyposazenie_json": str(result.get('other', {}).get("Wyposażenie", [])).replace("'", '"'),
            "contact": str(result.get('contact')).replace("'", '"'),
            "owner": str(result.get('owner')).replace("'", '"'),
        }

    def __transform(self, soup: BeautifulSoup, transformation: Callable) -> Any:
        return transformation(soup.text)

    def __get_item_from_soup(self, soup: BeautifulSoup, paths: list[dict[str, dict[str, str]]]) -> BeautifulSoup:
            for path in paths:
                tag, attributes = list(path.items())[0]
                soup = soup.find(tag, attributes)
                if not soup:
                    raise ValueError(f"Tag {tag} with attributes {attributes} not found in the soup.")
            return soup

    def __get_item_from_json(self, item: dict, paths: list[dict[str, dict[str, str]]]) -> dict[str, Any]:
        for path in paths:
            tag, attributes = list(path.items())[0]
            item = item.get(tag, None)
        return item

    def __get_links_from(self, soup: BeautifulSoup) -> list[str]:
        """
        Extract links from a sub-tag which contains the list of offers
        """
        item = self.get_item_from(soup, config.HIERARCHIES['offer_links'])
        a_tags = item.find_all('a', href=True)
        return [a['href'] for a in a_tags]


class Link_Extractor:
    START_URLS = config.START_URLS
    HEADERS = config.HEADERS
    DOMAIN_NAME = config.DOMAIN_NAME

    def __init__(
            self,
            listing: str='houses',
            run_time: dt=dt.now().isoformat()) -> None:
        self.run_time = run_time
        self.listing = listing
        self.detail_urls: list[str] = []
        self.pages_listing: list[requests.Response] = []
        self.pages_detail: dict[str, requests.Response] = {} # {url: requests.Response}
        self.pagination: dict[str, int] = {}

        self.session = requests.Session()

        self.set_listing_type(listing)

    def set_pagination(self, pagination: dict[str, int]) -> None:
        for key in config.HIERARCHIES['pagination']['attributes']:
            if key not in pagination:
                raise KeyError(f'Missing key in pagination dict: "{key}"')
        self.pagination = pagination

    def set_first_listing_page(self):
        log.info('Fetching listing pages')
        url = self.__build_url_for_listing(
            page=1,
            base_listing_url=self.__get_base_url()
        )
        self.pages_listing.append(self.__fetch_page(url))

    def set_remaining_listing_pages(self):
        for page_no in track(self.__pagination_iterator(),
                             description='Finding offers...',
                             total=self.pagination['totalPages'] - 1,
                             show_speed=False):
            url = self.__build_url_for_listing(
                page=page_no,
                base_listing_url=self.__get_base_url()
            )
            self.__sleep_randomly()
            self.pages_listing.append(self.__fetch_page(url))

    def set_detail_pages(self) -> None:
        for link in track(self.detail_urls,
                          description='Downloading offers...',
                          total=len(self.detail_urls),
                          show_speed=False):
            sleep(0.25)
            self.pages_detail[link] = self.__fetch_page(link)

    def set_detail_urls(self, paths: list[str]) -> None:
        self.detail_urls = list({
            self.__build_url_for_detail(link)
            for link
            in paths
        })
        log.info(f'{len(self.detail_urls)} URLs')

    def set_listing_type(self, listing_for: str) -> None:
        if listing_for not in self.START_URLS:
            raise ValueError(
                f'Trying to set unsupported listing type: {listing_for}'
            )
        self.listing = listing_for

    def __sleep_randomly(self) -> None:
        sleep_time = round(300 / randint(100, 180), 4)
        log.debug(f'Sleep for {sleep_time}s')
        sleep(sleep_time)

    def __fetch_page(self, url: str) -> requests.Response:
        """
        Sends a HTTP request to a url with predefined headers.

        Returns a tuple of status_code, html_text
        """
        log.debug(f'{url}')
        response = self.session.get(url, headers=self.HEADERS)
        code = response.status_code
        try:
            response.raise_for_status()
            if code in range(300, 400):
                log.warning(f'{code} {response.text}')
                # TODO: handle redirects
            else:
                log.debug(f'\033[92m{code} OK\033[0m')
        except requests.exceptions.HTTPError as ex:
            code = ex.response.status_code
            if code in range(400, 500):
                log.debug(f'\033[91m{code} EXPIRED\033[0m')
            elif code > 500:
                log.error(f'\033[91m{code} SERVER_FAULT\033[0m')
                # TODO retry-after?
        return response

    def __build_url_for_detail(self, path: str):
        return self.DOMAIN_NAME + path

    def __build_url_for_listing(self, page: int, base_listing_url: str) -> str:
        """
        Builds a paginated URL for an offer listing page.
        """
        url = base_listing_url
        if page > 1:
            url += f'&page={page}'
        return url
    
    def __get_base_url(self) -> str:
        return self.START_URLS.get(self.listing, None)

    def __pagination_iterator(self):
        if not self.pagination:
            raise ValueError(
                'Trying to make a pagination iterator before pagination was set.'
            )
        for x in range(2, self.pagination['totalPages'] + 1):
            yield x
