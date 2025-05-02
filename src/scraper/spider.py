from datetime import datetime as dt
from time import sleep

from rich.progress import track

from src.scraper.extraction import Link_Extractor, Page_Processor
from src.utils.file_utils import File_Util
from src.database import db
from src.utils.log_util import get_logger
from src.utils.gcp_utils import Reverse_Geocoding
import config


log = get_logger(__name__, 30, True, True)
log.setLevel('INFO')


class Scraper_Service:
    db = db

    def __init__(
            self,
            listing_for: str = 'houses',
            run_time: str = dt.now().isoformat(),
            extractor: Link_Extractor=Link_Extractor,
            processor: Page_Processor=Page_Processor,
            file_util: File_Util=File_Util,
        ):
        self.run_time = run_time
        self.listing_for = listing_for
        self.extractor: Link_Extractor = extractor(listing_for, run_time)
        self.processor: Page_Processor = processor(run_time)
        self.file_util: File_Util = file_util(run_time)

        self.filepaths: dict[str, str] = {}
        self.new_url_ids: list[str] = []

    def run(self):
        """
        Run the scraper.
        """
        self.__create_db_if_not_exists()
        self.__set_pagination()
        self.__set_urls_to_visit()
        self.__upsert_urls_in_database()
        self.__download_offer_pages()
        self.__update_urls_in_database()
        self.__parse_detail_pages()
        self.__set_google_maps_addresses()
        log.info(f'Finished scraping {self.listing_for}')

    def __set_urls_to_visit(self) -> None:
        self.__set_urls_to_offers_from_listing()
        self.__add_urls_to_offers_from_database_which_are_not_in_listing()

    def __set_pagination(self) -> dict[str, int]:
        """
        Get pagination from the first page of the listing.

        Returns a dict of:
            'totalPages': int,
            'itemsPerPage': int,
            'currentPage': int
        """
        self.extractor.set_first_listing_page()
        self.extractor.set_pagination(
            self.processor.get_pagination(self.extractor.pages_listing[0].text)
        )

    def __set_urls_to_offers_from_listing(self) -> None:
        """
        Set offer URLs from the listing pages.
        """
        self.extractor.set_remaining_listing_pages()
        paths = []
        for page in self.extractor.pages_listing:
            paths.extend(self.processor.get_links(page.text))
        self.extractor.set_detail_urls(paths)
        self.file_util.write_urls(
            self.extractor.detail_urls,
            self.file_util.get_url_list_path(self.listing_for)
        )

    def __add_urls_to_offers_from_database_which_are_not_in_listing(self) -> None:
        db_urls = set(self.db.get_active_urls(entity=self.listing_for))
        current_urls = set(self.extractor.detail_urls)
        urls_not_in_listing = db_urls.difference(current_urls)
        for url in urls_not_in_listing:
            log.debug(f'Not in listing {url}')
            self.extractor.detail_urls.append(url)

    def __upsert_urls_in_database(self) -> None:
        """
        Upsert URLs in the database.
        """
        for url in self.extractor.detail_urls:
            id4 = self.file_util.get_id4(url)
            if self.db.insert_url_if_not_exists(id4, url, self.run_time, self.listing_for):
                log.info(f'NEW {id4}')
                self.new_url_ids.append(id4)

    def __download_offer_pages(self) -> None:
        """
        Download offer pages.
        """
        self.extractor.set_detail_pages()
        self.filepaths = self.file_util.write_detail_files(self.extractor.pages_detail)

    def __update_urls_in_database(self) -> None:
        """
        Updates the urls table with:
        - updated_at
        - status_code

        In case of expiry or other HTTP error the following is updated:
        - expired_at # run_time timestamp
        - status # set to 2

        """
        for url, response in self.extractor.pages_detail.items():
            id4 = self.file_util.get_id4(url)
            if response.status_code in range(400, 500):
                log.info(f'EXPIRED {id4}')
                self.db.update_url(id4=id4, updated_at=self.run_time, expited_at=self.run_time, status=2)
            else:
                self.db.update_url(id4, self.run_time)
                log.debug(f'UPDATE LAST_VISITED {id4}')

            error_message = None if response.status_code == 200 else 'GONE'
            self.db.insert_audit_log(
                id4=id4,
                status_code=response.status_code,
                html_file_path=self.file_util.get_detail_filename(url),
                error_message=error_message,
                visited_at=self.run_time,
                status=1
            )

    def __create_db_if_not_exists(self) -> None:
        """
        Create the database if it does not exist.
        """
        self.file_util.create_file(config.DB_NAME)
        self.db.create_tables()

    def __parse_detail_pages(self) -> None:
        """
        Parse the detail pages and insert the data into the database.
        """
        for id4, filepath in track(self.filepaths.items(),
                          description=f'Parsing offers...',
                          total=len(self.filepaths),
                          show_speed=False):
            offer_data = self.__parse_detail_page(filepath)
            record = self.processor.prepare_data_for_insert(offer_data)

            db.upsert_offer(id4, self.listing_for, record)
            db.update_audit_log_parsed(
                html_file_path=filepath,
                parsed_at=self.run_time
            )
        log.info(f'Parsed {len(self.filepaths)} offers')
            
    def __parse_detail_page(self, file: str) -> dict[str, dict[str, str|int|None]]:
        offer = {}
        html = self.file_util.read_file(file)

        soup = self.processor.make_soup(html)
        details = self.processor.get_item_from(soup, config.HIERARCHIES['offer_details'])

        for name, hierarchy in config.HIERARCHY_DETAILS.items():
            value = self.processor.get_item_from(
                details,
                hierarchy
            )
            if hierarchy['transformation']:
                value = hierarchy['transformation'](value, hierarchy.get('attributes'))
            offer[name] = value
        return offer

    def __set_google_maps_addresses(self) -> None:
        """
        Set the address with Google Roads API based on an offers coordinates.
        Triggers only for url_ids that have no entry in the addresses_derrived table.
        """
        rows = db.get_urls_without_google_addresses()
        log.info(f'{len(rows)} URLs')
        for row in track(rows,
                         description='Fetching addresses...',
                         total=len(rows),
                         show_speed=False):
            latlon = row.get('coordinates_lat_lon')
            address_data = Reverse_Geocoding.get_geo(latlon)
            address_data['maps_url'] = Reverse_Geocoding.get_url(latlon)
            db.insert_address_derrived(row.get('url_id'), **address_data)
            sleep(0.25)
