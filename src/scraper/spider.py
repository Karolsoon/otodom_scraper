from datetime import datetime as dt
from time import sleep
from uuid import uuid4

from rich.progress import track

from src.scraper.extraction import Link_Extractor, Page_Processor, Detail_Page_Audit_Item
from src.utils.file_utils import File_Util
from src.database import db, queries
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
        self.run_id = uuid4().hex
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
        try:
            self.__create_db_if_not_exists()
            self.__create_run_id()
            self.__set_pagination()
            self.__set_urls_to_visit()
            self.__upsert_urls_in_database()
            self.__create_audit_logs_for_details()
            detail_page_audit_items = self.__download_offer_pages()
            self.__update_urls_and_logs_in_database(detail_page_audit_items)
            detail_page_audit_items = self.parse_detail_pages(detail_page_audit_items)
            self.__insert_parsed_offer_to_db(detail_page_audit_items)
            self.__set_google_maps_addresses()
            self.__close_run_log(1)
        except Exception as ex:
            log.error('Spider failed')
            log.exception(ex)
            self.__close_run_log(0)

        log.info(f'Finished scraping {self.listing_for}')

    def __create_run_id(self) -> None:
        self.run_id = db.execute_with_return(
            queries.Run_Logs.create_log,
            (self.listing_for, self.run_time)
        )[0].get('id') 

    def __close_run_log(self, is_success: bool) -> None:
        db.execute_no_return(
            queries.Run_Logs.update_finished_and_status,
            (dt.now().isoformat(), is_success, self.run_id)
        )

    def __set_urls_to_visit(self) -> None:
        self.__set_urls_to_offers_from_listing()
        self.__add_potentially_expired_urls()

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

    def __set_urls_to_offers_from_listing(self) -> list[str]:
        """
        Set offer URLs from the listing pages.
        """
        self.extractor.set_remaining_listing_pages()
        paths = []
        for page in self.extractor.pages_listing:
            paths.extend(self.processor.get_links(page.text))
        self.extractor.set_detail_urls(paths)

    def __create_audit_logs_for_details(self) -> None:
        for url in self.extractor.detail_urls:
            url_id = self.file_util.get_id4(url)
            path = self.file_util.get_detail_filename(url)
            db.execute_no_return(
                queries.Audit_Logs.create_log,
                (self.run_id, url_id, path, self.run_time)
            )
        log.debug(f'CREATED {len(self.extractor.detail_urls)}')

    def __add_potentially_expired_urls(self) -> None:
        """
        Creates audit logs for offers (details), that are not on the current listing
        and are still active in the urls table - potentially expired.
        """
        db_url_ids = set([self.file_util.get_id4(x) for x in self.db.get_active_urls(entity=self.listing_for)])
        current_url_ids = set([self.file_util.get_id4(x) for x in self.extractor.detail_urls])
        url_ids_not_in_listing = db_url_ids.difference(current_url_ids)
        count = 0
        for url in url_ids_not_in_listing:
            url_id = self.file_util.get_id4(url)
            path = self.file_util.get_detail_filename(url)
            db.execute_no_return(
                queries.Audit_Logs.create_log,
                (self.run_id, url_id, path, self.run_time)
            )
            log.debug(f'Not in listing {url}')
            count += 1
        log.info(f'{count} URLs potentially expired added')

    def __upsert_urls_in_database(self) -> None:
        """
        Upsert URLs in the database.
        """
        for url in self.extractor.detail_urls:
            id4 = self.file_util.get_id4(url)
            if self.db.insert_url_if_not_exists(id4, url, self.run_time, self.listing_for):
                log.info(f'NEW {id4}')
                self.new_url_ids.append(id4)

    def __download_offer_pages(self) -> list[Detail_Page_Audit_Item]:
        """
        Download offer pages.
        """
        detail_page_audit_items = self.make_detail_page_audit_item_objects('download')
        detail_page_audit_items = self.extractor.get_detail_pages(detail_page_audit_items)
        for item in detail_page_audit_items:
            item.filepath = self.file_util.write_detail_file(
                url=item.url,
                page=item.response.text
            )
        log.info(f'{len(detail_page_audit_items)} URLs visited')
        # TODO: bleh
        log.info(f'{len([x.id for x in detail_page_audit_items if not x.response.status_code == 200])} expired')
        if diff := self.__get_number_of_past_failed_tasks(detail_page_audit_items):
            log.warning(f'{diff} failed tasks picked up')
        return detail_page_audit_items

    def make_detail_page_audit_item_objects(self, stage: str) -> list[Detail_Page_Audit_Item]:
        stage_to_query_map = {
            'download': queries.Audit_Logs.get_for_download,
            'parsing': queries.Audit_Logs.get_for_parsing
        }
        items = []
        if q := stage_to_query_map.get(stage):
            detail_urls = db.execute_with_return(q)
            for detail in detail_urls:
                items.append(Detail_Page_Audit_Item(**detail))
            return items
        raise ValueError(f'Invalid value for stage provided: {stage}.')

    def __update_urls_and_logs_in_database(
            self,
            detail_page_audit_items: list[Detail_Page_Audit_Item]) -> None:
        """
        Updates the urls table with:
        - updated_at
        - status_code

        In case of expiry or other HTTP error the following is updated:
        - expired_at # run_time timestamp
        - status # set to 2

        """
        for item in detail_page_audit_items:
            if item.response.status_code in range(400, 500):
                log.info(f'EXPIRED {item.url_id}')
                self.db.update_url(id4=item.url_id, updated_at=self.run_time, expired_at=self.run_time, status=2)
            else:
                self.db.update_url(id4=item.url_id, updated_at=self.run_time)
                log.debug(f'UPDATE LAST_VISITED {item.url_id}')

            # TODO: make a proper mapping of status_codes and messages and set item.error_step if required
            # and take proper actions i.e. GONE is not a legit error, it's just a flag to change statuses to 2
            item.error_message = None if item.response.status_code == 200 else 'GONE'
            db.execute_no_return(
                queries.Audit_Logs.update_visited,
                (item.visited_at, item.response.status_code, item.error_step, item.error_message, item.id)
            )

    def __create_db_if_not_exists(self) -> None:
        """
        Create the database if it does not exist.
        """
        self.file_util.create_file(config.DB_NAME)
        self.db.create_tables()

    def parse_detail_pages(
            self,
            detail_page_audit_items: list[Detail_Page_Audit_Item]|None
        ) -> list[Detail_Page_Audit_Item]:
        """
        Parse the detail pages and insert the data into the database.
        """
        if not detail_page_audit_items:
            detail_page_audit_items = self.make_detail_page_audit_item_objects('parsing')
        for item in track(detail_page_audit_items,
                          description=f'Parsing offers...',
                          total=len(detail_page_audit_items),
                          show_speed=False):
            offer_data = self.__parse_detail_page(item.filepath)
            item.parsed_at = dt.now().isoformat()
            record = self.processor.prepare_data_for_insert(offer_data)
            record['status'] = db.get_latest_id4_status(item.url_id)
            item.extracted_offer_data = record

        return detail_page_audit_items
            
    def __parse_detail_page(self, file: str) -> dict[str, dict[str, str|int|None]]:
        # TODO: a try/except with dedicated Exceptions would be nice to catch later
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

    def __insert_parsed_offer_to_db(
            self,
            detail_page_audit_items: list[Detail_Page_Audit_Item]
            ) -> None:
        fails = []
        for item in detail_page_audit_items:
            if not db.upsert_offer(id4=item.url_id,
                                   entity=self.listing_for,
                                   data=item.extracted_offer_data):
                item.error_step = 'Parse'
                item.error_message = 'Failed while inserting'
                fails.append(item.url_id)
            db.execute_no_return(
                queries.Audit_Logs.update_parsed,
                (item.parsed_at, item.error_step, item.error_message, item.id)
            )
        log.info(f'Parsed {len(detail_page_audit_items) - len(fails)} offers')
        if fails:
            log.warning(f'Failed {len(fails)}: {str(fails)[1:-1]}')

    def __set_google_maps_addresses(self) -> None:
        """
        Set the address with Google Roads API based on an offers coordinates.
        Triggers only for url_ids that have no entry in the normalized_addresses table.
        """
        rows = db.execute_with_return(
            queries.Normalized_Addresses.get_coordinates_to_add
        )
        log.info(f'{len(rows)} URLs')
        for row in track(rows,
                         description='Fetching addresses...',
                         total=len(rows),
                         show_speed=False):
            latlon = row.get('coordinates_lat_lon')
            address_data = Reverse_Geocoding.get_geo(latlon)
            address_data['maps_url'] = Reverse_Geocoding.get_url(latlon)
            address_data['coordinates_lat_lon'] = latlon
            db.insert_address_derrived(row.get('url_id'), **address_data)
            sleep(0.25)

    def __get_number_of_past_failed_tasks(self, detail_page_audit_items: list[Detail_Page_Audit_Item]) -> int:
        """
        If the scraper run is a full run, then check if the number of
        tasks (detail page audit items) is higher than the count of
        links extracted from the listing pages.

        Returns the difference between audit log rows to be visited - extracted links
        """
        if self.extractor.detail_urls:
            return len(detail_page_audit_items) - len(self.extractor.detail_urls)
        return 0
