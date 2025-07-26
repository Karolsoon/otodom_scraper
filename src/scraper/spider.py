from datetime import datetime as dt
from time import sleep
from uuid import uuid4

from rich.progress import track

from src.exceptions import ParsingError
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
            listing_for: str,
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
        db_url_ids = set([
            self.file_util.get_id4(x['url'])
            for x
            in self.db.execute_with_return(queries.Urls.get_active_urls_by_entity, 
                                           (self.listing_for,))])
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
            if has_inserted := self.db.execute_with_return(db.queries.Urls.create_if_not_exists,
                                                           (id4, id4, url, 1, self.run_id, self.run_id, id4)):
                # TODO: This is trash, rework later
                # 0 - means new offer inserted
                # 1 - means offer was already expired but got revived. For more info see the db query.

                flag = has_inserted[0].get('exists_flag')
                if flag == 0:
                    log.info(f'NEW {id4}')
                elif flag == 1:
                    log.info(f'REVIVED {id4}')

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
            updated_run_id = self.run_id
            expired_run_id = None
            status = 1

            if item.response.status_code in range(400, 500):   # SET status 2 when inserting new offer row
                log.info(f'{item.url_id} {item.response.status_code} {item.url} EXPIRED')
                status = 2
                expired_run_id = self.run_id
                item.set_error(step='Download', message='EXPIRED')
            elif item.response.status_code in range(500, 600):
                log.error(f'{item.url_id} {item.response.status_code} {item.url} SERVER ERROR')
                item.set_error(step='Download', message='SERVER ERROR') #TODO: Enum? Dataclass?
            else:
                log.debug(f'{item.url_id} {item.response.status_code} {item.url} OK')

            self.db.execute_no_return(
                queries.Urls.update_status,
                (status, updated_run_id, expired_run_id, item.url_id)
            )

            self.update_audit_log(item, step='Download')

    def __create_db_if_not_exists(self) -> None:
        """
        Create the database if it does not exist.
        """
        self.file_util.create_file(config.DB_NAME)
        self.db.create_tables()

    def update_audit_logs(
            self,
            detail_page_audit_items: list[Detail_Page_Audit_Item],
            step: str
        ) -> None:
        """
        Update the audit logs for the detail pages.
        Performs a database update statement.
        """
        for item in detail_page_audit_items:
            self.update_audit_log(item, step)

    def update_audit_log(self, 
            item: Detail_Page_Audit_Item,
            step: str
        ) -> None:
        """
        Update an audit log for a detail page item.
        """
        if step == 'Parse':
            db.execute_no_return(
                queries.Audit_Logs.update_parsed,
                (item.parsed_at, item.error_step, item.error_message, item.id)
            )
        elif step == 'Download':
            db.execute_no_return(
                queries.Audit_Logs.update_visited,
                (item.visited_at, item.response.status_code, item.error_step, item.error_message, item.id)
            )


    def pick_up_tasks_manually(self) -> list[Detail_Page_Audit_Item]:
        """
        Parse the detail pages manually.
        This is used when the detail pages were downloaded before and need to be parsed again.
        """
        detail_page_audit_items = self.parse_detail_pages(None)
        self.__insert_parsed_offer_to_db(detail_page_audit_items)
        self.__set_google_maps_addresses()

    def parse_detail_pages(
            self,
            detail_page_audit_items: list[Detail_Page_Audit_Item]|None
        ) -> list[Detail_Page_Audit_Item]:
        """
        Returns a list of Detail_Page_Audit_Item objects that have a 200 response status code.
        """
        if not detail_page_audit_items:
            detail_page_audit_items = self.make_detail_page_audit_item_objects('parsing')
        
        active_detail_page_audit_items = []
        not_found = parsing_error = 0
        for item in track(detail_page_audit_items,
                          description='Parsing offers...',
                          total=len(detail_page_audit_items),
                          show_speed=False):
            item.set_parsed_at()
            if not self.__is_offer_active(item):
                continue

            try:
                offer_data = self.parse_detail_page(item.filepath)
            except ParsingError as exc:
                log.warning(f'Failed to parse {item.url_id}')
                item.set_error(step='Parse', message=str(exc))
                self.update_audit_log(item, step='Parse')
                parsing_error += 1
                continue
            except FileNotFoundError as exc:
                log.debug(f'File not found {item.filepath}')
                item.set_error(step='Parse', message=str(exc))
                self.update_audit_log(item, step='Parse')
                not_found += 1
                continue

            item.extracted_offer_data = self.processor.prepare_data_for_insert(
                offer_data,
                item.response)
            active_detail_page_audit_items.append(item)

        if not_found:
            log.warning(f'{not_found} files not found')
        if parsing_error:
            log.warning(f'{parsing_error} parsing errors')
        return active_detail_page_audit_items
            
    def parse_detail_page(self, filepath: str) -> dict[str, dict[str, str|int|None]]:
        offer = {}

        if not self.__does_offer_html_exist(filepath):
            raise FileNotFoundError(f'File not found: {filepath}')

        html = self.file_util.read_file(filepath)
        soup = self.processor.make_soup(html)
        details = self.processor.get_item_from(soup, config.HIERARCHIES['offer_details']['version'][0]) # TODO: [0] is a version, implement

        if not details:
            raise ParsingError(f'Could not parse out offer details')

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
                item.error_message = 'Failed while inserting' if not item.error_message else item.error_message
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
            db.execute_no_return(
                queries.Normalized_Addresses.insert_address,
                (row.get('url_id'),
                 address_data['city'],
                 address_data['postal_code'],
                 address_data['street'],
                 address_data['maps_url'],
                 address_data['coordinates_lat_lon']
                )
            )
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

    def __does_offer_html_exist(self, offer: Detail_Page_Audit_Item|str) -> bool:
        """
        Check if the offer HTML file exists.
        """
        if isinstance(offer, Detail_Page_Audit_Item):
            filepath = offer.filepath
        elif isinstance(offer, str):
            filepath = offer
        else:
            raise ValueError(f'Expected Detail_Page_Audit_Item or str, got {type(offer)}')
        return self.file_util.does_file_exist(filepath)

    @staticmethod
    def __is_offer_active(item: Detail_Page_Audit_Item) -> bool:
        """
        Check if the offer is expired based on the response status code and error message.
        """
        if item.response and item.response.status_code in range(400, 500):
            return False
        if item.error_message == 'EXPIRED':
            return False
        return True
