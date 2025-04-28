from datetime import datetime as dt

from src.scraper.home_spider import Link_Extractor, Page_Processor
from src.utils.file_utils import File_Util
from src.database import db
from src.utils.log_util import get_logger


log = get_logger(__name__, 30, True, True)


ts = dt.now().isoformat()
l = Link_Extractor(run_time=ts)
p = Page_Processor(run_time=ts)
f = File_Util(run_time=ts)


class Otodom_Scraper:
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

    def run(self):
        """
        Run the scraper.
        """
        self.__set_pagination()
        self.__set_urls_to_visit()
        self.__upsert_urls_in_database()
        self.__download_offer_pages()
        self.__update_urls_in_database()

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
            l.detail_urls,
            f.get_url_list_path('houses')
        )

    def __add_urls_to_offers_from_database_which_are_not_in_listing(self) -> None:
        db_urls = set(self.db.get_active_urls())
        current_urls = set(self.extractor.detail_urls)
        for url in db_urls.difference(current_urls):
            log.info(f'{url}')
            self.extractor.detail_urls.append(url)

    def __upsert_urls_in_database(self) -> None:
        """
        Upsert URLs in the database.
        """
        for url in self.extractor.detail_urls:
            id4 = self.file_util.get_id4(url)
            if self.db.insert_url_if_not_exists(id4, url, self.run_time):
                log.debug(f'NEW {id4}')

    def __download_offer_pages(self) -> None:
        """
        Download offer pages.
        """
        self.extractor.set_detail_pages()
        self.file_util.write_detail_files(self.extractor.pages_detail)

    def __update_urls_in_database(self) -> None:
        """
        Update URLs in the database.
        """
        for url, response in self.extractor.pages_detail.items():
            id4 = self.file_util.get_id4(url)
            if response.status_code in range(400, 500):
                log.info(f'EXPIRED {id4}')
                self.db.update_url(id4=id4, updated_at=self.run_time, expited_at=self.run_time, status=2)
            else:
                self.db.update_url(id4, self.run_time)
                log.debug(f'UPDATE LAST_VISITED {id4}')

            error_message = None if response.status_code == 200 else response.text
            self.db.insert_audit_log(
                id4=id4,
                status_code=response.status_code,
                html_file_path=self.file_util.get_detail_filename(url),
                error_message=error_message,
                visited_at=self.run_time,
                status=1
            )


scraper = Otodom_Scraper()
scraper.run()

# # Set pagination
# l.set_first_listing_page()
# pagination = p.get_pagination(l.pages_listing[0].text)
# l.set_pagination(pagination)

# # Set offer URLs
# l.set_remaining_listing_pages()
# # f.write_listing_files(l.pages_listing, 'houses', 'listing')
# paths = []
# for page in l.pages_listing:
#     paths.extend(p.get_links(page.text))
# l.set_detail_urls(paths)
# for url in l.detail_urls:
#     id4 = f.get_id4(url)
#     if db.insert_url_if_not_exists(id4, url, ts):
#         log.debug(f'NEW {id4}')

# f.write_urls(
#     l.detail_urls,
#     f.get_url_list_path('houses')
# )

# l.set_detail_pages()
# f.write_detail_files(l.pages_detail)

# # update url in database
# for url, response in l.pages_detail.items():
#     id4 = f.get_id4(url)
#     # Add url to urls table if it doesn't exist
#     if response.status_code in range(400, 500):
#         log.info(f'EXPIRED {id4}')
#         db.update_url(id4=id4, updated_at=ts, expited_at=ts, status=2)
#     else:
#         db.update_url(id4, ts)
#         log.debug(f'UPDATE LAST_VISITED {id4}')
    
#     # Add url to audit_logs table
#     error_message = None if response.status_code == 200 else response.text
#     db.insert_audit_log(
#         id4=id4,
#         status_code=response.status_code,
#         html_file_path=f.get_detail_filename(url),
#         error_message=error_message,
#         visited_at=ts,
#         status=1
#     )

# TODO: set.difference between current urls and urls in database
# TODO: visit these urls to ensure they are expired
# TODO: update as expired + logging + audit_logs + bla bla
