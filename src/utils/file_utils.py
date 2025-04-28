from pathlib import Path

import requests

import config
from src.utils.log_util import get_logger


log = get_logger(__name__, 30, True, True)
log.setLevel('INFO')


class File_Util:
    SOURCE_FOLDER = config.SOURCE_FOLDER
    DETAIL_HTML_FILEPATH_TEMPLATE = config.DETAIL_HTML_FILEPATH_TEMPLATE
    def __init__(
            self,
            run_time: str,
            source_folder: str|None = None):
        self.run_time = run_time
        folder = source_folder if source_folder else self.SOURCE_FOLDER
        self.source_folder = Path(folder)
        self.__create_clean_source_folder()

    def write(self, path: str):
        pass

    def write_urls(self, urls: list[str], filename: Path) -> None:
        """
        urls is a list of URLs
        filename - recommend to use staticmethod get_filename

        Be aware that the extension should be explicitly '.txt'
        get_filename(entity='houses', _type='detail', extension='.txt')
        """
        log.info(f'{filename}')
        with filename.open(mode='tw', encoding='utf-8') as f:
            for link in urls:
                f.write(link + '\n')

    def write_listing_files(self, list_of_html_to_dump: list[str], entity: str, _type: str) -> None:
        """
        list to dump is a list of str which contains html
        filename - recommend to use staticmethod get_filename
        entity is houses or flats
        _type is listing or detail

        For listing:
        get_filename(entity='houses', _type='listing', i=1, extension='.html')

        For details:
        get_filename(entity='houses', _type='detail', i=3, extension='.html')
        """
        for i, page in enumerate(list_of_html_to_dump):
            self.write_file(
                content=page,
                file=self.source_folder / self.get_listing_filename(entity, _type, i)
            )

    def write_detail_files(self, details: dict[str, requests.Response]) -> None:
        """
        details is a dict where key is the URL and value is the html text
        """
        for url, page in details.items():
            id4 = url.split('-')[-1]
            self.__create_id4_folder_if_not_exists(id4)
            filename = config.DETAIL_HTML_FILEPATH_TEMPLATE.format(
                id4=id4,
                timestamp=self.run_time
            )
            self.write_file(content=page.text, file=Path(filename))

    def write_file(self, content: str, file: Path) -> None:
        log.info(f'{file}')
        with file.open(mode='tw', encoding='utf-8') as f:
            f.write(content)

    @staticmethod
    def get_listing_filename(
            entity: str,
            _type: str,
            i: int|None=None,
            extension: str='.html'):
        _type = '_' + _type if _type else ''
        i = '_' + str(i) if i is not None else ''
        return f'{entity}{_type}{i}{extension}'
    
    def get_url_list_path(self, entity: str) -> Path:
        """
        entity is houses or flats
        _type is listing or detail
        """
        return Path(self.SOURCE_FOLDER + f'/{entity}_{self.run_time}_urls.txt')

    def get_detail_filename(self, url: str) -> str:
        """
        url is the URL of the offer
        """
        id4 = self.get_id4(url)
        return self.DETAIL_HTML_FILEPATH_TEMPLATE.format(
            id4=id4,
            timestamp=self.run_time
        )

    @staticmethod
    def get_id4(url: str) -> str:
        """
        url is the URL of the offer
        """
        return url.split('-')[-1]

    def exists_id4(self, id4: str) -> bool:
        """
        Check if the id4 folder exists
        """
        folder = self.source_folder / id4
        return folder.exists()

    def __create_clean_source_folder(self):
        if not self.source_folder.exists():
            self.source_folder.mkdir(parents=True, exist_ok=False)

    def __create_id4_folder_if_not_exists(self, id4: str) -> Path:
        folder = self.source_folder / id4
        if not folder.exists():
            folder.mkdir(parents=True, exist_ok=True)
        return folder
