from pathlib import Path

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

    def get_source_folder(self) -> Path:
        return self.source_folder

    def write(self, path: str|Path, data: str|bytes, mode: str='wt'):
        try:
            if isinstance(path, str):
                path = Path(path)
            with path.open(mode=mode) as file:
                file.write(data)
        except Exception as e:
            log.error(f'Failed to write to {path}')
            log.exception(e)
            raise e from e

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

    def write_detail_file(self, url: str, page: str) -> str:
        """
        details is a dict where key is the URL and value is the html text

        Returns a filepath to the file that was written
        """
        id4 = url.split('-')[-1]
        self.__create_id4_folder_if_not_exists(id4)
        filename = config.DETAIL_HTML_FILEPATH_TEMPLATE.format(
            id4=id4,
            timestamp=self.run_time
        )
        self.write_file(content=page, file=Path(filename))
        return filename

    def write_file(self, content: str, file: Path) -> None:
        log.debug(f'{file}')
        with file.open(mode='tw', encoding='utf-8') as f:
            f.write(content)


    def delete(self, f: Path) -> bool:
        try:
            f.unlink(missing_ok=False)
            return True
        except FileNotFoundError:
            return False

    def remove_htmls_except_two_latest_ones(self, folder: Path) -> int:
        files = self.get_files_from(folder)
        files.sort()
        deleted_count = 0
        for file in files[:-2]:
            if self.delete(file):
                deleted_count += 1
        log.debug(f'Deleted: {deleted_count}')
        return deleted_count

    @staticmethod
    def get_files_from(folder: Path, extension: str='*.html') -> list[Path]:
        return [file for file in folder.glob(extension)]

    @staticmethod
    def get_listing_filename(
            entity: str,
            _type: str,
            i: int|None=None,
            extension: str='.html'):
        _type = '_' + _type if _type else ''
        i = '_' + str(i) if i is not None else ''
        return f'{entity}{_type}{i}{extension}'

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

    @staticmethod
    def read_file(filepath: str, mode: str='tr', encoding: str='utf-8-sig') -> str|bytes:
        """
        Read a file and return its content
        """
        with open(filepath, mode=mode, encoding=encoding) as f:
            return f.read()

    @staticmethod
    def create_file(path: str):
        """
        Create a file if it does not exist
        """
        Path(path).touch(exist_ok=True)

    @staticmethod
    def create_folder(path: str):
        """
        Create a folder if it does not exist
        """
        p = Path(path)
        if p.is_file():
            raise ValueError(f'{path} is a file, not a folder')
        p.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def does_file_exist(path: str) -> bool:
        """
        Check if a file exists
        """
        return Path(path).exists()

    def __create_clean_source_folder(self):
        if not self.source_folder.exists():
            self.source_folder.mkdir(parents=True, exist_ok=False)

    def __create_id4_folder_if_not_exists(self, id4: str) -> Path:
        folder = self.source_folder / id4
        if not folder.exists():
            folder.mkdir(parents=True, exist_ok=True)
        return folder
