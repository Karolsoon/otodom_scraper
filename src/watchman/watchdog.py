import json
from datetime import datetime as dt
from time import sleep

import cv2
import numpy as np
from rich.progress import track

from src.database import db, queries
from src.utils.file_utils import File_Util
from src.utils.http_util import HTTP_Util
from src.utils.log_util import get_logger


log = get_logger(__name__, 30, 30, True)
log.setLevel('INFO')


class Watchdog:
    """
    Makes data cleanups i.e. removed source html files and leaves the 2 latest ones.
    Downloads images for offers.
    """
    def __init__(
            self,
            file_util: File_Util = File_Util,
            http_util: HTTP_Util = HTTP_Util(),
            run_time: str = dt.now().isoformat()
        ):
        self.run_time = run_time
        self.file_util: File_Util = file_util(run_time)
        self.http_util: HTTP_Util = http_util

    def clean_url_id_folders(self) -> None:
        total_count = 0
        log.info('Removing old HTML files. Leaving 2 latest ones.')
        for file in self.file_util.get_source_folder().iterdir():
            if file.is_dir():
                log.debug('Cleaning ', file)
                total_count += self.file_util.remove_htmls_except_two_latest_ones(file)
        log.info(f'Old files deleted: {total_count}')

    def download_images(self) -> None:
        """
        Download images for all url_ids where images were not downloaded yet
        """
        rows = db.execute_with_return(
            queries.Images.get_all_images_to_download
        )
        added = 0
        image_dict = self.load_image_urls(rows)
        for url_id, image_url_list in track(image_dict.items(),
                                            'Fetching images...',
                                            total=len(image_dict)):
            qty = self.__download_images(url_id=url_id, image_url_list=image_url_list)
            added += qty
            if qty:
                log.debug(f'NEW {qty} images for {url_id}')
        log.info(f'{added} new images')

    def download_images_for_url_id(self, url_id: str) -> None:
        """

        """
        rows = db.execute_with_return(
            queries.Images.get_images_to_download_by_url_id,
            (url_id,)
        )
        added = 0
        image_dict = self.load_image_urls(rows)
        for url_id, image_url_list in track(image_dict.items(),
                                            'Fetching images...',
                                            total=len(image_dict)):
            qty = self.__download_images(url_id=url_id, image_url_list=image_url_list)
            added += qty
        log.info(f'{added} new images for {url_id}')

    def __download_images(self, url_id: str, image_url_list: list[str]) -> int:
        """
        Downloads
        Logs
        Writes to db
        Writes image to folder

        Returns the quantity of downloaded images
        """
        added = 0
        for image_url in image_url_list:
            image_id = self._get_image_id(image_url)
            if self._is_image_in_db(url_id, image_id):
                log.debug(f'SKIP {image_id}')
                continue
            image, extension, http_status_code = self._fetch_image(image_url)
            image_path = self._get_image_path(url_id, image_id, extension)
            self.file_util.write(image_path, image, mode='wb')
            img_type = self.get_picture_type(str(image_path))
            db.execute_no_return(
                queries.Images.create_image_entry,
                (url_id, image_id, http_status_code, str(image_path), img_type)
            )
            added += 1
            sleep(0.04)
        return added

    def _fetch_image(self, url: str) -> bytes:
        resp = self.http_util.fetch_image(url)
        status_code = resp.status_code
        if self.http_util.can_fetch_data(resp) and self.http_util.is_image(resp):
            extension = self.http_util.get_image_type_from_accept_header(resp)
            return resp.content, extension, status_code
        return b'', '', status_code

    def _get_image_path(self, url_id: str, image_id: str, extension: str) -> str:
        return self.file_util.source_folder / url_id / f'{image_id}{"" if not extension else "." + extension}'

    def _get_image_id(self, url: str) -> str:
        # url = 'https://ireland.apollo.olxcdn.com/v1/files/eyJmbiI6ImgwOTd3cnozZjhwNTItQVBMIiwidyI6W3siZm4iOiJlbnZmcXFlMWF5NGsxLUFQTCIsInMiOiIxNCIsInAiOiIxMCwtMTAiLCJhIjoiMCJ9XX0.r0ng4tdZYGtDnVAsSc3KnV_fEkI4KhHJII8gx6XiKBU/image;s=1280x1024;q=80'
        return url.split('.')[-1].split('/')[0]

    def load_image_urls(self, rows: list[dict[str, str]]) -> dict[str, list[str]]:
        """
        Returns a dict of {url_id: [img_link, img_link]}
        """
        new = {}
        for row in rows:
            new[row['url_id']] = json.loads(row['images'])
        return new

    def _is_image_in_db(self, url_id: str, image_id: str):
        r = db.execute_with_return(
            queries.Images.get_image_id,
            (url_id, image_id)
        )
        if len(r) > 0:
            return True
        return False

    def calculate_edge_sharpness(self, image_gray) -> float:
        laplacian_var = cv2.Laplacian(image_gray, cv2.CV_64FC1, ksize=1, scale=1).var()
        return laplacian_var

    def calculate_colorfulness(self, image_bgr) -> float:
        (B, G, R) = cv2.split(image_bgr.astype("float"))
        rg = np.absolute(R - G)
        yb = np.absolute(0.45 * (R + G) - 1.1 * B)

        std_rg, std_yb = np.std(rg), np.std(yb)
        mean_rg, mean_yb = np.mean(rg), np.mean(yb)

        colorfulness = np.sqrt(std_rg**2 + std_yb**2) + 0.28 * np.sqrt(mean_rg**2 + mean_yb**2)
        return colorfulness

    def calculate_tone_mean(self, image_bgr) -> float:
        (B, G, R) = cv2.split(image_bgr.astype("float"))
        return np.average([np.mean(B), np.mean(R), np.mean(G)])

    def get_picture_type(self, image_path: str) -> str:
        image = cv2.imread(image_path)
        if image is None:
            log.warning(f"Failed to load image: {image_path}")
            return ''

        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        sharpness = self.calculate_edge_sharpness(gray)
        colorfulness = self.calculate_colorfulness(image)
        monotone = self.calculate_tone_mean(image)
        # print(f'Sharpness: {sharpness}')
        # print(f'Colorfulness: {colorfulness}')
        if (sharpness > 100 and colorfulness < 30 and monotone > 180) or (22 < colorfulness < 30 and sharpness > 60) and monotone > 180:
            return 'floor_plan'
        return "real_estate"
