import requests

import config
from src.utils.log_util import get_logger


log = get_logger(__name__, 30, True, True)
log.setLevel('INFO')


class HTTP_Util:
    HEADERS = config.HEADERS
    IMG_HEADERS = config.IMG_HEADERS


    def __init__(self, session: requests.Session = requests.Session()):
        self.session = session

    def fetch_page(self, url: str, headers: dict|None=None) -> requests.Response:
        """
        Sends a HTTP request to a url with predefined headers.

        Returns a requests.Response object
        """
        h = headers if headers else self.HEADERS
        log.debug(f'{url}')
        response = self.session.get(url, headers=h)
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

    def fetch_image(self, url: str) -> requests.Response:
        return self.fetch_page(url, self.IMG_HEADERS)

    def reset_session(self, session: requests.Session = requests.Session()) -> None:
        self.session = session

    def can_fetch_data(self, response: requests.Response) -> bool:
        return response.status_code in range(200, 300)

    def is_json(self, response: requests.Response) -> bool:
        return response.headers.get('Content-Type') in ['application/json']

    def is_image(self, response: requests.Response) -> bool:
        return response.headers.get('Content-Type', '').startswith('image/')

    def get_image_type_from_accept_header(self, response: requests.Response) -> str:
        return response.headers.get('Content-Type', '/').split('/')[-1]
