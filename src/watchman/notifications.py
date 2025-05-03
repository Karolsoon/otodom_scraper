import requests

from config import SMS_APIKEY, SMS_PASSWORD, SMS_NAME
from src.utils.log_util import get_logger



log = get_logger(__name__, 30, True, True)
log.setLevel('INFO')


class SMS:

    sms_url = 'https://api2.smsplanet.pl/sms'

    @classmethod
    def send(cls, msg: str, number: str) -> dict:
        cls.validate_number(number)
        data = {
            'key': SMS_APIKEY,
            'password': SMS_PASSWORD,
            'from': SMS_NAME,
            'to': [number],
            'msg': msg
        }
        response = requests.post(url=cls.sms_url, data=data, timeout=10)

        try:
            response.raise_for_status()
            message_id = response.json()['messageId']
            log.info('MESSAGE SENT')
            return message_id
        except requests.exceptions.HTTPError:
            log.warning('HTTP ERROR')
            return 0
        except KeyError:
            fail_id = ', '.join(response.json())
            log.error('messageId not in response')
            return 0

    @staticmethod
    def validate_number(number: str):
        if len(number) < 9:
            raise ValueError(f'Provided number "{number}" is too short')
        if len(number) > 9:
            raise ValueError(f'Provided number "{number}" is too long')
        if not number.isdigit():
            raise ValueError(f'"{number}" is not a valid phone number.')