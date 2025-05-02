import requests
from config import GCP_API_KEY


class Reverse_Geocoding:
    URL_TEMPLATE = '{base_url}?latlng={latlon}&location_type=ROOFTOP&key={api_key}'
    LOCATION_TYPE = 'ROOFTOP'
    BASE_URL = 'https://maps.googleapis.com/maps/api/geocode/json'
    GCP_API_KEY = GCP_API_KEY

    key_mapping = {
        'route': 'street',
        'locality': 'city',
        'postal_code': 'postal_code',
        'street_number': 'street_number'
    }
    
    @classmethod
    def get_geo(cls, latlon: str|None) -> dict[str, str]:
        """
        Returns the city name, postal code and street name
        based on the latitude and longitude.
        """
        if not latlon:
            return cls._get_empty_geo()
        raw_data = cls.__send_request(cls.__build_api_url(latlon))
        return cls.__extract(raw_data)

    @classmethod
    def get_url(cls, latlon: str) -> str:
        """
        Returns the URL for the given latitude and longitude.
        """
        return f'https://www.google.com/maps/search/?api=1&query={latlon}'

    @classmethod
    def __build_api_url(cls, latlon: str) -> str:
        return cls.URL_TEMPLATE.format(
            base_url=cls.BASE_URL,
            latlon=latlon,
            api_key=cls.GCP_API_KEY
        )

    @staticmethod
    def __send_request(url: str) -> requests.Response:
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f'Error: {response.status_code}')

    @classmethod
    def __extract(cls, raw_data: dict) -> dict[str, str]:
        r = {}
        address_component = {}

        for x in raw_data.get('results', {}):
            if 'street_address' in x['types']:
                address_component = x
                break

        for item in address_component.get('address_components', {}):
            for key in cls.key_mapping:
                if key in item['types']:
                    r[cls.key_mapping[key]] = item.get('long_name')
                    break
        if not r:
            r = cls._get_empty_geo()
        else:
            r['street'] = f"{r.get('street', '')} {r.get('street_number', '')}".strip()
            r.pop('street_number', None)
        return r

    @staticmethod
    def _get_empty_geo() -> dict[str, str]:
        return {
            'street': '',
            'city': '',
            'postal_code': ''
        }