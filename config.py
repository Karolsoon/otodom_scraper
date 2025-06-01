import json
from src.scraper import transformation


with open('secrets.json', 'r') as f:
    SECRETS: dict = json.load(f)


GCP_API_KEY = SECRETS.get('GCP_API_KEY')
SMS_APIKEY = SECRETS.get('SMS_APIKEY')
SMS_PASSWORD = SECRETS.get('SMS_PASSWORD')
SMS_NAME = SECRETS.get('SMS_NAME')
SMS_NUMBER_TO_NOTIFY = SECRETS.get('SMS_NUMBER_TO_NOTIFY')

DB_NAME = 'src/database/links.sqlite3'
START_URLS = {
    'houses_glogow': {
        'url': "https://www.otodom.pl/pl/wyniki/sprzedaz/dom/dolnoslaskie/glogowski/gmina-miejska--glogow/glogow?ownerTypeSingleSelect=ALL&distanceRadius=15&areaMin=70&viewType=listing",
        'entity_type': 'houses'
    },
    'houses_radwanice': {
        'url': 'https://www.otodom.pl/pl/wyniki/sprzedaz/dom/dolnoslaskie/polkowicki/radwanice?distanceRadius=3&limit=36&ownerTypeSingleSelect=ALL&roomsNumber=%5BFOUR%2CFIVE%2CSIX_OR_MORE%5D&by=DEFAULT&direction=DESC&viewType=listing',
        'entity_type': 'houses'
    },
    'flats': {
        'url': 'https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/dolnoslaskie/glogowski/gmina-miejska--glogow/glogow?priceMax=1500000&areaMin=60&distanceRadius=5&viewType=listing',
        'entity_type': 'flats'
    }
}
DOMAIN_NAME = 'https://www.otodom.pl:443'
OFFER_LINK_STARTSWITH = '/pl/oferta/'
SOURCE_FOLDER = 'source_folder'
DETAIL_HTML_FILEPATH_TEMPLATE = SOURCE_FOLDER + '/{id4}/{timestamp}.html'
# tag: attributes dict[key, value]
HIERARCHIES = {
    'pagination': {
        'stages': [
            {
                'path': [
                    {'body': {}},
                    {'script': {}}
                ],
                'transformation': json.loads,
                'input': 'soup'
            },
            {
                'path': [
                    {'props': {}},
                    {'pageProps': {}},
                    {'data': {}},
                    {'searchAds': {}},
                    {'pagination': {}}
                ],
                'transformation': None,
                'input': 'json'
            }
        ],
        'attributes': [
            'totalPages',
            'itemsPerPage',
            'currentPage'
        ]
    },
    'offer_links': {
        'stages': [
            {
                'path': [
                    {'body': {}},
                    {'div': {}},
                    {'div': {'data-sentry-element': 'MainLayoutWrapper'}},
                    {'main': {}},
                    {'div': {'data-sentry-element': 'NegativeMainLayoutSpacer'}},
                    {'div': {'data-sentry-element': 'Content'}},
                    {'div': {'data-sentry-element': 'ListingViewContainer'}},
                    {'div': {'data-sentry-element': 'Container'}},
                    {'div': {'data-sentry-element': 'Content'}}
                ],
                'transformation': None,
                'input': 'soup'
            }
        ]
    },
    'offer_details': {
        'stages': [
            {
                'path': [
                    {'body': {}},
                    {'script': {"id": "__NEXT_DATA__"}}
                ],
                'transformation': json.loads,
                'input': 'soup'
            },
            {
                'path': [
                    {'props': {}},
                    {'pageProps': {}},
                    {'ad': {}}
                ],
                'transformation': None,
                'input': 'json'
            }
        ]
    },
}


HIERARCHY_DETAILS = {
    'contact': {
        'stages': [
            {
                'path': [
                    {'contactDetails': {}}
                ],
                'transformation': None,
                'input': 'json'
            }
        ],
        'transformation': transformation.filter_dict,
        'attributes': [
            'name',
            'type',
            'phones'
        ]
    },
    'owner': {
        'stages': [
            {
                'path': [
                    {'owner': {}}
                ],
                'transformation': None,
                'input': 'json'
            }
        ],
        'transformation': transformation.filter_dict,
        'attributes': [
            'name',
            'type',
            'phones',
            'email',
            'contacts'
        ]
    },
    'coordinates': {
        'stages': [
            {
                'path': [
                    {'location': {}},
                    {'coordinates': {}}
                ],
                'transformation': None,
                'input': 'json'
            }
        ],
        'transformation': transformation.filter_dict,
        'attributes': [
            'latitude',
            'longitude'
        ]
    },
    'images_urls': {
        'stages': [
            {
                'path': [
                    {'images': {}}
                ],
                'transformation': None,
                'input': 'json'
            }
        ],
        'transformation': transformation.extract_from_list_of_dict,
        'attributes': [
            'large'
        ]
    },
    'city': {
        'stages': [
            {
                'path': [
                    {'location': {}},
                    {'address': {}},
                    {'city': {}}
                ],
                'transformation': None,
                'input': 'json'
            }
        ],
        'transformation': transformation.filter_dict,
        'attributes': [
            'id',
            'name'
        ]
    },
    'street': {
        'stages': [
            {
                'path': [
                    {'location': {}},
                    {'address': {}},
                    {'street': {}}
                ],
                'transformation': None,
                'input': 'json'
            }
        ],
        'transformation': None
    },
    'characteristics': {
        'stages': [
            {
                'path': [
                    {'characteristics': {}}
                ],
                'transformation': None,
                'input': 'json'
            }
        ],
        'transformation': transformation.extract_characteristics,
        'attributes': [
            'value',
            'localizedValue'
        ],
    },
    'posted_by': {
        'stages': [
            {
                'path': [
                    {'advertType': {}}
                ],
                'transformation': None,
                'input': 'json'
            }
        ],
        'transformation': None
    },
    'description': {
        'stages': [
            {
                'path': [
                    {'description': {}}
                ],
                'transformation': None,
                'input': 'json'
            }
        ],
        'transformation': None
    },
    'other': {
        'stages': [
            {
                'path': [
                    {'featuresByCategory': {}}
                ],
                'transformation': None,
                'input': 'json'
            }
        ],
        'transformation': transformation.extract_features
    },
    'build_year': {
        'stages': [
            {
                'path': [
                    {'target': {}},
                    {'buildYear': {}}
                ],
                'transformation': None,
                'input': 'json'
            }
        ],
        'transformation': None
    }
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "pl-PL,pl;q=0.9",
    "Connection": "keep-alive",
    "Host": "www.otodom.pl",
    "Priority": "u=0, i",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none"
}
IMG_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "pl-PL,pl;q=0.9",
    "Connection": "keep-alive",
    "Host": "ireland.apollo.olxcdn.com",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Safari/605.1.15"
}
LOGGING = {
    'formatter': {
        'fmt': '{asctime}\t{levelname}\t{module}\t{funcName}\t{message}',
        'style': '{',
        'datefmt': '%Y-%m-%d %H:%M:%S'
    },
    'levels': {
        'console': 'INFO',
        'file': 'WARNING'
    }
}