import json


DB_NAME = 'src/database/links.sqlite3'
START_URLS = {
    'houses': "https://www.otodom.pl/pl/wyniki/sprzedaz/dom/dolnoslaskie/glogowski/gmina-miejska--glogow/glogow?ownerTypeSingleSelect=ALL&distanceRadius=10&areaMin=80&viewType=listing",
    'flats': ''
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
LOGGING = {
    'formatter': {
        'fmt': '{asctime}\t{levelname}\t{name}\t{funcName}\t{message}',
        'style': '{',
        'datefmt': '%Y-%m-%d %H:%M:%S'
    },
    'levels': {
        'console': 'INFO',
        'file': 'WARNING'
    }
}