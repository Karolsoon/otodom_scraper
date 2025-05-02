import json
from time import sleep
from pathlib import Path
from datetime import datetime as dt

import config
from src.scraper.spider import Scraper_Service
from src.utils.gcp_utils import Reverse_Geocoding
from src.database import db


# houses_scraper = Scraper_Service(listing_for='houses')
# houses_scraper.run()

flats_scraper = Scraper_Service(listing_for='flats')
flats_scraper.run()


# TODO: download images? Maybe at watchman where it is decided 
# which offer is relatable/important?
# Sounds like a plan.
