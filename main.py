from src.scraper.spider import Scraper_Service
from src.watchman.notifications import SMS


# houses_scraper = Scraper_Service(listing_for='houses')
# houses_scraper.run()

flats_scraper = Scraper_Service(listing_for='flats')
flats_scraper.run()

# SMS.send('Nowe oferty na OTODOM zostaly dodane.', 'some_number')


# TODO: add logging of errors for run_ids

# TODO: download images? Maybe at watchman where it is decided 
# which offer is relatable/important?
# Sounds like a plan.
