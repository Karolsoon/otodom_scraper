from src.scraper.spider import Scraper_Service
from src.watchman.notifications import SMS


houses_scraper = Scraper_Service(listing_for='houses')
houses_scraper.run()

flats_scraper = Scraper_Service(listing_for='flats')
flats_scraper.run()


flats_scraper.clean_url_id_folders()


# SMS.send('Nowe oferty na OTODOM zostaly dodane.', 'some_number')

# TODO: add a file cleanup mechanism so that it only stores HTMLs where something changed (price, coords etc)
# TODO: log the fact that i.e. there was no change after parsing and the file will be deleted

# TODO: add logging of errors for run_ids

# TODO: add some generic SELECT queries to be able to fetch relevant offers. API? UI? CLI? JSON?

# TODO: download images? Maybe at watchman where it is decided 
# which offer is relatable/important and recognizes floor plans (identify by otodom image id, does not run on each scraping, but on demand)
# Sounds like a plan.

# TODO: replace created_at and updated_at in urls table with the run_id instead of timestamp.
