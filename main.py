from src.scraper.spider import Scraper_Service
from src.watchman.notifications import SMS
from src.watchman.watchdog import Watchdog


houses_glogow_scraper = Scraper_Service(listing_for='houses_glogow')
houses_glogow_scraper.run()

houses_radwanice_scraper = Scraper_Service(listing_for='houses_radwanice')
houses_radwanice_scraper.run()

flats_scraper = Scraper_Service(listing_for='flats')
flats_scraper.run()

w = Watchdog()
w.download_images()
w.clean_url_id_folders()

# SMS.send('Nowe oferty na OTODOM zostaly dodane.', 'some_number')

# TODO: add a file cleanup mechanism so that it only stores HTMLs where something changed (price, coords etc)
# TODO: log the fact that i.e. there was no change after parsing and the file will be deleted

# TODO: add logging of errors for run_ids

# TODO: add some generic SELECT queries to be able to fetch relevant offers. API? UI? CLI? JSON?

# TODO: replace created_at and updated_at in urls table with the run_id instead of timestamp.
