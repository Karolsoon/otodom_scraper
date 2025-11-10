from src.scraper.spider import Scraper_Service
from src.watchman.watchdog import Watchdog

houses_glogow_scraper = Scraper_Service(listing_for='houses_glogow')
houses_glogow_scraper.run()

houses_radwanice_scraper = Scraper_Service(listing_for='houses_radwanice')
houses_radwanice_scraper.run()

flats_scraper = Scraper_Service(listing_for='flats')
flats_scraper.run()

# flats_scraper.pick_up_tasks_manually()


w = Watchdog()
w.download_images()
w.clean_url_id_folders()
w.notify_about_recent_good_offer()


# TODO(Karol): handle redirects in scraper.

# TODO(Karol): add logging of errors for run_ids

# TODO(Karol): add a UI or dashboard
