from src.scraper.spider import Scraper_Service


houses_scraper = Scraper_Service(listing_for='houses')
houses_scraper.run()

# flats_scraper = Scraper_Service(listing_for='flats')
# flats_scraper.run()


# TODO: download images? Maybe at watchman where it is decided 
# which offer is relatable/important?
# Sounds like a plan.
