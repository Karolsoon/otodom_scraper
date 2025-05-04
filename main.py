from src.scraper.spider import Scraper_Service
from src.watchman.notifications import SMS


# houses_scraper = Scraper_Service(listing_for='houses')
# houses_scraper.run()

flats_scraper = Scraper_Service(listing_for='flats')
flats_scraper.run()

# SMS.send('Nowe oferty na OTODOM zostaly dodane.', 'some_number')


# TODO: Add to normalized_addresses table a column for coordinates so that
# The maps URL gets updated when the coordinates get updated (already happened)

# TODO: make error handling more robust i.e. failure in any of the
# parsing steps should result in removing the work item 
# and logging the failure somewhere.
# Maybe even include a fallback mechanism or "add to next queue" type of thing

# TODO: download images? Maybe at watchman where it is decided 
# which offer is relatable/important?
# Sounds like a plan.