"""Control BNMP scraping."""
import BNMP

bulk = BNMP.BulkScraper()
bulk.start()
details = BNMP.DetailsScraper()
details.start()
