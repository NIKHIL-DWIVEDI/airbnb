import datetime
import pyairbnb
import json

# Define search parameters
currency = "USD"  # Currency for the search in the US
check_in = "2025-10-01"  # Check-in date
check_out = "2025-10-04"  # Check-out date

# Approximate bounding box for Ohio, USA
ne_lat = 41.9775    # North-East latitude (near Ashtabula)
ne_long = -80.5187  # North-East longitude
sw_lat = 38.4034    # South-West latitude (near Portsmouth)
sw_long = -84.8219  # South-West longitude

zoom_value = 7  # Zoom level appropriate for state-wide search
price_min = 50  # Minimum price in USD
price_max = 500 # Max price (optional, adjust as needed)

place_type = "Private room"  # Options: "Private room", "Entire home/apt", or empty ""
amenities = [4, 7]  # 4 = WiFi, 7 = Pool
language = "en"  # Use English for results
proxy_url = ""  # Optional proxy


# Search listings within specified coordinates and date range using keyword arguments
search_results = pyairbnb.search_all(
    check_in=check_in,
    check_out=check_out,
    ne_lat=ne_lat,
    ne_long=ne_long,
    sw_lat=sw_lat,
    sw_long=sw_long,
    zoom_value=zoom_value,
    price_min=price_min,
    price_max=price_max,
    place_type=place_type,
    amenities=amenities,
    currency=currency,
    language=language,
    proxy_url=proxy_url
)

## take the current date and time for the filename
current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
# Save the search results to a JSON file with a timestamp
with open(f'results/search_results_{current_time}.json', 'w', encoding='utf-8') as f:
    f.write(json.dumps(search_results, indent=4))  # Convert results to JSON and write to files

print(f"Retrieved {len(search_results)} listings from search.")