import pyairbnb
import json
import os
import datetime


os.makedirs("results",exist_ok=True)


check_in = "2024-05-15"
check_out = "2024-05-17"
currency = "USD"
user_input_text = "Luxembourg"
locale = "pt"
proxy_url = ""  # Proxy URL (if needed)
zoom_value=2
api_key = pyairbnb.get_api_key("")
markets_data = pyairbnb.get_markets(currency,locale,api_key,proxy_url)
markets = pyairbnb.get_nested_value(markets_data,"user_markets", [])
if len(markets)==0:
    raise Exception("markets are empty")
config_token = pyairbnb.get_nested_value(markets[0],"satori_parameters", "")
country_code = pyairbnb.get_nested_value(markets[0],"country_code", "")
if config_token=="" or country_code=="":
    raise Exception("config_token or country_code are empty")
place_ids_results = pyairbnb.get_places_ids(country_code, user_input_text, currency, locale, config_token, api_key, proxy_url)
if len(place_ids_results)==0:
    raise Exception("empty places ids")
place_id = pyairbnb.get_nested_value(place_ids_results[0],"location.google_place_id", "")
location_name = pyairbnb.get_nested_value(place_ids_results[0],"location.location_name", "")
print("place_id: ",place_id)
print("location_name: ",location_name)
bb=place_ids_results[0]["location"]["bounding_box"]
ne_lat = bb["ne_lat"]
ne_long = bb["ne_lng"]
sw_lat = bb["sw_lat"]
sw_long = bb["sw_lng"]
price_min = 0
price_max = 0
place_type = ""
amenities = []
currency = currency
language = "en"
proxy_url = ""

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