# Airbnb scraper in Python

In extension to the above project [pybnb](https://github.com/johnbalvin/pybnb) I created PostgerSQL database and used the data to visualize and do analysis using metabase tool.

![alt text](image.png)
## STEPS TO RUN THE CODE LOCALLY
 - Clone the current repo locally
 - Fetch the airbnb data and store it in json format in results folder.
```
python test.py
```
NOTE: Change the parameters to fetch the required data.
 - Create the PostgreSQL database using docker command
 ```
  docker run --name airbnb-postgres \
  -e POSTGRES_USER=nikhil \
  -e POSTGRES_PASSWORD=nikhil123 \
  -e POSTGRES_DB=airbnb_db \
  -p 5432:5432 \
  -v airbnb_progress_data:/var/lib/postgresql/data \
  -d postgres:latest
 ```
NOTE: Change the database name, username, password and while mounting the volumes set the path where you want to persist the data.

 - To create the database schema and dump the json data in the database run the following command - 
 ```
    python postgres_db.py
 ```
 - Create metabase
 ```
   docker run -d -p 3000:3000 \
  --name metabase \
  -v ~/metabase-data:/metabase-data \          
  -e "MB_DB_FILE=/metabase-data/metabase.db" \ 
  metabase/metabase
 ```
 - To see all the features of open source metabase please refer to the official documentation [metabase](https://www.metabase.com/docs/latest/)


## Features
- Extract prices, available dates, reviews, host details and others
- Full search support with filtering by amenities
- Extracts detailed product information from Airbnb
- Implemented in Python just because it's popular
- Easy to integrate with existing Python projects

## Important
- With the new airbnb changes, if you want to get the price from a room url you need to specify the date range
the date range should be on the format year-month-day, if you leave the date range empty, you will get the details but not the price

### Example for Searching Listings

```python
import pyairbnb
import json

# Define search parameters
currency = "MXN"  # Currency for the search
check_in = "2025-10-01"  # Check-in date
check_out = "2025-10-04"  # Check-out date
ne_lat = -0.6747456399483214 # North-East latitude
ne_long = -90.30058677891441  # North-East longitude
sw_lat = -0.7596840340260731  # South-West latitude
sw_long = -90.36727562895442  # South-West longitude
zoom_value = 2  # Zoom level for the map
price_min = 1000
price_max = 0
place_type = "Private room" #or "Entire home/apt" or empty
amenities = [4, 7]  # Example: Filter for listings with WiFi and Pool or leave empty
language = "th"
proxy_url = ""

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

# Save the search results as a JSON file
with open('search_results.json', 'w', encoding='utf-8') as f:
    f.write(json.dumps(search_results))  # Convert results to JSON and write to file
```

### Example: Searching via a full Airbnb URL

```python
import pyairbnb
import json

# Define an Airbnb search URL using only the supported parameters
url = "https://www.airbnb.com/s/Luxembourg--Luxembourg/homes?checkin=2025-07-09&checkout=2025-07-16&ne_lat=49.76537&ne_lng=6.56057&sw_lat=49.31155&sw_lng=6.03263&zoom=10&price_min=22&price_max=100&room_types%5B%5D=Entire%20home%2Fapt&amenities%5B%5D=4&amenities%5B%5D=5"

# Use the URL wrapper
results = pyairbnb.search_all_from_url(url, currency="EUR", language ="es", proxy_url="")

# Save results and print count
with open('search_from_url.json', 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print(f"Found {len(results)} listings from URL search.")
```

### Retrieving Details for Listings

### Getting price
```python
room_url="https://www.airbnb.com/rooms/30931885"
check_in = "2025-10-10"
check_out = "2025-10-12"
proxy_url = ""  # Proxy URL (if needed)
language="de"
data, price_input, cookies = details.get(room_url, language, proxy_url)
product_id = price_input["product_id"]
api_key = price_input["api_key"]
currency = "USD"
adults=1
data = price.get(api_key, cookies, price_input["impression_id"], product_id, 
            check_in, check_out, adults, currency, language, proxy_url)

with open('price.json', 'w', encoding='utf-8') as f:
    f.write(json.dumps(data))
```


### Getting listings from user id
```Python
import pyairbnb
import json
host_id = 656454528
api_key = pyairbnb.get_api_key("")
listings = pyairbnb.get_listings_from_user(host_id,api_key,"")
with open('listings.json', 'w', encoding='utf-8') as f:
    f.write(json.dumps(listings))
```

### Getting experiences by just taking the first autocompletions that you would normally do manually on the website
```Python
import pyairbnb
import json
check_in = "2025-10-10"
check_out = "2025-10-12"
currency = "EUR"
user_input_text = "Estados Unidos"
locale = "es"
proxy_url = ""  # Proxy URL (if needed)
api_key = pyairbnb.get_api_key("")
experiences = pyairbnb.experience_search(user_input_text, currency, locale, check_in, check_out, api_key, proxy_url)
with open('experiences.json', 'w', encoding='utf-8') as f:
    f.write(json.dumps(experiences))
```

### Getting experiences by first getting the autocompletions
```Python
import pyairbnb
import json
check_in = "2025-10-06"
check_out = "2025-10-10"
currency = "USD"
user_input_text = "cuenca"
locale = "pt"
proxy_url = ""  # Proxy URL (if needed)
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
if place_id=="" or location_name=="":
    raise Exception("place_id or location_name are empty")
[result,cursor] = pyairbnb.experience_search_by_place_id("", place_id, location_name, currency, locale, check_in, check_out, api_key, proxy_url)
while cursor!="":
    [result_tmp,cursor] = pyairbnb.experience_search_by_place_id(cursor, place_id, location_name, currency, locale, check_in, check_out, api_key, proxy_url)
    if len(result_tmp)==0:
        break
    result = result + result_tmp
with open('experiences.json', 'w', encoding='utf-8') as f:
    f.write(json.dumps(result))
```

### Getting available/unavailable homes along with metadata
```Python
import pyairbnb
import json

# Define listing URL and parameters
room_url = "https://www.airbnb.com/rooms/51752186"  # Listing URL
currency = "USD"  # Currency for the listing details
checkin = "2025-10-12"
checkout = "2025-10-17"
# Retrieve listing details without including the price information (no check-in/check-out dates)
data = pyairbnb.get_details(room_url=room_url, currency=currency,adults=2, language="ja")

# Save the retrieved details to a JSON file
with open('details_data.json', 'w', encoding='utf-8') as f:
    f.write(json.dumps(data))  # Convert the data to JSON and save it
```

#### Retrieve Details Using Room ID with Proxy
You can also use `get_details` with a room ID and an optional proxy.

```python
import pyairbnb
from urllib.parse import urlparse
import json

# Define listing parameters
room_id = 18039593  # Listing room ID
currency = "MXN"  # Currency for the listing details
proxy_url = ""  # Proxy URL (if needed)

# Retrieve listing details by room ID with a proxy
checkin = "2025-10-12"
checkout = "2025-10-17"
data = pyairbnb.get_details(room_id=room_id, currency=currency, proxy_url=proxy_url,adults=3, language="ko")

# Save the retrieved details to a JSON file
with open('details_data.json', 'w', encoding='utf-8') as f:
    f.write(json.dumps(data))  # Convert the data to JSON and save it
```

### Retrieve Reviews for a Listing
Use `get_reviews` to extract reviews and metadata for a specific listing.

```python
import pyairbnb
import json

# Define listing URL and proxy URL
room_url = "https://www.airbnb.com/rooms/30931885"  # Listing URL
proxy_url = ""  # Proxy URL (if needed)
language = "fr"
# Retrieve reviews for the specified listing
reviews_data = pyairbnb.get_reviews(room_url, language, proxy_url)

# Save the reviews data to a JSON file
with open('reviews.json', 'w', encoding='utf-8') as f:
    f.write(json.dumps(reviews_data))  # Extract reviews and save them to a file
```

### Retrieve Availability for a Listing
The `get_calendar` function provides availability information for specified listings.

```python
import pyairbnb
import json

# Define listing parameters
room_id = "44590727"  # Listing room ID
proxy_url = ""  # Proxy URL (if needed)

# Retrieve availability for the specified listing
calendar_data = pyairbnb.get_calendar(room_id, "", proxy_url)

# Save the calendar data (availability) to a JSON file
with open('calendar.json', 'w', encoding='utf-8') as f:
    f.write(json.dumps(calendar_data))  # Extract calendar data and save it to a file
```

## Credits

This project is originally based on [pybnb](https://github.com/johnbalvin/pybnb). Credit goes to the original author(s).
