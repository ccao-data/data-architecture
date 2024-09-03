import random
import pandas as pd
import time
from redfin import Redfin
import requests
import re
from parsel import Selector
from bs4 import BeautifulSoup

client = Redfin()

df = pd.read_csv("populate_211_and_212s/redfin_9.2.csv")

# Create a custom User-Agent string
headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/58.0.3029.110 Safari/537.3"
    )
}

# Retry parameters
max_retries = 3
retry_delay = 2  # seconds between retries

results = []


# Function to extract data with context
def extract_with_context(pattern, text):
    adjusted_pattern = pattern.replace(" ", r"[-\s]?") + r"s?"
    matches = re.findall(
        rf"(.{{25}}{adjusted_pattern}.{{25}})", text, re.IGNORECASE
    )
    return matches


# Example list of User-Agent strings to rotate through
user_agents = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
]

df = df[~df["redfin_url"].str.endswith("nan", na=False)]

start_index = 2559

# Use the valid indices from the DataFrame
valid_indices = df.index[df.index >= start_index]

for index, row in df.loc[valid_indices, ["redfin_url"]].iterrows():
    url = row["redfin_url"]
    print(f"Processing URL: {url}")

    for attempt in range(max_retries):
        try:
            # Rotate headers for each request
            headers = {
                "User-Agent": random.choice(user_agents),
                "Accept-Language": "en-US,en;q=0.5",
                "Referer": "https://www.google.com/",
            }

            # ScraperAPI request
            payload = {
                "api_key": "27cca22ce12338a178137c8b5a71bff0",
                "url": url,
            }
            response = requests.get(
                "https://api.scraperapi.com/", params=payload, headers=headers
            )
            status_code = response.status_code
            print(f"Status code: {status_code} for URL {url}")

            if status_code == 200:
                html_content = response.text

                sel = Selector(text=html_content)

                char_apts_redfin = sel.xpath(
                    '//span[contains(., "# of Units:")]/span/text()'
                ).get()
                char_apts_in_building = sel.xpath(
                    '//span[contains(., "# Of Units in Building:")]/span/text()'
                ).get()

                # Extracting house_info using BeautifulSoup
                soup = BeautifulSoup(html_content, "html.parser")
                home_info_div = soup.find(
                    "div", class_="omdp-about-this-home-text"
                )
                house_info = (
                    home_info_div.get_text(strip=True)
                    if home_info_div
                    else None
                )

                # Extracting the remarks section
                remarks_div = soup.find(
                    "div",
                    class_="remarks",
                    attrs={"data-rf-test-id": "listingRemarks"},
                )
                remarks_text = (
                    remarks_div.get_text(strip=True) if remarks_div else None
                )

                extracted_text = extract_with_context(
                    r"# of Units", html_content
                )
                extracted_buildings = extract_with_context(
                    r"# of units in building", html_content
                )
                duplex = extract_with_context(r"Duplex", html_content)
                triplex = extract_with_context(r"Triplex", html_content)
                quad = extract_with_context(r"Quad", html_content)
                two_unit = extract_with_context(r"Two Unit", html_content)
                three_unit = extract_with_context(r"Three Unit", html_content)
                four_unit = extract_with_context(r"Four Unit", html_content)
                all_text = extract_with_context(r"unit", html_content)
                all_text_apartments = extract_with_context(
                    r"apartment", html_content
                )

                # Storing extracted values in the DataFrame
                df.at[index, "char_apts_redfin"] = (
                    char_apts_redfin if char_apts_redfin else None
                )
                df.at[index, "char_apts_in_building"] = (
                    char_apts_in_building if char_apts_in_building else None
                )
                df.at[index, "extracted_text"] = (
                    "; ".join(extracted_text) if extracted_text else None
                )
                df.at[index, "extracted_buildings"] = (
                    "; ".join(extracted_buildings)
                    if extracted_buildings
                    else None
                )
                df.at[index, "duplex"] = "; ".join(duplex) if duplex else None
                df.at[index, "triplex"] = (
                    "; ".join(triplex) if triplex else None
                )
                df.at[index, "quad"] = "; ".join(quad) if quad else None
                df.at[index, "two_unit"] = (
                    "; ".join(two_unit) if two_unit else None
                )
                df.at[index, "three_unit"] = (
                    "; ".join(three_unit) if three_unit else None
                )
                df.at[index, "four_unit"] = (
                    "; ".join(four_unit) if four_unit else None
                )
                df.at[index, "all_text"] = (
                    "; ".join(all_text) if all_text else None
                )
                df.at[index, "all_text_apartments"] = (
                    "; ".join(all_text_apartments)
                    if all_text_apartments
                    else None
                )
                df.at[index, "house_info"] = house_info if house_info else None
                df.at[index, "remarks"] = (
                    remarks_text if remarks_text else None
                )

                # Save the DataFrame after processing each URL
                df.to_csv("populate_211_and_212s/redfin_9.3.csv", index=False)

                break  # Exit retry loop on success

            elif status_code == 202:
                print(
                    f"Attempt {attempt + 1}: 202 Accepted, retrying after 120 seconds..."
                )
                time.sleep(retry_delay)
            else:
                print(f"Request failed with status code {status_code}")
                break  # Exit retry loop on error other than 202

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            break  # Exit retry loop on exception

    # Adding a randomized delay between requests to avoid rate limiting
    time.sleep(random.uniform(2, 4))
