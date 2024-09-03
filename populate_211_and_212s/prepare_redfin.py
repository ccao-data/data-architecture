import pandas as pd
from redfin import Redfin
import numpy as np

client = Redfin()

# Load your data
df = pd.read_csv("populate_211_and_212s/raw_download.csv")

# Replace NaN values with empty strings in the relevant columns
columns_to_replace = [
    "prop_address_street_number",
    "prop_address_street_dir",
    "prop_address_street_name",
    "prop_address_suffix_1",
    "prop_address_city_name",
    "prop_address_state",
    "prop_address_zipcode_1",
]

for col in columns_to_replace:
    df[col] = df[col].replace(np.nan, "")

# Construct the full_address column
df["full_address"] = (
    df["prop_address_street_number"].astype(str)
    + " "
    + df["prop_address_street_dir"].astype(str)
    + " "
    + df["prop_address_street_name"].astype(str)
    + " "
    + df["prop_address_suffix_1"].astype(str)
    + ", "
    + df["prop_address_city_name"].astype(str)
    + ", "
    + df["prop_address_state"].astype(str)
    + " "
    + df["prop_address_zipcode_1"].apply(
        lambda x: str(x).split(".")[0] if pd.notna(x) else ""
    )
)

# Remove any double spaces and trim the result
df["full_address"] = (
    df["full_address"].str.replace(r"\s+", " ", regex=True).str.strip()
)

# Step 1: Construct the full address without a unit first
df["full_address"] = (
    df["prop_address_street_number"].astype(str)
    + " "
    + df["prop_address_street_dir"].astype(str)
    + " "
    + df["prop_address_street_name"].astype(str)
    + " "
    + df["prop_address_suffix_1"].astype(str)
    + ", "
    + df["prop_address_city_name"].astype(str)
    + ", "
    + df["prop_address_state"].astype(str)
    + " "
    + df["prop_address_zipcode_1"].apply(
        lambda x: str(x).split(".")[0] if pd.notna(x) else ""
    )
)

property_ids = []
used_units = []

# Step 2: Search for properties and track if a unit was used
for address in df["full_address"]:
    unit_used = None
    try:
        # First attempt: Search for the exact address on Redfin
        response = client.search(address)

        # Extract the URL and initial information
        url = response["payload"]["exactMatch"]["url"]
        initial_info = client.initial_info(url)

        # Get the property ID
        property_id = initial_info["payload"]["propertyId"]

    except Exception:
        # If there's an error, attempt with "unit 1"
        print(
            f"Exact match not found for address {address}, trying with 'unit 1'"
        )
        try:
            modified_address = address + " unit 1"
            response = client.search(modified_address)

            url = response["payload"]["exactMatch"]["url"]
            initial_info = client.initial_info(url)
            property_id = initial_info["payload"]["propertyId"]
            unit_used = "1"  # Mark that unit 1 was used

        except Exception:
            # If there's still an error, attempt with "unit 2"
            print(
                f"'Unit 1' not found for address {address}, trying with 'unit 2'"
            )
            try:
                modified_address = address + " unit 2"
                response = client.search(modified_address)

                url = response["payload"]["exactMatch"]["url"]
                initial_info = client.initial_info(url)
                property_id = initial_info["payload"]["propertyId"]
                unit_used = "2"  # Mark that unit 2 was used

            except Exception:
                # If there's still an error, attempt with "unit 3"
                print(
                    f"'Unit 2' not found for address {address}, trying with 'unit 3'"
                )
                try:
                    modified_address = address + " unit 3"
                    response = client.search(modified_address)

                    url = response["payload"]["exactMatch"]["url"]
                    initial_info = client.initial_info(url)
                    property_id = initial_info["payload"]["propertyId"]
                    unit_used = "3"  # Mark that unit 3 was used

                except Exception as e3:
                    # If all attempts fail, handle as needed
                    print(
                        f"Error processing modified address {modified_address}: {e3}"
                    )
                    property_id = None

    # Append the property ID and the unit used (if any) to the lists
    property_ids.append(property_id)
    used_units.append(unit_used)

# Step 3: Add the property IDs and used units as new columns in the DataFrame
df["property_id"] = property_ids
df["used_unit"] = used_units

# Step 4: Construct the Redfin URL based on the property IDs


def construct_redfin_url(row):
    base_url = (
        "https://www.redfin.com/IL/"
        + row["prop_address_city_name"].replace(" ", "-")
        + "/"
        + row["full_address"].replace(" ", "-").replace(",", "")
    )
    if row["used_unit"]:
        # Include the unit in the URL if it was used
        base_url += f"-unit-{row['used_unit']}"
    base_url += "/home/" + str(row["property_id"])
    return base_url


df["redfin_url"] = df.apply(construct_redfin_url, axis=1)

# Step 5: Clean up the URL by removing trailing ".0" if any
df["redfin_url"] = df["redfin_url"].str.replace(",", "").str.rstrip(".0")

df.to_csv("populate_211_and_212s/intermediate_data_9.2.csv", index=False)
