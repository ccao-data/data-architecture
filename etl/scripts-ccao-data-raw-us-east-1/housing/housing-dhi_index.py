from io import StringIO

import pandas as pd
import requests

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    )
}


def load_csv_from_url(url):
    # Send a GET request to the URL
    response = requests.get(url)
    # Raise an exception if the request was unsuccessful
    response.raise_for_status()

    # Use StringIO to convert the text data into a file
    data = StringIO(response.text)
    # Read the data into a DataFrame
    df = pd.read_csv(data)

    return df


headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/58.0.3029.110 Safari/537.3"
    )
}

# URL of the file you want to load
file_url = {
    (
        "https://eig.org/dci-maps-2023/data/"
        "1cd12716-de4a-4ef6-884b-af6e1066b581.csv"
    )
}

# Load the data
df = load_csv_from_url(file_url)

# Display the first few rows of the DataFrame
print(df.head())
