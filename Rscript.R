library(httr)
library(rvest)
library(xml2)

# Define the URL as a character string
url <- "https://www.redfin.com/IL/Chicago/1028-N-Paulina-St-60622/home/14105262"

# Create a custom User-Agent string
user_agent <- "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"

# Make the GET request with the custom User-Agent
response <- GET(url, add_headers(`User-Agent` = user_agent))

# Check the status code of the response
status_code <- status_code(response)
status_code

# If the status code is 200, print the content
if (status_code == 200) {
  content <- content(response, as = "text")
  print(content)
} else {
  print(paste("Request failed with status code", status_code))
}

html_content <- content(response, as = "text")

result <- str_extract(html_content, "# of Units.{10}")

extracted_text <- str_extract(html_content, "(?<=# of Units).{10}")

# Extract only numeric values from the extracted text
numeric_values <- str_extract_all(extracted_text, "\\d+")[[1]]

# Combine the numeric values into a single string (if needed)
result <- paste(numeric_values, collapse = "")

result
