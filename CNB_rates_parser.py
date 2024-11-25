import requests
import pandas as pd
from io import StringIO
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Function to fetch and process daily exchange rates
def daily_exchange_rates(url, date):
    params = {"date": date.strftime("%d.%m.%Y")}  # Format date as dd.mm.yyyy
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        logging.info(f"Successfully fetched data from {url} with params {params}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data: {e}")
        raise
    
    data = response.content.decode("utf-8")
    lines = data.splitlines()
    
    if len(lines) < 3:
        raise ValueError("Unexpected response format: insufficient lines for parsing.")

    # Read data into DataFrame
    df = pd.read_csv(
        StringIO("\n".join(lines[2:])),  # Skip metadata and header lines
        sep="|",
        names=["Country", "Currency", "Amount", "Code", "Rate"],
        skipinitialspace=True
    )
    # Clean and format columns
    df["Rate"] = df["Rate"].str.replace(",", ".").astype(float)
    df["Amount"] = df["Amount"].astype(int)
    return df

# Function to fetch and process monthly additional currencies
def other_exchange_rates(url, year, month):
    params = {"rok": year, "mesic": month}
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        logging.info(f"Successfully fetched data from {url} with params {params}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data: {e}")
        raise
    
    data = response.content.decode("utf-8")
    lines = data.splitlines()
    
    if len(lines) < 3:
        raise ValueError("Unexpected response format: insufficient lines for parsing.")

    # Read data into DataFrame
    df = pd.read_csv(
        StringIO("\n".join(lines[2:])),  # Skip metadata and header lines
        sep="|",
        names=["Country", "Currency", "Amount", "Code", "Rate"],
        skipinitialspace=True
    )
    # Clean and format columns
    df["Rate"] = df["Rate"].str.replace(",", ".").astype(float)
    df["Amount"] = df["Amount"].astype(int)
    return df

if __name__ == "__main__":
    url = "https://www.cnb.cz/cs/financni-trhy/devizovy-trh/"
    url_daily = f"{url}kurzy-devizoveho-trhu/kurzy-devizoveho-trhu/denni_kurz.txt"
    url_other = f"{url}kurzy-ostatnich-men/kurzy-ostatnich-men/kurzy.txt"
    # Get the current date
    date = datetime.now()
    year = date.year
    month = date.month

    try:
        # Fetch daily exchange rates
        logging.info("Fetching daily exchange rates...")
        daily_rates = daily_exchange_rates(url_daily, date)

        # Fetch other exchange rates
        logging.info("Fetching additional exchange rates...")
        other_rates = other_exchange_rates(url_other, year, month)

        # Combine the data (union)
        exchange_rates = pd.concat([daily_rates, other_rates], ignore_index=True)

        # Save the combined data to a Parquet file
        output_file = f'cnb_exchange_rates_{date.strftime("%d.%m.%Y")}.parquet'
        exchange_rates.to_parquet(output_file, engine="pyarrow", index=False)

        logging.info(f"Exchange rates successfully saved to {output_file}")
        #print("Sample data:")
        #print(exchange_rates.head())
    except Exception as e:
        logging.error(f"An error occurred: {e}")
