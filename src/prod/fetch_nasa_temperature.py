#!/usr/bin/env python
"""
NASA POWER Temperature Data Extraction Script

This script extracts temperature data from the NASA POWER API for Mexican hexagons
and saves the data to text files and uploads them to S3.
"""

import os
import sys
import time
import logging
import yaml
import boto3
import awswrangler as wr
import pandas as pd
import requests
import backoff
from tqdm import tqdm
from datetime import datetime
from typing import Dict, List, Tuple, Any
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/nasa_temperature.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('nasa_temperature')

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)
os.makedirs("data/temperature", exist_ok=True)


def load_config() -> Dict[str, Any]:
    """Load configuration from YAML file."""
    try:
        with open("src/prod/config.yaml", "r") as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        raise


def load_hexagon_grid(file_path: str) -> pd.DataFrame:
    """Load the hexagon grid from a parquet file."""
    try:
        logger.info(f"Loading hexagon grid from {file_path}")
        df = pd.read_parquet(file_path)
        logger.info(f"Loaded {len(df)} hexagons")
        return df
    except Exception as e:
        logger.error(f"Error loading hexagon grid: {str(e)}")
        raise


def get_processed_hexagons(file_path: str) -> List[str]:
    """Get list of already processed hexagons."""
    if not os.path.exists(file_path):
        with open(file_path, 'w') as f:
            f.write('')
        return []

    with open(file_path, 'r') as f:
        processed = f.read().splitlines()

    logger.info(f"Found {len(processed)} already processed hexagons")
    return processed


def update_processed_hexagons(file_path: str, h3_id: str):
    """Add a processed hexagon to the tracking file."""
    with open(file_path, 'a') as f:
        f.write(f"{h3_id}\n")


def validate_date_range(start_date: str, end_date: str) -> Tuple[List[Tuple[str, str]], str, str]:
    """
    Validate and split date range into yearly batches.

    Returns:
        List of (start_date, end_date) tuples for each year,
        overall start date, overall end date
    """
    try:
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")

        if end_dt < start_dt:
            raise ValueError("End date must be after start date")

        # Split into yearly batches
        date_ranges = []
        current_start = start_dt

        while current_start <= end_dt:
            # Calculate year end or use end_dt if it's in the current year
            year_end = min(
                datetime(current_start.year, 12, 31),
                end_dt
            )

            date_ranges.append((
                current_start.strftime("%Y%m%d"),
                year_end.strftime("%Y%m%d")
            ))

            # Move to next year
            current_start = datetime(current_start.year + 1, 1, 1)
            if current_start > end_dt:
                break

        return date_ranges, start_date, end_date
    except Exception as e:
        logger.error(f"Date validation error: {str(e)}")
        raise


@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, requests.exceptions.HTTPError),
    max_tries=3,
    factor=2
)
def fetch_nasa_temperature(
    lat: float, lon: float, start_date: str, end_date: str, api_config: Dict[str, str]
) -> Dict:
    """
    Fetch temperature data from NASA POWER API with exponential backoff retry.

    Args:
        lat: Latitude of the point
        lon: Longitude of the point
        start_date: Start date in format YYYYMMDD
        end_date: End date in format YYYYMMDD
        api_config: API configuration dictionary

    Returns:
        JSON response from the API
    """
    url = (
        f"{api_config['base_url']}?"
        f"parameters={api_config['parameters']}"
        f"&community={api_config['community']}"
        f"&longitude={lon}"
        f"&latitude={lat}"
        f"&start={start_date}"
        f"&end={end_date}"
        f"&format={api_config['format']}"
        f"&user_id={api_config['user_id']}"
        f"&time-standard={api_config['time_standard']}"
    )

    logger.debug(f"API URL: {url}")
    response = requests.get(url)
    response.raise_for_status()  # Will trigger retry if the request fails

    # Check for API-specific errors in the response
    data = response.json()
    if "errors" in data and data["errors"]:
        error_msg = str(data["errors"])
        logger.error(f"API returned error: {error_msg}")
        raise requests.exceptions.RequestException(f"API error: {error_msg}")

    return data


def process_temperature_data(api_response: Dict) -> pd.DataFrame:
    """
    Process the API response and convert to DataFrame.

    Args:
        api_response: Response from the NASA POWER API

    Returns:
        DataFrame with temperature data
    """
    try:
        # Extract temperature data from the response
        temperature_data = api_response['properties']['parameter']['T2M']

        # Convert to DataFrame
        df = pd.DataFrame.from_dict(
            temperature_data,
            orient='index',
            columns=['temperature']
        )

        # Set index name and convert to datetime
        df.index.name = 'date'
        df.index = pd.to_datetime(df.index, format='%Y%m%d%H')

        # Reset index and get column of year, month, day and hour
        df = df.reset_index()
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['day'] = df['date'].dt.day
        df['hour'] = df['date'].dt.hour

        # Sort columns
        df = df[['year', 'month', 'day', 'hour', 'temperature']]

        return df
    except KeyError as e:
        logger.error(f"Key error processing API response: {str(e)}")
        logger.error(f"API response keys: {api_response.keys()}")
        raise
    except Exception as e:
        logger.error(f"Error processing temperature data: {str(e)}")
        raise


def save_temperature_data(df: pd.DataFrame, local_path: str) -> str:
    """
    Save temperature data to a text file.

    Args:
        df: DataFrame with temperature data
        local_path: Local path to save the file

    Returns:
        Path to the saved file
    """
    filepath = os.path.join(local_path, "temp.csv")
    try:
        # Save the DataFrame to a text file
        df.to_csv(filepath, index=False)
        logger.debug(f"Saved temperature data to {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Error saving temperature data: {str(e)}")
        raise


def upload_to_s3(
    file_path: str, bucket_name: str, s3_path: str, h3_id: str
) -> bool:
    """
    Upload a file to S3.

    Args:
        file_path: Local file path
        bucket_name: S3 bucket name
        s3_path: S3 path prefix
        h3_id: Hexagon ID

    Returns:
        True if successful, False otherwise
    """
    session = boto3.Session(profile_name='beway')
    s3_key = f"{s3_path}/{h3_id}.csv"

    try:
        # save to csv
        wr.s3.to_csv(
            df=pd.read_csv(file_path),
            path=f"s3://{bucket_name}/{s3_key}",
            dataset=True,
            mode="overwrite",
            boto3_session=session,
            index=False
        )
        logger.info(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
        return True
    except ClientError as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        return False


def process_hexagon(
    h3_id: str, lat: float, lon: float,
    date_ranges: List[Tuple[str, str]],
    api_config: Dict[str, str],
    local_path: str,
    s3_config: Dict[str, str]
) -> bool:
    """
    Process a single hexagon's temperature data.

    Args:
        h3_id: Hexagon ID
        lat: Latitude of the hexagon center
        lon: Longitude of the hexagon center
        date_ranges: List of (start_date, end_date) tuples
        api_config: API configuration dictionary
        local_path: Local path to save temperature data
        s3_config: S3 configuration dictionary

    Returns:
        True if successful, False otherwise
    """
    try:
        all_temp_data = []

        for start_date, end_date in date_ranges:
            logger.info(f"Fetching data for hexagon {h3_id} from {start_date} to {end_date}")

            # Fetch data from the API
            api_response = fetch_nasa_temperature(
                lat, lon, start_date, end_date, api_config
            )

            # Process the response
            temp_df = process_temperature_data(api_response)
            all_temp_data.append(temp_df)

            # Add a small delay to avoid hitting rate limits
            time.sleep(1)

        # Combine all data
        if len(all_temp_data) > 1:
            combined_df = pd.concat(all_temp_data)
        else:
            combined_df = all_temp_data[0]

        # Save locally
        file_path = save_temperature_data(combined_df, local_path)

        # Upload to S3 if enabled
        if s3_config.get('enabled', False):
            upload_success = upload_to_s3(
                file_path,
                s3_config['bucket_name'],
                s3_config['s3_path'],
                h3_id
            )
            if not upload_success:
                logger.warning(f"Failed to upload {h3_id} to S3")

        return True

    except Exception as e:
        logger.error(f"Error processing hexagon {h3_id}: {str(e)}")
        return False


def main():
    """Main function to coordinate the extraction process."""
    try:
        # Load configuration
        config = load_config()
        nasa_config = config['nasa_temperature']

        # Set up paths and dates
        hex_grid_path = config['mexico_hexgrid']['output_path']
        local_temp_path = "data/temperature"
        processed_file = nasa_config['processing']['progress_file']

        date_ranges, start_date, end_date = validate_date_range(
            nasa_config['start_date'],
            nasa_config['end_date']
        )

        # Load data
        hex_df = load_hexagon_grid(hex_grid_path)
        processed_hexagons = get_processed_hexagons(processed_file)

        # Filter out already processed hexagons
        remaining_hexagons = hex_df[~hex_df['h3_id'].isin(processed_hexagons)]

        if remaining_hexagons.empty:
            logger.info("All hexagons have been processed. Nothing to do.")
            return

        logger.info(f"Processing {len(remaining_hexagons)} hexagons from {start_date} to {end_date}")

        # Process hexagons
        for _, row in tqdm(remaining_hexagons.iterrows(), total=len(remaining_hexagons)):
            h3_id = row['h3_id']
            lat = row['latitude']
            lon = row['longitude']

            success = process_hexagon(
                h3_id, lat, lon,
                date_ranges,
                nasa_config['api'],
                local_temp_path,
                nasa_config['s3']
            )

            if success:
                update_processed_hexagons(processed_file, h3_id)

            # Add a delay between hexagons to respect API rate limits
            time.sleep(nasa_config['processing'].get('retry_delay', 5))

        logger.info("Temperature data extraction completed successfully")

    except Exception as e:
        logger.error(f"Error in main function: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()