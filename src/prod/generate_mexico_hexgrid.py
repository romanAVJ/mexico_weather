#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Mexico Hexagon Grid Generator

This script generates a hexagonal grid covering Mexico using the H3 library,
saves it as a Parquet file locally, and uploads it to S3.

Author: Generated based on S1_1__mexico_hexagons.ipynb
Date: May 2, 2025
"""

import os
import logging
import argparse
import yaml
from typing import Optional, Dict, Any, List

import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
import h3
import boto3
import awswrangler as wr
from botocore.exceptions import ClientError
from INEGIpy import MarcoGeoestadistico

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from a YAML file

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        Dictionary containing configuration parameters
    """
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            logger.info(f"Configuration loaded from {config_path}")
            return config.get('mexico_hexgrid', {})
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise


def get_mexico_shape(buffer_distance_meters: int = 10000) -> (gpd.GeoDataFrame, gpd.GeoDataFrame):
    """
    Get Mexico shapes from INEGI API with an optional buffer

    Args:
        buffer_distance_meters: Buffer distance in meters to apply to Mexico shapes

    Returns:
        GeoDataFrame containing Mexico shapes with no buffer and another with buffer applied
    """
    try:
        inegi_api = MarcoGeoestadistico()
        gdf_mexico = inegi_api.Entidades()

        # Buffer
        gdf_mexico_buffered = gdf_mexico.copy()
        # Convert to metric CRS, apply buffer, convert back to WGS84
        gdf_mexico_buffered['geometry'] = (
            gdf_mexico_buffered.to_crs(epsg=6372)['geometry']
            .buffer(buffer_distance_meters)
            .to_crs(epsg=4326)
        )
        return gdf_mexico, gdf_mexico_buffered

    except Exception as e:
        logger.error(f"Error getting Mexico shapes: {e}")
        raise


def generate_h3_grid(gdf: gpd.GeoDataFrame, gdf_buffered: gpd.GeoDataFrame, resolution: int = 5) -> gpd.GeoDataFrame:
    """
    Generate H3 hexagonal grid covering the buffered shape

    Args:
        gdf: GeoDataFrame containing shapes
        gdf_buffered: GeoDataFrame containing buffered shapes
        resolution: H3 resolution level (0-15), with lower values being larger hexagons

    Returns:
        GeoDataFrame containing H3 hexagons covering the buffered shape
    """
    try:
        logger.info(f"Generating H3 hexagonal grid at resolution {resolution}")

        # Union all geometries to get a single polygon
        union_buffered = gdf_buffered.geometry.buffer(0).union_all()
        union = gdf.geometry.buffer(0).union_all()

        # Generate H3 hexagons
        h3_cells = list(set(h3.geo_to_cells(union_buffered, res=resolution)))
        logger.info(f"Generated {len(h3_cells)} H3 hexagons")

        # Convert to DataFrame
        df_h3 = pd.DataFrame(h3_cells, columns=['h3_id'])

        # Convert H3 indices to geometries
        logger.info("Converting H3 indices to geometries")
        df_h3['geometry'] = df_h3['h3_id'].apply(h3.cell_to_boundary)

        # H3 returns coordinates as (lat, lng) but shapely expects (lng, lat)
        df_h3['geometry'] = df_h3['geometry'].apply(
            lambda coords: Polygon([(x[1], x[0]) for x in coords])
        )

        # Convert to GeoDataFrame
        gdf_h3 = gpd.GeoDataFrame(df_h3, geometry='geometry', crs="EPSG:4326")

        # Filter to keep only hexagons that intersect with
        logger.info("Filtering hexagons to keep only those intersecting with ")
        gdf_h3 = gdf_h3[gdf_h3.geometry.intersects(union)]

        # Add centroid coordinates
        gdf_h3['longitude'] = gdf_h3['geometry'].apply(lambda x: x.centroid.x)
        gdf_h3['latitude'] = gdf_h3['geometry'].apply(lambda x: x.centroid.y)

        logger.info(f"Final hexagonal grid contains {len(gdf_h3)} hexagons")
        return gdf_h3

    except Exception as e:
        logger.error(f"Error generating H3 grid: {e}")
        raise


def save_to_parquet(gdf: gpd.GeoDataFrame, output_path: str) -> str:
    """
    Save GeoDataFrame to Parquet file

    Args:
        gdf: GeoDataFrame to save
        output_path: Path where to save the Parquet file

    Returns:
        Path to the saved file
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        logger.info(f"Saving to parquet file: {output_path}")
        gdf.to_parquet(output_path)
        logger.info(f"Successfully saved to {output_path}")
        return output_path

    except Exception as e:
        logger.error(f"Error saving to parquet: {e}")
        raise


def upload_to_s3(file_path: str, bucket_name: str, s3_path: str) -> bool:
    """
    Upload a file to S3

    Args:
        file_path: Local path to the file
        bucket_name: S3 bucket name
        s3_path: Path within the S3 bucket

    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Uploading {file_path} to s3://{bucket_name}/{s3_path}")
        session = boto3.Session(profile_name='beway')
        s3_client = session.client('s3')
        wr.s3.to_parquet(
            df=pd.read_parquet(file_path),
            path=f"s3://{bucket_name}/{s3_path}",
            dataset=True,
            mode="overwrite",
            boto3_session=session
        )
        logger.info(f"Successfully uploaded to s3://{bucket_name}/{s3_path}")
        return True

    except ClientError as e:
        logger.error(f"Error uploading to S3: {e}")
        return False


def main(config_path: Optional[str] = None) -> None:
    """
    Main function to run the entire process

    Args:
        config_path: Path to configuration file
    """
    try:
        logger.info("Starting hexagon grid generation")

        # Load configuration
        if config_path:
            config = load_config(config_path)
        else:
            # Default values if no config provided
            config = {
                "output_path": "data/mexico_hexgrid.parquet",
                "s3": {
                    "enabled": False,
                    "bucket_name": "",
                    "s3_path": ""
                },
                "hex": {
                    "resolution": 5,
                    "buffer_size": 10000
                }
            }

        # Extract configuration values
        resolution = config.get("hex").get("resolution")
        buffer_size = config.get("hex").get("buffer_size")
        output_path = config.get("output_path")

        # Get shapes with buffer
        logger.info("Fetching shapes...")
        gdf_mexico, gdf_buffered_mexico = get_mexico_shape(buffer_distance_meters=buffer_size)

        # Generate hexagonal grid
        logger.info("Generating hexagonal grid...")
        gdf_h3_mexico = generate_h3_grid(gdf_mexico, gdf_buffered_mexico, resolution=resolution)

        # Save locally
        logger.info("Saving to local Parquet file...")
        saved_path = save_to_parquet(gdf_h3_mexico, output_path)

        # Upload to S3 if enabled in config
        logger.info("Saving to S3...")
        s3_config = config.get("s3")
        if s3_config.get("enabled"):
            bucket_name = s3_config.get("bucket_name")
            s3_path = s3_config.get("s3_path")

            if bucket_name and s3_path:
                upload_to_s3(saved_path, bucket_name, s3_path)
            else:
                logger.warning("S3 upload enabled but missing bucket_name or s3_path")

        logger.info("Mexico hexagon grid generation completed successfully! Bye.")

    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Mexico hexagonal grid using H3")
    parser.add_argument(
        "--config",
        type=str,
        help="Path to configuration file",
        default="src/prod/config.yaml"
    )

    args = parser.parse_args()
    main(config_path=args.config)
