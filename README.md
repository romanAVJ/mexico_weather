# mexico_weather
Code to extract information of CONAGUA to estimate the national temperature and weather of all the country in the last years

# Usage Guide :rocket:

## 1. Generate Mexico Hexagonal Grid

To generate a hexagonal grid covering Mexico using H3, run the following command:

```bash
python src/prod/generate_mexico_hexgrid.py --config src/prod/config.yaml
```

This will:
- Generate a hexagonal grid covering Mexico at resolution 5
- Save the resulting GeoDataFrame as a Parquet file in your specified location
- Optionally upload the file to S3 if configured

### Configuration

The script uses a YAML configuration file with the following structure:

```yaml
mexico_hexgrid:
  output_path: "data/mexico_hexgrid.parquet"
  s3:
    enabled: true
    bucket_name: "your-bucket-name"
    s3_path: "path/to/mexico_hexgrid.parquet"
  hex:
    resolution: 5  # H3 resolution (lower = larger hexagons)
    buffer_size: 10000  # buffer size in meters
```

You can customize these parameters to adjust the:
- Output file location
- S3 upload settings
- Hexagon resolution (lower values = larger hexagons)
- Buffer size around Mexico's boundary

# Setup :hammer:
## Python environment
To create the python environment, you can run the following command:

```bash
conda create -n mexico-weather-env python=3.12
conda activate mexico-weather-env
pip install -r requirements.txt
```