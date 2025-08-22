import duckdb
import logging
import os

from utils.steam_api import download_all_steam_games
from utils.views import create_view_if_not_exists

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    df = download_all_steam_games()
    df.write_parquet(f"s3://raw/games/steam_ids.parquet",
                     storage_options={"aws_access_key_id": 'minioadmin',
                                      "aws_secret_access_key": 'minioadmin',
                                      "aws_region": "us-east-1",
                                      "aws_endpoint_url": os.environ.get("MINIO_ENDPOINT_URL", "http://localhost:9000")})
    logging.info(f"Wrote {len(df)} rows to s3://raw/games/steam_ids.parquet")
    # Create app_ids view if it doesn't exist
    duckdb_conn = duckdb.connect('../data/steam.duckdb', read_only=False)
    create_view_if_not_exists(duckdb_conn, "app_ids", "s3://raw/games/steam_ids.parquet")
    duckdb_conn.close()
