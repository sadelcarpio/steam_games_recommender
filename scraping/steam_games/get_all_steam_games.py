import duckdb
import logging
import os

from utils.steam_api import download_all_steam_games

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
    if "app_ids" not in duckdb_conn.sql("SHOW TABLES").df().to_dict(orient="records"):
        duckdb_conn.sql("CREATE VIEW app_ids AS SELECT * FROM read_parquet('s3://raw/games/steam_ids.parquet')")
        logging.info("Created app_ids view")
    else:
        logging.info("app_ids view already exists. Skipping creation.")
    duckdb_conn.close()
