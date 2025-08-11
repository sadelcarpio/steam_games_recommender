from utils.steam_api import download_all_steam_games
import os

if __name__ == "__main__":
    df = download_all_steam_games()
    df.write_parquet(f"s3://raw/games/steam_ids.parquet",
                     storage_options={"aws_access_key_id": 'minioadmin',
                                      "aws_secret_access_key": 'minioadmin',
                                      "aws_region": "us-east-1",
                                      "aws_endpoint_url": os.environ.get("MINIO_ENDPOINT_URL", "http://localhost:9000")})
