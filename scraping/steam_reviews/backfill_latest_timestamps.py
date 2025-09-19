import logging
import os

import duckdb

from utils.db import make_db_client

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info("Reading from `stg_reviews` table for latest review timestamps.")
    with  duckdb.connect('../data/steam.duckdb', read_only=True) as duckdb_conn:
        duckdb_conn.sql(f"""SET s3_region='us-east-1';
                            SET s3_url_style='path';
                            SET s3_use_ssl=false;
                            SET s3_endpoint='{os.environ.get("MINIO_ENDPOINT_URL", "localhost:9000")}';
                            SET s3_access_key_id='';
                            SET s3_secret_access_key='';""")
        latest_timestamps_df = duckdb_conn.sql("SELECT game_id, MAX(timestamp_created) AS "
                                               "latest_timestamp FROM stg_reviews GROUP BY game_id").pl()
    latest_timestamps_dict = {str(elem["game_id"]): elem["latest_timestamp"] for elem in latest_timestamps_df.to_dicts()}
    db_client = make_db_client()
    db_client.update_latest_timestamps(latest_timestamps_dict)
    logging.info("Backfill completed successfully.")
