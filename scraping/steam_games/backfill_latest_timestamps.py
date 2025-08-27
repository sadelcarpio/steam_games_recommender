import logging

import duckdb
import os
from google.cloud import firestore


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    duckdb_conn = duckdb.connect('../data/steam.duckdb', read_only=True)
    duckdb_conn.sql(f"""SET s3_region='us-east-1';
                        SET s3_url_style='path';
                        SET s3_use_ssl=false;
                        SET s3_endpoint='{os.environ.get("MINIO_ENDPOINT_URL", "localhost:9000")}';
                        SET s3_access_key_id='';
                        SET s3_secret_access_key='';""")
    latest_timestamps_df = duckdb_conn.sql("SELECT game_id, MAX(timestamp_created) AS "
                                           "latest_timestamp FROM stg_reviews GROUP BY game_id").pl()

    os.environ["FIRESTORE_EMULATOR_HOST"] = os.environ.get("FIRESTORE_EMULATOR_HOST", "localhost:8080")
    db = firestore.Client(project="steam-games-recommender")

    total = 0
    for chunk in latest_timestamps_df.iter_slices(1000):
        batch = db.batch()
        for game_id, latest_ts in chunk.iter_rows():
            doc_ref = db.collection("games").document(str(game_id))
            batch.set(doc_ref, {"latest_timestamp": latest_ts}, merge=True)
        batch.commit()
        total += len(chunk)
        logging.info(f"Commited batch of {len(chunk)} documents. Total: {total} documents committed so far.")
