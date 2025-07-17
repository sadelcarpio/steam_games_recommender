import os

import polars as pl
from google.cloud import firestore

if __name__ == '__main__':
    os.environ["FIRESTORE_EMULATOR_HOST"] = "localhost:8080"
    parquet_path = "../../data/raw/reviews/steam_reviews_*.parquet"
    df = pl.read_parquet(parquet_path, columns=["appid", "timestamp_created"])
    df = df.with_columns(
        pl.from_epoch("timestamp_created", time_unit="s")
    )
    latest_reviews = df.sort("timestamp_created", descending=True).unique(subset=["appid"], keep="first")
    records = latest_reviews.to_dicts()
    num_records = len(records)

    db = firestore.Client(project="steam-games-recommender")
    batch = db.batch()
    batch_size = 0
    for i, record in enumerate(records):
        doc_ref = db.collection("games").document(str(record["appid"]))
        batch.set(doc_ref, {"latest_timestamp": record["timestamp_created"]})
        batch_size += 1
        print(f"Uploading record {i}/{num_records}: {record}")
        if batch_size == 500:
            batch.commit()
            batch = db.batch()
            batch_size = 0

    if batch_size > 0:
        batch.commit()

    print(f"Uploaded {num_records} records to Firestore.")
