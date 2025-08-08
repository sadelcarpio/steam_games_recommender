import os

import duckdb
import time
from datetime import datetime, UTC
from typing import Dict, List, Optional, Tuple
import polars as pl
from google.cloud import firestore

from utils.steam_api import get_app_reviews


class ReviewProcessor:
    def __init__(self, db_client: firestore.Client, batch_size: int = 100_000):
        self.db = db_client
        self.collection_ref = db_client.collection("games")
        self.batch_size = batch_size
        self.schema = {
            "rec_id": pl.Int64,
            "author_id": pl.Int64,
            "appid": pl.Int64,
            "playtime_forever": pl.Int64,
            "playtime_last_two_weeks": pl.Int64,
            "playtime_at_review": pl.Int64,
            "num_games_owned": pl.Int64,
            "num_reviews": pl.Int64,
            "last_played": pl.Int64,
            "language": pl.String,
            "review": pl.Utf8,
            "timestamp_created": pl.Int64,
            "timestamp_updated": pl.Int64,
            "voted_up": pl.Boolean,
            "votes_up": pl.Int64,
            "votes_funny": pl.Int64,
            "weighted_vote_score": pl.Float64,
            "comment_count": pl.Int64,
            "steam_purchase": pl.Boolean,
            "received_for_free": pl.Boolean,
            "written_during_early_access": pl.Boolean,
            "primarily_steam_deck": pl.Boolean,
            "scrape_date": pl.Date,
        }

        # Cache for latest timestamps - minimize Firestore reads
        self._timestamp_cache: Dict[str, datetime] = {}
        self._timestamp_updates: Dict[str, datetime] = {}  # Pending updates

    def load_latest_timestamps_cache(self) -> None:
        """Load all timestamps in a single batch operation"""
        print("Loading latest timestamps cache from Firestore...")
        docs = self.collection_ref.stream()
        for doc in docs:
            data = doc.to_dict()
            game_id = doc.id
            latest_ts = data.get("latest_timestamp")
            if latest_ts:
                self._timestamp_cache[game_id] = latest_ts
        print(f"Loaded {len(self._timestamp_cache)} timestamps from cache")

    def get_latest_timestamp(self, appid: str) -> Optional[datetime]:
        """Get latest timestamp from cache"""
        return self._timestamp_cache.get(appid)

    def update_latest_timestamp(self, appid: str, timestamp: datetime) -> None:
        """Update timestamp in cache and mark for batch update"""
        self._timestamp_cache[appid] = timestamp
        self._timestamp_updates[appid] = timestamp

    def flush_timestamp_updates(self) -> None:
        """Batch update all pending timestamp changes to Firestore"""
        if not self._timestamp_updates:
            return

        print(f"Flushing {len(self._timestamp_updates)} timestamp updates to Firestore...")
        batch = self.db.batch()

        for appid, timestamp in self._timestamp_updates.items():
            doc_ref = self.collection_ref.document(appid)
            batch.set(doc_ref, {"latest_timestamp": timestamp}, merge=True)

        batch.commit()
        self._timestamp_updates.clear()
        print("Timestamp updates flushed successfully")

    def create_review_record(self, review: dict, appid: int) -> dict:
        """Create a review record from Steam API response"""
        author = review["author"]
        return {
            "rec_id": int(review["recommendationid"]),
            "author_id": int(author["steamid"]),
            "appid": appid,
            "playtime_forever": int(author["playtime_forever"]) if author.get("playtime_forever") else None,
            "playtime_last_two_weeks": int(author["playtime_last_two_weeks"]) if author.get(
                "playtime_last_two_weeks") else None,
            "playtime_at_review": int(author.get("playtime_at_review")) if author.get("playtime_at_review") else None,
            "num_games_owned": int(author["num_games_owned"]) if author.get("num_games_owned") else None,
            "num_reviews": int(author["num_reviews"]) if author.get("num_reviews") else None,
            "last_played": int(author["last_played"]) if author.get("last_played") else None,
            "language": review["language"],
            "review": review["review"],
            "timestamp_created": int(review["timestamp_created"]),
            "timestamp_updated": int(review["timestamp_updated"]),
            "voted_up": review["voted_up"],
            "votes_up": int(review["votes_up"]),
            "votes_funny": int(review["votes_funny"]),
            "weighted_vote_score": float(review["weighted_vote_score"]),
            "comment_count": int(review["comment_count"]),
            "steam_purchase": review["steam_purchase"],
            "received_for_free": review["received_for_free"],
            "written_during_early_access": review["written_during_early_access"],
            "primarily_steam_deck": review["primarily_steam_deck"],
            "scrape_date": datetime.now(UTC).date()
        }

    def fetch_reviews_for_app(self, appid: int, app_name: str) -> Tuple[List[dict], bool]:
        """
        Fetch new reviews for a given app.
        Returns (reviews_list, has_new_reviews)
        """
        appid_str = str(appid)
        cached_timestamp = self.get_latest_timestamp(appid_str)
        user_reviews = []

        # Get first batch to check if there are new reviews
        try:
            data = self._fetch_reviews_with_retry(app_id=appid_str, cursor="*")
        except Exception as e:
            print(f"Error fetching initial reviews for app {appid}: {e}")
            return [], False

        if not data.get("reviews"):
            print(f"No reviews found for app {appid} ({app_name})")
            return [], False

        latest_review_timestamp = datetime.fromtimestamp(data["reviews"][0]["timestamp_created"], UTC)

        # Check if we have new reviews
        if cached_timestamp and latest_review_timestamp <= cached_timestamp:
            print(f"Skipping app {appid} ({app_name}) - no new reviews since {cached_timestamp}")
            return [], False

        print(f"Processing new reviews for app {appid} ({app_name})")

        # Process reviews from first batch
        new_reviews = self._process_reviews_batch(data["reviews"], appid, cached_timestamp)
        user_reviews.extend(new_reviews)

        # Update latest timestamp in cache (will be flushed later)
        self.update_latest_timestamp(appid_str, latest_review_timestamp)

        # Continue with pagination if needed
        total_reviews = data.get("query_summary", {}).get("total_reviews", 0)
        if total_reviews > 100:
            cursor = data.get("cursor")
            user_reviews.extend(self._fetch_remaining_reviews(appid, app_name, cursor, cached_timestamp, total_reviews))

        return user_reviews, True

    def _process_reviews_batch(self, reviews: List[dict], appid: int, cutoff_timestamp: Optional[datetime]) -> List[
        dict]:
        """Process a batch of reviews, filtering by timestamp"""
        processed_reviews = []
        cutoff_ts = int(cutoff_timestamp.timestamp()) if cutoff_timestamp else 0

        for review in reviews:
            if review["timestamp_created"] <= cutoff_ts:
                continue
            processed_reviews.append(self.create_review_record(review, appid))

        return processed_reviews

    def _fetch_remaining_reviews(self, appid: int, cursor: str,
                                 cutoff_timestamp: Optional[datetime], total_reviews: int) -> List[dict]:
        """Fetch remaining reviews using pagination"""
        user_reviews = []
        total_iter = total_reviews // 100 + (1 if total_reviews % 100 > 0 else 0)

        for i in range(1, total_iter):
            try:
                data = self._fetch_reviews_with_retry(app_id=str(appid), cursor=cursor)
            except Exception as e:
                print(f"Error on iteration {i} for app {appid}: {e}")
                time.sleep(10)
                continue

            if not data.get("reviews"):
                break

            # Check if we've gone past our cutoff
            oldest_in_batch = datetime.fromtimestamp(data["reviews"][-1]["timestamp_created"], UTC)
            if cutoff_timestamp and oldest_in_batch <= cutoff_timestamp:
                # Process only the new reviews in this batch and stop
                new_reviews = self._process_reviews_batch(data["reviews"], appid, cutoff_timestamp)
                user_reviews.extend(new_reviews)
                break

            # All reviews in this batch are new
            batch_reviews = [self.create_review_record(review, appid) for review in data["reviews"]]
            user_reviews.extend(batch_reviews)
            cursor = data.get("cursor")

            if not cursor:
                break

        return user_reviews

    @staticmethod
    def _fetch_reviews_with_retry(app_id: str, cursor: str, max_retries: int = 3) -> Optional[dict]:
        """Fetch reviews with retry mechanism"""
        for attempt in range(max_retries):
            try:
                return get_app_reviews(
                    "https://store.steampowered.com/appreviews",
                    appid=app_id,
                    filt="recent",
                    cursor=cursor
                )
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                time.sleep(10 * (attempt + 1))  # Exponential backoff


def main():
    os.environ["FIRESTORE_EMULATOR_HOST"] = "localhost:8080"
    db = firestore.Client(project="steam-games-recommender")
    processor = ReviewProcessor(db, batch_size=1000)
    processor.load_latest_timestamps_cache()  # Single read operation

    duckdb_conn = duckdb.connect('../data/steam.duckdb', read_only=True)
    recommended_games = duckdb_conn.sql("SELECT appid FROM stg_games").pl()
    reviews = pl.DataFrame(infer_schema_length=None, schema=processor.schema)

    batch_num = 0
    processed_count = 0

    for item, game in enumerate(recommended_games.iter_rows(named=True)):
        appid = game["appid"]
        app_name = game["name"]

        try:
            app_reviews, has_new_reviews = processor.fetch_reviews_for_app(appid, app_name)

            if not has_new_reviews:
                continue

            if app_reviews:
                reviews = pl.concat([
                    reviews,
                    pl.DataFrame(app_reviews, infer_schema_length=None)
                ], how='vertical')
                processed_count += len(app_reviews)
                print(f"Added {len(app_reviews)} reviews for app {appid}. Total: {processed_count}")

            # Batch write reviews and flush timestamp updates
            if len(reviews) >= processor.batch_size:
                print(f"Writing batch {batch_num} with {len(reviews)} reviews...")
                scrape_date = datetime.now(UTC).date()
                reviews.write_parquet(f"s3://raw/reviews/steam_reviews_{scrape_date}_{batch_num}.parquet",
                                      storage_options={"aws_access_key_id": 'minioadmin',
                                                       "aws_secret_access_key": 'minioadmin',
                                                       "aws_region": "us-east-1",
                                                       "aws_endpoint_url": "http://localhost:9000"})
                processor.flush_timestamp_updates()  # Batch Firestore updates

                # Reset for next batch
                reviews = pl.DataFrame(infer_schema_length=None, schema=processor.schema)
                batch_num += 1
                processed_count = 0

        except Exception as e:
            print(f"Error processing app {appid}: {e}")
            # Flush any pending updates before continuing
            processor.flush_timestamp_updates()
            continue

    # Final write
    if len(reviews) > 0:
        reviews.write_parquet(f"../../data/raw/reviews_2/steam_reviews_{batch_num}.parquet")

    # Final flush of any remaining timestamp updates
    processor.flush_timestamp_updates()
    print("Processing completed successfully!")


if __name__ == "__main__":
    main()
