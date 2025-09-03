from dataclasses import dataclass

import logging

import os

import duckdb
import time
from datetime import datetime, UTC
from typing import Dict, List, Optional, Tuple
import polars as pl
import psycopg
from google.cloud import firestore
from typing import Protocol, runtime_checkable

from utils.steam_api import get_app_reviews
from utils.views import create_view_if_not_exists


@runtime_checkable
class DbClient(Protocol):  # Interface for database clients
    def load_latest_timestamps(self) -> dict[str, datetime]: ...
    def update_latest_timestamps(self, updates: dict[str, datetime]) -> None: ...


def make_db_client() -> DbClient:  # Factory method for database clients
    if os.getenv("POSTGRES_HOST"):
        return PostgresDbClient.from_env()
    else:
        return FirestoreDbClient.from_env()


class PostgresDbClient(DbClient):
    def __init__(self, conn: psycopg.Connection, table: str = "games_last_processed_reviews"):
        self._conn = conn
        self._table = table

    @classmethod
    def from_env(cls):
        host = os.getenv("POSTGRES_HOST", "localhost")
        pg_user = os.getenv("POSTGRES_USER", "postgres")
        pg_password = os.getenv("POSTGRES_PASSWORD", "postgres")
        pg_port = os.getenv("POSTGRES_PORT", "5433")
        conn_str = os.getenv("POSTGRES_DSN", f"postgresql://{pg_user}:{pg_password}@{host}:{pg_port}/games_scraping")
        conn = psycopg.connect(conn_str)
        return cls(conn=conn)

    def __str__(self):
        return f"PostgresDbClient(table={self._table})"

    def update_latest_timestamps(self, updates: dict[str, datetime]) -> None:
        if not updates:
            return
        sql = f"""
                    INSERT INTO {self._table} (game_id, last_processed_timestamp)
                    VALUES (%s, %s)
                    ON CONFLICT (game_id)
                    DO UPDATE SET last_processed_timestamp = EXCLUDED.last_processed_timestamp
                """
        rows = [(k, v) for k, v in updates.items()]
        with self._conn.cursor() as cur:
            cur.executemany(sql, rows)
        self._conn.commit()

    def load_latest_timestamps(self) -> dict[str, datetime]:
        sql = f"SELECT game_id, last_processed_timestamp FROM {self._table}"
        out: Dict[str, datetime] = {}
        with self._conn.cursor() as cur:
            cur.execute(sql)
            for game_id, ts in cur.fetchall():
                if ts is not None:
                    out[str(game_id)] = ts.replace(tzinfo=UTC)
        return out


class FirestoreDbClient(DbClient):
    def __init__(self, client: firestore.Client, collection: str = "games"):
        os.environ["FIRESTORE_EMULATOR_HOST"] = os.environ.get("FIRESTORE_EMULATOR_HOST", "localhost:8080")
        self._client = client
        self._collection = client.collection(collection)

    @classmethod
    def from_env(cls):
        project = os.getenv("FIRESTORE_PROJECT", "steam-games-recommender")
        client = firestore.Client(project)
        return cls(client=client)

    def __str__(self):
        return f"FirestoreDbClient(collection={self._collection})"

    def update_latest_timestamps(self, updates: dict[str, datetime]) -> None:
        batch = self._client.batch()  # changes
        for appid, timestamp in updates.items():
            doc_ref = self._collection.document(appid)
            batch.set(doc_ref, {"latest_timestamp": timestamp}, merge=True)
        batch.commit()

    def load_latest_timestamps(self) -> dict[str, datetime]:
        out: dict[str, datetime] = {}
        for doc in self._collection.stream():
            data = doc.to_dict() or {}
            ts = data.get("latest_timestamp")
            if ts:
                out[doc.id] = ts
        return out


@dataclass(frozen=True, slots=True)
class App:
    app_id: str
    name: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "App":
        return cls(
            app_id=str(data["game_id"]),
            name=data.get("game_name")
        )

    def __str__(self):
        return f"{self.app_id} ({self.name})"

    def __repr__(self):
        return f"App(app_id={self.app_id}, name={self.name})"


class ReviewProcessor:
    def __init__(self, db_client: DbClient, batch_size: int = 100_000):
        self.logger = logging.getLogger(__name__ + ".ReviewProcessor")
        self.db = db_client
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

        # Cache for latest timestamps - minimize reads
        self._timestamp_cache: Dict[str, datetime] = {}
        self._timestamp_updates: Dict[str, datetime] = {}  # Pending updates

    def load_latest_timestamps_cache(self) -> None:
        """Load all timestamps in a single batch operation"""
        #
        self.logger.info("Loading latest timestamps cache from db...")
        self._timestamp_cache = self.db.load_latest_timestamps()
        self.logger.info(f"Loaded {len(self._timestamp_cache)} timestamps from cache")

    def get_latest_timestamp(self, appid: str) -> Optional[datetime]:
        """Get latest timestamp from cache"""
        return self._timestamp_cache.get(appid)

    def update_latest_timestamp(self, appid: str, timestamp: datetime) -> None:
        """Update timestamp in cache and mark for batch update"""
        self._timestamp_cache[appid] = timestamp
        self._timestamp_updates[appid] = timestamp

    def flush_timestamp_updates(self) -> None:
        """Batch update all pending timestamp changes to db"""
        if not self._timestamp_updates:
            return

        self.logger.info(f"Flushing {len(self._timestamp_updates)} timestamp updates to {self.db}...")

        self.db.update_latest_timestamps(self._timestamp_updates)

        self._timestamp_updates.clear()
        self.logger.info("Timestamp updates flushed successfully")

    def create_review_record(self, review: dict, appid: str) -> dict:
        """Create a review record from Steam API response"""
        author = review["author"]
        return {
            "rec_id": int(review["recommendationid"]),
            "author_id": int(author["steamid"]),
            "appid": int(appid),
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

    def fetch_reviews_for_app(self, app: App) -> Tuple[List[dict], bool]:
        """
        Fetch new reviews for a given app.
        Returns (reviews_list, has_new_reviews)
        """
        appid = app.app_id
        cached_timestamp = self.get_latest_timestamp(appid)
        user_reviews = []

        # Get first batch to check if there are new reviews
        try:
            self.logger.info(f"Fetching initial reviews for app {app}")
            data = self._fetch_reviews_with_retry(app=app, cursor="*")
        except Exception as e:
            self.logger.error(f"Error fetching initial reviews for app {appid}: {e}")
            return [], False

        if not data.get("reviews"):
            self.logger.warning(f"No reviews found for app {app}")
            return [], False

        latest_review_timestamp = datetime.fromtimestamp(data["reviews"][0]["timestamp_created"], UTC)

        # Check if we have new reviews
        if cached_timestamp and latest_review_timestamp <= cached_timestamp:
            self.logger.info(f"Skipping app {app} - no new reviews since {cached_timestamp}")
            return [], False

        self.logger.info(f"Processing new reviews for app {app}")

        # Process reviews from first batch
        new_reviews = self._process_reviews_batch(data["reviews"], appid, cached_timestamp)
        user_reviews.extend(new_reviews)

        # Update latest timestamp in cache (will be flushed later)
        self.update_latest_timestamp(appid, latest_review_timestamp)

        # Continue with pagination if needed
        total_reviews = data.get("query_summary", {}).get("total_reviews", 0)
        if total_reviews > 100:
            cursor = data.get("cursor")
            user_reviews.extend(self._fetch_remaining_reviews(app, cursor, cached_timestamp, total_reviews))

        return user_reviews, True

    def _process_reviews_batch(self, reviews: List[dict], appid: str, cutoff_timestamp: Optional[datetime]) -> List[
        dict]:
        """Process a batch of reviews, filtering by timestamp"""
        processed_reviews = []
        cutoff_ts = int(cutoff_timestamp.timestamp()) if cutoff_timestamp else 0

        for review in reviews:
            if review["timestamp_created"] <= cutoff_ts:
                continue
            processed_reviews.append(self.create_review_record(review, appid))

        return processed_reviews

    def _fetch_remaining_reviews(self, app: App, cursor: str,
                                 cutoff_timestamp: Optional[datetime], total_reviews: int) -> List[dict]:
        """Fetch remaining reviews using pagination"""
        user_reviews = []
        appid = app.app_id
        total_iter = total_reviews // 100 + (1 if total_reviews % 100 > 0 else 0)

        self.logger.info(f"Resolved {total_iter} iterations for app {app} (all history)")
        for i in range(1, total_iter):
            try:
                self.logger.info(f"Fetching reviews for app {app} - iteration {i}/{total_iter}")
                data = self._fetch_reviews_with_retry(app=app, cursor=cursor)
            except Exception as e:
                logging.error(f"Error on iteration {i} for app {app}: {e}")
                time.sleep(10)
                continue

            if not data.get("reviews"):
                break

            # Check if we've gone past our cutoff
            oldest_in_batch = datetime.fromtimestamp(data["reviews"][-1]["timestamp_created"], UTC)
            if cutoff_timestamp and oldest_in_batch <= cutoff_timestamp:
                self.logger.info(f"Stopping pagination for app {app}.")
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
    def _fetch_reviews_with_retry(app: App, cursor: str, max_retries: int = 3) -> Optional[dict]:
        """Fetch reviews with retry mechanism"""
        for attempt in range(max_retries):
            try:
                return get_app_reviews(
                    appid=app.app_id,
                    filt="recent",
                    cursor=cursor
                )
            except Exception as e:
                logging.warning(f"Error fetching reviews for app {app}: {e}. Retrying...")
                if attempt == max_retries - 1:
                    raise
                time.sleep(10 * (attempt + 1))  # Exponential backoff


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    scrape_date = datetime.now(UTC).date()
    db = make_db_client()
    processor = ReviewProcessor(db, batch_size=100_000)
    processor.load_latest_timestamps_cache()  # Single read operation

    duckdb_conn = duckdb.connect('../data/steam.duckdb', read_only=True)
    duckdb_conn.sql(f"""SET s3_region='us-east-1';
                    SET s3_url_style='path';
                    SET s3_use_ssl=false;
                    SET s3_endpoint='{os.environ.get("MINIO_ENDPOINT_URL", "localhost:9000")}';
                    SET s3_access_key_id='';
                    SET s3_secret_access_key='';""")
    recommended_games = duckdb_conn.sql("SELECT game_id, game_name FROM stg_games").pl()
    duckdb_conn.close()
    reviews = pl.DataFrame(infer_schema_length=None, schema=processor.schema)

    batch_num = 0
    processed_count = 0

    for item, game in enumerate(recommended_games.iter_rows(named=True)):
        app = App.from_dict(game)

        try:
            logging.info(f"Processing app ({item + 1}/{len(recommended_games)}) ...")
            app_reviews, has_new_reviews = processor.fetch_reviews_for_app(app)

            if not has_new_reviews:
                continue

            if app_reviews:
                reviews = pl.concat([
                    reviews,
                    pl.DataFrame(app_reviews, infer_schema_length=None)
                ], how='vertical')
                processed_count += len(app_reviews)
                logging.info(f"Added {len(app_reviews)} reviews for app {app.app_id} ({app.name}). Total: {processed_count}")

            # Batch write reviews and flush timestamp updates
            if len(reviews) >= processor.batch_size:
                logging.info(f"Writing batch {batch_num} with {len(reviews)} reviews...")
                scrape_date = datetime.now(UTC).date()
                reviews.write_parquet(f"s3://raw/reviews/steam_reviews_{scrape_date}_{batch_num}.parquet",
                                      storage_options={"aws_access_key_id": 'minioadmin',
                                                       "aws_secret_access_key": 'minioadmin',
                                                       "aws_region": "us-east-1",
                                                       "aws_endpoint_url": os.environ.get("MINIO_ENDPOINT_URL",
                                                                                          "http://localhost:9000")})
                processor.flush_timestamp_updates()

                # Reset for next batch
                reviews = pl.DataFrame(infer_schema_length=None, schema=processor.schema)
                batch_num += 1
                processed_count = 0

        except Exception as e:
            logging.error(f"Error processing app {app.app_id} ({app.name}): {e}")
            # Flush any pending updates before continuing
            processor.flush_timestamp_updates()
            continue

    # Final write
    if len(reviews) > 0:
        logging.info(f"Writing final batch {batch_num} with {len(reviews)} reviews...")
        reviews.write_parquet(f"s3://raw/reviews/steam_reviews_{scrape_date}_{batch_num}.parquet",
                              storage_options={"aws_access_key_id": 'minioadmin',
                                               "aws_secret_access_key": 'minioadmin',
                                               "aws_region": "us-east-1",
                                               "aws_endpoint_url": os.environ.get("MINIO_ENDPOINT_URL",
                                                                                  "http://localhost:9000")})

    # Final flush of any remaining timestamp updates
    processor.flush_timestamp_updates()
    logging.info("Processing completed successfully!")


if __name__ == "__main__":
    main()
    # Create raw_reviews view if it doesn't exist
    duckdb_conn = duckdb.connect('../data/steam.duckdb', read_only=False)
    create_view_if_not_exists(duckdb_conn, "raw_reviews", "s3://raw/reviews/steam_reviews_*.parquet",)
    duckdb_conn.close()
