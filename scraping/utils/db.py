import os
from datetime import datetime, UTC
from typing import Protocol, runtime_checkable

import psycopg
from google.cloud import firestore


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
        out: dict[str, datetime] = {}
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
