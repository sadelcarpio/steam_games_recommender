import logging
import os

import duckdb
import psycopg

TABLE_NAME = "games_last_processed_reviews"
BATCH_SIZE = 1000


def get_pg_connection() -> "psycopg.Connection":
    """
    Prefer POSTGRES_DSN if provided; otherwise rely on libpq env vars:
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD.
    """
    dsn = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5433/games_scraping")
    return psycopg.connect(dsn or "")


def ensure_table(conn: "psycopg.Connection") -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                game_id VARCHAR(255) PRIMARY KEY,
                last_processed_timestamp TIMESTAMP
            )
            """
        )
    conn.commit()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Read latest timestamps from DuckDB
    duckdb_conn = duckdb.connect("../data/steam.duckdb", read_only=True)
    duckdb_conn.sql(
        f"""
        SET s3_region='us-east-1';
        SET s3_url_style='path';
        SET s3_use_ssl=false;
        SET s3_endpoint='{os.environ.get("MINIO_ENDPOINT_URL", "localhost:9000")}';
        SET s3_access_key_id='';
        SET s3_secret_access_key='';
        """
    )
    latest_timestamps_df = duckdb_conn.sql(
        """
        SELECT game_id, MAX(timestamp_created) AS latest_timestamp
        FROM stg_reviews
        GROUP BY game_id
        """
    ).pl()
    duckdb_conn.close()

    # Connect to PostgreSQL
    conn = get_pg_connection()
    ensure_table(conn)

    upsert_sql = f"""
        INSERT INTO {TABLE_NAME} (game_id, last_processed_timestamp)
        VALUES (%s, %s)
        ON CONFLICT (game_id)
        DO UPDATE SET last_processed_timestamp = EXCLUDED.last_processed_timestamp
    """

    total = 0
    with conn.cursor() as cur:
        for chunk in latest_timestamps_df.iter_slices(BATCH_SIZE):
            params = []
            for game_id, latest_ts in chunk.iter_rows():
                params.append((str(game_id), latest_ts))
            if params:
                cur.executemany(upsert_sql, params)
                conn.commit()
                total += len(params)
                logging.info(
                    f"Committed batch of {len(params)} rows to '{TABLE_NAME}'. Total committed so far: {total}."
                )

    conn.close()
    logging.info("Backfill completed successfully.")
