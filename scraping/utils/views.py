import logging


def create_view_if_not_exists(db_conn, view: str):
    if (view,) not in db_conn.sql("SHOW TABLES").fetchall():
        db_conn.sql("""SET s3_region='us-east-1';
                    SET s3_url_style='path';
                    SET s3_use_ssl=false;
                    SET s3_endpoint='localhost:9000';
                    SET s3_access_key_id='';
                    SET s3_secret_access_key='';""")
        db_conn.sql(
            f"CREATE VIEW {view} AS SELECT * FROM read_parquet('s3://raw/reviews/steam_reviews_*.parquet')")
        logging.info("Created raw_games view")
    else:
        logging.info("raw_games view already exists. Skipping creation.")
