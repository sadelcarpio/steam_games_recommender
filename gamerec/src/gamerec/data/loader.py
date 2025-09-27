from datetime import datetime
from typing import Protocol, runtime_checkable, Literal, Callable

import duckdb
import polars as pl

@runtime_checkable
class OlapDb(Protocol):
    def load_game_features(self, cutoff: str) -> pl.DataFrame: ...
    def load_user_features(self, cutoff: str) -> pl.DataFrame: ...
    def load_reviews_data(self, cutoff: str) -> pl.DataFrame: ...


class DuckDb(OlapDb):
    def __init__(self, conn_str: str):
        self._conn = conn_str

    def _validate_cutoff(self, cutoff: str) -> None:
        try:
            datetime.strptime(cutoff, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid cutoff date: {cutoff}")

    def load_game_features(self, cutoff: str) -> pl.DataFrame:
        self._validate_cutoff(cutoff)
        with duckdb.connect(self._conn) as duckdb_conn:
            duckdb_conn.sql(f"""SET s3_region='us-east-1';
                                    SET s3_url_style='path';
                                    SET s3_use_ssl=false;
                                    SET s3_endpoint='localhost:9000';
                                    SET s3_access_key_id='';
                                    SET s3_secret_access_key='';""")
            rel = duckdb_conn.sql(f"SELECT * FROM game_features WHERE game_review_month <= '{cutoff}'").pl()
        return rel

    def load_user_features(self, cutoff: str) -> pl.DataFrame:
        self._validate_cutoff(cutoff)
        with duckdb.connect(self._conn) as duckdb_conn:
            duckdb_conn.sql(f"""SET s3_region='us-east-1';
                                                SET s3_url_style='path';
                                                SET s3_use_ssl=false;
                                                SET s3_endpoint='localhost:9000';
                                                SET s3_access_key_id='';
                                                SET s3_secret_access_key='';""")
            rel = duckdb_conn.sql(f"SELECT * FROM user_features WHERE current_month <= '{cutoff}'").pl()
        return rel

    def load_reviews_data(self, cutoff: str) -> pl.DataFrame:
        self._validate_cutoff(cutoff)
        with duckdb.connect(self._conn) as duckdb_conn:
            duckdb_conn.sql(f"""SET s3_region='us-east-1';
                                                SET s3_url_style='path';
                                                SET s3_use_ssl=false;
                                                SET s3_endpoint='localhost:9000';
                                                SET s3_access_key_id='';
                                                SET s3_secret_access_key='';""")
            rel = duckdb_conn.sql(f"SELECT * FROM training_features WHERE current_month <= '{cutoff}'").pl()
        return rel


class DataLoader:
    def __init__(self, db: OlapDb):
        self.db = db

    def load_data(self,
                   cutoff: str,
                   dataset_type: Literal["game", "user", "reviews"]) -> pl.DataFrame:
        if dataset_type == "game":
            df = self.db.load_game_features(cutoff)
        elif dataset_type == "user":
            df = self.db.load_user_features(cutoff)
        elif dataset_type == "reviews":
            df = self.db.load_reviews_data(cutoff)
        else:
            raise ValueError(f"Invalid dataset type: {dataset_type}")
        return df
