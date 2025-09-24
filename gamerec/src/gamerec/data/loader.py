from datetime import datetime
from typing import Protocol, runtime_checkable, Literal

import duckdb
import polars as pl
import torch


@runtime_checkable
class Tabular(Protocol):
    def to_polars(self) -> pl.DataFrame: ...


class DuckDbRelationAdapter(Tabular):
    def __init__(self, rel: "duckdb.DuckDBPyRelation"):
        self._rel = rel

    def to_polars(self) -> pl.DataFrame:
        return pl.from_arrow(self._rel.arrow())


class ReviewsDatasetFactory(Protocol):
    def from_tabular(self, t: Tabular) -> "ReviewsDataset": ...


class PolarsDfDatasetFactory(ReviewsDatasetFactory):
    def from_tabular(self, t: Tabular) -> "ReviewsDataset":
        return DataFrameReviewsDataset(t.to_polars())


class TorchDatasetFactory(ReviewsDatasetFactory):
    def from_tabular(self, t: Tabular) -> "ReviewsDataset":
        return TorchReviewsDataset(t.to_polars())


@runtime_checkable
class ReviewsDataset(Protocol):
    def split_data(self, cutoff: str) -> tuple["ReviewsDataset", "ReviewsDataset"]: ...


class TorchReviewsDataset(torch.utils.data.Dataset):
    def __init__(self, df: pl.DataFrame):
        self.df = df

    def split_data(self, cutoff: str) -> tuple["TorchReviewsDataset", "TorchReviewsDataset"]:
        pass

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx):
        return self.df.row(idx)


class DataFrameReviewsDataset(ReviewsDataset):
    def __init__(self, df: pl.DataFrame):
        self.df = df
    def split_data(self, cutoff: str) -> tuple["DataFrameReviewsDataset", "DataFrameReviewsDataset"]:
        return (
            DataFrameReviewsDataset(self.df.filter(pl.col("game_review_month") < cutoff)),
            DataFrameReviewsDataset(self.df.filter(pl.col("game_review_month") >= cutoff))
        )


@runtime_checkable
class OlapDb(Protocol):
    def load_game_features(self, cutoff: str) -> Tabular: ...
    def load_user_features(self, cutoff: str) -> Tabular: ...
    def load_reviews_data(self, cutoff: str) -> Tabular: ...


class DuckDb(OlapDb):
    def __init__(self, conn_str: str):
        self._conn = conn_str

    def _validate_cutoff(self, cutoff: str) -> None:
        try:
            datetime.strptime(cutoff, "%Y-%m")
        except ValueError:
            raise ValueError(f"Invalid cutoff date: {cutoff}")

    def load_game_features(self, cutoff: str) -> Tabular:
        self._validate_cutoff(cutoff)
        with duckdb.connect(self._conn) as duckdb_conn:
            rel = duckdb_conn.sql(f"SELECT * FROM game_features WHERE current_month <= '{cutoff}'")
        return DuckDbRelationAdapter(rel)

    def load_user_features(self, cutoff: str) -> Tabular:
        self._validate_cutoff(cutoff)
        with duckdb.connect(self._conn) as duckdb_conn:
            rel = duckdb_conn.sql(f"SELECT * FROM user_features WHERE current_month <= '{cutoff}'")
        return DuckDbRelationAdapter(rel)

    def load_reviews_data(self, cutoff: str) -> Tabular:
        self._validate_cutoff(cutoff)
        with duckdb.connect(self._conn) as duckdb_conn:
            rel = duckdb_conn.sql(f"SELECT * FROM training_features WHERE current_month <= '{cutoff}'")
        return DuckDbRelationAdapter(rel)


class DataLoader:
    def __init__(self, db: OlapDb):
        self.db = db

    def fetch_data(self, cutoff: str, dataset_type: Literal["game", "user", "reviews"]):
        if dataset_type == "game":
            return self.db.load_game_features(cutoff)
        elif dataset_type == "user":
            return self.db.load_user_features(cutoff)
        elif dataset_type == "reviews":
            return self.db.load_reviews_data(cutoff)
        else:
            raise ValueError(f"Invalid dataset type: {dataset_type}")
