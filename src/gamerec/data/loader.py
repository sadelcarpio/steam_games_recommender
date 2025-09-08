from datetime import datetime
from typing import Protocol, runtime_checkable, Sequence

import torch
import polars as pl


@runtime_checkable
class OlapDb(Protocol):
    def load_data(self) -> pl.DataFrame: ...


class FoldGenerator(Protocol):
    def generate_folds(self, n: int, ts: Sequence[datetime] | None = None) -> : ...


class DataLoader:
    def __init__(self, db: OlapDb):
        self.db = db
        self.df = None

    def fetch_data(self):
        self.df = self.db.load_data()

    def _split(self,
               initial_time: str,
               final_time: str,
               splitter: FoldGenerator
               ):


    def split_by_size(self, initial_time: str, final_time: str, test_ratio: float):
        pass

    def split_by_time(self, initial_time: str, final_time: str, cutoff_time: str):
        pass


class ReviewsDataset(torch.utils.data.Dataset):
    def __init__(self, df: pl.DataFrame):
        self.reviews = df
    def __len__(self):
        return len(self.reviews)
    def __getitem__(self, idx):
        review = self.reviews.row(idx, named=True)
        return {
            "user_id": torch.tensor(review["user_index"]),
            "game_id": torch.tensor(review["game_index"]),
            "voted_up": torch.tensor(review["voted_up"], dtype=torch.float)
        }
