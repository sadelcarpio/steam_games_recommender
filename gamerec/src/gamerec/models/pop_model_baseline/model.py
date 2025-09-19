from datetime import timedelta, datetime
from typing import Any

import polars as pl
from mlflow.pyfunc import PythonModel


class PopularityPyFunc(PythonModel):
    def __init__(self, k: int = 10):
        self.k = k
        self._month_to_recs = None

    def load_context(self, context):
        df = pl.read_parquet(context.artifacts["games_score"]).sort(
            ["game_review_month", "game_num_positive_reviews"],
            descending=[False, True],
        )
        grouped = df.group_by("game_review_month").agg(
            pl.col("game_id").head(self.k).alias("top_game_ids")
        )
        self._month_to_recs = {
            row["game_review_month"]: row["top_game_ids"]
            for row in grouped.iter_rows(named=True)
        }

    @staticmethod
    def _month_start_prev(dt: datetime):
        return (dt.replace(day=1) - timedelta(days=1)).replace(day=1)

    def predict(self, context, model_input, params: dict[str, Any] | None = None):
        # assumes model_input["current_month"] is datetime-like
        return model_input["current_month"].map_elements(lambda dt: self._month_to_recs.get(self._month_start_prev(dt), []))
