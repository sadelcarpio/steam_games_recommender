from typing import Any

import polars as pl
from mlflow.pyfunc import PythonModel


class RandomModel(PythonModel):
    def __init__(self, k: int = 10):
        self.k = k
        self.games: pl.DataFrame | None = None

    def load_context(self, context):
        self.games = pl.read_parquet(context.artifacts["games"])

    def predict(self, context, model_input, params: dict[str, Any] | None = None):
        recommended = model_input["current_month"].map_elements(
            lambda dt: self.games.filter(pl.col("game_release_month") < dt).sample(self.k)["game_id"].to_list(),
            return_dtype=pl.List(pl.Int64)
        )
        return recommended
