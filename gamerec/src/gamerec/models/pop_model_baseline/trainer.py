import polars as pl


class PopularityTrainer:
    def fit(self, games_df: pl.DataFrame) -> pl.DataFrame:
        return (games_df.select(
            pl.all().sort_by("game_num_positive_reviews", descending=True)
            .over("game_review_month", mapping_strategy="explode"))
                .sort("game_review_month", descending=True))
