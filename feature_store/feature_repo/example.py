from feast import FileSource, Entity, FeatureView, Field, Project, ValueType
from feast.data_format import ParquetFormat
from feast.types import Int64, Float32

project = Project(name="steam_recsys")

user = Entity(name="user_id", join_keys=["user_id"], value_type=ValueType.INT64)
game = Entity(name="game_id", join_keys=["game_id"], value_type=ValueType.INT64)

reviews_source = FileSource(
    name="reviews_source",
    path="../../data/preprocessed/training_dataset.parquet",
    file_format=ParquetFormat(),
    timestamp_field="timestamp_created"
)

user_features = FeatureView(
    name="user_features",
    entities=[user],
    ttl=None,
    schema=[
        Field(name="user_num_games_owned", dtype=Int64),
        Field(name="user_num_reviews", dtype=Int64),
    ],
    source=reviews_source
)

game_features = FeatureView(
    name="game_features",
    entities=[game],
    ttl=None,
    schema=[
        Field(name="game_total_reviews", dtype=Int64),
        Field(name="game_review_score", dtype=Float32),
    ],
    source=reviews_source
)
