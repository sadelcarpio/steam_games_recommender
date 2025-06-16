from datetime import datetime

import pandas as pd
from feast import FeatureStore

if __name__ == "__main__":
    store = FeatureStore(repo_path=".")
    entity_df = pd.DataFrame.from_dict(
        {'user_id': [76561199859057797], 'event_timestamp': [datetime.now()]}
    )

    training_df = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "user_features:user_num_games_owned",
            "user_features:user_num_reviews"
        ],
    ).to_df()
    print(training_df.head())
