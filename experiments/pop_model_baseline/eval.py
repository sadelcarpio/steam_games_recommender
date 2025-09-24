import argparse

import duckdb
import mlflow

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--eval_data", type=str, required=True)
    args = parser.parse_args()
    with duckdb.connect(args.eval_data, read_only=True) as duckdb_conn:
        duckdb_conn.sql(f"""SET s3_region='us-east-1';
                        SET s3_url_style='path';
                        SET s3_use_ssl=false;
                        SET s3_endpoint='localhost:9000';
                        SET s3_access_key_id='';
                        SET s3_secret_access_key='';""")
        ground_truth_users = duckdb_conn.sql("""
                                             WITH anchors AS (SELECT DISTINCT user_id, current_month
                                                              FROM training_features
                                                              WHERE voted_up = true
                                                                AND strftime(current_month, '%Y') = '2025' -- prefer date funcs over LIKE
                                             )
                                             SELECT a.user_id,
                                                    a.current_month,
                                                    (SELECT array_agg(f.game_id ORDER BY f.first_month, f.game_id)
                                                     FROM (SELECT tf.game_id, MIN(tf.current_month) AS first_month
                                                           FROM training_features AS tf
                                                           WHERE tf.user_id = a.user_id
                                                             AND tf.voted_up = true
                                                             AND tf.current_month >= a.current_month
                                                           GROUP BY tf.game_id) AS f) AS future_game_ids
                                             FROM anchors AS a
                                             ORDER BY a.user_id, a.current_month
                                             LIMIT 1000
                                             ;
                                             """).pl()
    my_model = mlflow.pyfunc.load_model("models:/m-2ba58bc4925144afbe3edd1bc7dd0b29")
    ground_truth_users = ground_truth_users.with_columns(
        my_model.predict(ground_truth_users).alias("predictions")
    )
    dataset = mlflow.data.from_pandas(
        ground_truth_users.to_pandas(),
        targets="future_game_ids",
        predictions="predictions",
        name="ground_truth_users_2025_limit_1k"
    )
    mlflow.models.evaluate(
        data=dataset,
        model_type="retriever",
        evaluators="default"
    )
