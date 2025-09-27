import logging
import os
import tempfile

import mlflow
import argparse
from gamerec.data import DataLoader, DuckDb
from gamerec.models.pop_model_baseline import PopularityTrainer, PopularityPyFunc

# Basic training loop
if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--duckdb_path", type=str, default="../../data/steam.duckdb")
    args.add_argument("--cutoff", type=str, default="2020-01-01")
    args.add_argument("--split_cutoff", type=str, default="2017-01-01")
    args.add_argument("--train_size", type=int, default=100_000)
    args.add_argument("--test_size", type=int, default=10_000)
    args.add_argument("--k", type=int, default=10)
    args.add_argument("--seed", type=int, default=42)
    args = args.parse_args()
    # Data loading
    logging.basicConfig(level=logging.INFO)
    mlflow.set_tracking_uri("http://localhost:5000")
    mlflow.set_experiment("steam-recsys")
    cutoff = args.cutoff
    split_boundary = args.split_cutoff
    seed = args.seed
    train_size = args.train_size
    test_size = args.test_size
    k = args.k
    duckdb_path = args.duckdb_path
    logging.info("Loading data ...")
    loader = DataLoader(DuckDb(conn_str=args.duckdb_path))
    training_data = loader.load_data(cutoff=cutoff, dataset_type="game")
    ground_truth_users = loader.load_data(cutoff=cutoff, dataset_type="user").to_pandas()
    logging.info("Splitting data ...")
    train_df = ground_truth_users[ground_truth_users["current_month"] <= split_boundary].sample(train_size, random_state=seed).copy()
    test_df = ground_truth_users[ground_truth_users["current_month"] > split_boundary].sample(test_size, random_state=seed).copy()
    # Model training
    logging.info("Training model ...")
    trainer = PopularityTrainer()
    output_artifact = trainer.fit(training_data)  # the whole dataset is needed to look up previous month
    # Log model
    mlflow.log_params({
        "model": "popularity_baseline",
        "k": k,
        "cutoff": cutoff,
        "split_boundary": split_boundary,
        "train_size": train_size,
        "test_size": test_size,
        "seed": seed
    })
    logging.info("Logging model ...")
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "games_score.parquet")
        output_artifact.write_parquet(path)
        model_info = mlflow.pyfunc.log_model(
            python_model=PopularityPyFunc(k=10),
            artifacts={"games_score": path},
            name="baseline-pop-model",
            registered_model_name="baseline-pop-model"
        )
    # Compute predictions
    logging.info("Converting array data to list in pandas df ...")
    train_df["future_game_ids"] = train_df["future_game_ids"].map(lambda x: x.tolist())
    test_df["future_game_ids"] = test_df["future_game_ids"].map(lambda x: x.tolist())
    # Log datasets
    logging.info("Logging datasets ...")
    train_dataset = mlflow.data.from_pandas(
        train_df,
        targets="future_game_ids",
        name="ground_truth_users_train_2010-2017"
    )
    test_dataset = mlflow.data.from_pandas(
        test_df,
        targets="future_game_ids",
        name="ground_truth_users_test_2017-2018"
    )
    mlflow.log_input(train_dataset, context="training")
    mlflow.log_input(test_dataset, context="testing")
    with mlflow.start_run(run_name="train", nested=True):
        logging.info("Evaluating model on train data ...")
        mlflow.models.evaluate(
            model=model_info.model_uri,
            data=train_dataset,
            extra_metrics=[
                mlflow.metrics.precision_at_k(5),
                mlflow.metrics.precision_at_k(10),
                mlflow.metrics.recall_at_k(5),
                mlflow.metrics.recall_at_k(10),
                mlflow.metrics.ndcg_at_k(5),
                mlflow.metrics.ndcg_at_k(10)
            ]
        )
    with mlflow.start_run(run_name="eval", nested=True):
        logging.info("Evaluating model on test data ...")
        mlflow.models.evaluate(
            model=model_info.model_uri,
            data=test_dataset,
            extra_metrics=[
                mlflow.metrics.precision_at_k(5),
                mlflow.metrics.precision_at_k(10),
                mlflow.metrics.recall_at_k(5),
                mlflow.metrics.recall_at_k(10),
                mlflow.metrics.ndcg_at_k(5),
                mlflow.metrics.ndcg_at_k(10)
            ]
        )
