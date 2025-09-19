from pathlib import Path
import mlflow
import uuid
from mlflow.projects import run as mlflow_run
from mlflow.tracking import MlflowClient
import yaml
from typing import Any
import argparse


def load_config(path: str) -> dict[str, Any]:
    p = Path(path)
    if p.suffix in {".yaml", ".yml"}:
        return yaml.safe_load(p.read_text())
    else:
        raise ValueError(f"Unsupported config file format: {p.suffix}")


def main(experiment_name: str = None):
    cfg = load_config("./experiments.yaml")
    tracking_uri = cfg.get("tracking_uri")
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)
    default_env_manager = cfg.get("default_env_manager", "local")
    for exp in cfg["experiments"]:
        if experiment_name and exp["name"] != experiment_name:
            continue
        project_uri = exp["project_uri"]
        entry_point = exp.get("entry_point", "main")
        experiment_name = exp.get("experiment_name", project_uri.split("/")[-1])
        params = exp.get("parameters", {})
        env_manager = exp.get("env_manager", default_env_manager)
        tags = exp.get("tags", {})
        submitted = mlflow_run(
            uri=project_uri,
            entry_point=entry_point,
            parameters=params,
            experiment_name=experiment_name,
            env_manager=env_manager,
            backend="local",
            run_name=f"{exp.get('name')}-{uuid.uuid4().hex[:4]}"
        )
        submitted.wait()
        run_id = submitted.run_id
        if tags:
            client = MlflowClient()
            for k, v in tags.items():
                client.set_tag(run_id, k, v)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment_name", type=str, default=None, help="Experiment name")
    args = parser.parse_args()
    main(args.experiment_name)
