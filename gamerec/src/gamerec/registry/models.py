import mlflow


def promote_model(experiment_name: str,
                  metric_name: str,
                  stage: str = "Production",
                  greater_is_better: bool = True,
                  filter_string: str | None = None,
                  tracking_uri: str | None = None) -> str:
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)
    client = mlflow.MlflowClient()
    exp = client.get_experiment_by_name(experiment_name)
    if exp is None:
        raise ValueError(f"Experiment {experiment_name} not found")
    order_dir = "DESC" if greater_is_better else "ASC"
    order_by = [f"metrics.{metric_name} {order_dir}"]
    if filter_string is None:
        filter_string = "attribute.status = 'FINISHED' and run_name = 'eval'"  # differentiate between train and eval metrics
    runs = client.search_runs(
        experiment_ids=[exp.experiment_id],
        filter_string=filter_string,  # sorts by chosen metric
        order_by=order_by,
        max_results=100
    )
    if not runs:
        raise ValueError(f"No runs found for experiment {experiment_name}")
    best_run = next((r for r in runs if metric_name in r.data.metrics), None)
    if best_run is None:
        raise ValueError(f"No run has metric '{metric_name}'")
    models = client.search_model_versions(f"run_id = '{best_run.info.run_id}'")
    if not models:
        # Search for parent run
        parent_run_id = best_run.data.tags.get("mlflow.parentRunId")
        models = client.search_model_versions(f"run_id = '{parent_run_id}'")
    model_version = models[0]
    client.set_registered_model_alias(
        name=model_version.name,
        alias=stage,
        version=model_version.version
    )
    return f"Promoted {model_version.name}:{model_version.version} to alias {stage}"
