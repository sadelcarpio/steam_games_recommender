from training_features_etl import dag_instance


def test_dag_loaded():
    assert dag_instance is not None


def test_dag_id():
    assert dag_instance.dag_id == "training_features_etl_pipeline"


def test_schedule():
    assert dag_instance.schedule == "@monthly"


def test_catchup():
    assert dag_instance.catchup is True


def test_max_active_runs():
    assert dag_instance.max_active_runs == 1


def test_task_count():
    assert len(dag_instance.task_ids) == 1


def test_task_id():
    assert "run_dbt_models" in dag_instance.task_ids
