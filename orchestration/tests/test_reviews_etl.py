from reviews_etl import dag_instance


def test_dag_loaded():
    assert dag_instance is not None


def test_dag_id():
    assert dag_instance.dag_id == "reviews_etl_pipeline"


def test_schedule():
    assert dag_instance.schedule == "0 10 * * SUN"


def test_catchup():
    assert dag_instance.catchup is False


def test_task_count():
    assert len(dag_instance.task_ids) == 2


def test_task_ids():
    assert set(dag_instance.task_ids) == {"start_reviews_scraping", "run_dbt_models"}


def test_task_order():
    start_reviews_scraping = dag_instance.get_task("start_reviews_scraping")
    assert "run_dbt_models" in start_reviews_scraping.downstream_task_ids
