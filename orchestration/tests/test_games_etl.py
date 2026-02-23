from games_etl import dag_instance


def test_dag_loaded():
    assert dag_instance is not None


def test_dag_id():
    assert dag_instance.dag_id == "games_etl_pipeline"


def test_schedule():
    assert dag_instance.schedule == "0 10 * * WED#1"


def test_catchup():
    assert dag_instance.catchup is False


def test_task_count():
    assert len(dag_instance.task_ids) == 4


def test_task_ids():
    assert set(dag_instance.task_ids) == {
        "get_all_candidate_ids",
        "run_dbt_antijoin",
        "start_game_scraping",
        "run_dbt_models",
    }


def test_task_order():
    get_all_candidate_ids = dag_instance.get_task("get_all_candidate_ids")
    run_dbt_antijoin = dag_instance.get_task("run_dbt_antijoin")
    start_game_scraping = dag_instance.get_task("start_game_scraping")

    assert "run_dbt_antijoin" in get_all_candidate_ids.downstream_task_ids
    assert "start_game_scraping" in run_dbt_antijoin.downstream_task_ids
    assert "run_dbt_models" in start_game_scraping.downstream_task_ids
