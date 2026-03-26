import pytest

from pg_embeddings_sync import dag_instance


def test_dag_loaded():
    assert dag_instance is not None


def test_dag_id():
    assert dag_instance.dag_id == "pg_embeddings_sync"


def test_schedule():
    assert dag_instance.schedule == "0 10 * * WED#1"


# Bug: `load_dim_games_into_postgres` is defined inside the DAG body but never called,
# so it is never registered as a task. The DAG currently has 0 tasks.
@pytest.mark.xfail(reason="Bug: task `load_dim_games_into_postgres` is never instantiated inside the DAG body")
def test_task_count():
    assert len(dag_instance.task_ids) > 0
