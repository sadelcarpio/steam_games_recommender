import pytest


def test_table_not_exists_when_table_missing(mocker):
    mock_conn = mocker.MagicMock()
    mock_conn.sql.return_value.df.return_value.to_dict.return_value = [
        {"name": "other_table"}
    ]
    mocker.patch("duckdb.connect", return_value=mock_conn)
    from utils import table_not_exists

    assert table_not_exists("raw_games", ctx={}) is True


# Bug: the implementation checks `table not in list_of_dicts` where list_of_dicts is
# a list of {"name": ...} records. A string can never be found in a list of dicts,
# so the function always returns True. The test below documents the intended behavior
# and is marked xfail until the bug is fixed.
@pytest.mark.xfail(reason="Bug: 'in' checks against list of dicts, not strings")
def test_table_not_exists_returns_false_when_table_exists(mocker):
    mock_conn = mocker.MagicMock()
    mock_conn.sql.return_value.df.return_value.to_dict.return_value = [
        {"name": "raw_games"}
    ]
    mocker.patch("duckdb.connect", return_value=mock_conn)
    from utils import table_not_exists

    assert table_not_exists("raw_games", ctx={}) is False
