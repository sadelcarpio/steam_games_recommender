import duckdb


def should_skip_antijoin(table, ctx):
    import duckdb
    duckdb_conn = duckdb.connect('data/steam.duckdb', read_only=True)
    if table not in duckdb_conn.sql("SHOW TABLES").df().to_dict(orient="records"):
        return True
    return False
