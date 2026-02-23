def semantic_search(query: str, openai_client, limit: int = 10):
    """Returns SQL query for semantic search."""
    query_embedding = openai_client.embeddings.create(input=query, model="text-embedding-3-small").data[
        0].embedding
    sql_query = f""""
                WITH sim_game_ids AS (
                    SELECT game_id, embedding, 1 - (embedding <=> %s::VECTOR) AS sim FROM game_profile_embeddings
                    ORDER BY sim DESC LIMIT {limit}
                    )
                SELECT d.game_id, d.game_name, s.sim FROM sim_game_ids s JOIN dim_games d ON d.game_id = s.game_id
            """
    return sql_query, query_embedding


def metadata_search(
        genres: list[str] | None = None,
        categories: list[str] | None = None,
        developers: list[str] | None = None,
        publishers: list[str] | None = None,
        release_date_after: str | None = None,
        release_date_before: str | None = None,
        review_score_min: int | None = None,
        review_score_description: str | None = None,
        limit: int = 100,
):
    """Returns query for metadata search."""
    sql_query = "SELECT game_id, game_name FROM dim_games WHERE 1=1"
    params = []
    if genres:
        sql_query += " AND game_genres && %s"
    params.append(genres)
    if categories:
        sql_query += " AND game_categories && %s"
    params.append(categories)
    if developers:
        sql_query += " AND game_developers::text ILIKE && %s"
    params.append(developers)
    if publishers:
        sql_query += " AND game_publishers::text ILIKE %s"
    params.append(publishers)
    if release_date_after:
        sql_query += " AND game_prerelease_date >= %s"
    if release_date_before:
        sql_query += " AND game_prerelease_date <= %s"
    if review_score_min:
        sql_query += " AND game_review_score >= %s"
    if review_score_description:
        sql_query += " AND game_review_score_description = %s"
    sql_query += (" AND game_review_score IS NOT NULL"
                  " AND game_review_score_description IS NOT NULL"
                  " ORDER BY game_review_score DESC")
    sql_query += f" LIMIT %s"
    params.append(limit)
    return sql_query, params


def execute_query(query: str, params: list, pg_dsn: str):
    """Executes query and returns results."""
    import psycopg2
    conn = psycopg2.connect(dsn=pg_dsn)
    cur = conn.cursor()
    cur.execute(query, params)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results
