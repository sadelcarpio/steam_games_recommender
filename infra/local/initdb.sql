CREATE DATABASE mlflow;
CREATE DATABASE games_scraping;

\connect games_scraping;

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS games_last_processed_reviews (
    game_id INTEGER PRIMARY KEY,
    last_processed_timestamp TIMESTAMP
);

-- base table, similar to DuckDB dim_games
CREATE TABLE IF NOT EXISTS dim_games (
    game_id BIGINT PRIMARY KEY,
    game_name TEXT,
    game_is_free BOOLEAN,
    game_developers TEXT[],
    game_publishers TEXT[],
    game_categories TEXT[],
    game_genres TEXT[],
    game_steam_release_date TIMESTAMP,
    game_release_date TIMESTAMP,
    game_prerelease_date TIMESTAMP,
    game_short_description TEXT,
    game_about TEXT,
    game_detailed_description TEXT,
    game_scrape_date DATE,
    game_review_score BIGINT,
    game_review_score_description TEXT
);

-- profile embeddings (one per game, built from metadata like name+tags)
CREATE TABLE IF NOT EXISTS game_profile_embeddings (
    game_id BIGINT PRIMARY KEY REFERENCES dim_games(game_id),
    embedding VECTOR(1536)
);

-- chunk embeddings (long descriptions split into chunks)
CREATE TABLE IF NOT EXISTS game_chunk_embeddings (
    game_id BIGINT REFERENCES dim_games(game_id) ON DELETE CASCADE,
    chunk_id INT NOT NULL,
    chunk_text TEXT,
    embedding VECTOR(1536),
    PRIMARY KEY (game_id, chunk_id)
);

-- add indexes for ANN search
CREATE INDEX IF NOT EXISTS idx_profile_embedding
    ON game_profile_embeddings USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

CREATE INDEX IF NOT EXISTS idx_chunk_embedding
    ON game_chunk_embeddings USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);