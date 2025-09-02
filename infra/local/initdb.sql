CREATE DATABASE mlflow;
CREATE DATABASE games_scraping;

\connect games_scraping;

CREATE TABLE IF NOT EXISTS games_last_processed_reviews (
    game_id VARCHAR(255) PRIMARY KEY,
    last_processed_timestamp DATE
);