INSTALL httpfs;
INSTALL s3;

LOAD httpfs;
LOAD s3;

SET s3_region='us-east-1';
SET s3_url_style='path';
SET s3_use_ssl=false;
SET s3_endpoint='localhost:9000';
SET s3_access_key_id='';
SET s3_secret_access_key='';

CREATE VIEW raw_games AS
SELECT * FROM read_parquet('s3://raw/games/steam_games_*.parquet');

CREATE VIEW raw_reviews AS
SELECT * FROM read_parquet('s3://raw/reviews/steam_reviews_*.parquet');

CREATE VIEW app_ids AS
SELECT * FROM read_parquet('s3://raw/games/steam_ids.parquet');