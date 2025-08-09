import re
import time
from datetime import datetime, UTC

import duckdb
import polars as pl
from utils.steam_api import get_app_data, get_app_reviews


def create_apps_features_df() -> pl.DataFrame:
    return pl.DataFrame(infer_schema_length=None, schema={
        "appid": pl.Int64,
        "name": pl.Utf8,
        "type": pl.String,
        "required_age": pl.Int64,
        "is_free": pl.Boolean,
        "minimum_pc_requirements": pl.Utf8,
        "recommended_pc_requirements": pl.Utf8,
        "controller_support": pl.String,
        "detailed_description": pl.Utf8,
        "about_the_game": pl.Utf8,
        "short_description": pl.Utf8,
        "supported_languages": pl.List(pl.String),
        "header_image": pl.String,
        "developers": pl.List(pl.String),
        "publishers": pl.List(pl.String),
        "price": pl.Float64,
        "categories": pl.List(pl.String),
        "genres": pl.List(pl.String),
        "windows_support": pl.Boolean,
        "mac_support": pl.Boolean,
        "linux_support": pl.Boolean,
        "release_date": pl.String,
        "coming_soon": pl.Boolean,
        "recommendations": pl.Int64,
        "dlc": pl.List(pl.Int64),
        "review_score": pl.Int64,
        "review_score_desc": pl.String,
        "scrape_date": pl.Date,
    })


if __name__ == "__main__":
    duckdb_conn = duckdb.connect('../data/steam.duckdb', read_only=True)
    batch_size = 10_000
    processed_count = 0
    batch_num = 0
    scrape_date = datetime.now(UTC).date()
    df = duckdb_conn.sql("SELECT appid FROM new_ids_to_scrape").pl()
    total_apps = len(df)
    duckdb_conn.close()
    apps_features_df = create_apps_features_df()
    try:
        for i, row in enumerate(df.iter_rows()):
            appid = row[0]
            data = None
            game_reviews_data = None
            for tries in range(10):
                try:
                    time.sleep(1.6)
                    data = get_app_data("https://store.steampowered.com/api/appdetails", appid)
                    game_reviews_data = get_app_reviews("https://store.steampowered.com/appreviews", appid=appid,
                                                        filt="recent")
                except Exception:
                    print(f"Quota limit reached for executor. {appid}. Left in row {i}")
                    time.sleep(10)
                else:
                    break

            if data and data.get(str(appid), {}).get("success") and game_reviews_data and game_reviews_data.get(
                    "reviews") is not None:
                app_info = data[str(appid)]["data"]
                if app_info["short_description"] == '' or not app_info.get("supported_languages") or not \
                        app_info["pc_requirements"] or not app_info.get("genres") or not app_info.get("categories"):
                    continue
                languages = re.sub(r"<.*?>", "", app_info["supported_languages"]).replace(", ", ",").split(",")
                for j, lang in enumerate(languages):
                    languages[j] = re.sub(r"\*(.*)?", "", lang)

                if app_info.get("price_overview"):
                    price = app_info["price_overview"]["initial"] / 100  # price without discount
                else:
                    price = None
                if app_info.get("recommendations"):
                    recommendations = int(app_info["recommendations"]["total"])
                else:
                    recommendations = None

                if app_info["pc_requirements"].get("recommended"):
                    recommended_pc_requirements = re.sub(r"<.*?>", "", app_info["pc_requirements"]["recommended"])
                else:
                    recommended_pc_requirements = None

                if app_info["pc_requirements"].get("minimum"):
                    minimum_pc_requirements = re.sub(r"<.*?>", "", app_info["pc_requirements"]["minimum"])
                else:
                    minimum_pc_requirements = None

                req_age = re.findall(r"\d+", str(app_info["required_age"]))
                if not req_age:
                    req_age = None
                else:
                    req_age = int(req_age[0])

                record = {
                    "appid": appid,
                    "name": app_info["name"],
                    "type": app_info["type"],
                    "required_age": req_age,
                    "is_free": app_info["is_free"],
                    "minimum_pc_requirements": minimum_pc_requirements,
                    "recommended_pc_requirements": recommended_pc_requirements,
                    "controller_support": app_info.get("controller_support", None),
                    "detailed_description": app_info["detailed_description"],
                    "about_the_game": app_info["about_the_game"],
                    "short_description": app_info["short_description"],
                    "supported_languages": languages,
                    "header_image": app_info.get("header_image", None),
                    "developers": app_info.get("developers", []),
                    "publishers": app_info.get("publishers", []),
                    "price": price,
                    "categories": [v["description"] for v in app_info["categories"]],
                    "genres": [v["description"] for v in app_info["genres"]],
                    "windows_support": app_info["platforms"]["windows"],
                    "mac_support": app_info["platforms"]["mac"],
                    "linux_support": app_info["platforms"]["linux"],
                    "release_date": app_info["release_date"]["date"],
                    "coming_soon": app_info["release_date"]["coming_soon"],
                    "recommendations": recommendations,
                    "dlc": app_info.get("dlc", []),
                    "review_score": game_reviews_data["query_summary"].get("review_score", None),
                    "review_score_desc": game_reviews_data["query_summary"].get("review_score_desc", None),
                    "scrape_date": scrape_date
                }

                print(f"Finished processing element #{appid} in iteration #{i} / {total_apps}")
                processed_count += 1
                apps_features_df = pl.concat([
                    apps_features_df,
                    pl.DataFrame([record])
                ], how='vertical')
                if processed_count >= batch_size:
                    print(f"Writing batch {batch_num}")
                    apps_features_df.write_parquet(f"s3://raw/games/steam_games_{scrape_date}_{batch_num}.parquet",
                                                   storage_options={"aws_access_key_id": 'minioadmin',
                                                                    "aws_secret_access_key": 'minioadmin',
                                                                    "aws_region": "us-east-1",
                                                                    "aws_endpoint_url": "http://localhost:9000"})
                    # Reset for next batch
                    apps_features_df = create_apps_features_df()
                    batch_num += 1
                    processed_count = 0
    except Exception as e:
        print(e)
    finally:
        if len(apps_features_df) > 0:
            print(f"Writing final batch {batch_num}")
            apps_features_df.write_parquet(f"s3://raw/games/steam_games_{scrape_date}_{batch_num}.parquet",
                                           storage_options={"aws_access_key_id": 'minioadmin',
                                                            "aws_secret_access_key": 'minioadmin',
                                                            "aws_region": "us-east-1",
                                                            "aws_endpoint_url": "http://localhost:9000"})
