import os
import time
from datetime import datetime, UTC
import polars as pl
from google.cloud import firestore

from utils.steam_api import get_app_reviews


def create_review_record(review: dict, appid: int):
    author = review["author"]
    return {
        "rec_id": int(review["recommendationid"]),
        "author_id": int(author["steamid"]),
        "appid": appid,
        "playtime_forever": int(author["playtime_forever"]) if author.get("playtime_forever") else None,
        "playtime_last_two_weeks": int(author["playtime_last_two_weeks"]) if author.get(
            "playtime_last_two_weeks") else None,
        "playtime_at_review": int(author.get("playtime_at_review")) if author.get("playtime_at_review") else None,
        "num_games_owned": int(author["num_games_owned"]) if author.get("num_games_owned") else None,
        "num_reviews": int(author["num_reviews"]) if author.get("num_reviews") else None,
        "last_played": int(author["last_played"]) if author.get("last_played") else None,
        "language": review["language"],
        "review": review["review"],
        "timestamp_created": int(review["timestamp_created"]),
        "timestamp_updated": int(review["timestamp_updated"]),
        "voted_up": review["voted_up"],
        "votes_up": int(review["votes_up"]),
        "votes_funny": int(review["votes_funny"]),
        "weighted_vote_score": float(review["weighted_vote_score"]),
        "comment_count": int(review["comment_count"]),
        "steam_purchase": review["steam_purchase"],
        "received_for_free": review["received_for_free"],
        "written_during_early_access": review["written_during_early_access"],
        "primarily_steam_deck": review["primarily_steam_deck"],
        "scrape_date": datetime.now(UTC).date()
    }


def fetch_latest_reviews_timestamps_per_app(ref):
    docs = ref.stream()
    latest_timestamps = {}
    for doc in docs:
        latest_ts_data = doc.to_dict()
        game_id = doc.id
        latest_ts = latest_ts_data.get("latest_timestamp")
        print(f"Game {game_id} has latest timestamp {latest_ts}")
        latest_timestamps[game_id] = latest_ts
    return latest_timestamps


PATH = "../../data/raw/games/steam_games.parquet"
os.environ["FIRESTORE_EMULATOR_HOST"] = "localhost:8080"
db = firestore.Client(project="steam-games-recommender")
collection_ref = db.collection("games")

recommended_games = pl.read_parquet(PATH)
schema = {
    "rec_id": pl.Int64,
    "author_id": pl.Int64,
    "appid": pl.Int64,
    "playtime_forever": pl.Int64,
    "playtime_last_two_weeks": pl.Int64,
    "playtime_at_review": pl.Int64,
    "num_games_owned": pl.Int64,
    "num_reviews": pl.Int64,
    "last_played": pl.Int64,
    "language": pl.String,
    "review": pl.Utf8,
    "timestamp_created": pl.Int64,
    "timestamp_updated": pl.Int64,
    "voted_up": pl.Boolean,
    "votes_up": pl.Int64,
    "votes_funny": pl.Int64,
    "weighted_vote_score": pl.Float64,
    "comment_count": pl.Int64,
    "steam_purchase": pl.Boolean,
    "received_for_free": pl.Boolean,
    "written_during_early_access": pl.Boolean,
    "primarily_steam_deck": pl.Boolean,
    "scrape_date": pl.Date,
}
reviews = pl.DataFrame(
    infer_schema_length=None,
    schema=schema
)

latest_timestamps = fetch_latest_reviews_timestamps_per_app(collection_ref)

batch = 0
processed = 0
finish = False
for item, game in enumerate(recommended_games.iter_rows(named=True)):
    if finish:
        finish = False
        continue
    cursor = "*"
    user_reviews = []
    appid = game["appid"]
    app_name = game["name"]
    # first fetch to see the total number of reviews
    try:
        data = get_app_reviews("https://store.steampowered.com/appreviews", appid=appid, filt="recent", cursor=cursor)
    except Exception:
        print(f"Quota limit reached for executor. {appid}. Left in row {item}")
        break
    if len(data.get("reviews")) == 0:
        print(f"No reviews found for app {appid} ({app_name})")
        continue
    latest_timestamp = datetime.fromtimestamp(data["reviews"][0]["timestamp_created"], UTC)
    if latest_timestamps.get(str(appid)) is None:
        print(f"Adding {appid} latest timestamp to {latest_timestamp}")
        latest_timestamps[str(appid)] = latest_timestamp
        doc_ref = db.collection("games").document(str(appid))
        doc_ref.set({"latest_timestamp": latest_timestamp})
    elif latest_timestamp > latest_timestamps.get(str(appid)):
        print(f"Modifying {appid} latest timestamp to {latest_timestamp}")
        doc_ref = db.collection("games").document(str(appid))
        doc_ref.update({"latest_timestamp": latest_timestamp})
    else:
        print(f"Skipping app {appid} ({app_name}) as it has no new reviews")
        continue
    for review in data["reviews"]:
        if review["timestamp_created"] < latest_timestamps.get(str(appid)):
            print(f"Skipping review {review['rec_id']} as it is older than latest timestamp")
            continue
        record = create_review_record(review, appid)
        processed += 1
        print(f"Processed {processed} reviews for app {appid} ({app_name})")
        user_reviews.append(
            record
        )
    print(data["query_summary"])
    if data["query_summary"].get("total_reviews"):
        total_reviews = data["query_summary"]["total_reviews"]
        total_iter = total_reviews // 100 + (1 if total_reviews % 100 > 0 else 0)
        cursor = data["cursor"]
        print(
            f"Iteration # {item}. Total reviews for app {appid} ({app_name}): {total_reviews}. Resolving {total_iter} iterations.")
    else:
        total_iter = 100000  # large value
    for i in range(1, total_iter + 1):
        if finish:
            break
        print(i)
        for tries in range(10):
            try:
                print(f"Using cursor: {cursor}")
                data = get_app_reviews("https://store.steampowered.com/appreviews", appid=appid, filt="recent",
                                       cursor=cursor)
            except Exception:
                print(
                    f"Quota limit reached for executor. {appid}. Left in row {item}, review # {i}. Cursor: {cursor}. Trying one more time")
                time.sleep(10)
            else:
                break
        if not data.get("reviews"):
            break
        latest_timestamp = datetime.fromtimestamp(data["reviews"][0]["timestamp_created"], UTC)
        if latest_timestamp > latest_timestamps.get(str(appid)):
            print(f"Modifying {appid} latest timestamp to {latest_timestamp}")
            doc_ref = db.collection("games").document(str(appid))
            doc_ref.update({"latest_timestamp": latest_timestamp})
            for review in data["reviews"]:
                if review["timestamp_created"] < latest_timestamps.get(str(appid)):
                    print(f"Skipping review {review['rec_id']} as it is older than latest timestamp")
                    continue
                record = create_review_record(review, appid)
                processed += 1
                print(f"Processed {processed} reviews for app {appid} ({app_name})")
                user_reviews.append(
                    record
                )
            cursor = data["cursor"]
        else:
            print(f"Skipping app {appid} ({app_name}) as it has no new reviews")
            finish = True
            break

    if user_reviews:
        reviews = pl.concat([
            reviews,
            pl.DataFrame(user_reviews, infer_schema_length=None)
        ], how='vertical')
    if len(reviews) >= 100_000:
        print(f"Reached more than 100_000 reviews. Left in row {item}. Writing to {batch}th batch.")
        reviews.write_parquet(f"../../data/raw/reviews_2/steam_reviews_{batch}.parquet")
        reviews = pl.DataFrame(
            infer_schema_length=None,
            schema=schema
        )
        processed = 0
        batch += 1

reviews.write_parquet(f"../../data/raw/reviews_2/test_steam_reviews_{batch}.parquet")
# LATEST LOG: Quota limit reached for executor. 3258560. Left in row 132635