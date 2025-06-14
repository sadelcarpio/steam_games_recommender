import requests
import time
import polars as pl


def get_app_reviews(url: str, appid: str, filt: str, cursor: str = "*"):
    response = requests.get(f"{url}/{appid}",
                            params={"json": "1", "filter": filt, "cursor": cursor, "num_per_page": "100"})
    response.raise_for_status()
    return response.json()


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
        "primarily_steam_deck": review["primarily_steam_deck"]
    }


PATH = "../../../data/steam_games.parquet"

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
    "primarily_steam_deck": pl.Boolean
}
reviews = pl.DataFrame(
    infer_schema_length=None,
    schema=schema
)

batch = 0
processed = 0
game_extra_features = []
for item, game in enumerate(recommended_games.iter_rows(named=True)):
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
    for review in data["reviews"]:
        record = create_review_record(review, appid)
        processed += 1
        print(f"Processed {processed} reviews for app {appid} ({app_name})")
        user_reviews.append(
            record
        )
    print(data["query_summary"])
    if data["query_summary"].get("total_reviews"):
        game_extra_features.append({
            "appid": appid,
            "review_score": data["query_summary"]["review_score"],
            "review_score_desc": data["query_summary"]["review_score_desc"],
            "total_positive_reviews": data["query_summary"]["total_positive"],
            "total_negative_reviews": data["query_summary"]["total_negative"],
            "total_reviews": data["query_summary"]["total_reviews"],
        })
        total_reviews = data["query_summary"]["total_reviews"]
        total_iter = total_reviews // 100 + (1 if total_reviews % 100 > 0 else 0)
        cursor = data["cursor"]
        print(
            f"Iteration # {item}. Total reviews for app {appid} ({app_name}): {total_reviews}. Resolving {total_iter} iterations.")
    else:
        total_iter = 100000  # large value
    for i in range(1, total_iter + 1):
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
        # extract useful information
        if not data.get("reviews"):
            break
        for review in data["reviews"]:
            record = create_review_record(review, appid)
            processed += 1
            print(f"Processed {processed} reviews for app {appid} ({app_name})")
            user_reviews.append(
                record
            )
        cursor = data["cursor"]

    if user_reviews:
        reviews = pl.concat([
            reviews,
            pl.DataFrame(user_reviews, infer_schema_length=None)
        ], how='vertical')
    if len(reviews) >= 1_000_000:
        print(f"Reached more than 1_000_000 reviews. Left in row {item}. Writing to {batch}th batch.")
        reviews.write_parquet(f"../../../data/steam_reviews_{batch}.parquet")
        reviews = pl.DataFrame(
            infer_schema_length=None,
            schema=schema
        )
        processed = 0
        batch += 1

reviews.write_parquet(f"../../../data/steam_reviews_{batch}.parquet")
game_extra_features_df = pl.DataFrame(game_extra_features, infer_schema_length=None,
                                      schema={
                                          'appid': pl.Int64,
                                          'review_score': pl.Int64,
                                          'review_score_desc': pl.String,
                                          'total_positive_reviews': pl.Int64,
                                          'total_negative_reviews': pl.Int64,
                                          'total_reviews': pl.Int64
                                      })
game_extra_features_df.write_parquet(f"../../../data/steam_games_extra_features.parquet")
