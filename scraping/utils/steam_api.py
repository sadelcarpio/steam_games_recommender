import polars as pl
import requests


def get_app_data(appid: str):
    response = requests.get("https://store.steampowered.com/api/appdetails", params={"appids": appid, "l": "english"})
    response.raise_for_status()
    return response.json()


def get_app_reviews(appid: str, filt: str, cursor: str = "*"):
    response = requests.get(f"https://store.steampowered.com/appreviews/{appid}",
                            params={"json": "1",
                                    "filter": filt,
                                    "language": "all",
                                    "cursor": cursor,
                                    "num_per_page": "100"})
    response.raise_for_status()
    return response.json()


def download_all_steam_games():
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v0002?skip_unvetted_apps=false"
    response = requests.get(url)
    data = response.json()["applist"]["apps"]
    df = pl.DataFrame(data)
    df = df.unique(subset=["appid", "name"])
    df = df.replace_column(1, df["name"].str.strip_chars())
    return df
