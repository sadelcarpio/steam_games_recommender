import requests
import polars


def download_all_steam_games():
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v0002?skip_unvetted_apps=false"
    response = requests.get(url)
    data = response.json()["applist"]["apps"]
    df = polars.DataFrame(data)
    df = df.unique(subset=["appid", "name"])
    df = df.replace_column(1, df["name"].str.strip_chars())
    df.write_csv("../data/steam_games.parquet")
    return response.json()


if __name__ == "__main__":
    download_all_steam_games()
