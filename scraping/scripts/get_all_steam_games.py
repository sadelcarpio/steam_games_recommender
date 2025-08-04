from utils.steam_api import download_all_steam_games

if __name__ == "__main__":
    df = download_all_steam_games()
    df.write_parquet("../../data/raw/games/steam_ids.parquet")
