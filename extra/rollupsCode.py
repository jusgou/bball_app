"""
def fetch_matchup_rollup_direct(season, per_mode="Totals", season_type="Regular Season",
                                league_id="00", def_player_id=None, def_team_id=None,
                                off_player_id=None, off_team_id=None):
    """
    #Fetches matchup rollup data directly from the NBA stats API using requests.
    """
    # Build the endpoint URL
    url = "https://stats.nba.com/stats/matchupsrollup"
    
    # Define query parameters
    params = {
        "Season": season,
        "PerMode": per_mode,
        "SeasonType": season_type,
        "LeagueID": league_id,
        "DefPlayerID": def_player_id or "",
        "DefTeamID": def_team_id or "",
        "OffPlayerID": off_player_id or "",
        "OffTeamID": off_team_id or ""
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://www.nba.com",
        "Referer": "https://www.nba.com/"
    }
    
    # Fetch the data
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
        data = response.json()  # Parse JSON response
        
        # Display data structure for inspection
        print("Data Structure:", data)

        # Extract data
        if "resultSets" in data and len(data["resultSets"]) > 0:
            df = pd.DataFrame(data["resultSets"][0]["rowSet"], columns=data["resultSets"][0]["headers"])
            return df
        else:
            print("No data found in resultSets.")
            return pd.DataFrame()
        
    except Exception as e:
        print(f"Error fetching matchup rollup data directly: {e}")
        return pd.DataFrame()
"""

"""
# Example usage with parameters (adjust as needed):
df = fetch_matchup_rollup_direct(season="2023-24", per_mode="Totals", season_type="Regular Season")

def save_previous_season_data(season, per_mode="Totals", season_type="Regular Season"):
    """Fetches and caches previous season matchup data."""
    # Define the cache directory for previous seasons
    cache_dir = "cached_data/previous_seasons"
    os.makedirs(cache_dir, exist_ok=True)
    file_path = os.path.join(cache_dir, f"matchup_rollup_{season}_{per_mode}_{season_type}.joblib")
    
    # Check if data already exists to avoid unnecessary API calls
    if os.path.exists(file_path):
        print(f"Loading cached data for season {season}...")
        return joblib.load(file_path)
    
    # Fetch data and save it to the cache
    df = fetch_matchup_rollup_direct(season=season, per_mode=per_mode, season_type=season_type)
    joblib.dump(df, file_path)
    print(f"Data for season {season} saved at {file_path}")
    return df

df_previous_season = save_previous_season_data(season="2021-22")
"""