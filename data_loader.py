# data_loader.py

import os
import joblib
import pandas as pd

def load_team_stats(game_date, team_type, team_abbr, season_type, stats_type="team_stats", cache_dir="cached_data"):
    """
    Loads cached team stats for the specified game date, team, and season type.
    """
    game_date_dir = os.path.join(cache_dir, game_date)
    
    if not os.path.exists(game_date_dir):
        print(f"Directory {game_date_dir} does not exist.")
        return pd.DataFrame()
    
    file_pattern = f"_{team_type}_team_{team_abbr}_{season_type}_{stats_type}.joblib"
    
    for file_name in os.listdir(game_date_dir):
        if file_pattern in file_name:
            filepath = os.path.join(game_date_dir, file_name)
            try:
                return joblib.load(filepath)
            except Exception as e:
                print(f"Error loading {file_name}: {e}")
                return pd.DataFrame()
    
    print(f"No cached file found for pattern: {file_pattern}")
    return pd.DataFrame()


def load_player_stats(game_date, team_type, team_abbr, season_type, cache_dir="cached_data"):
    """
    Loads cached player stats for the specified game date, team, and season type.
    
    Args:
        game_date (str): The game date in 'YYYY-MM-DD' format.
        team_type (str): 'home' or 'away' to indicate the team.
        team_abbr (str): Team abbreviation (e.g., 'MEM', 'NYK').
        season_type (str): 'prev' or 'curr' to indicate the season type.
        cache_dir (str): The base directory for cached data.
    
    Returns:
        pd.DataFrame: The loaded data as a DataFrame, or an empty DataFrame if not found.
    """
    game_date_dir = os.path.join(cache_dir, game_date)
    
    if not os.path.exists(game_date_dir):
        print(f"Directory {game_date_dir} does not exist.")
        return pd.DataFrame()
    
    file_pattern = f"_{team_type}_team_{team_abbr}_{season_type}.joblib"
    
    for file_name in os.listdir(game_date_dir):
        if file_pattern in file_name:
            filepath = os.path.join(game_date_dir, file_name)
            try:
                return joblib.load(filepath)
            except Exception as e:
                print(f"Error loading {file_name}: {e}")
                return pd.DataFrame()
    
    print(f"No cached file found for pattern: {file_pattern}")
    return pd.DataFrame()

# Helper function to load and concatenate team stats for a single team
def load_and_concatenate_team_stats(game_date, team_type, team_abbr):
    """
    Loads and concatenates previous and current season stats for a team into a single dataframe.

    Args:
        game_date (str): The game date in 'YYYY-MM-DD' format.
        team_type (str): 'home' or 'away' to indicate the team.
        team_abbr (str): Team abbreviation (e.g., 'MEM', 'NYK').

    Returns:
        pd.DataFrame: The concatenated DataFrame with a 'Season' column.
    """
    # Load previous and current season stats
    previous_stats = load_team_stats(game_date, team_type, team_abbr, "prev")
    current_stats = load_team_stats(game_date, team_type, team_abbr, "curr")
    
    # Add a 'Season' column to distinguish between previous and current season data
    previous_stats["Season"] = "Previous"
    current_stats["Season"] = "Current"
    
    # Concatenate both dataframes, if they have content
    combined_stats = pd.concat([previous_stats, current_stats], ignore_index=True) if not previous_stats.empty or not current_stats.empty else pd.DataFrame()
    
    return combined_stats

# Function to load and filter games for the next 7 days
def get_next_seven_days_games(schedule_file, dates):
    try:
        schedule_df = pd.read_csv(schedule_file)
        schedule_df['Game Date'] = pd.to_datetime(schedule_df['Game Date']).dt.strftime('%Y-%m-%d')
        # Filter for the next seven days
        next_seven_days_games = schedule_df[schedule_df['Game Date'].isin(dates)]
        return next_seven_days_games
    except FileNotFoundError:
        st.error("Schedule file not found. Please ensure 'nbaSchedule2425.csv' is in the correct folder.")
        return pd.DataFrame()

def get_team_roster(nba_data, team_abbr):
    """Fetches the roster for a specified team."""
    return nba_data.get_team_roster(team_abbr)

def get_player_game_logs(nba_data, player_id, season):
    """Fetches the game logs for a specified player and season."""
    player_logs = nba_data.fetch_player_game_logs(player_id, season)
    player_logs['GAME_DATE'] = pd.to_datetime(player_logs['GAME_DATE'])
    return player_logs

# Function to load last season’s matchup data
def load_matchup_rollup_from_cache():
    try:
        filepath = os.path.join("cached_data", "matchups_rollup_2023_24.joblib")
        return joblib.load(filepath)
    except Exception as e:
        st.error(f"Error loading cached matchup data: {e}")
        return pd.DataFrame()