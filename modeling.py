#!/usr/bin/env python
# coding: utf-8

# In[14]:


import pandas as pd
import matplotlib.pyplot as plt
from classes import NBATeamRosters
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime
import json
import seaborn as sns
from cache_manager import CacheManager
import joblib
import os
import re
import time


# In[15]:


nba_data = NBATeamRosters(season="2024")
cache_manager = CacheManager()


# In[16]:


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


# In[17]:


# Define the previous season
previous_season = "2023-24"
current_season = "2024-25"


# In[18]:


# Fetch Teams Master List 
nba_data.fetch_teams()
teams_df = nba_data.teams_df


# # Schedule

# In[19]:


# Load the schedule from your CSV file
nba_data.load_schedule('nbaSchedule2425.csv')  # Replace with your actual CSV path


# In[20]:


# Display the full schedule
schedule_df = nba_data.get_full_schedule()


# In[21]:


today_date = datetime.datetime.today().strftime('%Y-%m-%d')
game_date = today_date
# Retrieve today's games
todays_games = nba_data.get_todays_games(today_date) # Use today_date when in season


# In[22]:


todays_games


# # Load Cached Data

# In[23]:


# Base directory for cached data
cache_dir = "C:/Users/justi/Documents/__ballAnalytics/bball_app/cached_data" 
today_game_dir =  f"C:/Users/justi/Documents/__ballAnalytics/bball_app/cached_data/{today_date}"


# In[37]:


# List all the files in the directory
try:
    file_list = os.listdir(today_game_dir)
    print("Files in today's directory:")
    for file_name in file_list:
        print(file_name)
except FileNotFoundError:
    print(f"The directory {today_game_dir} does not exist.")


# In[39]:


# Initialize dictionaries to hold the dataframes
game_dataframes = {}

# Loop through the files in the game directory
for filename in os.listdir(today_game_dir):
    if filename.endswith(".joblib"):
        # Extract the game_id, team, season, and stat type from the filename
        parts = filename.split('_')
        
        # Extract key information from filename
        game_id = parts[1]  # Example: '22400061'
        home_or_away = parts[2]  # 'home' or 'away'
        team_abbr = parts[4]  # Example: 'NYK'
        season_type = 'prev' if 'prev' in filename else 'curr'  # 'prev' or 'curr'
        stat_type = 'team' if 'team_stats' in filename else 'player'  # 'team' or 'player'
        
        # Load the data
        filepath = os.path.join(today_game_dir, filename)
        data = joblib.load(filepath)
        
        # Generate a unique name for the dataframe
        df_name = f"game_{game_id}_{home_or_away}_team_{team_abbr}_{season_type}_{stat_type}_df"
        
        # Assign the dataframe to the dynamically generated variable name
        globals()[df_name] = data
        
        # Optionally, store the dataframes in a dictionary for easy access if needed
        game_dataframes[df_name] = data

# Check created dataframes
print("Created DataFrames:")
for df_name in game_dataframes.keys():
    print(df_name)


# In[31]:


def consolidate_traded_players(df):
    """
    Consolidates player stats by retaining only the 'TOT' row for players with multiple entries 
    due to trades within the season.

    Args:
        df (pd.DataFrame): The dataframe containing player stats with potential multiple entries per player.

    Returns:
        pd.DataFrame: The dataframe with only season totals for traded players.
    """
    # Identify players with multiple entries
    player_counts = df['PLAYER_ID'].value_counts()
    traded_players = player_counts[player_counts > 1].index

    # Filter out non-TOT entries for traded players
    df = df[~((df['PLAYER_ID'].isin(traded_players)) & (df['TEAM_ABBREVIATION'] != 'TOT'))]

    # Reset index for a clean DataFrame
    df = df.reset_index(drop=True)
    
    return df


# In[35]:


# Apply this function only to dataframes ending in '_prev_player_df' or '_curr_player_df'
for df_name in globals():
    if df_name.endswith('_prev_player_df') or df_name.endswith('_curr_player_df'):
        globals()[df_name] = consolidate_traded_players(globals()[df_name])


# # Select Top Players and Lineups

# In[ ]:


"""
# Function to tag top players by MIN_x and E_USG_PCT
def tag_top_players(player_df, df_name):
    if 'MIN_x' in player_df.columns and 'E_USG_PCT' in player_df.columns:
        # Sort by MIN_x and E_USG_PCT
        player_df = player_df.sort_values(by=['MIN_x', 'E_USG_PCT'], ascending=False)
        
        # Tag top 6 players as core players ('1')
        player_df['PLAYER_TAG'] = 0
        player_df.iloc[:6, player_df.columns.get_loc('PLAYER_TAG')] = 1
        
        # Tag next 3-4 players as bench players ('2')
        player_df.iloc[6:10, player_df.columns.get_loc('PLAYER_TAG')] = 2
        
        print(f"Tagged players for dataframe: {df_name}")
    else:
        print(f"MIN_x or E_USG_PCT not found in dataframe: {df_name}")
    
    return player_df
"""


# In[ ]:


"""
# Loop through the available player dataframes and apply the tagging
for df_name in globals():
    if df_name.endswith('_player_df'):  # Only apply to player dataframes
        globals()[df_name] = tag_top_players(globals()[df_name], df_name)
"""


# ## Manual Tagging

# In[ ]:


"""
# Dictionary to store manual tags, format: {PLAYER_ID: TAG}
manual_tags = {
    # Example: Add your manual player tags here
    # 'PLAYER_ID': tag_value (1 for core, 2 for bench, etc.)
    1628369: 1,  # Example: Manually tagging PLAYER_ID 1628369 as core player
    203507: 2,   # Example: Manually tagging PLAYER_ID 203507 as bench player
}

# Function to tag top players by MIN_x and E_USG_PCT with manual override option
def tag_top_players_with_manual_override(player_df, df_name):
    if 'MIN_x' in player_df.columns and 'E_USG_PCT' in player_df.columns:
        # Sort by MIN_x and E_USG_PCT
        player_df = player_df.sort_values(by=['MIN_x', 'E_USG_PCT'], ascending=False)
        
        # Tag top 6 players as core players ('1')
        player_df['PLAYER_TAG'] = 0
        player_df.iloc[:6, player_df.columns.get_loc('PLAYER_TAG')] = 1
        
        # Tag next 3-4 players as bench players ('2')
        player_df.iloc[6:10, player_df.columns.get_loc('PLAYER_TAG')] = 2

        # Apply manual tags if they exist
        for index, row in player_df.iterrows():
            player_id = row['PLAYER_ID']
            if player_id in manual_tags:
                player_df.at[index, 'PLAYER_TAG'] = manual_tags[player_id]
                print(f"Manual tag applied: Player ID {player_id} tagged as {manual_tags[player_id]} in {df_name}")
        
        print(f"Tagged players for dataframe: {df_name}")
    else:
        print(f"MIN_x or E_USG_PCT not found in dataframe: {df_name}")
    
    return player_df

# Loop through the available player dataframes and apply the tagging with manual override
for df_name in globals():
    if df_name.endswith('_player_df'):  # Only apply to player dataframes
        globals()[df_name] = tag_top_players_with_manual_override(globals()[df_name], df_name)

# Example check on one dataframe
print(game_22400061_away_team_NYK_prev_player_df.head())
"""


# # Lineup and GameLogs

# In[ ]:


def fetch_and_cache_game_logs_based_on_tags(player_df, df_name, season, game_date, cache_dir):
    """
    Fetches and caches player game logs for players tagged with '1' (core) or '2' (bench).
    
    Args:
        player_df (pd.DataFrame): The player dataframe with tags.
        df_name (str): The name of the dataframe for logging purposes.
        season (str): The NBA season in 'YYYY-YY' format.
        game_date (str): The date of the game in 'YYYY-MM-DD' format.
        cache_dir (str): The base cache directory.
    """
    # Filter for players tagged as core or bench (1 or 2)
    if 'PLAYER_TAG' in player_df.columns:
        tagged_players = player_df[player_df['PLAYER_TAG'].isin([1, 2])]
        
        if tagged_players.empty:
            print(f"No tagged players found in dataframe: {df_name}")
            return
        
        # Fetch and cache logs for tagged players
        for _, player in tagged_players.iterrows():
            player_id = player['PLAYER_ID']
            print(f"Fetching logs for Player ID {player_id} ({player['PLAYER']}) in season {season}")
            
            # Here, we would fetch the logs using NBA API or cached data
            # fetched_logs = nba_data.fetch_player_game_logs(player_id, season)
            
            # Cache the logs using the CacheManager or other mechanism
            # cache_manager.cache_player_logs(fetched_logs, player_id, game_date)

        print(f"Logs fetched and cached for dataframe: {df_name}")
    else:
        print(f"No PLAYER_TAG column in dataframe: {df_name}. Skipping.")



# In[ ]:


# Define pause time (in seconds) between each player log pull
pause_time = 1.5  # Adjust the pause time as necessary

# Collect all dataframe names with 'prev_player_df' in the name
prev_player_dfs = [df_name for df_name in globals() if 'prev_player_df' in df_name]

# Loop through collected dataframe names
for df_name in prev_player_dfs:
    print(f"Processing dataframe: {df_name}")
    
    # Get the dataframe
    player_df = globals()[df_name]
    
    # Check if the 'PLAYER_TAG' column exists
    if 'PLAYER_TAG' in player_df.columns:
        tagged_players = player_df[player_df['PLAYER_TAG'].isin([1, 2])]
        
        # Fetch and cache logs for each tagged player
        for _, player in tagged_players.iterrows():
            player_id = player['PLAYER_ID']
            game_id = df_name.split('_')[1]  # Extract game ID from the dataframe name
            
            # Generate a new filename based on game ID and player ID
            player_log_filename = f"game_{game_id}_player_{player_id}_logs"
            
            # Fetch player logs (adjust season as necessary)
            player_logs = nba_data.fetch_player_game_logs(player_id, "2023-24")
            
            if not player_logs.empty:
                # Cache the player logs with the new naming convention
                cache_manager.cache_data(player_logs, player_log_filename, game_date)
                print(f"Cached player logs for Player ID {player_id} in game {game_id}")
            else:
                print(f"No logs available for Player ID {player_id} in game {game_id}")
            
            # Pause between each player log pull to avoid overwhelming the API
            time.sleep(pause_time)
    else:
        print(f"No PLAYER_TAG column found in {df_name}")


# In[ ]:


def load_cached_game_logs(cache_dir):
    """
    Loads all cached player game logs from the specified directory into dataframes.
    
    Args:
        cache_dir (str): The directory where player game logs are stored.
        
    Returns:
        dict: A dictionary of dataframes, where the keys are the game log filenames.
    """
    player_game_logs = {}

    # Iterate over the game date folders
    for game_date_folder in os.listdir(cache_dir):
        game_date_path = os.path.join(cache_dir, game_date_folder)
        
        # Look for the player_logs subdirectory
        player_logs_dir = os.path.join(game_date_path, f"player_logs_{game_date_folder.replace('-', '')}")
        if os.path.exists(player_logs_dir):
            # Load all the player log files in this subdirectory
            for filename in os.listdir(player_logs_dir):
                if filename.endswith(".joblib"):
                    filepath = os.path.join(player_logs_dir, filename)
                    player_log_df = joblib.load(filepath)
                    player_game_logs[filename] = player_log_df
                    print(f"Loaded {filename}")
    
    return player_game_logs


# In[ ]:


# Define the base cache directory
cache_dir = "C:/Users/justi/Documents/__ballAnalytics/bball/NBAModel/cached_data"

# Call the function to load player game logs
player_game_logs = load_cached_game_logs(cache_dir)

# Print the loaded player game logs to see which ones are available
print(f"Loaded player game logs: {list(player_game_logs.keys())}")


# # Predictive Model

# In[ ]:


def calculate_possessions(team_stats_df):
    """
    Calculates the number of possessions for a team based on their stats.
    
    Args:
        team_stats_df (pd.DataFrame): DataFrame containing team stats.
        
    Returns:
        float: Estimated number of possessions for the team.
    """
    # Use the formula to estimate possessions
    fga = team_stats_df['FGA'].values[0]
    fta = team_stats_df['FTA'].values[0]
    oreb = team_stats_df['OREB'].values[0]
    tov = team_stats_df['TOV'].values[0]
    
    possessions = fga + (0.44 * fta) - oreb + tov
    return possessions


def calculate_ppp(team_stats_df):
    """
    Calculates points per possession (PPP) for a team based on their stats.
    
    Args:
        team_stats_df (pd.DataFrame): DataFrame containing team stats.
        
    Returns:
        float: Points per possession for the team.
    """
    pts = team_stats_df['PTS'].values[0]
    possessions = calculate_possessions(team_stats_df)
    
    # Avoid division by zero
    if possessions == 0:
        return 0

    ppp = pts / possessions
    return ppp


def calculate_team_total_with_ppp(team_stats_df, opp_stats_df):
    """
    Calculates the predicted points for a team based on PPP, offensive and defensive metrics, and pace.
    
    Args:
        team_stats_df (pd.DataFrame): DataFrame containing the team's offensive, pace, and PPP metrics.
        opp_stats_df (pd.DataFrame): DataFrame containing the opponent's defensive and pace metrics.
        
    Returns:
        float: Predicted points for the team.
    """
    # Calculate points per possession (PPP) for both the team and their opponent
    team_ppp = calculate_ppp(team_stats_df)
    opp_ppp = calculate_ppp(opp_stats_df)

    # Use offensive and defensive ratings, pace, and PPP to adjust prediction
    team_off_rating = team_stats_df['E_OFF_RATING'].values[0]
    opp_def_rating = opp_stats_df['E_DEF_RATING'].values[0]
    
    team_pace = team_stats_df['E_PACE'].values[0]
    opp_pace = opp_stats_df['E_PACE'].values[0]

    # Pace adjustment
    pace_adjustment = (team_pace + opp_pace) / 2

    # Predicted points for the team, factoring PPP, pace, and offensive/defensive rating
    predicted_pts = (team_ppp * pace_adjustment * (team_off_rating / opp_def_rating))

    return predicted_pts


def predict_team_totals_with_ppp(todays_games, team_stats_dict):
    """
    Loops through today's games and predicts total points for each game using PPP, offensive/defensive metrics, and pace.
    
    Args:
        todays_games (pd.DataFrame): DataFrame of today's games.
        team_stats_dict (dict): Dictionary containing team stats for each game (by game ID).
        
    Returns:
        dict: Dictionary with predicted points for home and away teams for each game.
    """
    predictions = {}

    for _, game in todays_games.iterrows():
        game_id = game['Game ID']
        home_team_abbr = game['Home Team Abbreviation']
        away_team_abbr = game['Visiting Team Abbreviation']

        # Fetch the team stats for home and away teams
        home_team_stats = team_stats_dict[f'game_{game_id}_home_team_{home_team_abbr}_prev_team_stats']
        away_team_stats = team_stats_dict[f'game_{game_id}_away_team_{away_team_abbr}_prev_team_stats']

        # Predict points for home and away teams using PPP
        home_team_total = calculate_team_total_with_ppp(home_team_stats, away_team_stats)
        away_team_total = calculate_team_total_with_ppp(away_team_stats, home_team_stats)

        # Store the predictions for this game
        predictions[game_id] = {
            'home_team': home_team_abbr,
            'home_team_total': home_team_total,
            'away_team': away_team_abbr,
            'away_team_total': away_team_total
        }

        print(f"Game ID {game_id}: Predicted Home ({home_team_abbr}) Points: {home_team_total:.2f}")
        print(f"Game ID {game_id}: Predicted Away ({away_team_abbr}) Points: {away_team_total:.2f}")

    return predictions


# In[ ]:


# Example team stats dictionary
team_stats_dict = {
    'game_22400063_away_team_IND_prev_team_stats': game_22400063_away_team_IND_prev_player_df,
    'game_22400063_home_team_DET_prev_team_stats': game_22400063_home_team_DET_prev_player_df,
    'game_22400064_away_team_BKN_prev_team_stats': game_22400064_away_team_BKN_prev_player_df,
    'game_22400064_home_team_ATL_prev_team_stats': game_22400064_home_team_ATL_prev_player_df,
    'game_22400065_away_team_ORL_prev_team_stats': game_22400065_away_team_ORL_prev_player_df,
    'game_22400065_home_team_MIA_prev_team_stats': game_22400065_home_team_MIA_prev_player_df,
    'game_22400066_away_team_MIL_prev_team_stats': game_22400066_away_team_MIL_prev_player_df,
    'game_22400066_home_team_PHI_prev_team_stats': game_22400066_home_team_PHI_prev_player_df,
    'game_22400067_away_team_CLE_prev_team_stats': game_22400067_away_team_CLE_prev_player_df,
    'game_22400067_home_team_TOR_prev_team_stats': game_22400067_home_team_TOR_prev_player_df,
    'game_22400068_away_team_CHA_prev_team_stats': game_22400068_away_team_CHA_prev_player_df,
    'game_22400068_home_team_HOU_prev_team_stats': game_22400068_home_team_HOU_prev_player_df,
    'game_22400069_away_team_CHI_prev_team_stats': game_22400069_away_team_CHI_prev_player_df,
    'game_22400069_home_team_NOP_prev_team_stats': game_22400069_home_team_NOP_prev_player_df,
    'game_22400070_away_team_MEM_prev_team_stats': game_22400070_away_team_MEM_prev_player_df,
    'game_22400070_home_team_UTA_prev_team_stats': game_22400070_home_team_UTA_prev_player_df,
    'game_22400071_away_team_PHX_prev_team_stats': game_22400071_away_team_PHX_prev_player_df,
    'game_22400071_home_team_LAC_prev_team_stats': game_22400071_home_team_LAC_prev_player_df,
    'game_22400072_away_team_GSW_prev_team_stats': game_22400072_away_team_GSW_prev_player_df,
    'game_22400072_home_team_POR_prev_team_stats': game_22400072_home_team_POR_prev_player_df
}


# In[ ]:


# Predict team totals for today's games using PPP
team_total_predictions = predict_team_totals_with_ppp(todays_games, team_stats_dict)


# # Predict Player Points

# In[ ]:


import os
import joblib
import pandas as pd

# Path where player logs are stored
player_logs_dir = "C:/Users/justi/Documents/__ballAnalytics/bball/NBAModel/cached_data/2024-10-22"

def calculate_expected_points_for_tagged_players(player_stats_df, player_logs_dir, opp_def_stats):
    """
    Calculate expected points for tagged players based on their game logs.
    
    Args:
        player_stats_df (pd.DataFrame): DataFrame with tagged player stats.
        player_logs_dir (str): Path to the directory containing player logs.
        opp_def_stats (pd.DataFrame): Opponent defensive statistics (def_rating, pace, etc.).
    
    Returns:
        pd.DataFrame: DataFrame containing players and their expected points.
    """
    expected_points_list = []
    
    for _, player in player_stats_df.iterrows():
        player_id = player['PLAYER_ID']
        player_name = player['PLAYER']
        player_tag = player['PLAYER_TAG']  # Core or bench tag

        # Build the file name for player logs (adjust for .joblib issue)
        player_log_file = os.path.join(player_logs_dir, f"game_22400062_player_{player_id}_logs.joblib.joblib")
        
        # Check if the player log file exists and load it
        if os.path.exists(player_log_file):
            player_logs = joblib.load(player_log_file)
            
            # Calculate average points from the logs
            avg_points_per_game = player_logs['PTS'].mean() if not player_logs.empty else 0
        else:
            print(f"Player log file for {player_name} (ID: {player_id}) not found.")
            avg_points_per_game = 0

        # Adjust expected points based on opponent defensive stats
        opp_def_rating = opp_def_stats['E_DEF_RATING'].values[0]
        opp_pace = opp_def_stats['E_PACE'].values[0]
        
        expected_points = avg_points_per_game * (opp_pace / 100) * (100 / opp_def_rating)

        # Store results
        expected_points_list.append({
            'Player_ID': player_id,
            'Player_Name': player_name,
            'Player_Tag': player_tag,
            'Expected_Points': expected_points
        })
    
    return pd.DataFrame(expected_points_list)

# Example call
# Let's assume we have home_player_previous, away_player_previous, and opp_def_stats available
home_expected_points_df = calculate_expected_points_for_tagged_players(home_player_previous, player_logs_dir, away_team_def_stats)
away_expected_points_df = calculate_expected_points_for_tagged_players(away_player_previous, player_logs_dir, home_team_def_stats)

print(home_expected_points_df)
print(away_expected_points_df)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


import os
import joblib

# Example player ID and game ID
player_id = 1628369  # Example: Jayson Tatum
game_id = "22400062"  # Example game ID, replace with actual game ID
game_logs_dir = "C:/Users/justi/Documents/__ballAnalytics/bball/NBAModel/cached_data/2024-10-22/game_logs_20241022"  # Adjust to your directory

# Construct the path based on the game and player ID format
player_logs_path = f"{game_logs_dir}/game_{game_id}_player_{player_id}_logs.joblib"

# Check for both extensions (.joblib and .joblib.joblib)
if os.path.exists(player_logs_path):
    player_logs = joblib.load(player_logs_path)
elif os.path.exists(player_logs_path + ".joblib"):
    player_logs = joblib.load(player_logs_path + ".joblib")
else:
    print(f"No game logs found for Player ID {player_id} in Game ID {game_id}.")
    player_logs = None

# If logs are found, print them
if player_logs is not None:
    print(f"Player logs for Player ID {player_id} in Game ID {game_id}:")
    print(player_logs.head())


# In[ ]:


import joblib
import os

# Path to a specific player log file
player_log_file = "C:/Users/justi/Documents/__ballAnalytics/bball/NBAModel/cached_data/2024-10-22/game_22400062_player_1628978_logs.joblib.joblib"

# Check if the file exists before loading
if os.path.exists(player_log_file):
    player_logs = joblib.load(player_log_file)
    print(player_logs.head())  # Display the first few rows to check
else:
    print(f"File {player_log_file} not found.")


# In[ ]:


player_logs


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


# Check if there are any logs for the player
def predict_player_points(player_id, player_logs, opp_def_stats, player_tag):
    """
    Predict points for a player based on player logs, opponent defensive stats, and player tag.
    """
    # Filter player's logs by 'Player_ID'
    recent_game_logs = player_logs[player_logs['Player_ID'] == player_id]
    
    if recent_game_logs.empty:
        print(f"No recent game logs found for Player ID {player_id}")
    
    # Calculate average points per game from recent logs
    avg_points_per_game = recent_game_logs['PTS'].mean() if not recent_game_logs.empty else 0
    
    # Ensure we have a valid average
    if avg_points_per_game == 0:
        print(f"Player ID {player_id} has an average of 0 points per game in the logs.")
    
    # Incorporate opponent defense stats to adjust the prediction
    def_factor = opp_def_stats['E_DEF_RATING'] / 100 if not opp_def_stats.empty else 1
    pace_factor = opp_def_stats['E_PACE'] / 100 if not opp_def_stats.empty else 1
    
    # Adjust prediction based on whether the player is a core player (1) or bench player (2)
    player_tag_factor = 1.1 if player_tag == 1 else 0.9
    
    # Calculate expected points
    expected_points = avg_points_per_game * def_factor * pace_factor * player_tag_factor

    # Ensure opponent stats are valid
    if opp_def_stats.empty:
        print(f"Opponent defensive stats are empty or missing for Player ID {player_id}")
    else:
        print(f"Using opponent defensive stats: DEF_RATING: {opp_def_stats['E_DEF_RATING']}, PACE: {opp_def_stats['E_PACE']}")
    
    return expected_points


# In[ ]:


def calculate_tagged_players_points(player_df, player_logs, opp_def_stats):
    """
    Calculate expected points for tagged players in a team, based on opponent defensive stats.
    
    Args:
        player_df (pd.DataFrame): DataFrame of player stats for the team.
        player_logs (pd.DataFrame): DataFrame of player game logs.
        opp_def_stats (pd.DataFrame): Defensive stats of the opponent team.
    
    Returns:
        pd.DataFrame: DataFrame with player ID, name, expected points, and tag.
    """
    expected_points_list = []
    
    # Loop through tagged players
    for _, player in player_df.iterrows():
        player_id = player['PLAYER_ID']
        player_name = player['PLAYER']
        player_tag = player['PLAYER_TAG']  # Core (1) or Bench (2)
        
        # Predict player points based on logs and opponent defense
        expected_points = predict_player_points(
            player_id, player_logs, opp_def_stats, player_tag
        )
        
        # Append results
        expected_points_list.append({
            'Player_ID': player_id,
            'Player_Name': player_name,
            'Expected_Points': expected_points,
            'Player_Tag': player_tag
        })
    
    # Return as DataFrame
    return pd.DataFrame(expected_points_list)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


def predict_team_total_based_on_players(player_stats, player_logs, team_stats, opp_team_stats):
    """
    Predicts team total based on the summation of individual player points and the team's average stats.
    
    Args:
        player_stats (pd.DataFrame): DataFrame containing stats for individual players.
        player_logs (pd.DataFrame): DataFrame of recent player game logs.
        team_stats (pd.DataFrame): Team's season stats.
        opp_team_stats (pd.DataFrame): Opponent's season stats.
    
    Returns:
        float: Predicted total points for the team.
    """
    total_team_points = 0

    # Loop through the top players based on minutes or usage
    for _, player in player_stats.iterrows():
        player_id = player['PLAYER_ID']

        # Fetch player game logs
        player_game_logs = player_logs[player_logs['PLAYER_ID'] == player_id]

        # Predict player points based on game logs and matchup
        player_points = predict_player_points(player_game_logs, player, opp_team_stats)
        
        total_team_points += player_points

    # Adjust the team total based on overall team stats and opponent's defense
    team_off_rating = team_stats['E_OFF_RATING'].values[0]
    opp_def_rating = opp_team_stats['E_DEF_RATING'].values[0]
    
    predicted_team_total = total_team_points * (team_off_rating / opp_def_rating)
    
    return predicted_team_total


# In[ ]:


def adjust_points_based_on_position(player_stats, opp_team_defense, position_defense_matrix):
    """
    Adjusts player points based on opponent's defense against the player's position.
    
    Args:
        player_stats (pd.DataFrame): Stats of the player.
        opp_team_defense (pd.DataFrame): Opponent's defensive stats.
        position_defense_matrix (pd.DataFrame): Matrix indicating how the opponent defends each position.
    
    Returns:
        float: Adjusted points for the player.
    """
    player_position = player_stats['POSITION'].values[0]
    
    # Get the opponent's defensive performance against this position
    position_def_rating = position_defense_matrix.get(player_position, opp_team_defense['E_DEF_RATING'].values[0])
    
    player_points = player_stats['PTS'].mean()  # Example: could be adjusted based on recent performance
    adjusted_points = player_points * (player_stats['E_OFF_RATING'].values[0] / position_def_rating)
    
    return adjusted_points


# In[ ]:





# In[ ]:





# In[ ]:


def calculate_expected_points_for_all_games(todays_games, player_logs):
    """
    Calculate expected points for all games in today's schedule, based on player logs and team defense.
    
    Args:
        todays_games (pd.DataFrame): DataFrame containing today's game information.
        player_logs (pd.DataFrame): DataFrame containing player game logs.
    
    Returns:
        dict: A dictionary with game IDs as keys and player expected points data as values.
    """
    all_games_expected_points = {}
    
    # Loop through each game
    for _, game in todays_games.iterrows():
        game_id = game['Game ID']
        home_team_abbr = game['Home Team Abbreviation']
        away_team_abbr = game['Visiting Team Abbreviation']
        game_date = game['Game Date']
        
        # Load home and away player dataframes (replace with actual dataframes for home and away teams)
        home_player_df = globals()[f'game_{game_id}_home_team_{home_team_abbr}_prev_player_df']
        away_player_df = globals()[f'game_{game_id}_away_team_{away_team_abbr}_prev_player_df']
        
        # Load defensive stats for opponent teams
        home_team_def_stats = globals()[f'game_{game_id}_home_team_{home_team_abbr}_prev_team_df']
        away_team_def_stats = globals()[f'game_{game_id}_away_team_{away_team_abbr}_prev_team_df']
        
        # Calculate expected points for home team players
        print(f"Calculating expected points for home team {home_team_abbr} players...")
        home_expected_points_df = calculate_tagged_players_points(home_player_df, player_logs, away_team_def_stats)
        
        # Calculate expected points for away team players
        print(f"Calculating expected points for away team {away_team_abbr} players...")
        away_expected_points_df = calculate_tagged_players_points(away_player_df, player_logs, home_team_def_stats)
        
        # Store results in the dictionary
        all_games_expected_points[game_id] = {
            'Home Team': home_team_abbr,
            'Away Team': away_team_abbr,
            'Home Expected Points': home_expected_points_df,
            'Away Expected Points': away_expected_points_df
        }
    
    return all_games_expected_points

# Call the function to calculate expected points for all games
all_games_expected_points = calculate_expected_points_for_all_games(todays_games, player_logs)


# In[ ]:


all_games_expected_points


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


def calculate_team_possessions_and_ppp(team_df):
    """
    Calculates the total possessions and points per possession (PPP) for a team.
    Args:
        team_df (pd.DataFrame): DataFrame containing team statistics.
    
    Returns:
        float: Points per possession (PPP).
    """
    possessions = team_df['FGA'] + 0.44 * team_df['FTA'] - team_df['OREB'] + team_df['TOV']
    ppp = team_df['PTS'] / possessions
    return ppp


# In[ ]:


def adjust_team_points_based_on_defense(team_ppp, opponent_def_ppp):
    """
    Adjusts the team's projected points based on the opponent's defensive efficiency.
    Args:
        team_ppp (float): Team's offensive PPP.
        opponent_def_ppp (float): Opponent's defensive PPP.
    
    Returns:
        float: Adjusted team points.
    """
    adjustment_factor = team_ppp / opponent_def_ppp
    adjusted_points = team_ppp * adjustment_factor
    return adjusted_points


# In[ ]:


def predict_player_points(player_df, opponent_def_stats):
    """
    Predicts player points based on usage percentage, minutes, and opponent defense.
    Args:
        player_df (pd.DataFrame): Player statistics.
        opponent_def_stats (pd.DataFrame): Opponent's defensive stats.
    
    Returns:
        float: Predicted points for the player.
    """
    player_minutes = player_df['MIN_x']
    player_usage = player_df['E_USG_PCT']
    
    # Factor in defensive effectiveness of the opponent's player at the same position
    opponent_def_rating = opponent_def_stats.loc[opponent_def_stats['POSITION'] == player_df['POSITION'], 'E_DEF_RATING'].values[0]
    
    # Simple scoring estimate based on usage and minutes, adjusted by opponent defense
    predicted_points = player_usage * player_minutes * (1 / opponent_def_rating)
    return predicted_points


# In[ ]:


def calculate_team_total_points(player_stats, opponent_def_stats):
    """
    Calculates total team points by aggregating individual player points.
    Args:
        player_stats (pd.DataFrame): Player statistics for the team.
        opponent_def_stats (pd.DataFrame): Opponent's defensive stats.
    
    Returns:
        float: Projected total team points.
    """
    total_points = 0
    for index, player_row in player_stats.iterrows():
        player_points = predict_player_points(player_row, opponent_def_stats)
        total_points += player_points
    return total_points


# In[ ]:


def list_available_dataframes():
    """
    Lists the names of all dataframes currently available in the global scope.
    """
    available_dataframes = [name for name in globals() if isinstance(globals()[name], pd.DataFrame)]
    print("Available DataFrames:")
    for df_name in available_dataframes:
        print(df_name)
    return available_dataframes

# Call the function to list the available DataFrames
available_dataframes = list_available_dataframes()


# In[ ]:




