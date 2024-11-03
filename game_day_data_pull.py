#!/usr/bin/env python
# coding: utf-8

# In[65]:


import pandas as pd
import matplotlib.pyplot as plt
from classes import NBATeamRosters
from cache_manager import CacheManager
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime
import json
import seaborn as sns
import joblib
import os
import time


# In[66]:


nba_data = NBATeamRosters(season="2024")
cache_manager = CacheManager()


# In[67]:


# Fetch the list of teams
#nba_data.fetch_teams()

# Display the teams DataFrame
#nba_data.teams_df


# # Set Constants

# In[68]:


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


# In[69]:


# Define the previous season
previous_season = "2023-24"
current_season = "2024-25"


# # Full Schedule

# In[70]:


# Fetch Teams Master List 
nba_data.fetch_teams()
teams_df = nba_data.teams_df


# In[71]:


# Load the schedule from your CSV file
nba_data.load_schedule('nbaSchedule2425.csv')  # Replace with your actual CSV path


# In[72]:


# Display the full schedule
schedule_df = nba_data.get_full_schedule()


# # Today's Games

# In[73]:


#today_date = ("2024-10-22")
today_date = datetime.datetime.today().strftime('%Y-%m-%d')
game_date = today_date
# Retrieve today's games
todays_games = nba_data.get_todays_games(today_date) # Use today_date when in season


# ## Fetch and Cache Previous Season Player Data

# In[61]:


pause_time = 10

# Loop through each game in 'todays_games'
for index, game in todays_games.iterrows():
    game_id = game['Game ID']
    home_team_abbr = game['Home Team Abbreviation']
    away_team_abbr = game['Visiting Team Abbreviation']
    game_date = game['Game Date']  # Game date in 'YYYY-MM-DD' format
    season = "2023-24"  # Set to the previous season

    print(f"Fetching data for Game ID {game_id}: Home: {home_team_abbr}, Away: {away_team_abbr} for {season}")
    
    # Fetch home team stats for the 2023-24 season
    try:
        home_team_stats = nba_data.fetch_player_stats_single_team(home_team_abbr, season)
        print(f"Fetched 2023-24 season data for home team {home_team_abbr}")
    except Exception as e:
        print(f"Error fetching 2023-24 season data for home team {home_team_abbr}: {e}")
        home_team_stats = pd.DataFrame()  # Return an empty DataFrame if an error occurs

    # Pause to avoid hitting API rate limits
    time.sleep(pause_time)
    
    # Fetch away team stats for the 2023-24 season
    try:
        away_team_stats = nba_data.fetch_player_stats_single_team(away_team_abbr, season)
        print(f"Fetched 2023-24 season data for away team {away_team_abbr}")
    except Exception as e:
        print(f"Error fetching 2023-24 season data for away team {away_team_abbr}: {e}")
        away_team_stats = pd.DataFrame()  # Return an empty DataFrame if an error occurs

    # Pause to avoid hitting API rate limits
    time.sleep(pause_time)
    
    # Cache the home and away team stats for the 2023-24 season, organized by game date with the new file naming format
    cache_manager.cache_data(home_team_stats, f"game_{game_id}_home_team_{home_team_abbr}_prev", game_date)
    cache_manager.cache_data(away_team_stats, f"game_{game_id}_away_team_{away_team_abbr}_prev", game_date)

    print(f"Cached data for Game ID {game_id}: Home: {home_team_abbr}, Away: {away_team_abbr} for {season}")


time.sleep(360)
# ## Fetch and Cache Current Season Player Data

# In[62]:


# Loop through each game in 'todays_games'
for index, game in todays_games.iterrows():
    game_id = game['Game ID']
    home_team_abbr = game['Home Team Abbreviation']
    away_team_abbr = game['Visiting Team Abbreviation']
    game_date = game['Game Date']  # Game date in 'YYYY-MM-DD' format
    
    # Handle only the current season
    current_season = "2024-25"

    print(f"Fetching data for Game ID {game_id}: Home: {home_team_abbr}, Away: {away_team_abbr} for {current_season}")
    
    # Fetch home team stats for current season (expecting null values)
    try:
        home_team_stats = nba_data.fetch_player_stats_single_team(home_team_abbr, current_season)
        if home_team_stats.empty:
            raise ValueError("No data for the current season")
        print(f"Fetched current season data for home team {home_team_abbr}")
    except Exception as e:
        print(f"No data available for current season for home team {home_team_abbr}: {e}")
        # Fill with NaN values since no current season data is available
        home_team_stats = pd.DataFrame(columns=["PLAYER", "PLAYER_ID", "SEASON_ID", "PTS", "AST", "REB", "etc..."])

    # Pause to avoid hitting API rate limits
    time.sleep(pause_time)
    
    # Fetch away team stats for current season (expecting null values)
    try:
        away_team_stats = nba_data.fetch_player_stats_single_team(away_team_abbr, current_season)
        if away_team_stats.empty:
            raise ValueError("No data for the current season")
        print(f"Fetched current season data for away team {away_team_abbr}")
    except Exception as e:
        print(f"No data available for current season for away team {away_team_abbr}: {e}")
        # Fill with NaN values since no current season data is available
        away_team_stats = pd.DataFrame(columns=["PLAYER", "PLAYER_ID", "SEASON_ID", "PTS", "AST", "REB", "etc..."])

    # Pause to avoid hitting API rate limits
    time.sleep(pause_time)
    
    # Cache the home and away team stats for the 2023-24 season, organized by game date with the new file naming format
    cache_manager.cache_data(home_team_stats, f"game_{game_id}_home_team_{home_team_abbr}_curr", game_date)
    cache_manager.cache_data(away_team_stats, f"game_{game_id}_away_team_{away_team_abbr}_curr", game_date)

    print(f"Cached data for Game ID {game_id}: Home: {home_team_abbr}, Away: {away_team_abbr} for {current_season}")


time.sleep(360)
### Fetch and Cache Team Stats 


# In[63]:


# Call the existing function to get the compiled team stats for today's games
team_stats_today = nba_data.get_team_stats_today_games(date=today_date)

all_games_list = []


# In[64]:


# Loop through the games in the dictionary to pair up home and away stats
for game_id, team_stats in team_stats_today.items():
    # Extract the stats for home and away teams for both previous and current seasons
    home_previous_stats = team_stats['home_previous_season']
    home_current_stats = team_stats['home_current_season']
    away_previous_stats = team_stats['away_previous_season']
    away_current_stats = team_stats['away_current_season']
    
    # If the DataFrame is not empty, add a 'Game ID' column for tracking
    if not home_previous_stats.empty:
        home_previous_stats['Game_ID'] = game_id
    if not home_current_stats.empty:
        home_current_stats['Game_ID'] = game_id
    if not away_previous_stats.empty:
        away_previous_stats['Game_ID'] = game_id
    if not away_current_stats.empty:
        away_current_stats['Game_ID'] = game_id

    # Get team abbreviations for file naming
    home_team_abbr = home_previous_stats.iloc[0]['TEAM_ABBREVIATION'] if not home_previous_stats.empty else "unknown"
    away_team_abbr = away_previous_stats.iloc[0]['TEAM_ABBREVIATION'] if not away_previous_stats.empty else "unknown"

    # Cache home team stats (previous and current season)
    if not home_previous_stats.empty:
        cache_manager.cache_data(home_previous_stats, f"game_{game_id}_home_team_{home_team_abbr}_prev_team_stats", today_date)
    if not home_current_stats.empty:
        cache_manager.cache_data(home_current_stats, f"game_{game_id}_home_team_{home_team_abbr}_curr_team_stats", today_date)

    # Cache away team stats (previous and current season)
    if not away_previous_stats.empty:
        cache_manager.cache_data(away_previous_stats, f"game_{game_id}_away_team_{away_team_abbr}_prev_team_stats", today_date)
    if not away_current_stats.empty:
        cache_manager.cache_data(away_current_stats, f"game_{game_id}_away_team_{away_team_abbr}_curr_team_stats", today_date)

    print(f"Cached team stats for Game ID {game_id}: Home: {home_team_abbr}, Away: {away_team_abbr}")


# In[ ]:




