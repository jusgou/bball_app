#!/usr/bin/env python
# coding: utf-8

# # Imports

# In[16]:


import pandas as pd
import numpy as np
import re


# In[17]:


pd.set_option('display.max_columns', None)


# In[18]:


pd.set_option('display.max_rows', 150)


# # TEAM STATS COMPILER

# ### Import Team List for Team IDs

# In[23]:


from nba_api.stats.static import teams

# get_teams returns a list of 30 dictionaries, each an NBA team.
nba_teams = teams.get_teams()
print("Number of teams fetched: {}".format(len(nba_teams)))
nba_teams[0:29]
teams = pd.DataFrame(nba_teams)
teams = teams.set_index('id') 


# # LEAGUE AND DIVISIONAL STANDINGS

# ## League Standings

# In[24]:


from nba_api.stats.endpoints._base import Endpoint
from nba_api.stats.library.http import NBAStatsHTTP
from nba_api.stats.library.parameters import (
    LeagueID,
    Season,
    SeasonType,
    SeasonNullable,
)
from nba_api.stats.endpoints.leaguestandingsv3 import LeagueStandingsV3


# In[25]:


def season_standings(season, season_type): 
    league_standings = LeagueStandingsV3(
        league_id="00",
        season=season,              # Example: "2022-23" for the 2022-2023 season
        season_type=season_type   # Example: "Regular Season", "Playoffs", etc.
    )
    standings_data = league_standings.standings.get_dict()
    standings = pd.DataFrame(standings_data['data'], columns=standings_data['headers'])
    standings.index = np.arange(1, len(standings) + 1)
    standings = standings.drop(columns=['SeasonID','LeagueID', 'TeamSlug', 'LeagueRank','LongHomeStreak', 'LongRoadStreak','CurrentHomeStreak','CurrentRoadStreak','CurrentStreak'])
    return standings 


# In[30]:


league_standings = season_standings('2024-25', 'Regular Season')


# In[37]:


league_standings = league_standings.rename(columns={'TeamID': 'id'})


# ## Standings By Division

# In[63]:


def process_league_standings(season, season_type):
    # Step 1: Retrieve and rename columns
    league_standings = season_standings(season, season_type)
    league_standings = league_standings.rename(columns={'TeamID': 'id'})
    
    # Step 2: Group by 'Division' and separate into individual DataFrames
    grouped = league_standings.groupby('Division')
    divisions = {division_name: group for division_name, group in grouped}
    
    # Step 3: Prepare and modify DataFrames for each division
    modified_dfs = []
    for division, df in divisions.items():
        # Create a separator row with the division name
        separator_df = pd.DataFrame([{col: '' for col in df.columns}])
        separator_df.iloc[0, df.columns.get_loc('Division')] = division
        
        # Concatenate the separator row and the division DataFrame
        combined_df = pd.concat([separator_df, df])
        
        # Append the combined DataFrame to the list
        modified_dfs.append(combined_df)
    
    # Step 4: Combine all modified DataFrames
    final_df = pd.concat(modified_dfs, ignore_index=True)
    
    # Step 5: Order by 'Conference' and set specific values
    conference_order = ['East', 'West']
    final_df['Conference'] = pd.Categorical(final_df['Conference'], categories=conference_order, ordered=True)
    
    # Setting specific 'Conference' values based on the provided indices
    final_df.loc[0, 'Conference'] = "East"
    final_df.loc[6, 'Conference'] = "East"
    final_df.loc[12, 'Conference'] = "West"
    final_df.loc[18, 'Conference'] = "West"
    final_df.loc[24, 'Conference'] = "East"
    final_df.loc[30, 'Conference'] = "West"
    
    # Step 6: Sort the DataFrame by 'Conference' and 'Division'
    final_df_sorted = final_df.sort_values(by=['Conference', 'Division'])
    
    return final_df_sorted


# In[66]:


NBA_Standings_2024 = process_league_standings('2024-25', 'Regular Season')


# In[ ]:




