from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import cumestatsteamgames, cumestatsteam, gamerotation
import pandas as pd
import datetime
import numpy as np
import json
import difflib
import time
import requests
import re
import time
from cache_manager import CacheManager
import os


# In[5]:


from nba_api.stats.static import teams as nba_teams_module
from nba_api.stats.endpoints.commonteamroster import CommonTeamRoster
from nba_api.stats.endpoints.leaguestandingsv3 import LeagueStandingsV3
from nba_api.stats.endpoints.playercompare import PlayerCompare
from nba_api.stats.endpoints import playercareerstats, playergamelog
from nba_api.stats.endpoints import winprobabilitypbp
from nba_api.stats.endpoints import teamestimatedmetrics, playerestimatedmetrics
from nba_api.stats.endpoints import teamdashboardbygeneralsplits
from nba_api.stats.endpoints import teaminfocommon
from nba_api.stats.endpoints import PlayerVsPlayer, matchupsrollup

pause_time = 10


class NBATeamRosters:
    def __init__(self, season):
        """
        Initializes the class and sets up the teams DataFrame and an empty dictionary for rosters.
        Args:
            season (str): The NBA season in 'YYYY' format, e.g., '2024' for the 2024-2025 season.
        """
        self.season = season
        self.teams_df = None
        self.rosters = {}
        self.standings_df = None
        self.schedule_df = None
        self.player_stats = {}
        self.fetch_teams()
        self.cache_manager = CacheManager()
    
    def load_schedule(self, csv_path):
        """
        Loads the NBA schedule from a CSV file into the class.
        Args:
            csv_path (str): The file path to the schedule CSV file.
        """
        self.schedule_df = pd.read_csv(csv_path)
        print(f"Schedule loaded: {len(self.schedule_df)} games")
    
    def get_full_schedule(self):
        """
        Returns the full schedule DataFrame.
        Returns:
            pd.DataFrame: DataFrame containing the entire schedule.
        """
        if self.schedule_df is not None:
            return self.schedule_df
        else:
            print("Schedule not loaded.")
            return None

    def filter_schedule_by_team(self, team_abbr):
        """
        Filters the schedule DataFrame to return games where the specified team is playing.
        
        Args:
            team_abbr (str): The team abbreviation (e.g., 'GSW', 'LAL') to filter by.
        
        Returns:
            pd.DataFrame: Filtered DataFrame with the team's games (both home and away).
        """
        if self.schedule_df is not None:
            # Filter games where the team is either the home or visiting team
            filtered_schedule = self.schedule_df[
                (self.schedule_df['Home Team Abbreviation'] == team_abbr) | 
                (self.schedule_df['Visiting Team Abbreviation'] == team_abbr)
            ]
            return filtered_schedule
        else:
            print("Schedule DataFrame not loaded.")
            return None
    
    def map_team_abbreviations_to_ids(self, abbreviation):
        """
        Maps a team abbreviation to the corresponding team ID.
        Args:
            abbreviation (str): The team's abbreviation (e.g., 'GSW').
        
        Returns:
            int: The corresponding team ID.
        """
        try:
            team_id = self.teams_df[self.teams_df['abbreviation'] == abbreviation].index[0]
            return team_id
        except IndexError:
            print(f"Team abbreviation {abbreviation} not found.")
            return None

    def add_team_ids_to_schedule(self):
        """
        Adds 'Home Team ID' and 'Visiting Team ID' columns to the schedule_df DataFrame.
        """
        if self.schedule_df is not None and self.teams_df is not None:
            self.schedule_df['Home Team ID'] = self.schedule_df['Home Team Abbreviation'].apply(self.map_team_abbreviations_to_ids)
            self.schedule_df['Visiting Team ID'] = self.schedule_df['Visiting Team Abbreviation'].apply(self.map_team_abbreviations_to_ids)
            print("Team IDs added to schedule.")
        else:
            print("Schedule or Teams DataFrame not loaded.")

    def get_todays_games(self, today):
        """
        Retrieves the games scheduled for a specific date.
        Args:
            today (str): The date in 'YYYY-MM-DD' format.

        Returns:
            pd.DataFrame: DataFrame containing the games scheduled for the specified date.
        """
        if self.schedule_df is not None:
            todays_games = self.schedule_df[self.schedule_df["Game Date"] == today]
            return todays_games
        else:
            print("Schedule not loaded.")
            return None
    
    def fetch_teams(self):
        """
        Fetches NBA teams and stores them in a DataFrame.
        """
        nba_teams = nba_teams_module.get_teams()
        self.teams_df = pd.DataFrame(nba_teams).set_index('id')

    def fetch_rosters(self):
        """
        Fetches and stores the rosters for each team.
        The rosters are stored in a dictionary with keys as the team's abbreviation, nickname, and city.
        """
        for team_id, team_data in self.teams_df.iterrows():
            team_roster = self.get_team_roster(team_id)
            if team_roster is not None:
                self.rosters[team_data['abbreviation']] = team_roster
                self.rosters[team_data['nickname']] = team_roster
                self.rosters[team_data['city']] = team_roster

    def get_team_roster(self, team_identifier):
        """
        Fetches the roster for a single team based on the team ID, abbreviation, or name.
        Args:
            team_identifier (int or str): The team's ID (int), abbreviation (str), or name (str).
        
        Returns:
            pd.DataFrame: DataFrame containing the team's roster.
        """
        # Determine the type of the input and find the team ID
        if isinstance(team_identifier, int):
            team_id = team_identifier
        elif isinstance(team_identifier, str):
            if team_identifier.upper() in self.teams_df['abbreviation'].values:
                team_id = self.teams_df[self.teams_df['abbreviation'] == team_identifier.upper()].index[0]
            elif team_identifier.title() in self.teams_df['nickname'].values:
                team_id = self.teams_df[self.teams_df['nickname'] == team_identifier.title()].index[0]
            else:
                print(f"Team with identifier '{team_identifier}' not found.")
                return None
        else:
            print(f"Invalid team identifier type: {type(team_identifier)}")
            return None
        
        # Fetch the roster based on the determined team ID
        try:
            rosters_raw = CommonTeamRoster(
                team_id=str(team_id),
                season=self.season
            ).common_team_roster.get_data_frame()
            return rosters_raw
        except Exception as e:
            print(f"Error fetching roster for team ID {team_id}: {e}")
            return None

    def get_roster_by_id(self, team_id):
        """
        Retrieves the roster DataFrame for a given team ID.
        Args:
            team_id (int): The team's ID.
        
        Returns:
            pd.DataFrame: DataFrame containing the team's roster.
        """
        return self.rosters_by_id.get(team_id, pd.DataFrame())

    def get_roster_by_name(self, team_name):
        """
        Retrieves the roster DataFrame for a given team name.
        Args:
            team_name (str): The team's name (nickname).
        
        Returns:
            pd.DataFrame: DataFrame containing the team's roster.
        """
        return self.rosters_by_name.get(team_name, pd.DataFrame())

    def get_roster_by_abbreviation(self, team_abbreviation):
        """
        Retrieves the roster DataFrame for a given team abbreviation.
        Args:
            team_abbreviation (str): The team's abbreviation (e.g., 'GSW').
        
        Returns:
            pd.DataFrame: DataFrame containing the team's roster.
        """
        return self.rosters_by_abbreviation.get(team_abbreviation, pd.DataFrame())

    
    def get_team_id_from_abbreviation(self, team_abbreviation):
        """
        Retrieves the team ID for a given team abbreviation.
        Args:
            team_abbreviation (str): The team abbreviation (e.g., 'GSW' for Golden State Warriors).
    
        Returns:
            int: The team ID corresponding to the abbreviation, or None if not found.
        """
        try:
            team_id = self.teams_df[self.teams_df['abbreviation'] == team_abbreviation].index[0]
            return team_id
        except IndexError:
            print(f"Team abbreviation {team_abbreviation} not found.")
            return None

    def fetch_team_stats(self, team_abbreviation, season):
        """
        Fetches team statistics for a given team abbreviation and season.
        Args:
            team_abbreviation (str): The team abbreviation (e.g., 'GSW' for Golden State Warriors).
            season (str): The NBA season in 'YYYY-YY' format (e.g., '2023-24').
    
        Returns:
            pd.DataFrame: DataFrame containing the team's statistics, including TEAM_ID and TEAM_NAME.
        """
        # Get team ID from abbreviation
        team_id = self.get_team_id_from_abbreviation(team_abbreviation)
        
        if not team_id:
            print(f"Team ID not found for abbreviation {team_abbreviation}.")
            return {}
        
        # Fetch team stats
        team_stats_df = teamdashboardbygeneralsplits.TeamDashboardByGeneralSplits(
            team_id=team_id,
            season=season,
            season_type_all_star='Regular Season'
        ).overall_team_dashboard.get_data_frame()
    
        # Add TEAM_ID and TEAM_NAME to the DataFrame
        team_stats_df['TEAM_ID'] = team_id
        team_stats_df['TEAM_NAME'] = self.teams_df.loc[team_id, 'full_name']  # Assuming self.teams_df has full team names
    
        return team_stats_df
    
    def fetch_league_standings(self, season_type):
        """
        Fetches the league standings for the specified season and season type.
        Args:
            season_type (str): The type of season ('Regular Season', 'Playoffs', etc.)

        Returns:
            pd.DataFrame: DataFrame containing the league standings.
        """
        league_standings = LeagueStandingsV3(
            league_id="00",
            season=self.season,
            season_type=season_type
        )
        standings_data = league_standings.standings.get_dict()
        self.standings_df = pd.DataFrame(standings_data['data'], columns=standings_data['headers'])
        self.standings_df.index = np.arange(1, len(self.standings_df) + 1)
        self.standings_df = self.standings_df.drop(columns=['SeasonID', 'LeagueID', 'TeamSlug', 'LeagueRank'])
        self.standings_df = self.standings_df.rename(columns={'TeamID': 'id'})
        return self.standings_df

    def process_league_standings(self, season_type):
        """
        Processes the league standings by division and conference.
        Args:
            season_type (str): The type of season ('Regular Season', 'Playoffs', etc.)

        Returns:
            pd.DataFrame: DataFrame sorted by Conference and Division.
        """
        league_standings = self.fetch_league_standings(season_type)

        grouped = league_standings.groupby('Division')
        divisions = {division_name: group for division_name, group in grouped}
        
        modified_dfs = []
        for division, df in divisions.items():
            separator_df = pd.DataFrame([{col: '' for col in df.columns}])
            separator_df.iloc[0, df.columns.get_loc('Division')] = division
            
            combined_df = pd.concat([separator_df, df])
            modified_dfs.append(combined_df)
        
        final_df = pd.concat(modified_dfs, ignore_index=True)
        
        conference_order = ['East', 'West']
        final_df['Conference'] = pd.Categorical(final_df['Conference'], categories=conference_order, ordered=True)
        
        final_df.loc[0, 'Conference'] = "East"
        final_df.loc[6, 'Conference'] = "East"
        final_df.loc[12, 'Conference'] = "West"
        final_df.loc[18, 'Conference'] = "West"
        final_df.loc[24, 'Conference'] = "East"
        final_df.loc[30, 'Conference'] = "West"
        
        final_df_sorted = final_df.sort_values(by=['Conference', 'Division'])
        
        return final_df_sorted

    def fetch_player_career_stats(self, player_id):
        """
        Fetches career statistics for a player.
        Args:
            player_id (int): The ID of the player.

        Returns:
            pd.DataFrame: DataFrame containing the player's career statistics.
        """
        try:
            career_stats = playercareerstats.PlayerCareerStats(player_id=player_id).get_data_frames()[0]
            self.player_stats[player_id] = {'career_stats': career_stats}
            return career_stats
        except Exception as e:
            print(f"Error fetching career stats for player ID {player_id}: {e}")
            return None

    def fetch_player_game_logs(self, player_id, season):
        """
        Fetches game logs for a player for a specific season.
        Args:
            player_id (int): The ID of the player.
            season (str): The NBA season in 'YYYY-YY' format, e.g., '2024-25'.

        Returns:
            pd.DataFrame: DataFrame containing the player's game logs for the season.
        """
        try:
            game_logs = playergamelog.PlayerGameLog(player_id=player_id, season=season).get_data_frames()[0]
            if player_id not in self.player_stats:
                self.player_stats[player_id] = {}
            self.player_stats[player_id]['game_logs'] = game_logs
            return game_logs
        except Exception as e:
            print(f"Error fetching game logs for player ID {player_id} in season {season}: {e}")
            return None

    def get_player_stats(self, player_id):
        """
        Retrieves the stored stats for a specific player.
        Args:
            player_id (int): The ID of the player.

        Returns:
            dict: A dictionary containing the player's career stats and game logs.
        """
        return self.player_stats.get(player_id, None)

    def compare_players(self, home_player_id, visiting_player_id, season="2023-24"):
        """
        Compares two players using the NBA API's PlayerCompare endpoint.
        Args:
            home_player_id (int): The ID of the home player.
            visiting_player_id (int): The ID of the visiting player.
            season (str): The NBA season in 'YYYY-YY' format, e.g., '2023-24'.

        Returns:
            dict: A dictionary with individual comparison data and overall comparison data.
        """
        try:
            player_compare_api = PlayerCompare(
                vs_player_id_list=str(visiting_player_id),
                player_id_list=str(home_player_id),
                season=season
            )
            
            # Retrieve the data
            individual_data = player_compare_api.individual.get_data_frame()
            overall_compare_data = player_compare_api.overall_compare.get_data_frame()
            
            return {
                "individual_data": individual_data,
                "overall_compare_data": overall_compare_data
            }
        except Exception as e:
            print(f"Error comparing players {home_player_id} and {visiting_player_id}: {e}")
            return None

    def fetch_win_probability(self, game_id, run_type='each second'):
        """
        Fetches the win probability data for a specific game using the NBA API.
        Args:
            game_id (str): The ID of the game.
            run_type (str): The granularity of the data ('each second', 'each possession').

        Returns:
            dict: A dictionary with game information and win probability data.
        """
        try:
            win_prob_api = winprobabilitypbp.WinProbabilityPBP(
                game_id=game_id,
                run_type=run_type
            )
            
            game_info = win_prob_api.game_info.get_data_frame()
            win_prob_data = win_prob_api.win_prob_pbp.get_data_frame()
            
            return {
                "game_info": game_info,
                "win_probability": win_prob_data
            }
        except Exception as e:
            print(f"Error fetching win probability data for Game ID {game_id}: {e}")
            return None

    def get_win_probability(self, game_id, run_type='each second'):
        """
        Retrieves the win probability data for a specific game.
        Args:
            game_id (str): The ID of the game.
            run_type (str): The granularity of the data ('each second', 'each possession').

        Returns:
            pd.DataFrame: DataFrame containing the win probability data.
        """
        win_prob_data = self.fetch_win_probability(game_id, run_type)
        if win_prob_data:
            return win_prob_data['win_probability']
        else:
            return None

    def convert_to_per_game_stats(self, player_stats):
        """
        Converts cumulative stats in a DataFrame to per-game stats.
        
        Args:
            player_stats (pd.DataFrame): The player stats DataFrame with cumulative totals.
        
        Returns:
            pd.DataFrame: A new DataFrame with per-game stats.
        """
        per_game_stats = player_stats.copy()
        
        # Calculate per-game averages for relevant columns
        stats_to_average = ['MIN', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']
        for stat in stats_to_average:
            per_game_stats[stat] = (per_game_stats[stat] / per_game_stats['GP']).round(2)
        return per_game_stats

    def fetch_team_estimated_metrics(self, season):
        """
        Fetches team estimated metrics for a given season.
        
        Args:
            season (str): The NBA season in 'YYYY-YY' format (e.g., '2023-24').
        
        Returns:
            pd.DataFrame: DataFrame containing the team estimated metrics.
        """
        try:
            team_metrics = teamestimatedmetrics.TeamEstimatedMetrics(season=season).get_data_frames()[0]
            return team_metrics
        except Exception as e:
            print(f"Error fetching team estimated metrics for season {season}: {e}")
            return None
    
    def fetch_player_estimated_metrics(self, season):
        """
        Fetches player estimated metrics for a given season.
        
        Args:
            season (str): The NBA season in 'YYYY-YY' format (e.g., '2023-24').
        
        Returns:
            pd.DataFrame: DataFrame containing the player estimated metrics.
        """
        try:
            player_metrics = playerestimatedmetrics.PlayerEstimatedMetrics(season=season).get_data_frames()[0]
            return player_metrics
        except Exception as e:
            print(f"Error fetching player estimated metrics for season {season}: {e}")
            return None

    def fetch_team_season_ranks(self, team_id, season):
        """
        Fetches the TeamSeasonRanks data for a specific team and season.
        
        Args:
            team_id (int): The ID of the team.
            season (str): The NBA season in 'YYYY-YY' format (e.g., '2023-24').
    
        Returns:
            pd.DataFrame: DataFrame containing the team's season ranks, including OPP_PTS_PG.
        """
        try:
            team_info = teaminfocommon.TeamInfoCommon(
                team_id=team_id,
                season_nullable=season,
                season_type_nullable="Regular Season"
            )
            
            # Extract the TeamSeasonRanks data
            team_season_ranks = team_info.team_season_ranks.get_data_frame()
            
            # Print the columns to verify
            print("Team Season Ranks Columns:", team_season_ranks.columns)
            
            return team_season_ranks
        except Exception as e:
            print(f"Error fetching TeamSeasonRanks for team ID {team_id}: {e}")
            return None

    def fetch_and_merge_team_stats(self, team_abbr, previous_season='2023-24', current_season='2024-25'):
        """
        Fetches and merges team stats, estimated metrics, and team season ranks for both previous and current seasons.
        
        Args:
            self (NBATeamRosters): The NBA data object (within the class).
            team_abbr (str): Team abbreviation (e.g., 'BOS', 'NYK').
            previous_season (str): Previous season year format (default: '2023-24').
            current_season (str): Current season year format (default: '2024-25').
    
        Returns:
            dict: Dictionary with merged DataFrames for both previous and current seasons.
        """
        # Fetch team stats for both seasons
        previous_season_stats = self.fetch_team_stats(team_abbr, season=previous_season)
        
        time.sleep(pause_time)
        
        current_season_stats = self.fetch_team_stats(team_abbr, season=current_season)
    
        # Drop unnecessary ranking columns
        columns_to_drop = ['GP_RANK', 'W_RANK', 'L_RANK', 'W_PCT_RANK', 'MIN_RANK']
        
        previous_season_stats = previous_season_stats.drop(columns=columns_to_drop)
        current_season_stats = current_season_stats.drop(columns=columns_to_drop)
    
        # Fetch estimated metrics for both seasons
        previous_season_metrics = self.fetch_team_estimated_metrics(season=previous_season)

        time.sleep(pause_time)
        
        current_season_metrics = self.fetch_team_estimated_metrics(season=current_season)

        time.sleep(pause_time)
        
        # Merge team stats with estimated metrics
        previous_merged_stats = previous_season_stats.merge(previous_season_metrics, on='TEAM_ID', how='left')
        current_merged_stats = current_season_stats.merge(current_season_metrics, on='TEAM_ID', how='left')
    
        # Fetch team season ranks from TeamInfoCommon for both seasons
        team_id = self.get_team_id_from_abbreviation(team_abbr)

        previous_team_info = self.fetch_team_season_ranks(team_id, previous_season)
        
        time.sleep(pause_time)
        
        current_team_info = self.fetch_team_season_ranks(team_id, current_season)

        time.sleep(pause_time)
        
        # Merge team season ranks with the stats data
        previous_merged_stats = previous_merged_stats.merge(previous_team_info[['TEAM_ID', 'PTS_RANK', 'PTS_PG', 'REB_RANK', 'REB_PG', 'AST_RANK', 'AST_PG', 'OPP_PTS_RANK', 'OPP_PTS_PG']], on='TEAM_ID', how='left')
        current_merged_stats = current_merged_stats.merge(current_team_info[['TEAM_ID', 'PTS_RANK', 'PTS_PG', 'REB_RANK', 'REB_PG', 'AST_RANK', 'AST_PG', 'OPP_PTS_RANK', 'OPP_PTS_PG']], on='TEAM_ID', how='left')
    
        # Rename columns for clarity and consistency
        previous_merged_stats = previous_merged_stats.rename(columns={
            'TEAM_NAME_x': 'TEAM_NAME',
            'GP_x': 'GP',
            'W_x': 'W',
            'L_x': 'L',
            'W_PCT_x': 'W_PCT',
            'MIN_x': 'MIN',
        })
    
        current_merged_stats = current_merged_stats.rename(columns={
            'TEAM_NAME_x': 'TEAM_NAME',
            'GP_x': 'GP',
            'W_x': 'W',
            'L_x': 'L',
            'W_PCT_x': 'W_PCT',
            'MIN_x': 'MIN',
        })

        # Add the team abbreviation to both DataFrames
        previous_merged_stats['TEAM_ABBREVIATION'] = team_abbr
        current_merged_stats['TEAM_ABBREVIATION'] = team_abbr
    
        # Move 'TEAM_NAME' to the leftmost side of the DataFrame
        def move_team_name_to_left(df):
            columns = ['TEAM_NAME'] + [col for col in df.columns if col != 'TEAM_NAME']
            return df[columns]
    
        previous_merged_stats = move_team_name_to_left(previous_merged_stats)
        current_merged_stats = move_team_name_to_left(current_merged_stats)
    
        # Drop unnecessary columns
        columns_to_drop_after_merge = ['GROUP_VALUE', 'GROUP_SET', 'TEAM_NAME_y', 'GP_y', 'W_y', 'L_y', 'W_PCT_y','MIN_y','GP_RANK','MIN_RANK','FGM_RANK','FGA_RANK','FG_PCT_RANK','FG3M_RANK','FG3A_RANK','FG3_PCT_RANK','FTM_RANK','FTA_RANK','FT_PCT_RANK','OREB_RANK','DREB_RANK','REB_RANK_x','AST_RANK_x','TOV_RANK','STL_RANK','BLK_RANK','BLKA_RANK','PF_RANK','PFD_RANK','PTS_RANK_x','PLUS_MINUS_RANK']
        
        previous_merged_stats = previous_merged_stats.drop(columns=columns_to_drop_after_merge, errors='ignore')
        current_merged_stats = current_merged_stats.drop(columns=columns_to_drop_after_merge, errors='ignore')

        # Convert cumulative stats to per-game stats
        previous_merged_stats = self.convert_to_per_game_stats(previous_merged_stats)
        current_merged_stats = self.convert_to_per_game_stats(current_merged_stats)
        
        # Return the final data as a dictionary
        return {
            'previous_season': previous_merged_stats,
            'current_season': current_merged_stats
        }

    def get_team_stats_today_games(self, date, previous_season='2023-24', current_season='2024-25'):
        """
        Fetches and compiles team stats for all games scheduled on the specified date,
        for both the previous and current NBA seasons.
    
        Args:
            date (str): The date of the games in 'YYYY-MM-DD' format (e.g., '2024-10-22').
            previous_season (str): The previous season year format (default: '2023-24').
            current_season (str): The current season year format (default: '2024-25').
    
        Returns:
            dict: A dictionary containing the home and away team stats for each game, with separate dataframes 
                  for both the previous and current seasons.
        """
        # Retrieve the games scheduled for the specified date
        todays_games = self.get_todays_games(date)
        
        # Initialize an empty dictionary to store team stats for all games
        all_games_team_stats = {}
    
        # Loop through each game in today's schedule
        for _, game in todays_games.iterrows():
            # Retrieve the abbreviations for home and away teams
            home_team_abbr = game['Home Team Abbreviation']
            away_team_abbr = game['Visiting Team Abbreviation']
            
            # Fetch and merge stats for the home team for both seasons
            home_team_stats = self.fetch_and_merge_team_stats(home_team_abbr, previous_season, current_season)

            time.sleep(pause_time)
            
            # Fetch and merge stats for the away team for both seasons
            away_team_stats = self.fetch_and_merge_team_stats(away_team_abbr, previous_season, current_season)
    
            # Store the compiled stats in the dictionary using the game ID as the key
            all_games_team_stats[game['Game ID']] = {
                'home_previous_season': home_team_stats['previous_season'],
                'home_current_season': home_team_stats['current_season'],
                'away_previous_season': away_team_stats['previous_season'],
                'away_current_season': away_team_stats['current_season']
            }
        
        return all_games_team_stats

    def get_player_ids_by_last_name(self, last_name):
        """
        Searches for players' IDs based on their last name and returns all matches.
        
        Args:
            last_name (str): The player's last name.
        
        Returns:
            list: A list of tuples with the player's name and unique ID if found, otherwise an empty list.
                  Example: [('LeBron James', 2544), ('Rickey James', 1629150)]
        """
        # Get all players from the NBA API
        all_players = players.get_players()
        
        # Use list comprehension to search for players by last name
        matches = [(f"{player['first_name']} {player['last_name']}", player['id']) 
                   for player in all_players if player['last_name'].lower() == last_name.lower()]
        
        if matches:
            return matches
        else:
            print(f"No players found with last name '{last_name}'.")
            return []

    
    def get_player_name_by_id(self, player_id):
        """
        Retrieves a player's full name based on their PLAYER_ID.
        
        Args:
            player_id (int): The player's unique ID.
        
        Returns:
            str: The full name of the player.
        """
        # Get player details from the NBA API
        all_players = players.get_players()
        
        for player in all_players:
            if player['id'] == player_id:
                return f"{player['first_name']} {player['last_name']}"
        
        return "Unknown Player"

    
    def get_individual_player_stats(self, player_identifier, previous_season='2023-24', current_season='2024-25'):
        """
        Fetches individual player stats for both previous and current seasons, including estimated metrics.
        The player can be identified using either PLAYER_ID or last name.
    
        Args:
            player_identifier (int or str): The player's unique ID (int) or last name (str).
            previous_season (str): The previous season in 'YYYY-YY' format (default: '2023-24').
            current_season (str): The current season in 'YYYY-YY' format (default: '2024-25').
    
        Returns:
            tuple: Two DataFrames - one for the previous season and one for the current season, 
                   with estimated metrics included.
        """
        # Determine the player ID based on the input type (ID or Last Name)
        player_id = None
        if isinstance(player_identifier, int):
            # If the input is an integer, assume it is the PLAYER_ID
            player_id = player_identifier
        elif isinstance(player_identifier, str):
            # If the input is a string, assume it's the player's last name
            player_id = self.get_player_ids_by_last_name(player_identifier)
        
        # If the player ID could not be determined, return empty DataFrames
        if player_id is None:
            print(f"Player with identifier '{player_identifier}' not found.")
            return pd.DataFrame(), pd.DataFrame()
        
        # Fetch career stats for the player
        player_stats = self.fetch_player_career_stats(player_id)
        if player_stats is None or player_stats.empty:
            print(f"No career stats available for player ID {player_id}.")
            return pd.DataFrame(), pd.DataFrame()
        
        # Filter stats for the specified seasons
        previous_season_stats = player_stats[player_stats['SEASON_ID'] == previous_season]
        current_season_stats = player_stats[player_stats['SEASON_ID'] == current_season]
    
        # Fetch estimated metrics for the player for both seasons
        previous_season_metrics = self.fetch_player_estimated_metrics(season=previous_season)
        current_season_metrics = self.fetch_player_estimated_metrics(season=current_season)
    
        # Merge player stats with estimated metrics on 'PLAYER_ID'
        if not previous_season_stats.empty:
            previous_season_stats = previous_season_stats.merge(
                previous_season_metrics, on='PLAYER_ID', how='left'
            )
        if not current_season_stats.empty:
            current_season_stats = current_season_stats.merge(
                current_season_metrics, on='PLAYER_ID', how='left'
            )

        # Drop duplicate columns and keep only the first occurrence (remove _y versions)
        def drop_duplicate_columns(df):
            # Identify columns with '_x' or '_y' suffixes
            columns_to_drop = [col for col in df.columns if '_y' in col]
            # Drop _y columns and remove suffix from _x columns
            df = df.drop(columns=columns_to_drop)
            df.columns = df.columns.str.replace('_x', '', regex=False)  # Remove '_x' suffix
            return df

        # Apply the duplicate column handling
        previous_season_stats = drop_duplicate_columns(previous_season_stats)
        current_season_stats = drop_duplicate_columns(current_season_stats)
            
        # Convert cumulative stats to per-game stats
        if not previous_season_stats.empty:
            previous_season_stats = self.convert_to_per_game_stats(previous_season_stats)
        if not current_season_stats.empty:
            current_season_stats = self.convert_to_per_game_stats(current_season_stats)
        
        # Add player name to the DataFrames
        player_name = self.get_player_name_by_id(player_id)
        if not previous_season_stats.empty:
            previous_season_stats['PLAYER'] = player_name
        if not current_season_stats.empty:
            current_season_stats['PLAYER'] = player_name
    
        # Reorder columns to have 'PLAYER' as the first column
        if not previous_season_stats.empty:
            previous_season_stats = previous_season_stats[['PLAYER'] + [col for col in previous_season_stats.columns if col != 'PLAYER']]
        if not current_season_stats.empty:
            current_season_stats = current_season_stats[['PLAYER'] + [col for col in current_season_stats.columns if col != 'PLAYER']]
        
        # Drop unnecessary ranking columns and duplicates (adjust this list as needed)
        columns_to_drop = [
            'GP_RANK', 'W_RANK', 'L_RANK', 'W_PCT_RANK', 'MIN_RANK', 'E_OFF_RATING_RANK', 'E_DEF_RATING_RANK',
            'E_NET_RATING_RANK', 'E_AST_RATIO_RANK', 'E_OREB_PCT_RANK', 'E_DREB_PCT_RANK', 'E_REB_PCT_RANK',
            'E_TOV_PCT_RANK', 'E_USG_PCT_RANK', 'E_PACE_RANK', 'LEAGUE_ID', 'PLAYER_NAME'
        ]
        if not previous_season_stats.empty:
            previous_season_stats = previous_season_stats.drop(columns=[col for col in columns_to_drop if col in previous_season_stats.columns], errors='ignore')
        if not current_season_stats.empty:
            current_season_stats = current_season_stats.drop(columns=[col for col in columns_to_drop if col in current_season_stats.columns], errors='ignore')
    
        return previous_season_stats, current_season_stats


    def fetch_player_stats(self, player_id, season):
        """
        Fetches career stats for a given player for the specified season.
        
        Args:
            player_id (int): The player's unique ID.
            season (str): The season in 'YYYY-YY' format (e.g., '2024-25').
        
        Returns:
            pd.DataFrame: DataFrame containing player stats for the given season.
        """
        try:
            player_stats = self.fetch_player_career_stats(player_id)
            if player_stats is not None and not player_stats.empty:
                return player_stats[player_stats['SEASON_ID'] == season]
            return pd.DataFrame()
        except Exception as e:
            print(f"Error fetching player stats for player ID {player_id} in season {season}: {e}")
            return pd.DataFrame()

    def fetch_player_metrics(self, player_id, season):
        """
        Fetches estimated metrics for a given player for the specified season.
        
        Args:
            player_id (int): The player's unique ID.
            season (str): The season in 'YYYY-YY' format (e.g., '2024-25').
        
        Returns:
            pd.DataFrame: DataFrame containing player estimated metrics.
        """
        try:
            player_metrics = self.fetch_player_estimated_metrics(season=season)
            return player_metrics[player_metrics['PLAYER_ID'] == player_id]
        except Exception as e:
            print(f"Error fetching player metrics for player ID {player_id} in season {season}: {e}")
            return pd.DataFrame()

    def fetch_team_roster_stats(self, team_abbr, season):
        """
        Fetches and compiles player career stats for a given team and season.
        
        Args:
            team_abbr (str): The team abbreviation (e.g., 'MEM', 'NYK').
            season (str): The season in 'YYYY-YY' format (e.g., '2024-25').
        
        Returns:
            pd.DataFrame: DataFrame containing all player stats for the given team and season.
        """
        team_roster = self.get_team_roster(team_abbr)
        season_stats_list = []

        # Fetch stats for each player in the team roster for the given season
        for _, player in team_roster.iterrows():
            player_id = player['PLAYER_ID']
            player_stats = self.fetch_player_stats(player_id, season)
            if not player_stats.empty:
                # Add player name for context
                player_stats = player_stats.merge(
                    team_roster[['PLAYER_ID', 'PLAYER']],
                    on='PLAYER_ID',
                    how='left'
                )
                season_stats_list.append(player_stats)

        # Concatenate player stats into a single DataFrame
        return pd.concat(season_stats_list, ignore_index=True) if season_stats_list else pd.DataFrame()

    def fetch_team_roster_metrics(self, team_abbr, season):
        """
        Fetches and compiles estimated metrics for a given team and season.
        
        Args:
            team_abbr (str): The team abbreviation (e.g., 'MEM', 'NYK').
            season (str): The season in 'YYYY-YY' format (e.g., '2024-25').
        
        Returns:
            pd.DataFrame: DataFrame containing all player metrics for the given team and season.
        """
        team_roster = self.get_team_roster(team_abbr)
        metrics_list = []

        # Fetch metrics for each player in the team roster for the given season
        for _, player in team_roster.iterrows():
            player_id = player['PLAYER_ID']
            player_metrics = self.fetch_player_metrics(player_id, season)
            if not player_metrics.empty:
                # Add player name for context
                player_metrics = player_metrics.merge(
                    team_roster[['PLAYER_ID', 'PLAYER']],
                    on='PLAYER_ID',
                    how='left'
                )
                metrics_list.append(player_metrics)

        # Concatenate player metrics into a single DataFrame
        return pd.concat(metrics_list, ignore_index=True) if metrics_list else pd.DataFrame()

    def compile_team_stats_for_season(self, team_abbr, current_season, previous_season='2023-24'):
        """
        Compiles player career stats and estimated metrics for both previous and current seasons.
        
        Args:
            team_abbr (str): The team abbreviation (e.g., 'MEM', 'NYK').
            current_season (str): The current season in 'YYYY-YY' format (e.g., '2024-25').
            previous_season (str): The previous season in 'YYYY-YY' format (default: '2023-24').
        
        Returns:
            tuple: Two DataFrames - one for the current season and one for the previous season.
        """
        # Fetch stats for previous and current seasons separately
        previous_season_stats_df = self.fetch_team_roster_stats(team_abbr, previous_season)
        current_season_stats_df = self.fetch_team_roster_stats(team_abbr, current_season)
        
        # Fetch metrics for previous and current seasons separately
        previous_season_metrics_df = self.fetch_team_roster_metrics(team_abbr, previous_season)
        current_season_metrics_df = self.fetch_team_roster_metrics(team_abbr, current_season)

        return (previous_season_stats_df, previous_season_metrics_df), (current_season_stats_df, current_season_metrics_df)

    # This function will now call the modular functions and compile the stats for all games on a given day.
    def compile_games_stats(self, current_season, date, previous_season='2023-24'):
        """
        Fetches and compiles player stats and metrics for all games on a given day.
        
        Args:
            current_season (str): The current season in 'YYYY-YY' format (e.g., '2024-25').
            date (str): The date of the games (e.g., 'YYYY-MM-DD').
            previous_season (str): The previous season in 'YYYY-YY' format (default: '2023-24').
        
        Returns:
            dict: A dictionary containing the home and away team stats and metrics for each game, for both seasons.
        """
        todays_games = self.get_todays_games(date)
        all_games_stats = {}

        for _, game in todays_games.iterrows():
            home_team_abbr = game['Home Team Abbreviation']
            away_team_abbr = game['Visiting Team Abbreviation']

            # Compile stats and metrics for home and away teams
            home_team_data = self.compile_team_stats_for_season(home_team_abbr, current_season, previous_season)
            away_team_data = self.compile_team_stats_for_season(away_team_abbr, current_season, previous_season)

            # Store in a structured format for easy access
            all_games_stats[game['Game ID']] = {
                'home_previous_season': home_team_data[0][0],
                'home_current_season': home_team_data[1][0],
                'away_previous_season': away_team_data[0][0],
                'away_current_season': away_team_data[1][0],
                'home_previous_metrics': home_team_data[0][1],
                'home_current_metrics': home_team_data[1][1],
                'away_previous_metrics': away_team_data[0][1],
                'away_current_metrics': away_team_data[1][1]
            }

        return all_games_stats

    def compile_team_player_stats(self, team_abbr, current_season, previous_season='2023-24'):
        """
        Fetches player stats for a given team and saves both previous season and current season stats.
        
        Args:
            nba_data (object): The NBA data object.
            team_abbr (str): The team abbreviation (e.g., 'MEM', 'NYK').
            current_season (str): The current season in 'YYYY-YY' format (e.g., '2024-25').
            previous_season (str): The previous season in 'YYYY-YY' format (default: '2023-24').
        
        Returns:
            tuple: Two DataFrames - one for the current season and one for the previous season.
        """
        # Get team roster
        team_roster = self.get_team_roster(team_abbr)
        
        # Initialize empty lists to store stats for each season
        previous_season_stats_list = []
        current_season_stats_list = []
    
        # Function to fetch stats for a given player
        def fetch_stats_for_season(player_id, season):
            player_stats = self.fetch_player_career_stats(player_id)
            if player_stats is not None and not player_stats.empty:
                return player_stats[player_stats['SEASON_ID'] == season]
            return pd.DataFrame()
    
        # Fetch stats for each player in the team roster
        for _, player in team_roster.iterrows():
            player_id = player['PLAYER_ID']
    
            # Fetch previous season stats
            previous_season_stats = fetch_stats_for_season(player_id, previous_season)
            if not previous_season_stats.empty:
                previous_season_stats_list.append(previous_season_stats)
    
            # Fetch current season stats
            current_season_stats = fetch_stats_for_season(player_id, current_season)
            if not current_season_stats.empty:
                current_season_stats_list.append(current_season_stats)
        
        # Combine stats for each season into separate DataFrames
        previous_season_stats_df = pd.concat(previous_season_stats_list, ignore_index=True) if previous_season_stats_list else pd.DataFrame()
        current_season_stats_df = pd.concat(current_season_stats_list, ignore_index=True) if current_season_stats_list else pd.DataFrame()
    
        # Merge player names into both DataFrames (only if they are not empty)
        if not previous_season_stats_df.empty:
            previous_season_stats_df = previous_season_stats_df.merge(
                team_roster[['PLAYER_ID', 'PLAYER']],
                on='PLAYER_ID',
                how='left'
            )
    
        if not current_season_stats_df.empty:
            current_season_stats_df = current_season_stats_df.merge(
                team_roster[['PLAYER_ID', 'PLAYER']],
                on='PLAYER_ID',
                how='left'
            )
        
        # Reorder columns to have 'PLAYER' as the first column
        columns = ['PLAYER'] + [col for col in previous_season_stats_df.columns if col != 'PLAYER']
        previous_season_stats_df = previous_season_stats_df[columns] if not previous_season_stats_df.empty else pd.DataFrame()
    
        current_season_columns = ['PLAYER'] + [col for col in current_season_stats_df.columns if col != 'PLAYER']
        current_season_stats_df = current_season_stats_df[current_season_columns] if not current_season_stats_df.empty else pd.DataFrame()
    
        return previous_season_stats_df, current_season_stats_df
    
    def fetch_team_roster_stats(self, team_abbr, season):
        """
        Fetches and compiles player career stats for a given team and season.
        
        Args:
            team_abbr (str): The team abbreviation (e.g., 'MEM', 'NYK').
            season (str): The season in 'YYYY-YY' format (e.g., '2023-24').
        
        Returns:
            pd.DataFrame: DataFrame containing all player stats for the given team and season.
        """
        team_roster = self.get_team_roster(team_abbr)
        season_stats_list = []

        if team_roster is None or team_roster.empty:
            print(f"No roster data for {team_abbr}")
            return pd.DataFrame()

        # Fetch stats for each player in the team roster for the given season
        for _, player in team_roster.iterrows():
            player_id = player['PLAYER_ID']
            player_stats = self.fetch_player_career_stats(player_id)
            if player_stats is not None and not player_stats.empty:
                season_stats_list.append(player_stats)

        # Concatenate player stats into a single DataFrame
        if season_stats_list:
            return pd.concat(season_stats_list, ignore_index=True)
        else:
            print(f"No player stats available for team {team_abbr} in season {season}")
            return pd.DataFrame()


    def fetch_stats_for_todays_games(self, todays_games):
        """
        Fetches player stats for home and visiting teams for each game on today's schedule.
        
        Args:
            todays_games (pd.DataFrame): DataFrame containing today's games.
        
        Returns:
            dict: Dictionary where keys are game IDs, and values are tuples of DataFrames
                  (home_team_stats_df, visiting_team_stats_df) for each game.
        """
        games_stats = {}

        for _, game in todays_games.iterrows():
            home_team_abbr = game['Home Team Abbreviation']
            visiting_team_abbr = game['Visiting Team Abbreviation']
            game_id = game['Game ID']
            
            # Fetch player stats for home and visiting teams for the current game
            home_team_stats_df = self.fetch_team_roster_stats(home_team_abbr, "2023-24")
            visiting_team_stats_df = self.fetch_team_roster_stats(visiting_team_abbr, "2023-24")
            
            # If stats exist, add a column for team abbreviation
            if not home_team_stats_df.empty:
                home_team_stats_df['TEAM'] = home_team_abbr
            if not visiting_team_stats_df.empty:
                visiting_team_stats_df['TEAM'] = visiting_team_abbr
            
            # Store separate DataFrames for each team's stats in this game
            games_stats[game_id] = {
                'home_team_stats_df': home_team_stats_df,
                'visiting_team_stats_df': visiting_team_stats_df
            }

        return games_stats

    def fetch_player_stats_single_team(self, team_abbr, season):
        """
        Fetches and cleans player stats for a single NBA team for the specified season, including roster details like player height.
        
        Args:
            team_abbr (str): The team abbreviation (e.g., 'MEM', 'NYK').
            season (str): The season to fetch stats for in 'YYYY-YY' format (e.g., '2022-23').
            
        Returns:
            pd.DataFrame: Cleaned DataFrame containing player stats and additional details for the given team.
        """
        # Fetch the team roster and select important columns
        team_roster = self.get_team_roster(team_abbr)
        team_roster = team_roster[['PLAYER_ID', 'PLAYER', 'HEIGHT']]  # Add 'HEIGHT' and other roster info as needed
    
        # Initialize list to store each player's seasonal stats
        player_stats_list = []
        
        # Fetch stats for each player in the roster
        for _, player in team_roster.iterrows():
            player_id = player['PLAYER_ID']
            player_stats = self.fetch_player_career_stats(player_id)
    
            if player_stats is not None and not player_stats.empty:
                # Filter to keep only stats from the specified season
                player_stats = player_stats[player_stats['SEASON_ID'] == season]
                
                if not player_stats.empty:
                    player_stats_list.append(player_stats)
    
        # Combine stats into a single DataFrame if any stats were collected
        if player_stats_list:
            team_stats = pd.concat(player_stats_list, ignore_index=True)
        else:
            print(f"No player stats available for {team_abbr} in season {season}")
            return pd.DataFrame()  # Return empty DataFrame if no player stats found
    
        # Merge player stats with team roster to add player names and height
        team_stats = team_stats.merge(
            team_roster,
            on='PLAYER_ID',
            how='left'
        )

        # Fetch estimated metrics and merge with player stats
        player_metrics_df = self.fetch_player_estimated_metrics(season)
        team_stats = team_stats.merge(player_metrics_df, on='PLAYER_ID', how='left')
    
        # Remove duplicate columns or unwanted columns with '_y' suffix
        team_stats = team_stats.loc[:, ~team_stats.columns.duplicated()]
        team_stats = team_stats.drop(columns=[col for col in team_stats.columns if '_y' in col], errors='ignore')
        
        # Drop the specified columns
        team_stats = team_stats.drop(columns=["SEASON_ID", "LEAGUE_ID", "GP_RANK", "W_RANK", "L_RANK", "W_PCT_RANK"], errors='ignore')
        
        # Rename specified columns
        team_stats = team_stats.rename(columns={"GP_x": "GP", "MIN_x": "MIN"})

        # Calculate per-game stats (e.g., FGM per game, REB per game) based on 'GP' (games played)
        if 'GP' in team_stats.columns and team_stats['GP'].gt(0).all():
            per_game_columns = ['MIN', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'OREB', 'DREB', 'REB', 
                                'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']
            for col in per_game_columns:
                if col in team_stats.columns:
                    team_stats[col] = (team_stats[col] / team_stats['GP']).round(2)
        
        # Move 'PLAYER' and 'HEIGHT' to the beginning and position 'PTS' right after 'MIN'
        columns = ['PLAYER', 'HEIGHT'] + [col for col in team_stats.columns if col not in ['PLAYER', 'HEIGHT', 'GP', 'MIN', 'PTS']]
        team_stats = team_stats[columns]
    
        return team_stats

    def fetch_logs_for_todays_games(self, todays_games, season):
        """
        Fetches player logs for the top 8 players of consequence for both home and away teams in today's games.
        """
        for _, game in todays_games.iterrows():
            home_team_abbr = game['Home Team Abbreviation']
            away_team_abbr = game['Visiting Team Abbreviation']
            game_date = game['Game Date']
            game_id = game['Game ID']
            
            # Fetch and cache logs for home team
            print(f"Fetching top 8 player logs for home team {home_team_abbr} on {game_date}")
            home_team_stats = self.load_team_stats(home_team_abbr, season, game_date, game_id, "home")
            if home_team_stats is not None:
                self.fetch_and_cache_player_logs_for_team(home_team_abbr, season, game_date, top_n=8)
            
            # Fetch and cache logs for away team
            print(f"Fetching top 8 player logs for away team {away_team_abbr} on {game_date}")
            away_team_stats = self.load_team_stats(away_team_abbr, season, game_date, game_id, "away")
            if away_team_stats is not None:
                self.fetch_and_cache_player_logs_for_team(away_team_abbr, season, game_date, top_n=8)

    def load_team_stats(self, team_abbr, season, game_date, game_id, home_or_away):
        """
        Loads the cached team stats for a given team, season, and game date from the CacheManager.
        """
        # Use cache_manager to load the cached team stats
        return self.cache_manager.load_team_stats(team_abbr, season, game_date, game_id, home_or_away)

    def fetch_and_cache_player_logs_for_team(self, team_abbr, season, game_date, top_n=8):
        """
        Fetches and caches player logs for the top N players by minutes for a given team.
        """
        # Fetch the team roster
        team_roster = self.get_team_roster(team_abbr)
        
        if team_roster is None or team_roster.empty:
            print(f"No roster found for team {team_abbr}.")
            return
        
        # Sort players by total minutes ('MIN') and select the top N players
        top_players = team_roster.sort_values(by='MIN', ascending=False).head(top_n)
    
        # Cache player logs directory structure
        log_dir = os.path.join(self.cache_manager.cache_dir, game_date)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Loop through each player and fetch their logs
        for _, player in top_players.iterrows():
            player_id = player['PLAYER_ID']
            
            # Check if logs are already cached
            cached_logs = self.cache_manager.load_player_logs(player_id, season)
            if cached_logs is not None:
                print(f"Logs already cached for Player ID {player_id}. Skipping.")
                continue
            
            # Fetch player game logs
            print(f"Fetching game logs for Player ID {player_id} ({player['PLAYER']})")
            player_logs = self.fetch_player_game_logs(player_id, season)
            
            if player_logs is not None and not player_logs.empty:
                # Cache the player logs
                self.cache_manager.cache_player_logs(player_logs, player_id, season, log_dir)
                print(f"Cached logs for Player ID {player_id}")
            else:
                print(f"No game logs available for Player ID {player_id} in season {season}.")

    def fetch_all_player_vs_player_data(self, player_id, vs_player_id, season):
        """
        Fetches all datasets for PlayerVsPlayer from the NBA API for a given player and season.
        
        Args:
            player_id (int): The ID of the player.
            vs_player_id (int): The ID of the opposing player.
            season (str): The season in 'YYYY-YY' format.
        
        Returns:
            dict: A dictionary with each dataset from the PlayerVsPlayer endpoint.
        """
        response = PlayerVsPlayer(
            player_id=player_id,
            vs_player_id=vs_player_id,
            season=season
        )

        # Retrieve each dataset and store it in a dictionary
        data = {
            'OnOffCourt': response.on_off_court.get_data_frame(),
            'Overall': response.overall.get_data_frame(),
            'PlayerInfo': response.player_info.get_data_frame(),
            'ShotAreaOffCourt': response.shot_area_off_court.get_data_frame(),
            'ShotAreaOnCourt': response.shot_area_on_court.get_data_frame(),
            'ShotAreaOverall': response.shot_area_overall.get_data_frame(),
            'ShotDistanceOffCourt': response.shot_distance_off_court.get_data_frame(),
            'ShotDistanceOnCourt': response.shot_distance_on_court.get_data_frame(),
            'ShotDistanceOverall': response.shot_distance_overall.get_data_frame(),
            'VsPlayerInfo': response.vs_player_info.get_data_frame(),
        }
        
        return data

    def fetch_matchup_rollup_direct(self, season, per_mode="Totals", season_type="Regular Season", league_id="00"):
        """
        Directly fetches the MatchupsRollup data from the NBA API.
        
        Args:
            season (str): Season in 'YYYY-YY' format.
            per_mode (str): Per mode, e.g., 'Totals' or 'PerGame'.
            season_type (str): Season type, e.g., 'Regular Season'.
            league_id (str): League ID, typically "00".
        
        Returns:
            pd.DataFrame: DataFrame with matchup rollup stats.
        """
        response = matchupsrollup.MatchupsRollup(
            league_id=league_id,
            per_mode_simple=per_mode,
            season=season,
            season_type_playoffs=season_type
        )
        return response.get_data_frames()[0]

    def fetch_current_season_matchup_rollup(self, per_mode="PerGame", season_type="Regular Season"):
        """
        Automatically fetches the matchup rollup data for the current NBA season.

        Args:
            per_mode (str): Per mode, e.g., 'PerGame'.
            season_type (str): Season type, e.g., 'Regular Season'.

        Returns:
            pd.DataFrame: DataFrame with matchup rollup stats for the current season.
        """
        # Determine the season based on the current date
        current_year = datetime.datetime.now().year
        season = f"{current_year-1}-{str(current_year)[-2:]}" if datetime.datetime.now().month < 10 else f"{current_year}-{str(current_year + 1)[-2:]}"
        
        # Fetch and return the current season's matchup rollup data
        return self.fetch_matchup_rollup_direct(season=season, per_mode=per_mode, season_type=season_type)