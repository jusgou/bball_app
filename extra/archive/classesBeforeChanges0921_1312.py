from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import cumestatsteamgames, cumestatsteam, gamerotation
import pandas as pd
import numpy as np
import json
import difflib
import time
import requests
import re


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

    def fetch_and_store_player_data(self, game_id, season):
        """
        Fetches and stores player career stats and game logs for all players in a specific game.
        Args:
            game_id (str): The ID of the game.
            season (str): The NBA season in 'YYYY-YY' format, e.g., '2023-24'.
        
        Returns:
            dict: A dictionary with player IDs as keys and another dictionary as values containing 'career_stats' and 'game_logs'.
        """
        # Initialize dictionaries to store player stats and game logs
        player_stats_dict = {}
        player_game_logs_dict = {}
    
        # Get the rosters for the home and away teams
        home_team_roster, away_team_roster = self.get_teams_and_rosters_for_game(game_id)
        
        # Combine home and away team rosters
        combined_roster = pd.concat([home_team_roster, away_team_roster])
    
        # Iterate through the combined roster to fetch and store player data
        for index, player in combined_roster.iterrows():
            player_id = player['PLAYER_ID']
            
            # Fetch player's career stats
            career_stats = self.fetch_player_career_stats(player_id)
            
            # Fetch player's game logs for the specified season
            game_logs = self.fetch_player_game_logs(player_id, season)
            
            # Store the stats and game logs in the dictionaries
            player_stats_dict[player_id] = career_stats
            player_game_logs_dict[player_id] = game_logs
    
        # Return the dictionaries
        return player_stats_dict, player_game_logs_dict

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

    def fetch_team_season_ranks(team_id, season):
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
            return team_season_ranks
        except Exception as e:
            print(f"Error fetching TeamSeasonRanks for team ID {team_id}: {e}")
            return None