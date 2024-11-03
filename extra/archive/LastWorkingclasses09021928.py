#!/usr/bin/env python
# coding: utf-8

# In[4]:


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
from nba_api.stats.endpoints import teamestimatedmetrics
from nba_api.stats.endpoints import teamdashboardbygeneralsplits


# In[74]:


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns',None)


# # Create Team and Roster Class

# In[7]:


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

    def get_team_roster(self, team_id):
        """
        Fetches the roster for a single team based on the team ID.
        Args:
            team_id (int): The team's ID.

        Returns:
            pd.DataFrame: DataFrame containing the team's roster.
        """
        try:
            rosters_raw = CommonTeamRoster(
                team_id=str(team_id),
                season=self.season
            ).common_team_roster.get_data_frame()
            return rosters_raw
        except Exception as e:
            print(f"Error fetching roster for team ID {team_id}: {e}")
            return None

    def get_teams_and_rosters_for_game(self, game_id):
        """
        Retrieves the teams and their rosters for a specific game from the schedule.
        Args:
            game_id (str): The ID of the game.
        
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: DataFrames for the rosters of the two teams playing the game.
        """
        # Ensure the 'Game ID' column is treated as a string
        self.schedule_df['Game ID'] = self.schedule_df['Game ID'].astype(str)
        
        # Check for the game ID in the schedule
        game_row = self.schedule_df.loc[self.schedule_df['Game ID'] == game_id]
        
        if game_row.empty:
            raise ValueError(f"Game ID {game_id} not found in the schedule.")
        
        game = game_row.iloc[0]
        home_team_abbr = game['Home Team Abbreviation']
        away_team_abbr = game['Visiting Team Abbreviation']
        
        # Fetch the rosters using the abbreviations
        home_team_id = self.teams_df[self.teams_df['abbreviation'] == home_team_abbr].index[0]
        away_team_id = self.teams_df[self.teams_df['abbreviation'] == away_team_abbr].index[0]
        
        home_team_roster = self.get_team_roster(home_team_id)
        away_team_roster = self.get_team_roster(away_team_id)
        
        return home_team_roster, away_team_roster

    def load_schedule(self, csv_path):
        """
        Loads the NBA schedule from a CSV file into the class.
        Args:
            csv_path (str): The file path to the schedule CSV file.
        """
        self.schedule_df = pd.read_csv(csv_path)
        print(f"Schedule loaded: {len(self.schedule_df)} games")

    def get_teams_from_schedule(self, schedule_df):
        # Access home and away teams for the first game
        home_team = nba_data.schedule_df['Home Team Abbreviation'].iloc[0]
        away_team = nba_data.schedule_df['Visiting Team Abbreviation'].iloc[0]
        return home_team, away_team

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
            dict: A dictionary containing the team's statistics.
        """
        # Map the team abbreviation to a team ID (this should be predefined or fetched)
        team_id = self.get_team_id_from_abbreviation(team_abbreviation)
        
        if not team_id:
            print(f"Team ID not found for abbreviation {team_abbreviation}.")
            return {}
    
        # Fetch team stats using the NBA API
        team_stats = teamdashboardbygeneralsplits.TeamDashboardByGeneralSplits(
            team_id=team_id,
            season=season,
            season_type_all_star='Regular Season'
        )
        
        # Convert the stats into a dictionary or DataFrame
        team_stats_dict = team_stats.overall_team_dashboard.get_data_frame().iloc[0].to_dict()
        
        return team_stats_dict

    
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
        # Fetch and process the standings
        league_standings = self.fetch_league_standings(season_type)

        # Group by 'Division' and create separate DataFrames for each division
        grouped = league_standings.groupby('Division')
        divisions = {division_name: group for division_name, group in grouped}
        
        # Prepare and modify DataFrames for each division
        modified_dfs = []
        for division, df in divisions.items():
            # Create a separator row with the division name
            separator_df = pd.DataFrame([{col: '' for col in df.columns}])
            separator_df.iloc[0, df.columns.get_loc('Division')] = division
            
            # Concatenate the separator row and the division DataFrame
            combined_df = pd.concat([separator_df, df])
            
            # Append the combined DataFrame to the list
            modified_dfs.append(combined_df)
        
        # Combine all modified DataFrames
        final_df = pd.concat(modified_dfs, ignore_index=True)
        
        # Order by 'Conference' and set specific values
        conference_order = ['East', 'West']
        final_df['Conference'] = pd.Categorical(final_df['Conference'], categories=conference_order, ordered=True)
        
        # Set specific 'Conference' values based on the provided indices
        final_df.loc[0, 'Conference'] = "East"
        final_df.loc[6, 'Conference'] = "East"
        final_df.loc[12, 'Conference'] = "West"
        final_df.loc[18, 'Conference'] = "West"
        final_df.loc[24, 'Conference'] = "East"
        final_df.loc[30, 'Conference'] = "West"
        
        # Sort the DataFrame by 'Conference' and 'Division'
        final_df_sorted = final_df.sort_values(by=['Conference', 'Division'])
        
        return final_df_sorted

    def get_player_team(self, player_id):
        """
        Fetches the team abbreviation for a given player based on their player ID.
        Args:
            player_id (int): The ID of the player.
    
        Returns:
            str: The team abbreviation.
        """
        # Assuming `self.rosters` is a DataFrame containing the full roster information
        player_roster = None
        for team_roster in self.rosters.values():
            if not team_roster.empty:
                player_roster = team_roster[team_roster['PLAYER_ID'] == player_id]
                if not player_roster.empty:
                    break
    
        if player_roster is not None and not player_roster.empty:
            team_abbreviation = player_roster['TEAM_ABBREVIATION'].iloc[0]
            return team_abbreviation
        else:
            print(f"Team not found for player ID {player_id}.")
            return None
    
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

    def get_team_metrics_by_id(team_id, metrics_df):
        """
        Filters the team metrics DataFrame for a specific team ID.
        
        Args:
            team_id (int): The team's ID.
            metrics_df (pd.DataFrame): DataFrame containing estimated metrics.
        
        Returns:
            pd.Series: The team's estimated metrics.
        """
        team_metrics = metrics_df.loc[metrics_df['TEAM_ID'] == team_id].iloc[0]
        return team_metrics

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

    def calculate_player_per(player_stats, team_metrics, league_metrics=None):
        """
        Calculates Player Efficiency Rating (PER) for a player.
        Args:
            player_stats (pd.Series): A series containing the player's stats.
            team_metrics (pd.Series): A series containing the team's metrics (including E_PACE).
            league_metrics (pd.Series or dict): A series or dictionary containing the league's metrics (optional).
    
        Returns:
            float: The player's Player Efficiency Rating (PER).
        """
        # Simplified formula focusing on main stats
        uPER = (
            player_stats['PTS']
            + player_stats['REB'] * 0.3
            + player_stats['AST'] * 0.3
            + player_stats['STL'] * 0.3
            + player_stats['BLK'] * 0.3
            - player_stats['TOV'] * 0.4
        )
        
        # If league metrics aren't provided, default to using team metrics for scaling
        if league_metrics is None:
            league_pace = team_metrics['E_PACE']
        else:
            league_pace = league_metrics['E_PACE']
    
        # Calculate PER with simplified scaling by team or league pace
        per = uPER * (team_metrics['E_PACE'] / league_pace)
        return per
    
    def calculate_ts_percentage(self, player_stats):
        """
        Calculates True Shooting Percentage (TS%) for a player.
        Args:
            player_stats (pd.Series): A series containing the player's stats.

        Returns:
            float: The player's TS%.
        """
        pts = player_stats['PTS']
        fga = player_stats['FGA']
        fta = player_stats['FTA']
        return pts / (2 * (fga + 0.44 * fta))

    def calculate_efg_percentage(self, player_stats):
        """
        Calculates Effective Field Goal Percentage (eFG%) for a player.
        Args:
            player_stats (pd.Series): A series containing the player's stats.

        Returns:
            float: The player's eFG%.
        """
        fg = player_stats['FGM']
        fga = player_stats['FGA']
        three_pointers = player_stats['FG3M']
        return (fg + 0.5 * three_pointers) / fga

    def calculate_usg_percentage(row, team_stats):
        fga = row['FGA']
        fta = row['FTA']
        tov = row['TOV']
        minutes = row['MIN']
        
        team_fga = team_stats['FGA'].sum()
        team_fta = team_stats['FTA'].sum()
        team_tov = team_stats['TOV'].sum()
        team_minutes = team_stats['MIN'].sum() * 5  # 5 players on the court
        
        if minutes == 0 or team_minutes == 0:
            return 0  # Return 0 if the minutes or team minutes are zero to avoid division by zero
        
        return 100 * ((fga + 0.44 * fta + tov) * team_minutes) / (minutes * (team_fga + 0.44 * team_fta + team_tov))
    

    def calculate_ppp(self, player_stats, possessions):
        """
        Calculates Points Per Possession (PPP) for a player.
        Args:
            player_stats (pd.Series): A series containing the player's stats.
            possessions (int): The number of possessions.
    
        Returns:
            float: The player's Points Per Possession (PPP).
        """
        pts = player_stats['PTS']
        return pts / possessions if possessions else 0

    def calculate_per(self, player_stats, team_stats, league_stats):
        """
        Calculates Player Efficiency Rating (PER) for a player.
        Args:
            player_stats (pd.Series): A series containing the player's stats.
            team_stats (pd.Series): A series containing the team's stats.
            league_stats (pd.Series): A series containing the league's stats.
    
        Returns:
            float: The player's Player Efficiency Rating (PER).
        """
        # Simplified formula focusing on main stats
        uPER = (
            player_stats['PTS']
            + player_stats['REB'] * 0.3
            + player_stats['AST'] * 0.3
            + player_stats['STL'] * 0.3
            + player_stats['BLK'] * 0.3
            - player_stats['TOV'] * 0.4
        )
        
        per = uPER * (team_stats['Pace'] / league_stats['Pace'])
        return per

    def calculate_pie(player_stats, team_stats, opponent_stats):
        player_total = (
            player_stats['PTS'] + player_stats['FGM'] + player_stats['FTM']
            - player_stats['FGA'] - player_stats['FTA']
            + player_stats['OREB'] + player_stats['DREB']
            + player_stats['AST'] + player_stats['STL'] + player_stats['BLK']
            - player_stats['PF'] - player_stats['TOV']
        )
        
        team_total = (
            team_stats['PTS'] + team_stats['FGM'] + team_stats['FTM']
            - team_stats['FGA'] - team_stats['FTA']
            + team_stats['OREB'] + team_stats['DREB']
            + team_stats['AST'] + team_stats['STL'] + team_stats['BLK']
            - team_stats['PF'] - team_stats['TOV']
        )
        
        opponent_total = (
            opponent_stats['PTS'] + opponent_stats['FGM'] + opponent_stats['FTM']
            - opponent_stats['FGA'] - opponent_stats['FTA']
            + opponent_stats['OREB'] + opponent_stats['DREB']
            + opponent_stats['AST'] + opponent_stats['STL'] + opponent_stats['BLK']
            - opponent_stats['PF'] - opponent_stats['TOV']
        )
        
        pie = player_total / (team_total + opponent_total)
        return pie

    def calculate_ortg(self, player_stats, team_possessions):
        """
        Calculates Offensive Rating (ORtg) for a player.
        Args:
            player_stats (pd.Series): A series containing the player's stats.
            team_possessions (int): The total number of team possessions.
    
        Returns:
            float: The player's Offensive Rating (ORtg).
        """
        points_produced = player_stats['PTS'] + 0.5 * player_stats['AST']
        ortg = (points_produced / team_possessions) * 100
        return ortg

    def calculate_player_ortg(player_stats, team_possessions):
        """
        Calculates Offensive Rating (ORtg) for a player.
        Args:
            player_stats (pd.Series): A series containing the player's stats.
            team_possessions (int): The total number of team possessions.
    
        Returns:
            float: The player's Offensive Rating (ORtg).
        """
        points_produced = player_stats['PTS'] + 0.5 * player_stats['AST']
        ortg = (points_produced / team_possessions) * 100
        return ortg

    def calculate_drtg(self, player_stats, opponent_possessions):
        """
        Calculates Defensive Rating (DRtg) for a player.
        Args:
            player_stats (pd.Series): A series containing the player's stats.
            opponent_possessions (int): The total number of opponent possessions.
    
        Returns:
            float: The player's Defensive Rating (DRtg).
        """
        points_allowed = player_stats['PTS_AGAINST']
        drtg = (points_allowed / opponent_possessions) * 100
        return drtg

    def calculate_orb_percentage(player_stats, team_stats, opponent_stats):
        orb = player_stats['OREB']
        team_orb = team_stats['E_OREB_PCT']
        opponent_drb = opponent_stats['E_DREB_PCT']
        orb_percentage = (orb * (team_stats['MIN'] * 5)) / (player_stats['MIN'] * (team_orb + opponent_drb))
        return orb_percentage
        
    def calculate_drb_percentage(self, player_stats, team_stats, opponent_stats):
        """
        Calculates Defensive Rebound Percentage (DRB%) for a player.
        Args:
            player_stats (pd.Series): A series containing the player's stats.
            team_stats (pd.Series): A series containing the team's stats.
            opponent_stats (pd.Series): A series containing the opponent's stats.
    
        Returns:
            float: The player's DRB%.
        """
        drb = player_stats['DREB']
        team_drb = team_stats['DREB']
        opponent_orb = opponent_stats['OREB']
        drb_percentage = (drb * (team_stats['MIN'] * 5)) / (player_stats['MIN'] * (team_drb + opponent_orb))
        return drb_percentage

    def calculate_ast_percentage(player_stats, team_stats):
        """
        Calculates Assist Percentage (AST%) for a player.
        Args:
            player_stats (pd.Series): A series containing the player's stats.
            team_stats (pd.Series): A series containing the team's stats.
    
        Returns:
            float: The player's AST%.
        """
        assists = player_stats['AST']
        team_field_goals = team_stats.get('FGM', 0)
        if team_field_goals == 0:
            return 0  # Avoid division by zero
        ast_percentage = 100 * (assists / (team_field_goals - player_stats['FGM']))
        return ast_percentage
        
    def calculate_tov_percentage(player_stats):
        tov = player_stats['TOV']
        fga = player_stats['FGA']
        fta = player_stats['FTA']
        tov_percentage = 100 * (tov / (fga + 0.44 * fta + tov))
        return tov_percentage

    def calculate_stl_percentage(self, player_stats, opponent_possessions):
        """
        Calculates Steal Percentage (STL%) for a player.
        Args:
            player_stats (pd.Series): A series containing the player's stats.
            opponent_possessions (int): The total number of opponent possessions.
    
        Returns:
            float: The player's STL%.
        """
        steals = player_stats['STL']
        stl_percentage = 100 * (steals * (team_stats['MIN'] * 5)) / (player_stats['MIN'] * opponent_possessions)
        return stl_percentage
    
    def calculate_blk_percentage(self, player_stats, opponent_possessions):
        """
        Calculates Block Percentage (BLK%) for a player.
        Args:
            player_stats (pd.Series): A series containing the player's stats.
            opponent_possessions (int): The total number of opponent possessions.
    
        Returns:
            float: The player's BLK%.
        """
        blocks = player_stats['BLK']
        blk_percentage = 100 * (blocks * (team_stats['MIN'] * 5)) / (player_stats['MIN'] * opponent_possessions)
        return blk_percentage

    def calculate_pace(self, team_stats, opponent_stats):
        """
        Calculates Team Pace.
        Args:
            team_stats (pd.Series): A series containing the team's stats.
            opponent_stats (pd.Series): A series containing the opponent's stats.
    
        Returns:
            float: The team's Pace.
        """
        team_possessions = team_stats['POSS']
        opponent_possessions = opponent_stats['POSS']
        minutes_played = team_stats['MIN']
        pace = 48 * ((team_possessions + opponent_possessions) / (2 * (minutes_played / 5)))
        return pace

    def calculate_offensive_efficiency(self, player_stats, team_possessions):
        """
        Calculates Offensive Efficiency.
        Args:
            player_stats (pd.Series): A series containing the player's stats.
            team_possessions (int): The number of team possessions.
    
        Returns:
            float: The team's Offensive Efficiency.
        """
        points = player_stats['PTS']
        return points / team_possessions if team_possessions else 0
    
    def calculate_defensive_efficiency(self, opponent_stats, opponent_possessions):
        """
        Calculates Defensive Efficiency.
        Args:
            opponent_stats (pd.Series): A series containing the opponent's stats.
            opponent_possessions (int): The number of opponent possessions.
    
        Returns:
            float: The team's Defensive Efficiency.
        """
        points_allowed = opponent_stats['PTS']
        return points_allowed / opponent_possessions if opponent_possessions else 0

    def calculate_bpm(player_stats, team_stats, league_stats):
        bpm = (
            player_stats['PTS'] + player_stats['AST'] * 0.5
            - player_stats['TOV'] * 0.5 - player_stats['PF'] * 0.5
            + player_stats['STL'] * 0.5 + player_stats['BLK'] * 0.5
        ) / player_stats['MIN'] * 100
        
        bpm_adjusted = bpm * (team_stats['E_PACE'] / league_stats['E_PACE'])
        return bpm_adjusted

    def calculate_vorp(self, bpm, minutes_played):
        """
        Calculates Value Over Replacement Player (VORP).
        Args:
            bpm (float): The player's Box Plus/Minus.
            minutes_played (float): The number of minutes played by the player.
    
        Returns:
            float: The player's VORP.
        """
        replacement_level = -2.0  # Approximate replacement-level BPM
        vorp = (bpm - replacement_level) * (minutes_played / 2000)
        return vorp

    def calculate_dws(self, drtg, team_drtg, minutes_played, team_minutes):
        """
        Calculates Defensive Win Shares (DWS) for a player.
        Args:
            drtg (float): The player's Defensive Rating.
            team_drtg (float): The team's Defensive Rating.
            minutes_played (float): The player's minutes played.
            team_minutes (float): The team's total minutes played.
    
        Returns:
            float: The player's DWS.
        """
        marginal_defense = (team_drtg - drtg) * (minutes_played / team_minutes)
        dws = marginal_defense * (minutes_played / 48) * (team_minutes / 5)
        return dws


    def convert_to_per_game_stats(player_stats):
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
            per_game_stats[stat] = per_game_stats[stat] / per_game_stats['GP']
        
        return per_game_stats

    def convert_to_per_game_stats(player_stats):
        """
        Converts cumulative stats in a DataFrame to per-game stats and rounds them to 2 decimal points.
        
        Args:
            player_stats (pd.DataFrame): The player stats DataFrame with cumulative totals.
        
        Returns:
            pd.DataFrame: A new DataFrame with per-game stats rounded to 2 decimal points.
        """
        per_game_stats = player_stats.copy()
        
        # Calculate per-game averages for relevant columns and round to 2 decimal points
        stats_to_average = ['MIN', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']
        for stat in stats_to_average:
            per_game_stats[stat] = (per_game_stats[stat] / per_game_stats['GP']).round(2)
        
        return per_game_stats


# In[ ]:




