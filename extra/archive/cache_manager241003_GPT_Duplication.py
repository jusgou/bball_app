import os
import joblib

class CacheManager:
    """
    Class for handling caching of data using joblib, organized by game date.
    """
    def __init__(self, cache_dir="cached_data"):
        """
        Initializes the CacheManager.
        
        Args:
            cache_dir (str): Base directory for caching.
        """
        self.cache_dir = cache_dir
        # Create the base cache directory if it doesn't exist
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

    def cache_data(self, data, filename, game_date):
        """
        Caches the given data using joblib, organized by game date.
        
        Args:
            data: The data to be cached (DataFrame, dict, etc.)
            filename (str): The name of the file to save the data in.
            game_date (str): The game date in 'YYYY-MM-DD' format.
        """
        # Create a directory for the specific game date
        game_date_dir = os.path.join(self.cache_dir, game_date)
        if not os.path.exists(game_date_dir):
            os.makedirs(game_date_dir)

        # Save the data using joblib
        filepath = os.path.join(game_date_dir, f"{filename}.joblib")
        joblib.dump(data, filepath)
        print(f"Data cached as {filename}.joblib in {game_date_dir}")

    def load_cached_data(self, filename, game_date):
        """
        Loads cached data from a file organized by game date.
        
        Args:
            filename (str): The name of the cached file.
            game_date (str): The game date in 'YYYY-MM-DD' format.
        
        Returns:
            The loaded data.
        """
        game_date_dir = os.path.join(self.cache_dir, game_date)
        filepath = os.path.join(game_date_dir, f"{filename}.joblib")
        
        if os.path.exists(filepath):
            return joblib.load(filepath)
        else:
            print(f"Cached file {filename}.joblib not found in {game_date_dir}.")
            return None

    def clear_cache(self, game_date=None):
        """
        Clears the cache. If a game_date is provided, it will clear only that date's cache.
        
        Args:
            game_date (str, optional): The game date to clear, in 'YYYY-MM-DD' format.
                                       If None, clears the entire cache directory.
        """
        if game_date:
            # Delete the folder for the specific game date
            game_date_dir = os.path.join(self.cache_dir, game_date)
            if os.path.exists(game_date_dir):
                for file in os.listdir(game_date_dir):
                    file_path = os.path.join(game_date_dir, file)
                    os.remove(file_path)
                os.rmdir(game_date_dir)
                print(f"Cleared cache for game date {game_date}.")
            else:
                print(f"No cached data found for game date {game_date}.")
        else:
            # Clear all cached data
            for subdir in os.listdir(self.cache_dir):
                subdir_path = os.path.join(self.cache_dir, subdir)
                for file in os.listdir(subdir_path):
                    os.remove(os.path.join(subdir_path, file))
                os.rmdir(subdir_path)
            print(f"Cleared all cached data.")


    def cache_player_logs(self, data, player_id, season):
        """
        Caches player logs for a given player and season.
        Args:
            data (pd.DataFrame): The player's game logs.
            player_id (int): The player's unique ID.
            season (str): The season year format (e.g., '2023-24').
        """
        file_path = os.path.join(f"cached_data/player_logs/{season}/{player_id}.joblib")
        joblib.dump(data, file_path)
        print(f"Cached player logs for Player ID {player_id} for season {season}.")

    def load_player_logs(self, player_id, season):
        """
        Loads cached player logs for a given player and season, if they exist.
        Args:
            player_id (int): The player's unique ID.
            season (str): The season year format (e.g., '2023-24').
        Returns:
            pd.DataFrame: The player's game logs if cached, otherwise None.
        """
        file_path = os.path.join(f"cached_data/player_logs/{season}/{player_id}.joblib")
        if os.path.exists(file_path):
            return joblib.load(file_path)
        else:
            return None

    def fetch_player_game_logs_for_team(self, team_abbr, season, game_date):
        """
        Fetch and cache game logs for players in a team for a specific game date and season.
        
        Args:
            team_abbr (str): The team's abbreviation (e.g., 'MEM').
            season (str): The NBA season in 'YYYY-YY' format.
            game_date (str): The date of the game in 'YYYY-MM-DD' format.
        """
        team_roster = self.get_team_roster(team_abbr)
        
        for _, player in team_roster.iterrows():
            player_id = player['PLAYER_ID']
            
            # Check if the player's logs for this game day are already cached
            cached_logs = self.load_player_logs(player_id, game_date)
            if cached_logs is not None:
                print(f"Player logs already cached for Player ID {player_id} on {game_date}.")
                continue
            
            # Fetch player logs for the given season
            player_logs = self.fetch_player_game_logs(player_id, season)
            
            # Cache the logs
            if player_logs is not None and not player_logs.empty:
                self.cache_player_game_logs(player_logs, player_id, game_date)

    def fetch_previous_season_logs_for_all_teams(self, todays_games, previous_season):
        """
        Fetches player logs for all teams in today's games for the previous season, ensuring no duplication.
        Args:
            todays_games (pd.DataFrame): The games scheduled for today.
            previous_season (str): The previous season year format (e.g., '2023-24').
        """
        processed_teams = set()  # Track teams that have already been processed
        
        for _, game in todays_games.iterrows():
            home_team_abbr = game['Home Team Abbreviation']
            away_team_abbr = game['Visiting Team Abbreviation']
            
            # Fetch logs for home team if not already processed
            if home_team_abbr not in processed_teams:
                print(f"Fetching logs for home team {home_team_abbr}")
                self.fetch_player_game_logs_for_team(home_team_abbr, previous_season)
                processed_teams.add(home_team_abbr)  # Mark team as processed
            
            # Fetch logs for away team if not already processed
            if away_team_abbr not in processed_teams:
                print(f"Fetching logs for away team {away_team_abbr}")
                self.fetch_player_game_logs_for_team(away_team_abbr, previous_season)
                processed_teams.add(away_team_abbr)  # Mark team as processed

    def fetch_and_cache_game_logs(self, todays_games, season, game_date):
        """
        Fetches and caches game logs for players of consequence from 'todays_games'.
        
        Args:
            todays_games (DataFrame): DataFrame of today's games.
            season (str): The current NBA season (e.g., '2024-25').
            game_date (str): The date of the game in 'YYYY-MM-DD' format.
        """
        for _, game in todays_games.iterrows():
            home_team_abbr = game['Home Team Abbreviation']
            away_team_abbr = game['Visiting Team Abbreviation']
            
            # Process home team logs
            print(f"Fetching player logs for home team {home_team_abbr}")
            self.fetch_player_game_logs_for_team(home_team_abbr, season, game_date)
            
            # Process away team logs
            print(f"Fetching player logs for away team {away_team_abbr}")
            self.fetch_player_game_logs_for_team(away_team_abbr, season, game_date)

    def cache_player_game_logs(self, player_logs, player_id, game_date):
        """
        Caches player game logs in the appropriate game date folder within 'player_logs_YYMMDD'.
        
        Args:
            player_logs (DataFrame): The player's game logs.
            player_id (int): The player's ID.
            game_date (str): The date of the game in 'YYYY-MM-DD' format.
        """
        # Create the folder structure: cached_data/game_date/player_logs_YYMMDD/
        game_date_str = game_date.replace("-", "")
        folder_path = os.path.join(self.CACHE_DIR, game_date, f"player_logs_{game_date_str}")
        
        # Create the subfolder if it doesn't exist
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        
        # Cache the player logs as a joblib file
        filename = f"player_{player_id}_logs_{game_date_str}.joblib"
        filepath = os.path.join(folder_path, filename)
        joblib.dump(player_logs, filepath)
        print(f"Player logs cached for Player ID {player_id} on {game_date} as {filename}")

    def fetch_top_players_logs(self, todays_games, season='2024-25', top_n=8):
        """
        Fetches game logs for the top N players per team based on minutes or usage percentage.
        
        Args:
            todays_games (pd.DataFrame): DataFrame containing today's games.
            season (str): The season for which to fetch logs (default is '2024-25').
            top_n (int): The number of top players (based on MIN or USG%) to fetch logs for.
        """
        # Loop through each game
        for _, game in todays_games.iterrows():
            game_id = game['Game ID']
            home_team_abbr = game['Home Team Abbreviation']
            away_team_abbr = game['Visiting Team Abbreviation']
            game_date = game['Game Date']
            
            # Fetch top players from both teams
            for team_abbr in [home_team_abbr, away_team_abbr]:
                print(f"Fetching top {top_n} player logs for team {team_abbr}")
                
                # Load roster stats (minutes/usage data assumed to be cached already)
                team_stats = self.load_team_stats(team_abbr, season)
                
                # Sort by minutes or usage percentage, selecting the top N players
                if team_stats is not None and not team_stats.empty:
                    top_players = team_stats.sort_values(by=['MIN', 'USG%'], ascending=False).head(top_n)
                    
                    for _, player in top_players.iterrows():
                        player_id = player['PLAYER_ID']
                        
                        # Check if game logs are already cached
                        cached_logs = self.load_player_logs(player_id, season)
                        if cached_logs is None:
                            # Fetch and cache game logs for the player
                            player_logs = self.fetch_player_game_logs(player_id, season)
                            
                            if player_logs is not None and not player_logs.empty:
                                self.cache_player_logs(player_logs, player_id, season, game_date)
                                print(f"Cached logs for Player ID {player_id} for {season}")
                            else:
                                print(f"No game logs available for Player ID {player_id} for {season}")
                        else:
                            print(f"Game logs for Player ID {player_id} are already cached.")

    def load_team_stats(self, team_abbr, season):
        """
        Loads the cached team stats for a given team and season.
        
        Args:
            team_abbr (str): The team abbreviation (e.g., 'MEM', 'NYK').
            season (str): The season to load stats for in 'YYYY-YY' format (e.g., '2024-25').
        
        Returns:
            pd.DataFrame: The cached team stats DataFrame, or None if not found.
        """
        cache_dir = os.path.join(self.cache_dir, f'team_stats_{season}')
        file_name = f'{team_abbr}_team_stats_{season}.joblib'
        file_path = os.path.join(cache_dir, file_name)

        if os.path.exists(file_path):
            print(f"Loading cached team stats for {team_abbr} ({season}) from {file_name}")
            return joblib.load(file_path)
        else:
            print(f"No cached team stats found for {team_abbr} ({season})")
            return None

    def fetch_and_cache_player_logs_for_team(self, team_abbr, season, game_date, top_n=8):
        """
        Fetches and caches player game logs for the top N players by minutes/usage for a given team.
        Args:
            team_abbr (str): The team abbreviation (e.g., 'MEM', 'NYK').
            season (str): The season to fetch logs for in 'YYYY-YY' format (e.g., '2023-24').
            game_date (str): The date of the game (YYYY-MM-DD).
            top_n (int): The number of top players to fetch logs for (default is 8).
        """
        # Fetch the team roster
        team_roster = self.nba_data.get_team_roster(team_abbr)
        
        if team_roster is None or team_roster.empty:
            print(f"No roster found for team {team_abbr}.")
            return
        
        # Sort players by minutes or usage (assuming you have minutes/usage in the roster data)
        top_players = team_roster.sort_values(by='MIN', ascending=False).head(top_n)

        # Cache player logs directory structure
        log_dir = os.path.join(self.cache_dir, f"game_logs_{game_date}")
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Loop through each player and fetch their logs
        for _, player in top_players.iterrows():
            player_id = player['PLAYER_ID']
            
            # Check if logs are already cached
            cached_logs = self.load_player_logs(player_id, season)
            if cached_logs is not None:
                print(f"Logs already cached for Player ID {player_id}. Skipping.")
                continue
            
            # Fetch player game logs
            print(f"Fetching game logs for Player ID {player_id} ({player['PLAYER']})")
            player_logs = self.nba_data.fetch_player_game_logs(player_id, season)
            
            if player_logs is not None and not player_logs.empty:
                # Cache the player logs
                self.cache_player_logs(player_logs, player_id, season, log_dir)
                print(f"Cached logs for Player ID {player_id}")
            else:
                print(f"No game logs available for Player ID {player_id} in season {season}.")

    def fetch_logs_for_todays_games(self, todays_games, season):
        """
        Loops through today's games and fetches logs for top 7-8 players based on minutes/usage.
        Args:
            todays_games (pd.DataFrame): The games scheduled for today.
            season (str): The season to fetch logs for in 'YYYY-YY' format.
        """
        for _, game in todays_games.iterrows():
            home_team_abbr = game['Home Team Abbreviation']
            away_team_abbr = game['Visiting Team Abbreviation']
            game_date = game['Game Date']
            
            # Fetch and cache logs for home team
            print(f"Fetching top 8 player logs for home team {home_team_abbr} on {game_date}")
            self.fetch_and_cache_player_logs_for_team(home_team_abbr, season, game_date, top_n=8)
            
            # Fetch and cache logs for away team
            print(f"Fetching top 8 player logs for away team {away_team_abbr} on {game_date}")
            self.fetch_and_cache_player_logs_for_team(away_team_abbr, season, game_date, top_n=8)

    def cache_player_logs(self, player_logs, player_id, season, log_dir):
        """
        Caches player game logs in the specified directory.
        Args:
            player_logs (pd.DataFrame): The player game logs DataFrame.
            player_id (int): The player's ID.
            season (str): The season in 'YYYY-YY' format.
            log_dir (str): The directory where logs should be cached.
        """
        file_name = f"player_{player_id}_logs_{season}.joblib"
        file_path = os.path.join(log_dir, file_name)
        joblib.dump(player_logs, file_path)
        print(f"Cached logs for player {player_id} in {file_name}.")