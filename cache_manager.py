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

    def load_team_stats(self, team_abbr, season, game_date, game_id, home_or_away):
        """
        Loads the cached team stats for a given team, season, and game date.
        
        Args:
            team_abbr (str): The team abbreviation (e.g., 'BOS', 'NYK').
            season (str): The season to load stats for in 'YYYY-YY' format (e.g., '2023-24').
            game_date (str): The game date in 'YYYY-MM-DD' format.
            game_id (str): The game ID.
            home_or_away (str): 'home' or 'away' to specify which team.
    
        Returns:
            pd.DataFrame: The cached team stats DataFrame, or None if not found.
        """
        game_date_dir = os.path.join(self.cache_dir, game_date)
        file_name = f"game_{game_id}_{home_or_away}_team_{team_abbr}_prev.joblib"
        file_path = os.path.join(game_date_dir, file_name)
        
        if os.path.exists(file_path):
            print(f"Loading cached team stats for {team_abbr} ({season}) from {file_name}")
            return joblib.load(file_path)
        else:
            print(f"No cached team stats found for {team_abbr} ({season})")
            return None
    
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

    def cache_player_game_logs(self, player_logs, player_id, game_date):
        """
        Caches player game logs in the appropriate game date folder within 'player_logs'.
        
        Args:
            player_logs (DataFrame): The player's game logs.
            player_id (int): The player's ID.
            game_date (str): The date of the game in 'YYYY-MM-DD' format.
        """
        # Create the folder structure inside the game date folder
        game_date_dir = os.path.join(self.cache_dir, game_date)
        logs_dir = os.path.join(game_date_dir, "player_logs")
        
        # Create the logs subfolder if it doesn't exist
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)
        
        # Cache the player logs as a joblib file
        filename = f"player_{player_id}_logs_{game_date}.joblib"
        filepath = os.path.join(logs_dir, filename)
        joblib.dump(player_logs, filepath)
        print(f"Player logs cached for Player ID {player_id} on {game_date} as {filename}")


    def load_player_logs(self, player_id, season):
        """
        Loads cached player logs for a given player and season.
        """
        file_path = os.path.join(f"cached_data/player_logs/{season}/{player_id}.joblib")
        if os.path.exists(file_path):
            return joblib.load(file_path)
        return None

