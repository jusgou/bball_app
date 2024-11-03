import streamlit as st
import pandas as pd
import datetime
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from cache_manager import CacheManager
from data_loader import load_team_stats
from data_loader import load_player_stats
from data_loader import load_and_concatenate_team_stats
from data_loader import get_next_seven_days_games
from data_loader import get_team_roster, get_player_game_logs
from data_loader import load_matchup_rollup_from_cache
from classes import NBATeamRosters
import courtMap 
from courtMap import get_shooting_splits_by_distance, generate_shot_chart

# Set page layout to wide
st.set_page_config(layout="wide")

# Initialize CacheManager with the correct cache directory
cache_manager = CacheManager(cache_dir="cached_data")

# Initialize NBATeamRosters for data retrieval
nba_data = NBATeamRosters("2024")

# Fetch teams and populate team abbreviations
nba_data.fetch_teams()
teams_df = nba_data.teams_df
# Create a list of team abbreviations
team_abbr_list = teams_df['abbreviation'].tolist()

# Sidebar navigation
st.sidebar.title("Navigation")
option = st.sidebar.radio("Go to", ["Schedule", "Matchup Analyzer", "Player Matchup Analyzer", "Player Analyzer", "Matchup Predictor"])

# Load NBA Schedule CSV
schedule_file = "nbaSchedule2425.csv"

# Get today's date
today = datetime.datetime.now().strftime("%Y-%m-%d")
next_seven_days = [(datetime.datetime.now() + datetime.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

# Function to load and filter today's games
def get_todays_games(schedule_file):
    try:
        schedule_df = pd.read_csv(schedule_file)
        schedule_df['Game Date'] = pd.to_datetime(schedule_df['Game Date']).dt.strftime('%Y-%m-%d')
        todays_games = schedule_df[schedule_df['Game Date'] == today]
        return todays_games
    except FileNotFoundError:
        st.error("Schedule file not found. Please ensure 'nbaSchedule2425.csv' is in the correct folder.")
        return pd.DataFrame()

# Function to load cached team or player stats
def load_cached_stats(today_game_dir, team_type, team_abbr, season_type, stats_type="team_stats"):
    if not os.path.exists(today_game_dir):
        st.error(f"The directory {today_game_dir} does not exist.")
        return pd.DataFrame()

    file_pattern = f"_{team_type}_team_{team_abbr}_{season_type}_{stats_type}"
    for file_name in os.listdir(today_game_dir):
        if file_pattern in file_name:
            st.write(f"Loading data from: {file_name}")
            return cache_manager.load_cached_data(file_name, today)

    st.write(f"No cached file found for pattern: {file_pattern}")
    return pd.DataFrame()

# Display the next 7 days of games under Schedule
if option == "Schedule":
    st.title("Game Schedule")
    st.write("This page displays the games scheduled for the next 7 days.")

    # Get and display the schedule for the next 7 days
    games_for_next_seven_days = get_next_seven_days_games(schedule_file, next_seven_days)
    
    if not games_for_next_seven_days.empty:
        for date_str in next_seven_days:
            # Display the date in "October 26, 2024" format
            formatted_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").strftime("%B %d, %Y")
            st.write(f"### {formatted_date}")
            
            # Filter games for this specific date
            games_on_date = games_for_next_seven_days[games_for_next_seven_days['Game Date'] == date_str]
            if not games_on_date.empty:
                st.dataframe(games_on_date[['Game Date', 'Game Time', 'Home Team Abbreviation', 
                                            'Visiting Team Abbreviation', 'Arena', 'Divisional Game']])
            else:
                st.write("No games scheduled.")
                
# Matchup Analyzer to display team stats and player stats for each matchup
elif option == "Matchup Analyzer":
    st.title("Matchup Analyzer")
    st.write("This page will analyze team and player statistics for each game today.")

    # Get and display today's games
    todays_games = get_todays_games(schedule_file)
    
    if not todays_games.empty:
        st.write("### Today's Matchups:")
        
        # Create a tab for each game
        game_tabs = st.tabs([f"{game['Home Team Abbreviation']} vs. {game['Visiting Team Abbreviation']}" for _, game in todays_games.iterrows()])
        
        # Loop through each game and display stats in respective tab
        for tab, (_, game) in zip(game_tabs, todays_games.iterrows()):
            home_team = game['Home Team Abbreviation']
            away_team = game['Visiting Team Abbreviation']
            game_id = game['Game ID']

            with tab:
                st.write(f"## {home_team} vs. {away_team}")

                # Create subtabs for Team Stats and Player Stats
                stats_tabs = st.tabs(["Team Stats", "Player Stats"])

                # Team Stats tab
                with stats_tabs[0]:
                    st.write("### Team Stats")

                    # Load and display consolidated team stats for home and away teams
                    st.write(f"### {home_team} Team Stats")
                    home_consolidated_stats = load_and_concatenate_team_stats(today, "home", home_team)
                    st.dataframe(home_consolidated_stats)
                
                    st.write(f"### {away_team} Team Stats")
                    away_consolidated_stats = load_and_concatenate_team_stats(today, "away", away_team)
                    st.dataframe(away_consolidated_stats)

                # Player Stats tab
                with stats_tabs[1]:
                    st.write("### Player Stats")

                    # Load and display home team player stats (previous and current seasons)
                    st.write(f"### {home_team} Player Stats (2023-2024)")
                    home_previous_player_stats = load_player_stats(today, "home", home_team, "prev")
                    st.dataframe(home_previous_player_stats)
                    
                    st.write(f"### {home_team} Player Stats (2024-2025)")
                    home_current_player_stats = load_player_stats(today, "home", home_team, "curr")
                    st.dataframe(home_current_player_stats)
                    
                    st.write(f"### {away_team} Player Stats (2023-2024)")
                    away_previous_player_stats = load_player_stats(today, "away", away_team, "prev")
                    st.dataframe(away_previous_player_stats)
                    
                    st.write(f"### {away_team} Player Stats (2024-2025)")
                    away_current_player_stats = load_player_stats(today, "away", away_team, "curr")
                    st.dataframe(away_current_player_stats)

    else:
        st.write("No games scheduled for today or schedule file not loaded properly.")

elif option == "Player Analyzer":
    st.title("Player Analyzer")
    
    # Team dropdown selection
    team_abbr = st.selectbox("Select Team", team_abbr_list)
    
    if team_abbr:
        team_roster = get_team_roster(nba_data, team_abbr)
        if not team_roster.empty:
            selected_player = st.selectbox("Select a Player", team_roster['PLAYER'])
            player_id = team_roster[team_roster['PLAYER'] == selected_player]['PLAYER_ID'].values[0]
            season = st.selectbox("Select Season", ["2024", "2023", "2022", "2021"])
            # Statistic selector (now placed with other selectors)
            stat_options = ["PTS", "AST", "REB", "FG3M", "BLK", "STL", "FTM", "PF", "PFD", "TOV"]
            selected_stat = st.selectbox("Select Statistic", stat_options)

            if st.button("Analyze Player"):
                # Fetch game logs for the selected player and season
                player_logs = get_player_game_logs(nba_data, player_id, season)

                # Create tabs for Plot, Statistics, and Game Logs Dataframe
                tab1, tab2, tab3 = st.tabs(["Plot Analysis", "Game Logs Dataframe", "Season Comparison"])

                # Plot Analysis tab
                with tab1:

                    
                    # Calculate mean and standard deviation for the selected stat
                    mean_stat = player_logs[selected_stat].mean()
                    std_dev_stat = player_logs[selected_stat].std()

                    # Plot selected stat over time
                    st.write(f"### {selected_stat} per Game Over Time")
                    plt.figure(figsize=(10, 6))
                    plt.plot(player_logs['GAME_DATE'], player_logs[selected_stat], marker='o', color='b', label=f'{selected_stat} per Game')
                    plt.axhline(mean_stat, color='r', linestyle='--', label=f'Average {selected_stat}')
                    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
                    plt.gca().xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
                    plt.gcf().autofmt_xdate(rotation=45)
                    plt.xlabel('Game Date')
                    plt.ylabel(selected_stat)
                    plt.legend()
                    plt.grid(True)
                    st.pyplot(plt)


                    st.write(f"### Statistics for {selected_stat}")
                    
                    # Calculate basic statistics
                    mean_stat = player_logs[selected_stat].mean()
                    median_stat = player_logs[selected_stat].median()
                    mode_stat = player_logs[selected_stat].mode()[0] if not player_logs[selected_stat].mode().empty else None
                    stdev_stat = player_logs[selected_stat].std()
                
                    # Frequency of deviations within standard deviations with point ranges
                    st.write("### Frequency of Deviations from Mean with Point Ranges")
                    std_dev_ranges = [(0, 0.5), (0.5, 1), (1, 1.5), (1.5, 2), (2, 2.5), (2.5, 3)]
                    freq_within_std_dev = {
                        f'Between {low} SD and {high} SD': [
                            player_logs[
                                (player_logs[selected_stat].sub(mean_stat).abs() > low * stdev_stat) &
                                (player_logs[selected_stat].sub(mean_stat).abs() <= high * stdev_stat)
                            ].shape[0],
                            f"{(low * stdev_stat):.2f} to {(high * stdev_stat):.2f} {selected_stat}"
                        ]
                        for low, high in std_dev_ranges
                    }
                    
                    # Convert dictionary to DataFrame and rename columns
                    freq_df = pd.DataFrame.from_dict(freq_within_std_dev, orient='index', columns=['Games Count', 'Range in Points'])
                    freq_df.index.name = 'Standard Deviation Range'
                    st.dataframe(freq_df)
                
                    # Display stats summary
                    st.write("### Summary Statistics")
                    st.write(f"Mean {selected_stat}: {mean_stat:.2f}")
                    st.write(f"Median {selected_stat}: {median_stat:.2f}")
                    st.write(f"Mode {selected_stat}: {mode_stat:.2f}")
                    st.write(f"Standard Deviation of {selected_stat}: {stdev_stat:.2f}")

                                # Game Logs Dataframe tab
                with tab2:
                    st.write("### Game Logs")
                    st.dataframe(player_logs)

                # Tab 3: Season Comparison
                with tab3:
                    st.header("Season Comparison")
                    seasons_to_compare = st.multiselect("Select Seasons for Comparison", ["2024", "2023", "2022", "2021"], default=["2024", "2023"])
                    
                    # Fetch the game logs for the current season to determine the number of games played
                    current_season_logs = get_player_game_logs(nba_data, player_id, "2024")
                    games_played = len(current_season_logs)
                    
                    # Comparison data
                    comparison_data = {}
                    for season in seasons_to_compare:
                        season_logs = get_player_game_logs(nba_data, player_id, season)
                        if season_logs.empty:
                            st.warning(f"No data available for {season}")
                        else:
                            # Limit the logs to the number of games played in the current season
                            comparison_data[season] = season_logs.head(games_played)
                
                    # Plot only if data is available for comparison
                    if comparison_data:
                        plt.figure(figsize=(12, 6))
                        
                        # Plot each season's data limited to the same number of games
                        for season, season_logs in comparison_data.items():
                            season_logs = season_logs.reset_index(drop=True)  # Reset index to use game count
                            plt.plot(season_logs.index + 1, season_logs[selected_stat], marker='o', label=f"{season} {selected_stat}")
                        
                        plt.title(f"{selected_player}'s {selected_stat} Comparison (First {games_played} Games)")
                        plt.xlabel('Game Number')
                        plt.ylabel(selected_stat)
                        plt.legend()
                        plt.grid(True)
                        st.pyplot(plt)
                
                        # Summary statistics
                        st.write("### Summary Statistics for First Games")
                        for season, season_logs in comparison_data.items():
                            season_mean = season_logs[selected_stat].mean()
                            season_std_dev = season_logs[selected_stat].std()
                            st.write(f"**{season} {selected_stat}** - Mean: {season_mean:.2f}, Standard Deviation: {season_std_dev:.2f}")
                    else:
                        st.write("No data available for any selected season.")


# In the navigation sidebar
elif option == "Player Matchup Analyzer":
    st.title("Player Matchup Analyzer")
    
    # Create the three tabs under Player Matchup Analyzer
    matchup_tabs = st.tabs(["Player vs Player", "Player vs Position", "Shot Chart"])
    
    # Tab 1: Player vs Player analysis
    with matchup_tabs[0]:
        st.header("Player vs Player")
    
        # Main team and player dropdowns with unique keys
        main_team_abbr = st.selectbox("Select Main Team", team_abbr_list, key="pvp_main_team_abbr")
        
        # When a main team is selected, load its roster and populate main player dropdown
        if main_team_abbr:
            main_team_roster = nba_data.get_team_roster(main_team_abbr)
            main_player_name_id_map = {row['PLAYER']: row['PLAYER_ID'] for _, row in main_team_roster.iterrows()}
            
            # Select main player with unique key
            main_player_name = st.selectbox("Select Player", list(main_player_name_id_map.keys()), key="pvp_main_player")
            main_player_id = main_player_name_id_map[main_player_name]
            
            # Opposing team and player dropdowns with unique keys
            opposing_team_abbr = st.selectbox("Select Opposing Team", team_abbr_list, key="pvp_opposing_team_abbr")
            
            if opposing_team_abbr:
                opposing_team_roster = nba_data.get_team_roster(opposing_team_abbr)
                opposing_player_name_id_map = {row['PLAYER']: row['PLAYER_ID'] for _, row in opposing_team_roster.iterrows()}
                
                # Select opposing player with unique key
                opposing_player_name = st.selectbox("Select Opposing Player", list(opposing_player_name_id_map.keys()), key="pvp_opposing_player")
                opposing_player_id = opposing_player_name_id_map[opposing_player_name]
                
                # Season selection with unique key
                season = st.selectbox("Select Season", ["2022-23", "2023-24", "2024-25"], key="pvp_season")
    
                # Fetch and display filtered Player vs Player data when button is clicked
                if st.button("Fetch Player Matchup Data", key="pvp_fetch_data"):
                    try:
                        # Retrieve all data sets
                        all_matchup_data = nba_data.fetch_all_player_vs_player_data(main_player_id, opposing_player_id, season)
                        
                        # Group datasets to display on specific tabs
                        filtered_data = {
                            "OnOffCourt": all_matchup_data.get("OnOffCourt", pd.DataFrame()),
                            "Shot Area": [
                                ("ShotAreaOnCourt", all_matchup_data.get("ShotAreaOnCourt", pd.DataFrame())),
                                ("ShotAreaOffCourt", all_matchup_data.get("ShotAreaOffCourt", pd.DataFrame())),
                                ("ShotAreaOverall", all_matchup_data.get("ShotAreaOverall", pd.DataFrame()))
                            ],
                            "Shot Distance": [
                                ("ShotDistanceOnCourt", all_matchup_data.get("ShotDistanceOnCourt", pd.DataFrame())),
                                ("ShotDistanceOffCourt", all_matchup_data.get("ShotDistanceOffCourt", pd.DataFrame())),
                                ("ShotDistanceOverall", all_matchup_data.get("ShotDistanceOverall", pd.DataFrame()))
                            ]
                        }
    
                        # Create tabs for each category
                        matchup_category_tabs = st.tabs(filtered_data.keys())
                        
                        # Display each dataset or group of datasets in respective tabs
                        for tab, (category, data) in zip(matchup_category_tabs, filtered_data.items()):
                            with tab:
                                if category == "OnOffCourt":
                                    st.write("OnOffCourt Dataset")
                                    st.dataframe(data)
                                else:
                                    st.write(f"{category} Datasets")
                                    for dataset_name, df in data:
                                        st.write(f"**{dataset_name}**")
                                        st.dataframe(df)
                                        st.write("---")  # Separator between datasets
                    except Exception as e:
                        st.error(f"Error fetching Player Matchup Data: {e}")

    # Tab 2: Player vs Position analysis
    with matchup_tabs[1]:
        st.header("Player vs Position")
        
        # Select teams with unique keys
        home_team_abbr = st.selectbox("Select Home Team", team_abbr_list, index=0, key="pvp_home_team")
        away_team_abbr = st.selectbox("Select Away Team", team_abbr_list, index=1, key="pvp_away_team")
    
        # Get rosters for the selected teams
        home_team_data = nba_data.get_team_roster(home_team_abbr)
        away_team_data = nba_data.get_team_roster(away_team_abbr)
    
        # Fetch the current season's matchup rollup data
        st.write(f"Fetching defensive matchup rollup data for {home_team_abbr} and {away_team_abbr}")
        try:
            current_season_rollup_df = nba_data.fetch_current_season_matchup_rollup(per_mode="Totals", season_type="Regular Season")
            
            # Set up columns for side-by-side comparison of home and away team matchups
            col1, col2 = st.columns(2)
            
            # Display matchup rollup stats for each player on the home team as a defender
            with col1:
                st.write(f"**{home_team_abbr} Defensive Matchup Rollups**")
                for _, player in home_team_data.iterrows():
                    player_id = player['PLAYER_ID']
                    player_name = player['PLAYER']
                    st.subheader(f"{player_name}")
                    
                    # Filter the rollup data for each player as a defender
                    player_matchup_data = current_season_rollup_df[current_season_rollup_df['DEF_PLAYER_ID'] == player_id]
                    if not player_matchup_data.empty:
                        st.dataframe(player_matchup_data)
                    else:
                        st.write("No matchup data available.")
    
            # Display matchup rollup stats for each player on the away team as a defender
            with col2:
                st.write(f"**{away_team_abbr} Defensive Matchup Rollups**")
                for _, player in away_team_data.iterrows():
                    player_id = player['PLAYER_ID']
                    player_name = player['PLAYER']
                    st.subheader(f"{player_name}")
                    
                    # Filter the rollup data for each player as a defender
                    player_matchup_data = current_season_rollup_df[current_season_rollup_df['DEF_PLAYER_ID'] == player_id]
                    if not player_matchup_data.empty:
                        st.dataframe(player_matchup_data)
                    else:
                        st.write("No matchup data available.")
        except Exception as e:
            st.error(f"Error fetching matchup rollup data: {e}")

    # Tab for Shooting Splits analysis
    with matchup_tabs[2]:
        st.header("Shooting Splits Data")

        # Team and player selection
        team_abbr = st.selectbox("Select Team", team_abbr_list, key="shooting_splits_team")
        if team_abbr:
            team_roster = nba_data.get_team_roster(team_abbr)
            selected_player = st.selectbox("Select Player", team_roster['PLAYER'], key="shooting_splits_player")
            player_id = team_roster[team_roster['PLAYER'] == selected_player]['PLAYER_ID'].values[0]
            season = st.selectbox("Select Season", ["2023-24", "2022-23", "2021-22"], key="shooting_splits_season")

            if st.button("Fetch Shooting Splits Data"):
                # Fetch the shooting splits data
                try:
                    from nba_api.stats.endpoints import PlayerDashboardByShootingSplits

                    # Retrieve all datasets from the API
                    response = PlayerDashboardByShootingSplits(
                        player_id=player_id,
                        season=season,
                        per_mode_detailed="Totals",
                        measure_type_detailed="Base",
                        season_type_playoffs="Regular Season"
                    )
                    data_frames = response.get_data_frames()

                    # Display each dataset in separate sections
                    st.subheader(f"Shooting Splits for {selected_player} - {season} Season")
                    dataset_names = [
                        "Assisted By",
                        "Assisted Shot Player Dashboard",
                        "Overall Player Dashboard",
                        "Shot 5FT Player Dashboard",
                        "Shot 8FT Player Dashboard",
                        "Shot Area Player Dashboard",
                        "Shot Type Player Dashboard",
                        "Shot Type Summary Player Dashboard"
                    ]

                    for name, df in zip(dataset_names, data_frames):
                        if not df.empty:
                            st.write(f"**{name}**")
                            st.dataframe(df)
                            st.write("---")  # Separator between tables
                        else:
                            st.write(f"No data available for {name}.")

                except Exception as e:
                    st.error(f"Error fetching shooting splits data: {e}")