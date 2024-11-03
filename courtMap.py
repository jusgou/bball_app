# courtMap.py
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np
import pandas as pd
from nba_api.stats.endpoints import PlayerDashboardByShootingSplits

def get_shooting_splits_by_distance(player_id, season):
    """
    Fetch shooting data by distance for a given player and season.
    Returns a DataFrame with relevant shooting metrics by distance.
    """
    try:
        response = PlayerDashboardByShootingSplits(
            player_id=player_id,
            season=season,
            per_mode_detailed="Totals",
            measure_type_detailed="Base",
            season_type_playoffs="Regular Season"
        )
        
        # Retrieve data from the "Shot5FTPlayerDashboard" dataset for close-range shots
        shot_data_5ft = response.get_data_frames()[4]
        
        # Retrieve data from the "Shot8FTPlayerDashboard" dataset for mid-range shots
        shot_data_8ft = response.get_data_frames()[5]
        
        # Retrieve data from the "ShotAreaPlayerDashboard" dataset for other shot areas
        shot_area_data = response.get_data_frames()[6]
        
        # Concatenate the data into a single DataFrame
        shot_data = pd.concat([shot_data_5ft, shot_data_8ft, shot_area_data], ignore_index=True)
        
        # Select and rename relevant columns for consistency
        shot_data = shot_data[['GROUP_VALUE', 'FGM', 'FGA', 'FG_PCT']]
        shot_data.columns = ['distance', 'fgm', 'fga', 'fg_pct']
        
        return shot_data
    except Exception as e:
        print(f"Error fetching shooting splits data: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error

def generate_shot_chart(shot_data):
    """
    Generate a shot chart based on the player's shooting data.
    """
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Draw the basketball court (using simplified lines)
    # Example: Draw a rectangle for the key
    ax.add_patch(plt.Rectangle((-80, -47.5), 160, 19, color='gray', alpha=0.3))  # Paint area
    ax.add_patch(plt.Circle((0, 0), 7.5, color='gray', fill=False))  # Basket
    ax.plot([-22, 22], [-47.5, -47.5], color="gray")  # Free throw line
    
    # Plot each shot zone
    for i, row in shot_data.iterrows():
        x, y = i * 20 - 60, 0  # Sample positions (adjust as needed for each zone)
        size = row['fga'] * 0.2  # Adjust size based on FGA
        color = plt.cm.RdYlGn(row['fg_pct'] / 100)  # Color based on FG%
        
        # Plot as a circle on the chart
        ax.scatter(x, y, s=size, color=color, label=f"{row['distance']} FG%: {row['fg_pct']:.1f}")
    
    # Adding labels, title, etc.
    ax.set_title("Player Shot Chart by Distance")
    ax.set_xlim(-100, 100)
    ax.set_ylim(-50, 50)
    plt.legend(loc="upper right")
    
    return fig