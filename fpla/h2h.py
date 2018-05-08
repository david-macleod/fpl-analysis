import pandas as pd 
import re
import numpy as np
from matplotlib import pyplot as plt

def first_name(name):
	return name.split()[0].capitalize()

def plot_rival_radar(df, colour_dict):
    """
    Plot rivalry between h2h players on a radar chart
    :param df: input DataFrame 
    :param colour_dict: dictionary mapping player names to colours {name: hexcolour}
    """
     
    # groupy by player/opponent pairs and calculate proportion of points won
    df = (df.loc[df.result != '-']
                 .groupby(['name', 'name_o'])
                 .apply(lambda x: x['h2h_points'].sum() / (len(x) * 3))
                 .reset_index(name='value'))
    
    players = colour_dict.keys()
    n_players = len(players)

    plt.style.use('default')
    fig = plt.figure(figsize=(8,6))

    for i, player in zip(range(n_players), players):
        # get points
        points = df.loc[df.name == player, 'value'].values
        points = np.concatenate((points, [points[0]]))
        
        # get list of opponents for axis label
        opponents = df.loc[df.name == player, 'name_o'].apply(lambda x: first_name(x)).values
        
        # get angles for polar co-ordinates projection
        angles = np.linspace(0, 2*np.pi, len(opponents), endpoint=False)
        angles = np.concatenate((angles,[angles[0]]))

        # create subplot
        ax = fig.add_subplot(2, 3, i+1, polar=True)
        ax.plot(angles, points, 'o-', linewidth=2, markersize=3, color=colour_dict[player])
        ax.fill(angles, points, alpha=0.25, color=colour_dict[player])
        
        # format subplot
        ax.set_thetagrids(angles * 180/np.pi, opponents, size=7)
        ax.set_theta_zero_location("S")
        ax.set_ylim([0, 1])
        ax.set_yticks(np.linspace(0, 1, 5))
        ax.set_yticklabels([])
        ax.grid(True, color='grey')
        ax.set_title(first_name(player))

    fig.tight_layout()
   

def plot_form(df, colour_dict):
    players = colour_dict.keys()
    
    # exclude unplayed (future) games
    df = df[df.result != '-']
    
    # get league average (mean) total h2h points per gameweek
    df_mean_total = df.groupby('gw').total.mean().reset_index().rename(columns={'total':'mean_total'})
    df = df.merge(df_mean_total, on='gw')
    
    # calculate total points relative to league average
    df['mean_total_diff'] = df['total'] - df['mean_total']
    
    # plot result
    fig, ax = plt.subplots(figsize=(6, 3.5))
    ax.axhline(color='grey', linewidth='0.5')

    for player in players:
        df_player = df.loc[df.name == player]
        gameweeks = np.insert(df_player.gw.values, 0, 0)
        points = np.insert(df_player.mean_total_diff.values, 0, 0)

        ax.plot(gameweeks, points, c=colour_dict[player], label=first_name(player))
        ax.legend(fontsize='x-small', frameon=False)
        ax.set_xlim(left=0)