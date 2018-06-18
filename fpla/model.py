import numpy as np 
import matplotlib.pyplot as plt 
import pandas as pd
from sklearn.metrics import mean_squared_log_error, mean_squared_error, mean_absolute_error


def rmse(y_true, y_pred):
    return np.sqrt(mean_squared_error(y_true, y_pred))

def rmsle(y_true, y_pred):
    return np.sqrt(mean_squared_log_error(y_true, y_pred))
    

def plot_predictions(df, player_names, y_pred, n_cols=3, height=2):
    """
    Plot subplots showing actual and predicted values for multiple players
    TODO filter prior to method so we don't predict players not plotted, currently fine for oob 
    :param df: input DataFrame 
    :param player_names: list of players to include in plots 
    :param y_pred: list of predicted values for all df
    :param n_cols: number of columns in subplot grid 
    :param height: height of each plot
    """
    
    groups = (df.assign(y_pred=y_pred)
                .loc[df['player_name'].isin(player_names)]
                .sort_values(['player_name','gw'])
                .groupby('player_name'))
        
    n_rows = int(np.ceil((len(groups) / n_cols)))
    
    _, axes = plt.subplots(n_rows, n_cols, figsize=(12, n_rows * height))
    
    if n_rows * n_cols == 1:
        axes = [axes]
    else:
        axes = axes.flatten()

    for ax, group in zip(axes, groups):
        gkey, gdf = group
        ax.axhline()
        ax.bar(gdf['gw'], gdf['y'], color='steelblue')
        ax.plot(gdf['gw'], gdf['y_pred'], c='red' , marker='.', linestyle='None')
        ax.set_title(gkey) 
        plt.tight_layout()
    plt.show()






