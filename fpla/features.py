# -*- coding: utf-8 -*-
import pandas as pd 
import numpy as np 

def append_one_hot(df, columns):
    """
    Convert columns to one-hot encoded versions
    :param df: Input DataFrame
    :param columns: List of columns to be converted
    :returns: Input DataFrame with one-hot encoded columns appended
    """
    df = df.copy()
    existing_columns = df.columns 
    df = pd.get_dummies(df, columns=columns, prefix=columns)
    
    new_columns = df.columns.difference(existing_columns) 
    df = df.rename(columns={column: f'{column}_ft' for column in new_columns}) 
    return df


def append_ordinal(df, column, order):
    """
    Convert input column to categorical AND append category codes as feature 
    :param df: Input DataFrame
    :param column: Column to convert
    :param order: Order of categories
    :returns: Input DataFrame with converted columns
    """
    df = df.copy()
    df[column] = pd.Categorical(df[column], categories=order, ordered=True) 
    df[f'{column}_ft'] = df[column].cat.codes
    return df


def append_lags(df, base_col, offsets, group_col='player_id'):
    """ 
    Calculate "lag" of base_col (raw of previous nth row)
    :param df: Input DataFrame
    :param base_col: Name of column to "lag"
    :param offsets: List of integer offsets (number of rows to lag)
    :param group_col: Column to group on prior to lag calculation
    :returns: Input DataFrame with {base_col}_lag{offset}_ft column appended
    """
    df = df.copy().sort_values(['player_id','gw']) # guarantee correct order 
    for offset in offsets:
        lag_col = f"{base_col}_lag{offset}_ft"
        df[lag_col] = df.groupby(group_col)[base_col].shift(offset)
    return df


def append_diffs(df, base_col, offsets, group_col='player_id'):
    """ 
    Calculate difference between consecutive rows with a given lag offset
    :param df: Input DataFrame
    :param base_col: Name of column to calculate difference
    :param offsets: List of integer offsets (number of rows to lag)
    :param group_col: Column to group on prior to lag calculation
    :returns: Input DataFrame with {base_col}_diff{offset}_ft column appended
    """
    df = df.copy().sort_values(['player_id','gw']) # guarantee correct order 
    for offset in offsets:
        diff_col = f"{base_col}_diff{offset}_ft"
        df[diff_col] = df.groupby(group_col)[base_col].shift(offset).diff(1) 
    return df

def append_gw_teams(df, df_match):
    """
    Count the number of teams playing in a single gameweek
    :param df: Input DataFrame (must contain 'player_id', 'gw' columns)
    :param df: Match DataFrame
    :returns: Input DataFrame with 'gw_teams_ft' column appended
    """
    df = df.copy()
    df_teams = (df_match.groupby('gw')['team_id'].nunique()
                        .reset_index()
                        .rename(columns={'team_id':'gw_teams_ft'}))

    return df.merge(df_teams, on='gw').sort_values(['player_id', 'gw'])


def append_double_gw(df):
    """ Return the number of matches played by a player in a gameweek """
    # Already avaible in input so we can just rename
    df = df.copy()
    df = df.rename(columns={'gw_match':'double_gw_ft'}) 
    return df

    
def append_status_type(df):
    """ Extract 'status_type' from 'status': 1 = 'positive', 0 = 'negative """
    df = df.copy()
    
    status_map = {
        'a': 1,
        'i': 0,
        'l': 0,
        'u': 0,
        's': 0,
        np.NaN: 1 # for players that have no recorded status change we assume they are available
    }
    df['status_type'] = df['status'].map(status_map)
    return df


def append_status_type_run(df):
    """ Create incremental counter for consecutive gameweeks of the same statys type """
    df = df.copy().sort_values(['player_id','gw']) # guarantee correct order 
    
    # add counter to consecutive rows of the same status type
    #print(df.groupby(['player_id', (df['status_type'] != df['status_type'].shift(1)).cumsum()]).groups)
    status_groups = (df['status_type'] != df['status_type'].shift(1)).cumsum()
    df['st_run'] =  df.groupby(['player_id', status_groups])['gw'].rank()
            
    df['st1_run_ft'] = np.where(df['status_type'] == 1 , df['st_run'], 0)
    df['st0_run_ft'] = np.where(df['status_type'] == 0 , df['st_run'], 0)
    return df


def append_status_type_change(df):
    """ Flag gameweeks where we see a transition between status types """
    df = df.copy()
    
    # find changes in status type (except in gameweek 1)
    df['st_change_ft'] = df.apply(lambda x: _encode_status_type_change(x['st1_run_ft'], x['st0_run_ft']), axis=1)
    return df
    

def _encode_status_type_change(st1_run, st0_run):
    if st1_run == 1:
        flag = 1
    elif st0_run == 1:
        flag = -1
    else:
        flag = 0
    return flag
         
    
def append_player_status(df, df_match, df_status):
    """
    Find and append latest status for each gameweek/player combination
    :param df: Base DataFrame to which result columns will be attached (requires player_id, team_id, gw columns)  
    :param df_match: Match DataFrame 
    :param df_status: Status DataFrame 
    :returns: existing DataFrame with `status`, `prob`, `step` attached
    """
    df_latest_status = (df
        .merge(df_match.drop_duplicates(['team_id','gw']) , on=['team_id','gw']) # drop "double gameweek" duplicates
        .merge(df_status, on=['player_id']) 
        .groupby(['player_id', 'gw'])
        .apply(_get_latest_status)
        .reset_index() 
        .filter(['player_id','gw','new_status','new_prob','step'])
        .rename(columns={'new_prob':'prob', 'new_status':'status'}))
    
    # append new columns to original DataFrame
    df = df.merge(df_latest_status, how='left', on=['player_id','gw'])
    return df
                    

def _get_latest_status(df):
    """ Get the latest available status for a single player/gameweek combination """
    df = df.copy()
    # only retain statuses which were recorded before current gameweek deadline (SLOW)
    df_history  = df.loc[df['status_date'].dt.date < df['deadline_time'].dt.date]
    # check if we have any statuses available
    if len(df_history) > 0:
        # return row of latest available status (row with largest step value)
        df = df_history.loc[[df_history['step'].idxmax()], ['old_status','new_status','old_prob', 'new_prob','step']]
    else:
        # if no history is available then our gameweek is "too early", so we extrapolate backwards from step 1
        df = df.loc[df['step'] == 1, ['old_status','new_status','old_prob', 'new_prob','step']]
        df['new_status'], df['new_prob'] = df['old_status'], df['old_prob']
        # set step to 0 so we can easily identify back-filled statuses 
        df['step'] = 0
    return df
