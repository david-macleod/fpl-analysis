# -*- coding: utf-8 -*-
import sqlite3
import json
import pandas as pd
from pathlib import Path
from argparse import ArgumentParser

from fpl.fpl import FPL
from fpl.user import User
from fpl.h2h_league import H2HLeague
from fpl.classic_league import ClassicLeague

from . import maps 


def parse_player_data(player):
    df = (pd.DataFrame.from_records(player.history, exclude=maps.PLAYER_COLUMNS_DROP)
            .rename(columns=maps.PLAYER_COLUMNS_RENAME))
    
     # create unique identifier for individual matches in double gameweeks
    df['gw_match'] = df.groupby('gw').match_id.rank(ascending=False)
    
    # add player name and position
    df['name'] = full_name(player.first_name, player.second_name)
    df['pos'] = player.position.lower()[0]
    
    return df
    

def parse_h2h_league_data(h2h): 
    df = pd.DataFrame.from_records(h2h.fixtures)

    # currently we have one row per "fixture" with with separate columns for "teams 1" and "team 2"
    # we will instead create a single row per team/fixture combination, which is easier to analyse 
    # we create a "mirror" dataframe by mapping the column names, then concatenate "mirror" and base dataframes
    df_reverse = df.rename(columns=lambda x: x.translate(str.maketrans('12', '21')))
    df = pd.concat([df, df_reverse], axis=0).reset_index(drop=True)

    df.rename(columns=maps.H2H_LEAGUE_COLUMNS_RENAME, inplace=True)

    # create result column (outputs 'w','d' or 'l')
    df['result'] = (df.loc[:, ['entry_1_win', 'entry_1_draw', 'entry_1_loss']].idxmax(axis=1)
                      .str.split('_').str[-1].str[0].str.lower()) # extract first letter from result
    
    # set result for "unplayed" gameweeks to '-'
    df.loc[df[['h2h_points', 'h2h_points_o']].max(axis=1) == 0, 'result'] =  '-'

    # remove excess columns and sort dataframe
    df = df.loc[:, maps.H2H_LEAGUE_COLUMNS_KEEP].sort_values(['gw', 'name'])

    # add running points total now that we have sorted the dataframe
    df['total'] = df.groupby('name').h2h_points.cumsum()
    
    # lower-case names
    df['name'] = df['name'].str.lower()
    df['name_o'] = df['name_o'].str.lower()
    
    return df


def parse_user_data(user):
    
    df = (pd.DataFrame.from_records(user.history['history'], exclude=maps.USER_COLUMNS_DROP)
            .rename(columns=maps.USER_COLUMNS_RENAME))
    
    # create column identifying gameweek each chip was used
    chip_gw_map = {chip['event']: chip['name'] for chip in user.history['chips']}
    df['chip'] = df['gw'].map(chip_gw_map)
    
    # add user name
    df['name'] = full_name(user.first_name, user.second_name)
    
    return df


def parse_user_picks_gameweek(gameweek_picks):
    
    df = (pd.DataFrame.from_records(gameweek_picks['picks']) 
            .rename(columns=maps.USER_COLUMNS_RENAME))
    
    # set multiplier to zero for substitutes (unless Bench Boost is active) for easy points total calculations
    df.loc[(df.position > 11) & (gameweek_picks['active_chip'] != 'bboost'), 'multiplier'] = 0

    df['gw'] = gameweek_picks['event']['id'] 
    
    return df

     
def parse_user_picks(user):
    
    # parse user pick data for all "finished" gameweeks 
    dfs = (parse_user_picks_gameweek(picks) for picks in user.picks.values() if picks['event']['finished'])
    df = pd.concat(dfs)
    
    df['user_id'] = user.id
    
    
    return df


def extract_players(**db_kwargs):

    players = FPL().get_players()
    
    df = pd.concat(parse_player_data(player) for player in players)
    
    export(df, **db_kwargs)


def extract_h2h_leagues(h2h_league_ids, **db_kwargs):
    
    # get User objects from ids
    h2h_leagues = [H2HLeague(h2h_league_id) for h2h_league_id in h2h_league_ids]
    
    df = pd.concat(parse_h2h_league_data(h2h_league) for h2h_league in h2h_leagues)
    
    export(df, **db_kwargs)


def extract_users(user_ids, **db_kwargs):
    
    # get User objects from ids
    users = [User(user_id) for user_id in user_ids]
    
    df = pd.concat(parse_user_data(user) for user in users)
    
    export(df, **db_kwargs)
    
    
def extract_user_picks(user_ids, **db_kwargs):
    
    # get User objects from ids
    users = [User(user_id) for user_id in user_ids]
    
    df = pd.concat(parse_user_picks(user) for user in users)
    
    export(df, **db_kwargs)
    
    
def export(df, db, table):
    """
    Export dataframe to database table
    :param df: input DataFrame 
    :param db: database connection object 
    :param table: output table string
    """
    # as we are truncating output table, first check that size will not decrease
    count = count_records(db, table) 
    new_count = len(df.index)
    assert new_count >= count, "new table is smaller than existing table" 
    
    df.to_sql(table, con=db, index=False, if_exists='replace')
	
	
def count_records(db, table):
    """ Check table exists and count number of records """
    
    check_exists_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
    count_query = f'SELECT count(*) FROM {table}'
    
    cursor = db.cursor()
    if cursor.execute(check_exists_query).fetchone():
        return cursor.execute(count_query).fetchone()[0]
    else:
        return 0
    
    
def leagues_to_user_ids(leagues):
    """ Retrieve a list of user ids from a list of league objects """
    return [user['entry'] for league in leagues for user in league.standings]


def full_name(first_name, last_name):
    """ Concatenate first and last names """
    return ' '.join([first_name, last_name]).lower()
	
    
def main(config_path):

    config = utils.load_config(config_path) 
    db = sqlite3.connect(config['DB_CON_STRING'])
    
    user_ids_all = set() 
    
    if 'PLAYER' in config.keys():
        print('exporting players')
        player = config['PLAYER'] 
        
        extract_players(
            db=db,
            table=player['TABLE']
        )
        
        
    if 'H2H_LEAGUE' in config.keys():
        print('exporting h2h_leagues')
        h2h_league = config['H2H_LEAGUE']
        
        extract_h2h_leagues(
            h2h_league_ids=h2h_league['IDS'], 
            db=db,
            table=h2h_league['TABLE']
        )
        
        # get user ids from leagues 
        league_user_ids = leagues_to_user_ids(map(H2HLeague, h2h_league['IDS']))
        user_ids_all.update(league_user_ids)
        
    
    if 'CLASSIC_LEAGUE' in config.keys():
        pass 
    
    
    if 'USER' in config.keys():
        print('exporting users')
        user = config['USER'] 
        # add specific user ids which are not in leagues defined in config 
        user_ids_all.update(user['IDS']) 
        
        extract_users(
            user_ids=user_ids_all,
            db=db, 
            table=user['TABLE']
        ) 
        extract_user_picks(
            user_ids=user_ids_all, 
            db=db,
            table=user['PICKS_TABLE']
        ) 
        
        
    if 'FIXTURE' in config.keys():
        print('exporting fixtures')
        pass
        
         
    
if __name__ == '__main__':
    
    parser = ArgumentParser("Main method to extract all sources defined in config")
    
    parser.add_argument('-c', '--config-path', action='store', dest='config_path', type=Path, help='full path of config file')
    
    args = parser.parse_args()
    
    main(args.config_path) 
    
