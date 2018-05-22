# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
from pathlib import Path
from argparse import ArgumentParser

from fpl.fpl import FPL
from fpl.user import User
from fpl.h2h_league import H2HLeague
from fpl.classic_league import ClassicLeague
from fpl.gameweek import Gameweek

from . import maps, utils


class Pipeline(object):

    "Generate sequences of FPL objects, then extract into dataframes and export"
   
    def __init__(self, config):
        self.config = config
        self.db_connection = sqlite3.connect(self.config['DB_CONNECTION'])
        # instantiate lists of FPL objects from sources defined in config
        self.players = self.get_source('PLAYER')
        self.gameweeks = self.get_source('GAMEWEEK')
        self.h2h_leagues = self.get_source('H2H_LEAGUE') 
        self.classic_leagues = self.get_source('CLASSIC_LEAGUE') 
        self.users = self.get_source('USER') 
        
        
    def get_source(self, source_name):
        """ Run "get" method for specified source to return a list of FPL objects """
        if source_name in self.config['SOURCES']:
            print(f'Loading source {source_name}')
            get_method = self.config['SOURCES'][source_name]['GET_METHOD']
            fpl_objects = getattr(self, get_method)()
            return fpl_objects
        else:
            return list()
     
    def get_players(self):
        #TODO create FPL PR which makes returning single players easier, so make this method consistent with other get_ methods 
        return FPL().get_players()
        
    def get_gameweeks(self):
        gameweek_ids = self.config['SOURCES']['GAMEWEEK']['IDS']
        return [Gameweek(gameweek_id) for gameweek_id in gameweek_ids]
        
    def get_h2h_leagues(self):
        h2h_league_ids = self.config['SOURCES']['H2H_LEAGUE']['IDS']
        return [H2HLeague(h2h_league_id) for h2h_league_id in h2h_league_ids]
    
    def get_classic_leagues(self):
        classic_league_ids = self.config['SOURCES']['CLASSIC_LEAGUE']['IDS']
        return [ClassicLeague(classic_league_id) for classic_league_id in classic_league_ids]
    
    def get_users(self):
        user_ids = set(self.config['SOURCES']['USER']['IDS']) 
        # add users ids found in "league" objects 
        leagues = self.h2h_leagues + self.classic_leagues
        user_ids.update(user['entry'] for league in leagues for user in league.standings)
        return [User(user_id) for user_id in user_ids]
        
    
    def run_pipeline(self):
        """ Iterate through available sources, parse objects into DataFrames and export """
        
        for source_name, source in self.config['SOURCES'].items():
            # retrieve inputs for current source 
            fpl_objects = getattr(self, f'{source_name}s'.lower())
            
            for output_name, output in source['OUTPUTS'].items():
                print(f'Processing output {output_name}')
                # parse FPL objects into DataFrame
                parser = getattr(self, output['PARSE_METHOD'])
                df = pd.concat(parser(fpl_object) for fpl_object in fpl_objects)
                
                # export DataFrame to database
                self.export(df, table=output['TABLE']) 
                
                
    def parse_player(self, player):
        df = (pd.DataFrame.from_records(player.history, exclude=maps.PLAYER_DROP)
                .rename(columns=maps.PLAYER_RENAME))
        
         # create unique identifier for individual matches in double gameweeks
        df['gw_match'] = df.groupby('gw').match_id.rank(ascending=False)
        
        # add player name and position
        df['player_name'] = self.full_name(player.first_name, player.second_name)
        df['pos'] = player.position.lower()[0]
        
        return df
   
     
    def parse_player_history(self, player):
        
        # early return empty dataframe if history does not exist (i.e. new player this season)
        if len(player.history_past) == 0:
            return pd.DataFrame()
    	
        df = (pd.DataFrame.from_records(player.history_past, exclude=maps.PLAYER_HISTORY_DROP)
                .rename(columns=maps.PLAYER_HISTORY_RENAME))
        
        # add player name
        df['player_name'] = self.full_name(player.first_name, player.second_name)
        
        # add player_id for current season (that history was taken from) 
        df['player_id'] = player._id 
        
        # season_name is a string e.g. "2016/17", convert this to a number of the start year e.g. 2017
        df['season'] = df['season_name'].str[:4].astype('int') 
        # drop now redundant column
        df.drop(columns=['season_name'], inplace=True)
        
        return df
        
    
    def parse_h2h_league(self, h2h): 
        df = pd.DataFrame.from_records(h2h.fixtures)
    
        # currently we have one row per "fixture" with separate columns for "teams 1" and "team 2"
        # we will instead create a single row per team/fixture combination, which is easier to analyse 
        # we create a "mirror" dataframe by mapping the column names, then concatenate "mirror" and base dataframes
        df_reverse = df.rename(columns=lambda x: x.translate(str.maketrans('12', '21')))
        df = pd.concat([df, df_reverse], axis=0).reset_index(drop=True)
    
        df.rename(columns=maps.H2H_LEAGUE_RENAME, inplace=True)
    
        # create result column (outputs 'w','d' or 'l')
        df['result'] = df['result'] = df.apply(lambda x: self.match_result(x['points'], x['points_o']), axis=1)
        
        # set result for "unplayed" gameweeks to '-'
        df.loc[(df[['h2h_points', 'h2h_points_o']].max(axis=1) == 0), 'result'] = '-'
    
        # remove excess columns and sort dataframe
        df = df.loc[:, maps.H2H_LEAGUE_KEEP].sort_values(['gw', 'user_name'])
    
        # add running points total now that we have sorted the dataframe
        df['total'] = df.groupby('user_name').h2h_points.cumsum()
        
        # lower-case names
        df['user_name'] = df['user_name'].str.lower()
        df['user_name_o'] = df['user_name_o'].str.lower()
        
        return df
    
    
    def parse_user(self, user):
        
        df = (pd.DataFrame.from_records(user.history['history'], exclude=maps.USER_DROP)
                .rename(columns=maps.USER_RENAME))
        
        # create column identifying gameweek each chip was used
        chip_gw_map = {chip['event']: chip['name'] for chip in user.history['chips']}
        df['chip'] = df['gw'].map(chip_gw_map)
        
        # add user name
        df['user_name'] = self.full_name(user.first_name, user.second_name)
        
        return df
    
    
    def parse_user_picks_gameweek(self, gameweek_picks):
        
        df = (pd.DataFrame.from_records(gameweek_picks['picks']) 
                .rename(columns=maps.USER_RENAME))
        
        # set multiplier to zero for substitutes (unless Bench Boost is active) for easy points total calculations
        df.loc[(df.position > 11) & (gameweek_picks['active_chip'] != 'bboost'), 'multiplier'] = 0
    
        df['gw'] = gameweek_picks['event']['id'] 
        
        return df
    
         
    def parse_user_picks(self, user):
        
        # parse user pick data for all "finished" gameweeks 
        dfs = (self.parse_user_picks_gameweek(picks) for picks in user.picks.values() if picks['event']['finished'])
        df = pd.concat(dfs).rename(columns=maps.USER_PICKS_RENAME)
        
        # add user_id
        df['user_id'] = user.id
        
        return df
        
    
    def parse_match(self, gameweek):
        
        # parse all matches for a single gameweek
        df = pd.DataFrame.from_records(gameweek.fixtures, exclude=maps.MATCH_DROP)
        
        # currently we have one row per "match" with separate columns for "teams h" and "team a"
        # we will instead create a single row per team/match combination, which is easier to analyse
        # we create a "mirror" dataframe by mapping the column names, then concatenate "mirror" and base dataframes
        df_home = df.rename(columns=maps.MATCH_HOME_RENAME).assign(venue='home')
        df_away = df.rename(columns=maps.MATCH_AWAY_RENAME).assign(venue='away')
    
        df = pd.concat([df_home, df_away], axis=0).reset_index(drop=True)
        
        # convert string to datetime object
        df['kickoff_time'] = pd.to_datetime(df['kickoff_time'], format='%Y-%m-%dT%H:%M:%SZ')
        
        # add team names
        df['team_name'] = df['team_id'].map(maps.TEAM_ID)
        df['team_name_o'] = df['team_id_o'].map(maps.TEAM_ID)
        
        # add result
        df['result'] = df.apply(lambda x: self.match_result(x['score'], x['score_o']), axis=1)
        
        df['gw'] = gameweek.id
        
        return df
    
    
    def export(self, df, table):
        """
        Export dataframe to database table
        :param df: input DataFrame 
        :param table: output table string
        """
        # as we are refreshing output table, first check that size will not decrease
        count = self.count_records(table) 
        new_count = len(df.index)
        assert new_count >= count, "size of new table much exceed or equal existing table" 
        
        df.to_sql(table, con=self.db_connection, index=False, if_exists='replace')
    
    
    def count_records(self, table):
        """ Check table exists and count number of records """
        check_exists_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
        count_query = f"SELECT count(*) FROM {table}" 
    
        cursor = self.db_connection.cursor()
        if cursor.execute(check_exists_query).fetchone():
            return cursor.execute(count_query).fetchone()[0]
        else:
            return 0
        
        
    @staticmethod 
    def full_name(first_name, last_name):
        """ Concatenate first and last names """
        return ' '.join([first_name, last_name]).lower()

    @staticmethod
    def match_result(team_score, team_score_o):
        """ 
        Return match result relative to team
        :param team_score: int score of team
        :param team_score: int score of opposition team 
        :return: string 'w', 'd' or 'l' 
        """
        if team_score > team_score_o:
            return 'w'
        elif team_score < team_score_o:
            return 'l'
        elif team_score == team_score_o:
            return 'd'
       
       
    
if __name__ == '__main__':
    
    parser = ArgumentParser("Main method to extract all sources defined in config")
    
    parser.add_argument('-c', '--config-path', action='store', dest='config_path', type=Path, help='full path of config file')
    
    args = parser.parse_args()
    
    # load config into dictionary
    config = utils.load_config(args.config_path)
    
    # run pipeline
    pipeline = Pipeline(config)
    pipeline.run_pipeline()
    
