# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
from pathlib import Path
from argparse import ArgumentParser

from fpl.fpl import FPL
from fpl.player import Player
from fpl.user import User
from fpl.h2h_league import H2HLeague
from fpl.classic_league import ClassicLeague
from fpl.gameweek import Gameweek

from . import maps, utils


class FPLSource(object):
    
    """ Parent class for sources which is subclassed for each source type """
	
    source_type = FPL
    output_parsers = {}
	
    def __init__(self, db_con, ids, output_tables):
        """ 
        :param db_con: database connection string
        :param ids: list of ids used to create FPL objects of `source_type`
        :param output_tables: dict of {OUTPUT_NAME: output_table} 
        """
        self.db_con = db_con
        self.inputs = self.get_source(ids) 
        self.output_tables = output_tables

	
    def get_source(self, fpl_ids):
        """ Create list of FPL objects of from passed fpl_ids """
        return [self.source_type(fpl_id) for fpl_id in fpl_ids] 
            
    def process_source(self):
        """ Parse FPL objects into a DataFrame and export to database """
        for output_name, table_name in self.output_tables.items():
            # parse FPL objects into a single DataFrame 
            parser = self.output_parsers[output_name] 
            df = pd.concat(parser(fpl_object) for fpl_object in self.inputs)
            
            # export DataFrame to database
            self.export(df=df, table=table_name)
                     
    def export(self, df, table):
        """
        Export DataFrame to database table
        :param df: input DataFrame 
        :param table: output table name
        """
        # as we are refreshing output table, first check that size will not decrease
        count = self.count_records(table) 
        new_count = len(df.index)
        assert new_count >= count, "size of new table much exceed or equal existing table" 
        
        df.to_sql(table, con=self.db_con, index=False, if_exists='replace')
     
    def count_records(self, table):
        """ Check table exists and count number of records """
        check_exists_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
        count_query = f"SELECT count(*) FROM {table}" 
    
        cursor = self.db_con.cursor()
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
            
            
class PlayerSource(FPLSource):
	
    source_type = Player
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.output_parsers = {
            'PLAYER': self.parse_player,
            'PLAYER_HISTORY': self.parse_player_history
        }
		
    def get_source(self, fpl_ids):
        #TODO create FPL PR which makes returning single players easier, which will make this override obsolete 
        return FPL().get_players()
    
    def parse_player(self, player):
        """ Extract data for a single player """
        df = (pd.DataFrame.from_records(player.history, exclude=maps.PLAYER_DROP)
                .rename(columns=maps.PLAYER_RENAME))
        
         # create unique identifier for individual matches in double gameweeks
        df['gw_match'] = df.groupby('gw').match_id.rank(ascending=False)
        
        # add player name and position
        df['player_name'] = self.full_name(player.first_name, player.second_name)
        df['pos'] = player.position.lower()[0]
        
        return df
   
    def parse_player_history(self, player):
        """ Extract historical data for a single player """
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
       
        

class GameweekSource(FPLSource):
	
    source_type = Gameweek
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.output_parsers = {
            'MATCH': self.parse_match
        }
    
    def parse_match(self, gameweek):
        """ Extract data for all matches in a single gameweek """
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
    
    
class UserSource(FPLSource):
	
    source_type = User
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.output_parsers = {
            'USER': self.parse_user,
            'USER_PICKS': self.parse_user_picks
        }
    
    def parse_user(self, user):
        """ Extract season data for a single user """
        df = (pd.DataFrame.from_records(user.history['history'], exclude=maps.USER_DROP)
                .rename(columns=maps.USER_RENAME))
        
        # create column identifying gameweek each chip was used
        chip_gw_map = {chip['event']: chip['name'] for chip in user.history['chips']}
        df['chip'] = df['gw'].map(chip_gw_map)
        
        # add user name
        df['user_name'] = self.full_name(user.first_name, user.second_name)        
   
        return df
 
    
    def parse_user_picks_gameweek(self, gameweek_picks):
        """ Extract user's team 'picks' for a single gamweek """
        df = (pd.DataFrame.from_records(gameweek_picks['picks']) 
                .rename(columns=maps.USER_RENAME))
        
        # set multiplier to zero for substitutes (unless Bench Boost is active) for easy points total calculations
        df.loc[(df.position > 11) & (gameweek_picks['active_chip'] != 'bboost'), 'multiplier'] = 0
    
        df['gw'] = gameweek_picks['event']['id'] 
        
        return df
           
    def parse_user_picks(self, user):
        """ Extract user's team 'picks' for all gameweeks """
        # exclude gameweeks yet to be played
        dfs = (self.parse_user_picks_gameweek(picks) for picks in user.picks.values() if picks['event']['finished'])
        df = pd.concat(dfs).rename(columns=maps.USER_PICKS_RENAME)
        
        # add user_id
        df['user_id'] = user.id
        
        return df
        
    
class H2HLeagueSource(FPLSource):
	
    source_type = H2HLeague
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.output_parsers = {
            'H2H_LEAGUE': self.parse_h2h_league
        }
    
    def parse_h2h_league(self, h2h): 
        """ Extract league fixtures/results for a single H2H league """
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
    
    
class ClassicLeagueSource(FPLSource):
	
    source_type = ClassicLeague 
    
    
class Pipeline(object):
	
    """ Generate sequences of FPL objects defined in config, extract into DataFrames and export """
	
    source_classes = {
	    'PLAYER': PlayerSource, 
	    'GAMEWEEK': GameweekSource, 
	    'H2H_LEAGUE': H2HLeagueSource, 
	    'CLASSIC_LEAGUE': ClassicLeagueSource, 
	    'USER': UserSource
	 }
   
    def __init__(self, config):
        """
        :param config: config dict (ref. example-config.json)
        """
        self.db_con = sqlite3.connect(config['db_connection']) 
        self.sources = self.get_config_sources(config['sources']) 
        
    def get_config_sources(self, config_sources):
        """
        For sources defined in config create list of Source objects
        :param config_sources: dict of config sources (ref. example-config.json)
        :returns: list of Source object instances
        """
        sources = [] 
        for source_name, source_class in self.source_classes.items():
            if source_name in config_sources:
                print(f'Loading source {source_name}')
                source_kwargs = config_sources[source_name]
                source = source_class(db_con=self.db_con, **source_kwargs)
                sources.append(source)       
        return sources 
                
    def run_pipeline(self):
        """ Process all sources """
        for source in self.sources:
            source.process_source()
                
            
    
if __name__ == '__main__':
    
    parser = ArgumentParser("Main method to extract all sources defined in config")
    
    parser.add_argument('-c', '--config-path', action='store', dest='config_path', type=Path, help='full path of config file')
    
    args = parser.parse_args()
    
    # load config into dictionary
    config = utils.load_config(args.config_path)
    
    # run pipeline
    pipeline = Pipeline(config)
    pipeline.run_pipeline()
    
