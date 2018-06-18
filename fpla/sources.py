# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np 
from pathlib import Path
from argparse import ArgumentParser
from datetime import datetime
import requests
import itertools

from fpl.fpl import FPL
from fpl.player import Player
from fpl.user import User
from fpl.h2h_league import H2HLeague
from fpl.classic_league import ClassicLeague
from fpl.gameweek import Gameweek

from . import maps
from . import utils
from . import db


class FPLSource(object):
    
    """ Parent class for sources which is subclassed for each source type """
	
    source_type = FPL
    output_parsers = {}
	
    def __init__(self, db, year, ids, output_tables):
        """ 
        :param db: database object
        :param year: year of data (start of season) 
        :param ids: list of ids used to create FPL objects of `source_type`
        :param output_tables: dict of {OUTPUT_NAME: output_table} 
        """
        self.db = db
        self.year = year 
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
            self.db.export(df=df, table=_fix_table_name(table_name, self.year))
                     
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

    """ Instantiating and parsing FPL Player objects """

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
        
        df['year'] = self.year
        
         # create unique identifier for individual matches in double gameweeks
        df['gw_match'] = df.groupby('gw').match_id.rank(ascending=False)
        
        # add player name and position
        df['player_name'] = self.full_name(player.first_name, player.second_name)
        df['pos'] = player.position.lower()[0]
        
        # add team_id
        df['team_id'] = player.team_id

        # adding team names for convenience 
        df['team_name'] = df['team_id'].map(maps.TEAM_ID) 
        df['team_name_o'] = df['team_id_o'].map(maps.TEAM_ID) 
        
        return df
   
    def parse_player_history(self, player):
        """ Extract historical data for a single player """
        # early return empty dataframe if history does not exist (i.e. new player this season)
        if len(player.history_past) == 0:
            return pd.DataFrame()
    	
        df = (pd.DataFrame.from_records(player.history_past, exclude=maps.PLAYER_HISTORY_DROP)
                .rename(columns=maps.PLAYER_HISTORY_RENAME))
        
        df['year'] = self.year
        
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
	
    """ Instantiating and parsing FPL Gameweek objects """
	
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
        
        df['year'] = self.year
        
        # currently we have one row per "match" with separate columns for "teams h" and "team a"
        # we will instead create a single row per team/match combination, which is easier to analyse
        # we create a "mirror" dataframe by mapping the column names, then concatenate "mirror" and base dataframes
        df_home = df.rename(columns=maps.MATCH_HOME_RENAME).assign(venue='home')
        df_away = df.rename(columns=maps.MATCH_AWAY_RENAME).assign(venue='away')
    
        df = pd.concat([df_home, df_away], axis=0).reset_index(drop=True)
        
        # convert string to datetime object
        df['kickoff_time'] = pd.to_datetime(df['kickoff_time'], format='%Y-%m-%dT%H:%M:%SZ')
        df['deadline_time'] = pd.to_datetime(df['deadline_time'], format='%Y-%m-%dT%H:%M:%SZ')
        
        # add team names
        df['team_name'] = df['team_id'].map(maps.TEAM_ID)
        df['team_name_o'] = df['team_id_o'].map(maps.TEAM_ID)
        
        # add result
        df['result'] = df.apply(lambda x: self.match_result(x['score'], x['score_o']), axis=1)
        
        df['gw'] = gameweek.id
        
        return df
    
    
class UserSource(FPLSource):
	
    """ Instantiating and parsing FPL User classes """
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
        
        df['year'] = self.year
        
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
	
    """ Instantiating and parsing FPL H2HLeague objects """
	
    source_type = H2HLeague
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.output_parsers = {
            'H2H_LEAGUE': self.parse_h2h_league
        }
    
    def parse_h2h_league(self, h2h): 
        """ Extract league fixtures/results for a single H2H league """
        df = pd.DataFrame.from_records(h2h.fixtures)
        
        df['year'] = self.year
    
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
	
    """ Instantiating and parsing FPL ClassicLeague objects """
	
    source_type = ClassicLeague 
    
    
class StatusSource(object):
	
    """ Scraping and parsing fplstatistics.co.uk player status change events """
 
    def __init__(self, db, year, url, output_tables):
        """ 
        :param db: database object
        :param year: (base) year that data corresponds to
        :param url:  url to scrape 
        :param output_tables: dict of {OUTPUT_NAME: output_table} 
        """
        self.db = db
        self.year = year
        self.inputs = self.get_source(url)
        self.output_tables = output_tables
        self.output_parsers = {
            'STATUS': self.parse_status
        }
        
    def get_source(self, url):
        """ Return json (dict) object from crawled url """
        response = requests.get(url)
        assert response.status_code == 200, "fplstatistics.co.uk bad status" 
        return response.json()
        
    def process_source(self):
        """ Parse input into a single DataFrame and export to database """
        for output_name, table_name in self.output_tables.items():
            parser = self.output_parsers[output_name] 
            df = parser(self.inputs)           
            # export DataFrame to database
            self.db.export(df=df, table=_fix_table_name(table_name, self.year))

    def parse_status(self, status_json):
        """ Extract player status changes from json into a DataFrame """
        df = pd.DataFrame.from_records(status_json['aaData'], columns=maps.STATUS_NAME)
        
        df['year'] = self.year
        
        df['news'] = df['news'].str.lower()
        
        # split probability/status transition strings into separate columns
        # probability values (0, 25, 50, 100)
        df['old_prob'], df['new_prob'] = df['probability'].str.split(' to ').str
        # status values ('i', 'a', 's', 'l', 'u') 
        df['old_status'], df['new_status'] = df['status'].str.lower().str.split(' to ').str
        
        # convert specified columns to int type 
        df[maps.STATUS_NUMERIC] = df[maps.STATUS_NUMERIC].apply(pd.to_numeric) 
        
        # convert to datetime and append correct year 
        df['status_date'] = df['status_date'].apply(lambda x: self.fix_date(x, base_year=self.year))
        
        # create incremental step (at player level) for each status change
        df['step'] = df.groupby(['player_id'])['status_date'].rank(method='min')
        # resolve duplicate steps
        df = (df.groupby('player_id')
                .apply(self.resolve_duplicate_steps)
                .reset_index(drop=True))

        df = df.drop(columns=maps.STATUS_DROP)
    
        return df
   
    @staticmethod     
    def fix_date(date_string, base_year):
        """ 
        Convert string to datetime and add correct year
        :param date_string: date string
        :param base_year: int year
        :returns: datetime 
        """
        try:
            date = datetime.strptime(date_string, '%d %b') 
        except ValueError:
            # ignore dates that are badly formatted 
            return pd.NaT
        # adding correct years, need to check if date after May
        if date.month > 6:
            date = date.replace(year = base_year) 
        else:
            date = date.replace(year = base_year + 1) 
        return date
    
    def resolve_duplicate_steps(self, df):
        """ 
        As we can have multiple status changes on the same status date this will result in duplicate steps
        This method resolves step duplication, giving a continuous sequence of unique steps
        """
        df = df.copy()
        # check to see if we have duplicates that need to be resolved
        step_counts = df['step'].value_counts()

        for step, count in step_counts[step_counts > 1].items():
            duplicates_idx = df[df['step'] == step].index
            # iterate through all possible permutations of duplicate rows
            for permutation_idx in itertools.permutations(duplicates_idx):
                """
                Check that permutation is valid 
                i.e. the rows are ordered in a way that gives continuous transitions between statuses/probabilities
                We also add the previous/next step (if they exist) to ensure proposed steps are valid globally
                """
                previous_step_idx = self._get_step_index(df['step'], step - 1)
                next_step_idx = self._get_step_index(df['step'], step + 1)
                idx_validation = previous_step_idx + list(permutation_idx) + next_step_idx
                
                if self._validate_steps(df.loc[idx_validation]):
                    # update with resolved step values
                    resolved_steps = np.arange(len(duplicates_idx)) + step
                    df.loc[permutation_idx, "step"] =  resolved_steps
                    
        remaining_duplicates = len(df.groupby(['player_id', 'step']).filter(lambda x: len(x) > 1)) 
        assert remaining_duplicates == 0, "something went wrong, not all duplicates resolved" 
        return df
                
    @staticmethod        
    def _validate_steps(df):
        """ Check that the ordered DataFrame rows make up a valid, continuous sequence """
        df_check = (df.loc[(df['old_prob'] == df['new_prob'].shift(1)) &
                           (df['old_status'] == df['new_status'].shift(1))])

        return len(df) == len(df_check) + 1 
    
    @staticmethod
    def _get_step_index(series, step):
        """ Get index corresponding to step in a series"""
        step_index = series.loc[series == step].index.tolist()
        # check we return exactly one row. zero rows means step doesn't exist and > 1 means we have duplicates
        if len(step_index) == 1:
            return step_index
        else:
            return []

    
class Pipeline(object):
	
    """ Generate sequences of FPL objects defined in config, extract into DataFrames and export """
	
    source_classes = {
	    'PLAYER': PlayerSource, 
	    'GAMEWEEK': GameweekSource, 
	    'H2H_LEAGUE': H2HLeagueSource, 
	    'CLASSIC_LEAGUE': ClassicLeagueSource, 
	    'USER': UserSource, 
	    'STATUS': StatusSource, 
	 }
   
    def __init__(self, config, exclusions=[]):
        """
        :param config: config dict (ref. example-config.json)
        :param exclusions: set of sources to exclude 
        """
        self.db = db.DB(config['db_connection']) 
        self.sources = self.get_config_sources(config, exclusions)
        
    def get_config_sources(self, config, exclusions={}):
        """
        For sources defined in config create list of Source objects
        :param config: dict of config  (ref. example-config.json)
        :param exclusions: set of source names to exclude 
        :returns: list of Source object instances
        """
        config_sources = config['sources'] 
        config_globals = config['globals'] 
        sources = [] 
        for source_name, source_class in self.source_classes.items():
            if source_name in config_sources.keys() - exclusions:
                print(f'Loading source {source_name}')
                source_kwargs = config_sources[source_name]
                global_kwargs = config_globals 
                kwargs = {**source_kwargs, **global_kwargs} # > python 3.4
                source = source_class(db=self.db, **kwargs)
                sources.append(source)       
        return sources 
                
    def run_pipeline(self):
        """ Process all sources """
        for source in self.sources:
            source.process_source()
 
def _fix_table_name(table_name, year):
    return '{}_{}'.format(table_name, year)             
            
    
if __name__ == '__main__':
    
    parser = ArgumentParser("Main method to extract all sources defined in config")
    
    parser.add_argument('-c', '--config-path', action='store', dest='config_path', type=Path, help='full path of config file')
    
    args = parser.parse_args()
    
    # load config into dictionary
    config = utils.load_config(args.config_path)
    
    # run pipeline
    pipeline = Pipeline(config)
    pipeline.run_pipeline()
    
