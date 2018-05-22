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

class FPLSource(object):

    def __init__(self, config):
        self.config = config
        self.db_connection = sqlite3.connect(self.config['DB_CONNECTION'])

    def get_source(self):
        raise NotImplementedError

    def parse_source(self):
        raise NotImplementedError

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

        df.to_sql(table, con=self.db_connection, index=False, if_exists='replace'

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


class PlayerSource(FPLSource):

    def get_source(self):
        # TODO create FPL PR which makes returning single players easier, so make this method consistent with other get_ methods
        return FPL().get_players()

    def parse_source(self):
        pass


class GameweekSource(FPLSource):

    def get_source(self):
        gameweek_ids = self.config['SOURCES']['GAMEWEEK']['IDS']
        return [Gameweek(gameweek_id) for gameweek_id in gameweek_ids]

    def parse_source(self):
        pass


class H2HLeagueSource(FPLSource):

    def get_source(self):
        h2h_league_ids = self.config['SOURCES']['H2H_LEAGUE']['IDS']
        return [H2HLeague(h2h_league_id) for h2h_league_id in h2h_league_ids]

    def parse_source(self):
        pass


class ClassicLeagueSource(FPLSource):

    def get_source(self):
        classic_league_ids = self.config['SOURCES']['CLASSIC_LEAGUE']['IDS']
        return [ClassicLeague(classic_league_id) for classic_league_id in classic_league_ids]

    def parse_source(self):
        pass

class UserSource(FPLSource):

    def get_source(self):
        user_ids = set(self.config['SOURCES']['USER']['IDS'])
        # add users ids found in "league" objects
        leagues = self.h2h_leagues + self.classic_leagues
        user_ids.update(user['entry'] for league in leagues for user in league.standings)
        return [User(user_id) for user_id in user_ids]

    def parse_source(self):
        pass


KEY_SOURCE_MAP = {
    "PLAYERS": PlayerSource,
    "USERS": UserSource,
### etc....
}
       
    
if __name__ == '__main__':
    
    parser = ArgumentParser("Main method to extract all sources defined in config")
    
    parser.add_argument('-c', '--config-path', action='store', dest='config_path', type=Path, help='full path of config file')
    
    args = parser.parse_args()
    
    # load config into dictionary
    config = utils.load_config(args.config_path)
    
    # run pipeline
    pipeline = Pipeline(config)
    pipeline.run_pipeline()
    
