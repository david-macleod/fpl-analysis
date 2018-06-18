# -*- coding: utf-8 -*-
import sqlite3


class DB(object):

    def __init__(self, db_con):
        self.conn = sqlite3.connect(db_con)
       
    def export(self, df, table, dtypes={}):
        """
        Export DataFrame to database table
        :param df: input DataFrame 
        :param table: output table name
        :param dtypes: dict of {column_name, sql_column_type} 
        """
        # as we are refreshing output table, first check that size will not decrease
        count = self.count_records(table) 
        new_count = len(df.index)
        assert new_count >= count, "size of new table much exceed or equal existing table" 
        
        df.to_sql(table, con=self.conn, index=False, if_exists='replace', dtype=dtypes )
     
    def count_records(self, table):
        """ Check table exists and count number of records """
        check_exists_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
        count_query = f"SELECT count(*) FROM {table}" 
    
        cursor = self.conn.cursor()
        if cursor.execute(check_exists_query).fetchone():
            return cursor.execute(count_query).fetchone()[0]
        else:
            return 0
           