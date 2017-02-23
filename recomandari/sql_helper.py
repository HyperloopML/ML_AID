# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 19:43:09 2017

@author: Andrei Ionut DAMIAN
@library: SQLHELPER
@description: Custom MS SQL Server Helper engine
    
@project: HyperLoop RECOMMENDER MATRIX

@modified: 2017-02-14


"""

from __future__ import print_function
import pandas as pd
import pyodbc
import urllib 
import json
from sqlalchemy import create_engine
import datetime
import time as tm



def start_timer():    
    return tm.time()

def end_timer(start_timer):
    return(tm.time()-start_timer)

def print_progress(str_text):
    print("\r"+str_text, end='\r', flush=True)
    return

class MSSQLHelper:
    def __init__(self, config_file = "sql_config.txt"):
        self.MODULE = '[MSSQL RECOMMENDER ENGINE]'
        self._logger("__init__ "+self.MODULE)
        cfg_file = open(config_file)
        config_data = json.load(cfg_file)
        self.driver   = config_data["driver"]
        self.server   = config_data["server" ]
        self.database = config_data["database"]
        self.username = config_data["username"]
        self.password = config_data["password"]
        
        self.connstr = 'DRIVER=' + self.driver
        self.connstr+= ';SERVER=' + self.server
        self.connstr+= ';DATABASE=' + self.database
        self.connstr+= ';UID=' + self.username
        self.connstr+= ';PWD=' + self.password
        
        sql_params = urllib.parse.quote_plus(self.connstr)
        
        try:
            self.conn = pyodbc.connect(self.connstr)   
            self.engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % sql_params)
            self._logger("Connection created on "+self.server)
        except Exception as err: #pyodbc.Error as err:
            self.HandleError(err)        
        return
    
    
    def Select(self,str_select):
        df = None
        try:
            t0 = tm.time()
            df = pd.read_sql(str_select, self.conn)
            self._logger("Data downloaded {} rows: ".format(df.shape[0],
                                                            str_select))
            self._logger("Dataset head(3):\n{}".format(df.head(3)))
            t1 = tm.time()
            tsec = t1-t0
            tmin = float(tsec) / 60
            self._logger("READ TABLE time: {:.2f}s ({:.2f}min)".format(tsec,tmin))
        except Exception as err: #pyodbc.Error as err:
            self.HandleError(err)
        return df
    
    
    def ReadTable(self, str_table):
        str_select = "SELECT * FROM ["+str_table+"]"
        return self.Select(str_select)

        
    def ExecInsert(self, sInsertQuery):        
        try:
            t0 = tm.time()
            cursor = self.conn
            cursor.execute(sInsertQuery)
            self.conn.commit()
            t1 = tm.time()
            tsec = t1-t0
            tmin = float(tsec) / 60
            self._logger("EXEC SQL  time: {:.2f}s ({:.2f}min)".format(tsec,tmin))
        except Exception as err: #pyodbc.Error as err:
            self.HandleError(err)                        
        return


    def SaveTable(self, df, sTable):
        try:      
            self._logger("SAVING TABLE ({} records)...".format(df.shape[0]))
            t0 = tm.time()
            df.to_sql(sTable, 
                      self.engine, 
                      index = False, 
                      if_exists = 'append')
            t1 = tm.time()
            tsec = t1-t0
            tmin = float(tsec) / 60
            self._logger("SAVE TABLE time: {:.2f}s ({:.2f}min)".format(tsec,tmin))
        except Exception as err: #pyodbc.Error as err:
            self.HandleError(err)                    
        return

        
    def Close(self):
        self.conn.close()
        return


    def HandleError(self, err):
        strerr = "ERROR: "+ str(err)
        self._logger(strerr)
        return
        
    
    def _logger(self, logstr, show = True):
        if not hasattr(self, 'log'):        
            self.log = list()
        nowtime = datetime.datetime.now()
        strnowtime = nowtime.strftime("[SQLHelper][%Y-%m-%d %H:%M:%S] ")
        logstr = strnowtime + logstr
        self.log.append(logstr)
        if show:
            print(logstr)
        return

    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
        self._logger("__exit__")
        return
        

            
        

        
if __name__ == '__main__':

    print("ERROR: MSSQLHelper is library only!")
    
    
