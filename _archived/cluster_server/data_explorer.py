# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 21:25:35 2017

@author: Andrei Ionut DAMIAN
@project: HyperLoop
"""

import pandas as pd
from sql_helper import MSSQLHelper
from cluster_repository import ClusterRepository

if __name__=="__main__":
    
    pd.set_option('expand_frame_repr', False)
    
    sql = MSSQLHelper()
    
    p1 = '20160101'
    p2 = '20161230'
    p3 = 1048608
    
    strqry = """
            exec 
                [uspGetPartnerAmountsByPeriod] 
                '"""+p1+"""',
                '"""+p2+"""',
                """+str(p3)+"""
            """

    #df_rfm = sql.Select(strqry)
    
    df = pd.read_csv('saved_data/20170202_1622_rfm_data_clusters.csv')
    
    clRep  = ClusterRepository()
    
    clConfig = clRep._config_by_id('1')
    clRep.UploadCluster(df,clConfig, sql)

    
    
    