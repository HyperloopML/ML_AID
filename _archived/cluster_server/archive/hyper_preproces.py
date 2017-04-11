# -*- coding: utf-8 -*-
"""
Created on Tue Dec 06 23:52:28 2016

@author: Andrei Ionut DAMIAN, 4E SOFTWARE SRL
"""

import pandas as pd
import numpy as np

from datetime import datetime as dt


if __name__=='__main__':
    

    
    df_tran = pd.read_csv('_tran.csv')
    df_cust = pd.read_csv('_cust.csv')
    
    # merge inner join on the intersection of columns
    df_all = df_tran.merge(df_cust)
    
    #df_all['M']=df_all['Date'].str
    
    # drop duplicates
    df_all = df_all.drop_duplicates()
    
    # drop zero variance columns
    df_all = df_all.loc[:,df_all.std()>0.1]

    df_all.drop('State', axis=1, inplace = True)
                        
                        
    # convert Date to string
    df_all['Date'] = pd.to_datetime(df_all['Date'].astype(str))
    
    # reference date
    dt_ref = dt.strptime('20160101','%Y%m%d')
    
    df_all['DateDiff'] = (df_all['Date'] - dt_ref).dt.days
                        
    group_cols = ['CustomerID','Age','Sex']
    
    gb_all = df_all.groupby(group_cols)
    
    #ser_sums 
    df_sums = gb_all.agg({
                          "OrderTotal":{
                                        "M_Score":np.sum,
                                        "BestOrder":np.max
                                        },
                          "TransactionID":{
                                           "F_Score":np.count_nonzero
                                           },
                          "DateDiff":{
                                      "R_Score":np.max
                                      }
                          }
                         )
    df_sums.reset_index(inplace=True)
    
    df_sums.columns = [x[0] if len(x[1])==0 else x[1] for x in df_sums.columns.values]
    
    
    print("\nGrouped data:\n {}".format(df_sums.head()))
    print("\nAll data:\n{}".format(df_all.head()))
    
    
    
    