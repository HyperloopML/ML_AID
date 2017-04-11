# -*- coding: utf-8 -*-
"""
Created on Tue Dec 06 23:52:28 2016

@author: Andrei Ionut DAMIAN, 4E SOFTWARE SRL
"""

import pandas as pd


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

    # convert Date to string
    df_all['Date'] = df_all['Date'].astype(str)
                        
    group_cols = ['CustomerID','Age','Sex','State']
    
    groupby = df_all.groupby(group_cols)
    
    df_sums = groupby['OrderTotal'].sum().to_frame()
    df_sums = df_sums.sort_values('OrderTotal')



        
