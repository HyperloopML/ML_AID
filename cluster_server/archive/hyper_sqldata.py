# -*- coding: utf-8 -*-
"""
Created on Wed Dec  7 12:01:34 2016

@author: slash_000
"""


import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from sklearn import preprocessing

if __name__ == '__main__':
    
    df_rfm = pd.read_csv('rfm_data.csv')
    df_rfm.sort_values('M_TotVal')
    
    cluster_fields = ['R_Score','F_NrTr','M_TotVal']
    
    df_model = df_rfm.loc[:,cluster_fields]
    
    np_arr = preprocessing.scale(df_model)
    df_model = pd.DataFrame(data = np_arr, columns = cluster_fields )
    
    clf = KMeans(n_clusters = 4, n_jobs=-1)
    clf.fit(df_model)
    
    df_model['Categ']=clf.labels_
    print(df_model)
    
    fig = plt.figure(0, figsize=(10, 10))
    plt.clf()
    ax = Axes3D(fig, elev=48, azim=134)

    
    ax.scatter(df_model['R_Score'], 
               df_model['F_NrTr'], 
               df_model['M_TotVal'], 
               c=df_model['Categ'])
    
    ax.set_xlabel('R_Score')
    ax.set_ylabel('F_NrTr')
    ax.set_zlabel('M_TotVal')
    
    fig = plt.figure(1, figsize=(10, 10))
    
    plt.scatter(df_model['R_Score'], 
                df_model['F_NrTr'], 
                c=df_model['Categ'])
    plt.xlabel('R_Score')
    plt.ylabel('F_NrTr')
    
    plt.show()
    
    
    
    
    