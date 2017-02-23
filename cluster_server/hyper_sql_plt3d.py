# -*- coding: utf-8 -*-
"""
Created on Wed Dec  7 12:01:34 2016

@author: slash_000
"""


import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn import preprocessing

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

from matplotlib import animation

import os

def animate(i):
    ax.view_init(elev=10., azim=i)

if __name__ == '__main__':
    
    df_rfm = pd.read_csv('rfm_data.csv')
    #df_rfm.sort_values('M_TotSum')
    
    cID = 'CustID'
    
    cf1 = 'RecencyScore'
    cf2 = 'NrTransactions'
    cf3 = 'TotalValue'
    cfc = 'Categ'
    nr_cl = 4
    

            
    origin_fields = [cf1,cf2,cf3]
    mf1 = 'R_Score'
    mf2 = 'F_Score'
    mf3 = 'M_Score'
    cluster_fields = [mf1,mf2,mf3]
    
    df_model = df_rfm.loc[:,origin_fields]
    
    # log transoft total value
    df_model[cf3] = np.log(df_model[cf3])
    
    np_arr = preprocessing.scale(df_model)
    df_model = pd.DataFrame(data = np_arr, columns = cluster_fields )
    
    clf = KMeans(n_clusters = nr_cl, n_jobs=-1)
    clf.fit(df_model)
    
    df_model[cfc]= clf.labels_
    
    
    #c = df_model[cfc]
    
    colors = ['red','green','blue','black'] ##,'yellow']
    
    if False:
        legends= ['AVERAGE','GOOD','VIP','LOW']    
        for i in range(len(legends)):
            df_model[cfc].replace(i,legends[i],inplace=True)
        
    labels = list(np.unique(df_model[cfc]))

    df_model[cID] = df_rfm[cID]
    
    df_model.to_csv('rfm_data_clusters.csv')
    
    
    fig = plt.figure(1, figsize=(10, 10))
    ax = Axes3D(fig, elev=-150, azim=110)
    ax.scatter(df_model.loc[:, mf1], df_model.loc[:, mf2], df_model.loc[:, mf3], 
               c=df_model[cfc],
               cmap=plt.cm.Set1)
    ax.set_title("Plot 3D RFM cu date preprocesate") 
    ax.set_xlabel(mf1)
    ax.w_xaxis.set_ticklabels([])
    ax.set_ylabel(mf2)
    ax.w_yaxis.set_ticklabels([])
    ax.set_zlabel(mf3)
    ax.w_zaxis.set_ticklabels([])
    
    plt.show()
    # Animate
    anim = animation.FuncAnimation(fig, animate,frames=360, interval=20, blit=False, repeat=True)

