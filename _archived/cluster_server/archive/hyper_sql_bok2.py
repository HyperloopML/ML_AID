# -*- coding: utf-8 -*-
"""
Created on Wed Dec  7 12:01:34 2016

@author: slash_000
"""


import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.tree import DecisionTreeClassifier
from sklearn.tree import export_graphviz
from sklearn import preprocessing

from bokeh.io import output_file, show
from bokeh.plotting import figure
from bokeh.plotting import ColumnDataSource
from bokeh.layouts import column, row 
from bokeh.layouts import gridplot

from bokeh.charts import Scatter


from bokeh.models import CategoricalColorMapper

import pydotplus


import os

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
    cluster_fields = ['R_Score','F_Score','M_Score']
    
    df_model = df_rfm.loc[:,origin_fields]
    
    # log transoft total value
    df_model[cf3] = np.log(df_model[cf3])
    
    if True:
        np_arr = preprocessing.MinMaxScaler().fit_transform(df_model)
    else:
        np_arr = preprocessing.scale(df_model)
    
    df_model = pd.DataFrame(data = np_arr, columns = cluster_fields )
    
    clf = KMeans(n_clusters = nr_cl, n_jobs=-1)
    clf.fit(df_model)
    
    df_model[cfc]= clf.labels_

    
    if False:
        legends= ['AVERAGE','GOOD','VIP','LOW']    
        for i in range(len(legends)):
            df_model[cfc].replace(i,legends[i],inplace=True)
        
    labels = list(np.unique(df_model[cfc]))


    df_model[cID] = df_rfm[cID]
    
    for i in range(len(cluster_fields)):
        df_rfm[cluster_fields[i]] = df_model[cluster_fields[i]]
    
    df_rfm[cfc] = df_model[cfc]
    
    df_rfm.to_csv('rfm_data_clusters.csv')
    
    html_output = os.path.basename(__file__+'.html')
    output_file(html_output)    
    
    nr_plots =3
    fff = 0
    plot_list = list()
    for i in range(nr_plots):
        fi1 = fff
        fi2 = fff + 1
        if fi2 >= len(cluster_fields):
            fi2 = 0
            fff = 0
        fff += 1
        fldX = cluster_fields[fi1]
        fldY = cluster_fields[fi2]
        sTitle = 'Graficul RFM nr. {} cu {} vs. {} pentru {} clustere'.format(i,fldX,
                                                                        fldY,nr_cl)
        p = Scatter(df_model, x = fldX, y = fldY, 
                    tools = 'box_select',
                    color = cfc,
                    marker = cfc,
                    title = sTitle)

        plot_list.append(p)

    if (len(plot_list) % 2) != 0:
        plot_list.append(None)
    
    grid_plots = list()
    k = 0
    for i in range(round(nr_plots / 2)):
        row_plots = list()
        for j in range(2):
            row_plots.append(plot_list[k])
            k += 1
        grid_plots.append(row_plots)
        
    layout = gridplot(grid_plots)

    show(layout)    
    
    
    
    clf_tree = DecisionTreeClassifier(max_depth = 4)
    clf_tree.fit(df_model.loc[:,cluster_fields],df_model[cfc])
    
    dot_data = export_graphviz(clf_tree, out_file='rfm_tree.dot', 
                         feature_names=cluster_fields,  
                         #class_names=labels,  
                         filled=True, rounded=True,  
                         special_characters=True)  
    #graph = pydotplus.graph_from_dot_data(dot_data)  
    #png = graph.create_png()
    

    
    
    
    