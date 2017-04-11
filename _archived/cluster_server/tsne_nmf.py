# -*- coding: utf-8 -*-
"""
Created on Sat Mar 11 22:32:10 2017

@author: Andrei
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sklearn.cluster import KMeans
from sklearn.manifold import TSNE

df = pd.read_csv('test.csv')

n_centers = 5
n_downsample = 100
column_list = ["R","F","M"]
centroid_labels = ["c_R","c_F","c_M"]
labels = ['cluster','subcluster']

km = KMeans(n_clusters = n_centers)
print("Main fit ...", flush = True)
km.fit(df[column_list])
df['cluster'] = km.labels_

for cluster_no in range(len(np.unique(km.labels_))):
    print("Subcluster {} fit ...".format(cluster_no), flush = True)
    kms = KMeans(n_clusters=n_downsample, n_init=2)
    kms.fit(df.loc[df[labels[0]]==cluster_no,column_list])
    df.loc[df[labels[0]]==cluster_no,labels[1]] = kms.labels_

plt.figure()
plt.scatter(df.R,df.M, c=df[labels[0]])
plt.show()

gby = df[column_list+labels].groupby(labels)
df_down = gby[column_list].mean().reset_index()

plt.figure()
plt.scatter(df_down.R,df_down.M, c=df_down[labels[0]])
plt.show()


print("TSNE DF...", flush = True)
tsne = TSNE(verbose = 1000,
            early_exaggeration = 10, #separate well the clusters
            n_iter = 250)

res = tsne.fit_transform(df[column_list])

plt.figure()
plt.scatter(res[:,0],res[:,1], c=df[labels[0]])
plt.show()

print("TSNE DOWN DF...", flush = True)
tsne = TSNE(verbose = 1000,
            early_exaggeration = 10, #separate well the clusters
            n_iter = 1000)

res = tsne.fit_transform(df_down[column_list])

plt.figure()
plt.scatter(res[:,0],res[:,1], c=df_down[labels[0]])
plt.show()
