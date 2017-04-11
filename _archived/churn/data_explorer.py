# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 21:25:35 2017

@author: Andrei Ionut DAMIAN
@project: HyperLoop
"""

import pandas as pd
import numpy as np
from sql_helper import MSSQLHelper

from sklearn.cluster import KMeans
from sklearn import preprocessing
#from cluster_repository import ClusterRepository

from time import time as tm

if __name__=="__main__":
    
    pd.set_option('expand_frame_repr', False)
    pd.options.display.float_format = "{:.3f}".format
    
    sql = MSSQLHelper()
    
    p1 = '20150101'
    p2 = '20151231'
    p3 = '1048608'
    FORCE_LOAD = False
    
    strqry = """
            exec 
                [uspGetPartnerAmountsByPeriod_CHURNED] 
                '"""+p1+"""',
                '"""+p2+"""',
                """+str(p3)+"""
            """
    t0=tm()
    if FORCE_LOAD or (not ("dfinit" in locals())):
        dfinit = sql.Select(strqry)     
    t1=tm()
    
    print("Downloaded in {:.2f}min".format((t1-t0)/60))
    
    #df = pd.read_csv('saved_data/20170202_1622_rfm_data_clusters.csv')    
    #clRep  = ClusterRepository()    
    #clConfig = clRep._config_by_id('1')
    #clRep.UploadCluster(df,clConfig, sql)

    
    final_fields = ['PartnerId','TotalAmount','TranCount','NetMargin','MaxDateIndex']
    scale_fields = ["TotalAmount","TranCount", "MaxDateIndex","NetMargin"]
    new_fields =   ["Revs","Tran","Recy","Marg"]
    
    dfinit = dfinit[dfinit.PartnerId!=-1]
    df = pd.DataFrame()
    df['PartnerId'] = dfinit.PartnerId
    df['CHURN'] = dfinit.CHURNED
    
    #minmaxScaler = preprocessing.MinMaxScaler()
    #scaled  = minmaxScaler.fit_transform(df[scale_fields])
    
    for i in range(len(new_fields)):
        fld_src = scale_fields[i]
        fld_dst = new_fields[i]
        attr_vec = np.array(dfinit[fld_src])
        zero_indexes = attr_vec<=0
        print("Fld {} {} zeros".format(fld_src,zero_indexes.sum()))
        attr_vec[zero_indexes] = 0.0001
        attr_vec = np.log(attr_vec)
        min_val = attr_vec.min()
        max_val = attr_vec.max()
        attr_vec = (attr_vec - min_val) / (max_val)
        df[fld_dst] = attr_vec
    