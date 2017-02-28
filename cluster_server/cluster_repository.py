# -*- coding: utf-8 -*-
"""
Created on Tue Feb  7 09:40:53 2017
"""



from datetime import datetime as dt
import pandas as pd
import numpy as np
import json

__author__     = "Andrei Ionut DAMIAN"
__copyright__  = "Copyright 2007, HTSS"
__credits__    = ["Ionut Canavea","Ionut Muraru"]
__license__    = "GPL"
__version__    = "1.0.2"
__maintainer__ = "Andrei Ionut DAMIAN"
__email__      = "ionut.damian@htss.ro"
__status__     = "Production"
__library__    = "CLUSTER REPO"
__created__    = "2017-01-25"
__modified__   = "2017-02-23"

class ClusterRepository:
    def __init__(self):
        
        self.MODULE = "{} v{}".format(__library__,__version__)
        self._logger("INIT "+self.MODULE)
        
        self.FULL_DEBUG = False
        
        self._loadconfig()
        
        
        return
        
    def _logger(self, logstr, show = True):
        if not hasattr(self, 'log'):        
            self.log = list()
        nowtime = dt.now()
        strnowtime = nowtime.strftime("[CLREP][%Y-%m-%d %H:%M:%S] ")
        logstr = strnowtime + logstr
        self.log.append(logstr)
        if show:
            print(logstr, flush = True)
        return
        
    def UploadCluster(self,df, cluster_dict, sql_helper):
        
        sClusterName = cluster_dict['Name']
        sClusterObs = cluster_dict['Description']
        centroids = df['CentroidID'].unique()
        nCentroidNo = centroids.shape[0]
                      
        nCustomerNo = df.shape[0] 
        #nClusterDate =  
        sClusterAuthor = 'Andrei Ionut DAMIAN'
        sClusterAlgorithm = cluster_dict['Algorithm']
        sF1_Obs = cluster_dict['Fields'][0]
        sF2_Obs = cluster_dict['Fields'][1]
        sF3_Obs = cluster_dict['Fields'][2]
        sF4_Obs = cluster_dict['Fields'][3]
        sF5_Obs = cluster_dict['Fields'][4]
        df_cluster = df
        
        sClusterGrade = 'TESTS'

        df_desc = pd.DataFrame()
        
        df_desc['NewID'] =  df['CentroidID']
        df_desc['Segment'] =  df['Segment']
        
        df_desc.drop_duplicates(inplace = True)
        
        df_desc['Scor'] = np.repeat(0,df_desc.shape[0]) 
        df_desc['Clienti'] = np.repeat(0,df_desc.shape[0]) 
       
        self.UploadClusterDetail(sClusterName, sClusterObs, sClusterGrade,
                                 nCentroidNo, nCustomerNo, 
                                 sClusterAuthor, sClusterAlgorithm,
                                 sF1_Obs, sF2_Obs, sF3_Obs, sF4_Obs, sF5_Obs, 
                                 df_cluster,
                                 df_desc,
                                 cluster_dict,
                                 sql_helper)
        
        return
        
        

    def UploadClusterDetail(self, sClusterName, sClusterObs, sClusterGrade,
                            nCentroidNo,
                            nCustomerNo, sClusterAuthor,
                            sClusterAlgorithm,
                            sF1_Obs, sF2_Obs, sF3_Obs, sF4_Obs, sF5_Obs, 
                            df_cluster,
                            df_desc,
                            cluster_config_dict,
                            sql_helper):
        
        cConfig = cluster_config_dict
        nowtime = dt.now()
        strDay = nowtime.strftime("%Y%m%d")
        sYear = nowtime.strftime("%Y")[-2:]
        strID = sYear + nowtime.strftime("%m%d%H%M%S")
        cID = int(strID)
        
        
        s_ClusterID = str(cID)
        s_ClusterName = sClusterName
        s_ClusterObs = sClusterObs
        s_nCentroidNo = str(nCentroidNo)
        s_nCustomerNo = str(nCustomerNo)
        s_nDay = strDay
        s_ClusterGrade = sClusterGrade

        self._logger("Creating new DB cluster: {}...".format(s_ClusterName))
        
        str_UploadCluster = """
                                INSERT INTO [dbo].[Clusters]
                                   (
                                   [ClusterID]
                                   ,[ClusterName]
                                   ,[ClusterObs]
                                   ,[ClusterGrade]
                                   ,[CentroidNo]
                                   ,[CustomerNo]
                                   ,[ClusterDate]
                                   ,[ClusterAuthor]
                                   ,[ClusterAlgorithm]
                                   ,[F1_Obs]
                                   ,[F2_Obs]
                                   ,[F3_Obs]
                                   ,[F4_Obs]
                                   ,[F5_Obs]
                                   )
                                 VALUES
                                   (
                                   """ +s_ClusterID+""",
                                   '"""+s_ClusterName+"""',
                                   '"""+s_ClusterObs+"""',
                                   '"""+s_ClusterGrade+"""',
                                   """ +s_nCentroidNo+""",
                                   """ +s_nCustomerNo+""",
                                   """ +s_nDay+""",
                                   '"""+sClusterAuthor+"""',
                                   '"""+sClusterAlgorithm+"""',
                                   '"""+sF1_Obs+"""',
                                   '"""+sF2_Obs+"""',
                                   '"""+sF3_Obs+"""',
                                   '"""+sF4_Obs+"""',
                                   '"""+sF5_Obs+"""'
                                   )
                            """
        sql_helper.ExecInsert(str_UploadCluster)
        
        self._logger("Done creating new DB cluster.")


        
        df = pd.DataFrame()
        df_desc.reset_index(drop = True, inplace = True)
        nr_recs_seg = df_desc.shape[0]
        df['ClusterID']    = np.repeat(cID,nr_recs_seg)
        df['AssignmentID'] = df_desc["NewID"]
        df['AssignmentLabel'] = df_desc["Segment"]
        df['AssignmentDescr'] = "Total "+df_desc["Clienti"].astype(str)+" clienti cu scor "+df_desc["Scor"].astype(str)
        df['AssignmentNoClients'] = df_desc["Clienti"]
        df['AssignmentScore'] = df_desc["Scor"]
        self._logger("Saving segments structure in DB...\n{}".format(df))
        sql_helper.SaveTable(df,'Cluster_Assignments')
        self._logger("Done saving segments structure in DB.")
        
        
        self._logger("Saving cluster lines in DB...")
        save_df = pd.DataFrame()
        nr_recs = df_cluster.shape[0]
        save_df['ClusterID'] = np.repeat(cID,nr_recs)
        save_df['CustomerID']= df_cluster[cConfig["ID"]]
        save_df['AssignmentID']= df_cluster["CentroidID"]
        nr_fields = len(cConfig["Fields"])
        for i in range(nr_fields):
            fld_src = cConfig["Fields"][i]
            if fld_src != '':
                fld_dst = "F"+str(i+1)
                save_df[fld_dst] = df_cluster[fld_dst] #df_cluster[fld_src]
        
        sql_helper.SaveTable(save_df,'Clusters_Lines')
        self._logger("Done saving cluster lines in DB.")       
        

        return
        
    def _loadconfig(self, config_file = "cluster_config.txt"):
        self._logger("Loading config '{}'...".format(config_file))
        cfg_file = open(config_file)
        self.config_data = json.load(cfg_file)
        if self.FULL_DEBUG:
            self._logger(self.config_data)
        self.cluster_config_list = list()
        for cluster_key in self.config_data.keys():
            cluster_def = self.config_data[cluster_key]
            cluster_fields = cluster_def["Fields"]
            cluster_ID = cluster_def["ID"]
            cluster_name = cluster_def["Name"]       
            if self.FULL_DEBUG:
                self._logger("Loading definition for cluster: {}".format(cluster_key))
                self._logger("  ID    : {}".format(cluster_ID))
                self._logger("  Name  : {}".format(cluster_name))
                self._logger("  Fields: {}".format(cluster_fields))
                
            cluster_def["DATA"] = None
            self.cluster_config_list.append(cluster_def)
    
        return    
        
    def _config_by_id(self,sID):
                
        return self.config_data[sID]
