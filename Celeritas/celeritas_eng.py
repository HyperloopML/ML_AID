# -*- coding: utf-8 -*-
"""
Created on Wed Apr 12 08:42:09 2017

@author: Andrei Ionut DAMIAN
"""

import pandas as pd
import numpy as np
import json
import time as tm

import os
from sql_helper import MSSQLHelper
from datetime import datetime as dt

import tensorflow as tf


__author__     = "Andrei Ionut DAMIAN"
__copyright__  = "Copyright 2017, HTSS"
__credits__    = ["Alex Purdila","Ionut Canavea","Ionut Muraru"]
__version__    = "0.0.1"
__maintainer__ = "Andrei Ionut DAMIAN"
__email__      = "ionut.damian@htss.ro"
__status__     = "R&D"
__library__    = "HYPERLOOP CELERITAS ENGINE"
__created__    = "2017-04-11"
__modified__   = "2017-04-12"
__lib__        = "CELERS"



class CeleritasEngine:
    def __init__(self):
        self.FULL_DEBUG = False
        pd.options.display.float_format = '{:,.3f}'.format
        pd.set_option('expand_frame_repr', False)

        self.MODULE = "{} v{}".format(__library__,__version__)
        self._logger("INIT "+self.MODULE)

        self.s_prefix = dt.strftime(dt.now(),'%Y%m%d')
        self.s_prefix+= "_"
        self.s_prefix+=dt.strftime(dt.now(),'%H%M')
        self.s_prefix+= "_"
        self.save_folder = "saved_data/"
        if not os.path.exists(self.save_folder):
            os.makedirs(self.save_folder)
        self.out_file = self.save_folder + self.s_prefix + __lib__+"_data.csv"
        self.log_file = self.save_folder + self.s_prefix + __lib__+"_log.txt"

        if self.FULL_DEBUG:
            self._logger(self.s_prefix)
            self._logger("__name__: {}".format(__name__))
            self._logger("__file__: {}".format(__file__))
        
        self.PredictorList = list()
        
        self.TESTING = True
        
        self.GPU_PRESENT = self.has_gpu()
        
        self.USE_TF = True
        
        self.CUST_FIELD = "MicroSegmendId"
        self.PROD_ID_FIELD = "ItemId"
        self.PROD_NAME_FIELD = "ItemName"
        self._load_config()
        
        self.sql_eng = MSSQLHelper(parent_log = self)
        
        return


    def has_gpu(self):
        from tensorflow.python.client import device_lib
        local_device_protos = device_lib.list_local_devices()
        types = [x.device_type for x in local_device_protos]
        self.GPU_NAME
        nr_gpu = sum([1 for x in types if x=='GPU'])
        if 'GPU' in types:
            gpu_names = [x.name for x in local_device_protos 
                                                if x.device_type=="GPU"]
            gpu_descr = [x.physical_device_desc for x in local_device_protos 
                                                if x.device_type=="GPU"]
            self.GPU_NAME = gpu_names[0]
            self.GPU_DESC = gpu_descr[0]
            self._logger("TensorFlow HAS GPUs={} Device:{} {}".format(
                    nr_gpu,
                    self.GPU_NAME,
                    self.GPU_DESC))            
        else:
            self._logger("CPU ONLY TensorFlow")
        return nr_gpu
    
    def _start_timer(self):
        self.t0 = tm.time()
        return

    def _stop_timer(self):
        self.t1 = tm.time()
        return self.t1-self.t0

    def _logger(self, logstr, show = True):
        if not hasattr(self, 'log'):        
            self.log = list()
        nowtime = dt.now()
        strnowtime = nowtime.strftime("[{}][%Y-%m-%d %H:%M:%S] ".format(__lib__))
        logstr = strnowtime + logstr
        self.log.append(logstr)
        if show:
            print(logstr, flush = True)
        try:
            log_output = open(self.log_file, 'w')
            for log_item in self.log:
              log_output.write("%s\n" % log_item)
            log_output.close()
        except:
            print(strnowtime+"Log write error !", flush = True)
        return
    
    
    def _get_recomm(self, df_cust_matrix,df_prod, pred_list = None):
        """ compute score matrix based on cust coefs and prod dataframes"""
        if pred_list != None:
            self.PredictorList = pred_list
        
        if len(self.PredictorList)==0:
            exclude_fields  = [self.CUST_FIELD, self.PROD_ID_FIELD, self.PROD_NAME_FIELD]
            all_fields = df_prod.columns            
            self.PredictorList = list(set(all_fields)-set(exclude_fields))
            

        cust_list = list(df_cust_matrix[self.CUST_FIELD])
        
        np_cust = np.array(df_cust_matrix[self.PredictorList])    
        np_prod = np.array(df_prod[self.PredictorList])
        np_cust_t = np_cust.T
        if self.TESTING:
            ##
            ## run both std numpy and TF computation
            ##          
            # first run numpy
            self._start_timer()
            np_scores = np_prod.dot(np_cust_t)
            np_time = self._stop_timer()
            self._logger("Numpy time: {:.2}s".format(np_time))
            
        #now run TF
        self._start_timer()
        tf_cust_t = tf.constant(np_cust_t, shape = np_cust_t.shape, name = "CustT")
        tf_prod = tf.constant(np_prod, shape = np_prod.shape, name = "Prod")
        tf_scores_tensor = tf.matmul(tf_prod,tf_cust_t)
        
        sess = tf.Session(config=tf.ConfigProto(log_device_placement=True))                
        tf_scores = sess.run(tf_scores_tensor)
        
        tf_time = self._stop_timer()
        
        self._logger("TF    time: {:.2}s".format(tf_time))
        
        if self.USE_TF:
            df_res = pd.DataFrame(tf_scores)
        else:
            df_res = pd.DataFrame(np_scores)
        
        df_res.columns = cust_list
        df_res[self.PROD_ID_FIELD] = df_prod[self.PROD_ID_FIELD]
        df_res[self.PROD_NAME_FIELD] = df_prod[self.PROD_NAME_FIELD]
            
        return df_res
    
    def _load_config(self, str_file = 'data_config.txt'):
        
        cfg_file = open(str_file)
        config_data = json.load(cfg_file)
        cfg_file.close()
        self.SQL_MATRIX         = config_data["COMPUTED_BEHAVIOR_MATRIX_SQL"]
        self.SQL_PRODUCTS       = config_data["PRODUCT_FEATURE_MATRIX"]
        self.CUST_FIELD         = config_data["CUSTOMER_ID_FIELD"]
        self.PROD_ID_FIELD      = config_data["PRODUCT_ID_FIELD"]
        self.PROD_NAME_FIELD    = config_data["PRODUCT_NAME_FIELD"]        
        
        return

    def QuickComputeScores(self):
        
        self._load_config()
        df_cust = self.sql_eng.Select(self.SQL_MATRIX)
        df_prod = self.sql_eng.Select(self.SQL_PRODUCTS)
        df = self._get_recomm(df_cust,df_prod)
        df.to_csv(self.out_file)
        return

    def _load_trans(self, str_sql):
        """ load in-class transaction dataframe"""
        return
    
    def _load_prods(self, str_sql):
        """ load in-class products dataframe"""
        return
    
    def CalculateMatrix(self, df_trans):
        """ calculate user coeficients based on transaction matrix """
        
        
        return


if __name__ == "__main__":
    """ test code """
    
    eng = CeleritasEngine()
    
    eng.QuickComputeScores()