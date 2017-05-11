# -*- coding: utf-8 -*-
"""
Created on Wed Apr 12 08:42:09 2017

@author: Andrei Ionut DAMIAN

@ToDo:
    
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
__version__    = "0.1.1"
__maintainer__ = "Andrei Ionut DAMIAN"
__email__      = "ionut.damian@htss.ro"
__status__     = "R&D"
__library__    = "HYPERLOOP CELERITAS ENGINE"
__created__    = "2017-04-11"
__modified__   = "2017-04-12"
__lib__        = "CELERS"



class CeleritasEngine:
    """ 
    Hyperloop Celeritas Recommendations Engine with tf/GPU backend
    """
    def __init__(self):
        self.FULL_DEBUG = False
        pd.options.display.float_format = '{:,.3f}'.format
        pd.set_option('expand_frame_repr', False)
        np.set_printoptions(precision = 3, suppress = True)


        self.MODULE = "{} v{}".format(__library__,__version__)
        self.s_prefix = dt.strftime(dt.now(),'%Y%m%d')
        self.s_prefix+= "_"
        self.s_prefix+=dt.strftime(dt.now(),'%H%M')
        self.s_prefix+= "_"
        self.cwd = os.getcwd()
        self.save_folder = os.path.join(self.cwd,"temp")
        self.log_file = os.path.join(self.save_folder, 
                             self.s_prefix + __lib__+"_log.txt")
        
        if not os.path.exists(self.save_folder):
            print("CREATED TEMP LOG FOLDER: {}".format(self.save_folder))
            os.makedirs(self.save_folder)
        else:
            print("TEMP LOG FOLDER: {}".format(self.save_folder))
        self.sql_eng = MSSQLHelper(parent_log = self)
        self.setup_folder()
        self._logger("Work folder: [{}]".format(self.save_folder))


        self._logger("INIT "+self.MODULE)

        if self.FULL_DEBUG:
            self._logger(self.s_prefix)
            self._logger("__name__: {}".format(__name__))
            self._logger("__file__: {}".format(__file__))
        
        self.PredictorList = list()
        
        self.TESTING = True
        
        self.GPU_PRESENT = self.has_gpu()
        
        if self.GPU_PRESENT:
            self.USE_TF = True
        
        self.CUST_FIELD = "MicroSegmendId"
        self.PROD_ID_FIELD = "ItemId"
        self.PROD_NAME_FIELD = "ItemName"
        self.TARGET_FIELD = "Count"
        
        self._load_config()
        
        
        # default hyperparameters
        self.USE_MOM_SGD = False # if False use Adam Optimizer in TF
        self.ALPHA = 0.001
        self.BETA = 0.1
                
        self.USE_NP_MOMENTUM = True
        self.mom_speed = 0.9
        self.momentum = 0
        
        self.UserData = {}
        
        self._df_prod = None
        
        return
    
    def setup_folder(self):
        self.s_prefix = dt.strftime(dt.now(),'%Y%m%d')
        self.s_prefix+= "_"
        self.s_prefix+=dt.strftime(dt.now(),'%H%M')
        self.s_prefix+= "_"
        self.save_folder = self.sql_eng.data_folder
        self.out_file = os.path.join(self.save_folder, 
                                     self.s_prefix + __lib__+"_result_data.csv")
        self.log_file = os.path.join(self.save_folder, 
                                     self.s_prefix + __lib__+"_log.txt")

        return


    def has_gpu(self):
        """ TensorFlow based GPU testing"""
        from tensorflow.python.client import device_lib
        local_device_protos = device_lib.list_local_devices()
        types = [x.device_type for x in local_device_protos]
        nr_gpu = sum([1 for x in types if x=='GPU'])
        for x in local_device_protos:
            self._logger("TensorFlow Devs: [{}][{}]".format(x.name[:15],
                                                 x.physical_device_desc[:35]))
        if 'GPU' in types:
            gpu_names = [x.name for x in local_device_protos 
                                                if x.device_type=="GPU"]
            gpu_descr = [x.physical_device_desc for x in local_device_protos 
                                                if x.device_type=="GPU"]
            self.GPU_NAME = gpu_names[0]
            self.GPU_DESC = gpu_descr[0]
            self._logger("TensorFlow HAS GPUs={} [{}][{}]".format(
                    nr_gpu,
                    self.GPU_NAME[:15],
                    self.GPU_DESC[:35]))            
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
        """ 
        log processing method 
        """
        if not hasattr(self, 'log'):        
            self.log = list()
        nowtime = dt.now()
        strnowtime = nowtime.strftime("[{}][%Y-%m-%d %H:%M:%S] ".format(__lib__))
        logstr = strnowtime + logstr
        self.log.append(logstr)
        if show:
            print(logstr, flush = True)
        try:
            if self.FULL_DEBUG:
                print(strnowtime+" Saving log to {}".format(self.log_file))
            log_output = open(self.log_file, 'w')
            for log_item in self.log:
              log_output.write("%s\n" % log_item)
            log_output.close()
        except:
            print(strnowtime+"Log write error !", flush = True)
        return

    def _setup_predictors(self, df):
        exclude_fields  = ([ self.CUST_FIELD, 
                             self.PROD_ID_FIELD, 
                             self.PROD_NAME_FIELD]
                           +self.DROP_FIELDS)
        if len(exclude_fields)==0:
            self._logger("WARNING: no fields exclude from training dataset")
        all_fields = df.columns            
        self.PredictorList = [x for x in all_fields if not(x in exclude_fields)]
        return

    
    def _get_recomm(self, df_cust_matrix,df_prod, pred_list = None):
        """ 
        Compute score matrix based on cust coefs and prod dataframes
        """
        if (df_cust_matrix is None) or (df_prod is None):
            self._logger("ERROR WITHIN INPUT DATA")
            return
        
        if not (pred_list is None):
            self.PredictorList = pred_list
        else:
            self._setup_predictors(df_prod)
            

        cust_list = list(df_cust_matrix[self.CUST_FIELD])
        
        np_cust = np.array(df_cust_matrix[self.PredictorList], dtype = float)    
        np_prod = np.array(df_prod[self.PredictorList], dtype = float)
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
        tf_cust_t = tf.constant(np_cust_t, dtype = tf.float32, 
                                shape = np_cust_t.shape, name = "CustT")
        tf_prod = tf.constant(np_prod,dtype = tf.float32, # cast to float32 int prod matrix
                              shape = np_prod.shape, name = "Prod")
        tf_scores_tensor = tf.matmul(tf_prod,tf_cust_t)
        
        sess = tf.Session(config=tf.ConfigProto(log_device_placement=True))                
        self._start_timer()
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
        """
        Load JSON configuration file
        """
        
        cfg_file = open(str_file)
        config_data = json.load(cfg_file)
        CUST_KEY = "COMPUTED_BEHAVIOR_MATRIX_SQL"
        PROD_KEY = "PRODUCT_FEATURE_MATRIX"
        CUID_KEY = "CUSTOMER_ID_FIELD"
        PRID_KEY = "PRODUCT_ID_FIELD"
        PRNM_KEY = "PRODUCT_NAME_FIELD"
        DROP_KEY = "DROPFIELDS"
        USRT_KEY = "USER_TRANSACTIONS"
        USRL_KEY = "USER_LIST"
        TARG_KEY = "TARGET_FIELD"
        
        if CUST_KEY in config_data.keys():
            self.SQL_MATRIX = config_data[CUST_KEY]
        else:
            self.SQL_MATRIX = ""
            
        if PROD_KEY in config_data.keys():    
            self.SQL_PRODUCTS = config_data[PROD_KEY]
        else:
            self.SQL_PRODUCTS = ""
        

        if CUID_KEY in config_data.keys():          
            self.CUST_FIELD = config_data[CUID_KEY]

            
        if PRID_KEY in config_data.keys():    
            self.PROD_ID_FIELD = config_data[PRID_KEY]

            
        if PRNM_KEY in config_data.keys():    
            self.PROD_NAME_FIELD = config_data[PRNM_KEY]    

            
        if DROP_KEY in config_data.keys():            
            self.DROP_FIELDS = config_data[DROP_KEY]
        else:
            self.DROP_FIELDS = []   
            
        if USRT_KEY in config_data.keys():
            self.SQL_USER_TRANS = config_data[USRT_KEY]
        else:
            self.SQL_USER_TRANS = ""

        if USRL_KEY in config_data.keys():
            self.SQL_USER_LIST = config_data[USRL_KEY]
        else:
            self.SQL_USER_LIST = ""
        
        if TARG_KEY in config_data.keys():
            self.TARGET_FIELD = config_data[TARG_KEY]
        
        return

    
    def _get_user_list(self):
        if self.SQL_USER_LIST != "":
            df_usr = self.sql_eng.Select(self.SQL_USER_LIST, caching = True)
        else:
            self._logger("ERROR: NO USER LIST AVAILABLE !!!")
        
        self.UserList = list(df_usr.iloc[:,0])
        return

    def _load_user_trans(self, str_userid):
        """ 
        Load in-class transaction dataframe for a certain user
        """
        df = self.sql_eng.Select(self.SQL_USER_TRANS+str_userid.__repr__(),
                                 caching = True,
                                 convert_ascii=[self.PROD_NAME_FIELD])
        return df
    
    def _load_prods(self, str_sql=''):
        """ 
        load in-class products dataframe
        """
        if not (self._df_prod is None):
            self._logger("Products allready loaded. Skipping.")
            return
        
        self._logger("Loading products attributes matrix...")
        if str_sql=='':
            self._df_prod = self.sql_eng.Select(self.SQL_PRODUCTS, caching = True,
                                                convert_ascii = [self.PROD_NAME_FIELD])
        else:
            self._df_prod = self.sql_eng.Select(str_sql, caching = True)
            
        return 
    
    def _train_user_vector(self
                           ,user_id
                           ,df_usrtrans
                           ,predictor_list
                           ,target_field
                           ,np_prev_vector = None 
                           ,batch_size = 128
                           ,epochs = 1
                           ):
        """
        Train user behavior vector based on transactions using either TF backend
        or standard Numpy. Algorithm used is implicit feedback content based 
        training
        
        Returns a Pandas dataframe with a single row
        
        np_vector is predefined behaviour vector 
        """
        beta = self.BETA
        alpha = self.ALPHA

        
        USE_VALIDATION = 0.2 # 0.0 for standard training
        
        # first run a shuffle
        df_usrtrans = df_usrtrans.sample(frac=1).reset_index(drop=True)
        
        nr_predictors = len(predictor_list)
        nr_obs = df_usrtrans.shape[0]
        np_usr_trans = np.array(df_usrtrans[predictor_list], dtype = float)
        np_usr_targets = np.array(df_usrtrans[target_field], dtype = float)
        
        
        if np_prev_vector is None:
            np_vector = np.zeros(shape=(nr_predictors+1,1), dtype = float)
        else:
            np_vector = np.array(np_prev_vector)
                    
        np_bias = np_vector[0]
        np_vector = np_vector[1:]

        if np_vector.ndim != 2:
            np_vector = np_vector.reshape(nr_predictors,1)
        
        if batch_size > nr_obs:
            batch_size = nr_obs
            if epochs == 1:
                epochs = 2

        steps = nr_obs / batch_size

        if np_vector.shape[0] != nr_predictors:
            self._logger("Error user vector and transaction feats do not corespond")
            raise Exception("np_vector != np_user_trans")

        if USE_VALIDATION>0:
            train_size = int(nr_obs*USE_VALIDATION)
            train_dataset = np_usr_trans[:-train_size,:]
            train_targets = np_usr_targets[:-train_size]
            valid_dataset = np_usr_trans[-train_size,:]
            valid_dataset = np_usr_trans[-train_size:]
        else:
            train_dataset = np_usr_trans
            train_targets = np_usr_targets
            valid_dataset = None 
            valid_dataset = None 
        
        # prepare TF graph
        graph = tf.Graph()
        with graph.as_default():
            tf_train_data = tf.placeholder(tf.float32, 
                                           shape=(batch_size,nr_predictors))
            tf_train_targets = tf.placeholder(tf.float32, shape=(batch_size,1))
            
            tf_weights = tf.Variable(np_vector, dtype = tf.float32, name = "Weights")
            tf_bias = tf.Variable(np_bias, dtype = tf.float32, name = "Bias")
            
            tf_yhat = tf.add(tf.matmul(tf_train_data,tf_weights),tf_bias)
            
            #loss = tf.reduce_mean(
            #            tf.squared_difference(tf_yhat,tf_train_targets)
            #            + beta * tf.nn.l2_loss(tf_weights)
            #            )
            tf_loss = tf.losses.mean_squared_error(tf_train_targets, tf_yhat)
            
            global_step = tf.Variable(0)
            learning_rate = tf.Variable(alpha)
            if self.USE_MOM_SGD:
                print("Using momentum SGD")
                learning_rate = tf.train.exponential_decay(learning_rate = alpha, # start
                                                           global_step = global_step,
                                                           #decay_steps = steps_until_decay, 
                                                           #decay_rate = rate_of_decay,
                                                           )
                optimizer = tf.train.MomentumOptimizer(learning_rate = learning_rate,
                                                       momentum = self.mom_speed)
            else:
                print("Using Adam SGD")
                optimizer = tf.train.AdamOptimizer() #learning_rate = learning_rate)
        
            tf_opt_oper = optimizer.minimize(loss = tf_loss, global_step = global_step)
            initializer = tf.global_variables_initializer()
            
            
        self._start_timer()
        # create object level list of optimization loss
        self.tflosslist = list()
        self.nplosslist = list()
        
        # now start TF based optimization
        with tf.Session(graph = graph) as session:
            session.run(initializer)
            for epoch in range(epochs):
                
                for step in range(steps):                   
                    # train batch
                    total_samples = train_dataset.shape[0]
                    soffs = (step * batch_size) % (total_samples - batch_size)
                    eoffs = soffs + batch_size
                    batch_data = train_dataset[soffs:eoffs,:]
                    batch_targets = train_targets[soffs:eoffs]
                    train_dict = {
                                  tf_train_data : batch_data, 
                                  tf_train_targets : batch_targets,
                                 }
                    
                    
                    opt_res, loss_res, batch_train_pred = session.run(
                                                [tf_opt_oper, tf_loss, tf_yhat],
                                                feed_dict=train_dict)
                    self.tflosslist.add(loss_res)
                    # done step
                #done epoch
            # done training
            f_time = self._stop_timer()
            self._logger("User/Micro: {} TF train-time: {:.2}s".format(
                                                user_id,
                                                f_time))
            np_b = tf_bias.eval()
            np_v = tf_weights.eval()
            np_tf_vector = np.c_[np_b,np_v]

        if not self.USE_TF or self.TESTING:
            # start stohastic gradient descent
            self._start_timer()
            np_vector = np.c_[np_bias, np_vector] # reconstruct weights
            train_dataset = np.c_[np.ones(nr_obs),train_dataset]
            for epoch in range(epochs):
                for step in range (steps):
                    total_samples = train_dataset.shape[0]
                    soffs = (step * batch_size) % (total_samples - batch_size)
                    eoffs = soffs + batch_size
                    batch_data = train_dataset[soffs:eoffs,:]
                    batch_targets = train_targets[soffs:eoffs]
                    m = batch_date.shape[0]
                    
                    y_hat = batch_data.dot(np_vector)
                    H = y_hat - batch_targets
                    np_loss = np.mean(H**2)
                    self.nplosslist.add(np_loss)
                    grad = (1.0/m) * batch_data.T.dot(H) 
                    if self.USE_NP_MOMENTUM:
                        self.momentum = self.momentum * self.mom_speed
                        self.momentum = self.momentum + grad
                    else:
                        self.momentum = grad
                    np_vector = np_vector - alpha * self.momentum         
                
                    
            f_nptime = self._stop_timer()        
            self._logger("User/Micro: {} NP train-time: {:.2}s".format(
                                                user_id,
                                                f_nptime))
            
            
        df_vector = pd.DataFrame(columns = (['BIAS'] + predictor_list))
        df_vector.loc[0,:] = np_tf_vector
        df_vector.loc[1,:] = np_vector
        df_vector[self.CUST_FIELD] = [str(user_id),str(user_id)]
                     
        # now return updated vector
        return df_vector
    
    
    def CalculateUserVector(self, user_id, df_usrtrans):
        """
        Calculate user behavior vector based on executed tranzactions
        """
        if len(self.PredictorList)==0:
            self._setup_predictors(df_usrtrans)

        df_user_vector = self._train_user_vector(
                           user_id
                           ,df_usrtrans
                           ,self.PredictorList
                           ,self.TARGET_FIELD
                           #,np_vector = None 
                           ,batch_size = 128
                           ,epochs = 1
                           )
        return df_user_vector
    
    def CalculateUsersBehaviorMatrix(self):
        """ 
        Calculate user coeficients based on transaction matrix 
        using stohastic gradient descent (small batches)
        for all users defined by the USER_LIST query
        """
        df_result = pd.DataFrame()
        self._get_user_list()
        for c_user in self.UserList:
            s_user = str(c_user)
            self._logger("Preparing Behavior Vector for user: {}".format(s_user))
            df_user_tran = self._load_user_trans(s_user)
            if df_user_tran.shape[0]>0:
                df_c_user = self.CalculateUserVector(c_user, df_user_tran)
                df_result = df_result.append(df_c_user)
        
        return

    def LoadData(self, user_list = None):
        """
        Loads all products features based on config file
        Loads all user transactions (based on user_list param) in a data dict
        """
        self._load_prods()
        if not (user_list is None):
            for user in user_list:
                self.UserData[user] = self._load_user_trans(user)
        
        return
    
    def CalcultateUsersVectors(self, user_list):
        """
        Calculates behavior vectors for the user_list (partial behavior matrix)
        """
        df_result = pd.DataFrame()
        self.LoadData(user_list)
        for user,data in self.UserData.items():
            self._logger("Training behavior vector for user %d (%d rows)".format(
                    user,
                    data.shape[0]))
            df_usr_vect = self.CalculateUserVector(user, data)
            df_result = df_result.append(df_usr_vect)
        
        return df_result
            
    def QuickComputeScores(self):
        """
        Compute scores based on configuration file
        SQL tables: Customer Behavior MATRIX and
        Products Properties
        """
        df_cust = self.sql_eng.Select(self.SQL_MATRIX, caching = True)
        self._load_prods()
        df = self._get_recomm(df_cust,self._df_prod)
        if not (df is None):
            self._logger("Saving ...{}".format(self.out_file[-50:]))
            df.to_csv(self.out_file)
        return
    
    
            


if __name__ == "__main__":
    """ test code """
    
    test_users_2016 = [153, 120, 2, 16, 63]
    test_users_2014 = [315, 200, 247, 2, 3]
    
    test_users = test_users_2014
    
    
    eng = CeleritasEngine()
    
    eng.CalcultateUsersVectors(test_users)

    
    #eng.QuickComputeScores()