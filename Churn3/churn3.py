# -*- coding: utf-8 -*-
"""
Created on Thu Aug 10 10:15:40 2017

@author: Andrei
"""

from keras.layers import Dense, Dropout, BatchNormalization, Activation
from keras.models import Sequential
from keras.wrappers.scikit_learn import KerasClassifier

from sklearn.metrics import confusion_matrix

from sklearn.metrics import recall_score, make_scorer

from sklearn.model_selection import GridSearchCV, train_test_split

from logger import Logger

from sql_azure_helper import MSSQLHelper

import numpy as np
import pandas as pd

output_folder = "D:\\GoogleDrive\\_hyperloop_data\\churn_v3"

global_logger = Logger(lib_name = "CHRN3", base_folder = output_folder)

current_nn_design = ""
best_overall_score = 0

TOTAL_ITERATIONS = 0
CURRENT_ITERATION = 0
FULL_DEBUG = False

def _my_binary_recall_scorer(clf, X_val, y_true_val):  
    # do all the work and return some of the metrics
    global best_overall_score

    y_pred_val = clf.predict(X_val)
    
    TP = np.sum(y_true_val.ravel().dot(y_pred_val))
    CP = np.sum(y_true_val)
    
    recall = TP/CP
    global_logger.VerboseLog("  Current recall score: {:.3f}".format(recall))
    if recall > best_overall_score:
      best_overall_score = recall
      global_logger.VerboseLog("\nBest so far {:.3f} for {}".format(best_overall_score,
                               current_nn_design))

    return recall


  
def get_nn_model(opt = 'adam',
                 w = 'glorot_uniform',
                 act = 'relu',
                 bn = True,
                 drp = 1,
                 nri = 8,
                 hid  = []):
  """
  returns a keras model based on input dataframe
  """   
  global CURRENT_ITERATION
  global current_nn_design
  assert len(hid)>=1, "At least one hidden and output is required"
  model = Sequential()
  CURRENT_ITERATION += 1
  s_layers ="{}/{} I({})".format(CURRENT_ITERATION, TOTAL_ITERATIONS,
                                 nri)

  for i in range(len(hid)):
    nr_hid = hid[i]
    if drp>=1 and i>0:
      # add dropout if needed between hidden layers
      model.add(Dropout(rate=0.5))
      s_layers +="->D"
    if i==0:
      # first layer has input_dim
      model.add(Dense(units = nr_hid, kernel_initializer=w, 
                      input_dim=nri))
    else:
      model.add(Dense(units = nr_hid, kernel_initializer=w))
    s_layers +="->F({})".format(nr_hid)
    if bn:
      # add BN if needed
      model.add(BatchNormalization())
      s_layers +="->B"
    model.add(Activation(activation = act))
    s_layers +="->A({})".format(act)
    
  if drp>=2:
    # add dropout before readout if needed
    model.add(Dropout(0.5))
    s_layers +="->D"
  
  model.add(Dense(units = 1, kernel_initializer=w, activation='sigmoid'))
  s_layers +="->F(1)"
  
  model.compile(optimizer=opt, 
                loss="binary_crossentropy", 
                metrics = ["accuracy"]
                )  
  s_layers += " [{}]".format(opt)
  global_logger.VerboseLog(s_layers)
  current_nn_design = s_layers
  if FULL_DEBUG:
    global_logger.LogKerasModel(model)
  
  return model



if __name__ == '__main__':
  
  s_sql = "EXEC [SP_GET_CHURN] @TRAN_PER_ID = 2, @CHURN_PER_ID = 3"
  sql_eng = MSSQLHelper(parent_log = global_logger)
  
  df = sql_eng.Select(s_sql)
  df.fillna(0, inplace = True)
  
  fields_overall = ["R", "F", "M"]
  
  ##
  ## GRID PARAMETERS
  ##
  optimizers = ['rmsprop', 'adam']
  init = ['glorot_uniform', ]#'he_uniform'] 
  epochs = [2] #, 10,]
  batches = [512] #, 128, 512,]
  grid_layers = [
           [98, 24],
           #[64, 16],
           #[128, 32],
           #[32, 16, 8],
           #[24, 12],
          ]
  grid_activ = ['elu'] # 'relu',]
  droputs = [1] #,2]
  batch_norm_opt = [False,]# True]
  ##
  ## DONE GRID PARAMETERS
  ##
  
  
  fields_selection = list(df.columns[(df.var()>0).values])
  fields_zerovar = [x for x in list(df.columns) if x not in fields_selection ]
  
  target = "CHURN"
  fields_nonpred = [target, "CUST_ID",]
  
  fields_selection = [x for x in fields_selection if x not in fields_nonpred]
  
  global_logger.VerboseLog("Removed {} zero-var: {}".format(len(fields_zerovar),fields_zerovar))
  global_logger.VerboseLog("Total {} preds: {}".format(len(fields_selection),fields_selection))
  
  f_sels = [[x for x in fields_selection if x not in fields_overall],
            fields_selection,
           ]

  param_grid = dict(opt=optimizers, #
                    epochs=epochs, #
                    batch_size=batches, #
                    w=init, #
                    act = grid_activ, #
                    hid = grid_layers, #
                    drp = droputs,
                    bn = batch_norm_opt,
                    verbose = [1])
  
  TOTAL_ITERATIONS = np.prod([len(x) for x in param_grid.values()])
  
  TOTAL_ITERATIONS *= 3*2 + len(f_sels) # 3 CV folds and 2 iterations for pred subsets

  df_results = pd.DataFrame()
  
  best_clfs = []
  best_params = []
  
  for fields_list in f_sels:
    global_logger.VerboseLog("\n GridSearch for {} predictors".format(
                                  len(fields_list)))
    X_full = np.array(df[fields_list])
    y_full = np.array(df[target])
    X_train,X_test,y_train,y_test = train_test_split(X_full,y_full, 
                                                     test_size = 0.15)
    
    nr_dim = X_train.shape[1]  
    
    param_grid['nri'] = [nr_dim]
    
    
    
    global_logger.VerboseLog("Grid dict: {}".format(param_grid))
    
    keras_clf = KerasClassifier(build_fn=get_nn_model, 
                                verbose=10000)
  
    clf_scoring = make_scorer(recall_score, greater_is_better=True)
    
    grid = GridSearchCV(estimator=keras_clf, 
                        param_grid=param_grid,
                        scoring = _my_binary_recall_scorer, #clf_scoring,
                        #n_jobs=-1,
                        verbose = 100000)
    
    grid_result = grid.fit(X_full, y_full)
    # summarize results
    global_logger.VerboseLog("Best score: {} using {}".format(grid_result.best_score_, 
                                                        grid_result.best_params_))
    best_nn_clf = grid_result.best_estimator_
    global_logger.VerboseLog("Best NN design so far:")
    best_nn_clf.model.summary()
    best_clfs.append(best_nn_clf)
    best_params.append(grid_result.best_params_)
    means = grid_result.cv_results_['mean_test_score']
    stds = grid_result.cv_results_['std_test_score']
    params = grid_result.cv_results_['params']
    df_partials = pd.DataFrame({
                                "mean_sc":means,
                                "std":stds,
                                "layout":["{}".format(x["hidden_layers"]) for x in params],
                                "opt":["{}".format(x["optimizer"]) for x in params],
                                "w_init":["{}".format(x["weight_init"]) for x in params],
                                "dropout":["{}".format(x["dropout"]) for x in params],
                                "BN":["{}".format(x["batch_norm"]) for x in params],
                                "activ":["{}".format(x["activation"]) for x in params],
                                "nr_inp":["{}".format(x["nr_inputs"]) for x in params],
                                })
    
    df_results = df_results.append(df_partials)
  
  pd.set_option('display.max_rows', 1000)
  pd.set_option('display.max_columns', 500)
  pd.set_option('display.width', 1000)
  global_logger.VerboseLog("\nFinal results:\n{}".format(df_results))
  
  ChurnThreshold = 0.4
  
  for fields_list, c_best_params,c_best_clf in zip(f_sels, best_params,best_clfs):
    global_logger.VerboseLog("Full pred prob w best: {}".format(c_best_params))
    X_full = np.array(df[fields_list])
    y_full = np.array(df[target])
    y_probs = c_best_clf.predict_proba(X_full)
    churn_column = np.argmax(c_best_clf.classes_)
    y_preds = (y_probs[:,churn_column] >= ChurnThreshold).astype(int)
    global_logger.VerboseLog("ConMat with ChurnThreshold: {:.2f}".format(ChurnThreshold))
    cnf = confusion_matrix(y_full, y_preds)
    global_logger.LogConfusionMatrix(cnf)
    global_logger.PlotConfusionMatrix(cnf)
    TP = np.sum(y_full.ravel().dot(y_preds.ravel()))
    CP = np.sum(y_full)
    PP = np.sum(y_preds)
    recall = TP / CP
    precision = TP / PP
    global_logger.VerboseLog("  Recall: {}".format(recall))
    global_logger.VerboseLog("  Precision: {}".format(precision))
