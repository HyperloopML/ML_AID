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

output_folder = "c:\\GoogleDrive\\_hyperloop_data\\churn_v3"

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
  drp: 0 none, 1 only between hidden layers, 2 include drop before readout
  bn: 0 none, 1 after liniarity, 2 after activation 
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
    if bn==1:
      # add BN after liniarity if needed
      model.add(BatchNormalization())
      s_layers +="->B"
    model.add(Activation(activation = act))
    s_layers +="->A({})".format(act[:3])
    if bn==2:
      # add BN after liniarity if needed
      model.add(BatchNormalization())
      s_layers +="->B"
    
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
  s_layers += " [{}/{}]".format(opt,w[:3])
  global_logger.VerboseLog(s_layers)
  current_nn_design = s_layers
  if FULL_DEBUG:
    global_logger.LogKerasModel(model)
  
  return model



if __name__ == '__main__':
  new_params = ", @CUST_ID = NULL, @SGM_ID = 22"
  s_sql = "EXEC [SP_GET_CHURN] @TRAN_PER_ID = 2, @CHURN_PER_ID = 3, @SGM_ID=22" # + new_params
  sql_eng = MSSQLHelper(parent_log = global_logger)
  
  df = sql_eng.Select(s_sql)
  df.fillna(0, inplace = True)
  
  fields_overall = ["R", "F", "M"]
  
  NewFields =  ['SGM_NO', 'MICRO_SGM_ID', 'SEX', 'AGE', 'GFK_SGM', 'C_R', 'C_F', 'C_M', 
                'C_MARGIN', 'C_PHARMA', 'C_COSMETICE', 'C_DERMOCOSMETICE', 'C_BABY', 
                'C_NEASOCIATE', 'C_DR_HART', 'C_NUROFEN', 'C_APIVITA', 'C_AVENE', 'C_L_OCCITANE', 
                'C_VICHY', 'C_BIODERMA', 'C_LA_ROCHE_POSAY', 'C_L_ERBOLARIO', 'C_PARASINUS', 'C_TRUSSA', 
                'C_SORTIS', 'C_NESTLE', 'C_OXYANCE', 'C_TERTENSIF', 'C_ASPENTER', 'C_ALPHA', 'C_BATISTE', 
                'C_STREPSILS', 'C_CHAPTER', 'C_DR_ORGANIC', 'C_FARMEC', 'C_HERBOSOPHY', 'C_IVATHERM', 'C_KLORANE_BEBE', 
                'C_MELVITA', 'C_SPLAT', 'C_ZDROVITAL',
                'MARGIN']
  AllExtraFields = list(NewFields)
  AllExtraFields.extend(fields_overall)
  
  
  
  initial_fields = list(df.columns)
  global_logger.VerboseLog("Generating dummy variables...")
  df = pd.get_dummies(df, prefix_sep = "__")
  new_fields = list(df.columns)
  removed_fields = list(set(initial_fields)-set(new_fields))
  added_fields = list(set(new_fields) - set(initial_fields))
  AllExtraFields.extend(added_fields)
  global_logger.VerboseLog("Removed {} and added {}".format(removed_fields,
                                                                added_fields))
  
  global_logger.VerboseLog("Removing zero var columns...")
  zero_var_result = df.var()==0
  zero_var_cols = list(df.columns[zero_var_result.values])
  all_cols = list(df.columns)
  fields_selection = list(set(all_cols) - set(zero_var_cols))
  
  target = "CHURN"
  fields_nonpred = [target, "CUST_ID",]
  
  fields_selection = [x for x in fields_selection if x not in fields_nonpred]
  
  global_logger.VerboseLog("Removed {} zero-var: {}".format(len(zero_var_cols),zero_var_cols))
  global_logger.VerboseLog("Total {} preds: {}".format(len(fields_selection),fields_selection))
  
  f_sels = [#[x for x in fields_selection if x not in AllExtraFields],
            [x for x in fields_selection if x not in fields_overall],
            #fields_selection,
           ]


  ##
  ## GRID PARAMETERS
  ##
  optimizers = ['rmsprop',]# 'adam']
  init = ['he_uniform', ]  #'glorot_uniform', 
  epochs = [5] #, 10,]
  batches = [512, 256] #, 128, 512,]
  grid_layers = [
           #[98, 24],
           #[64, 16],
           #[46, 23],
           [256, 64, 16],
           [512, 256, 128, 64, 32, 16],
           #[100, 20],
           #[32, 16, 8],
           #[24, 12],
          ]
  grid_activ = ['elu',]# 'relu',]
  droputs = [1,]#2]
  batch_norm_opt = [0,]#1,2]
  ##
  ## DONE GRID PARAMETERS
  ##

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
                        n_jobs=1,
                        verbose = 100000)
    
    grid_result = grid.fit(X_full, y_full)
    # summarize results
    global_logger.VerboseLog("Best score: {} using {}".format(grid_result.best_score_, 
                                                        grid_result.best_params_))
    best_nn_clf = grid_result.best_estimator_
    global_logger.VerboseLog("Best NN design so far:")
    global_logger.LogKerasModel(best_nn_clf.model)
    best_clfs.append(best_nn_clf)
    best_params.append(grid_result.best_params_)
    means = grid_result.cv_results_['mean_test_score']
    stds = grid_result.cv_results_['std_test_score']
    params = grid_result.cv_results_['params']
    df_partials = pd.DataFrame({
                                "mean_sc":means,
                                "std":stds,
                                "layout":["{}".format(x["hid"]) for x in params],
                                "opt":["{}".format(x["opt"]) for x in params],
                                "w_init":["{}".format(x["w"]) for x in params],
                                "dropout":["{}".format(x["drp"]) for x in params],
                                "BN":["{}".format(x["bn"]) for x in params],
                                "activ":["{}".format(x["act"]) for x in params],
                                "nr_inp":["{}".format(x["nri"]) for x in params],
                                "batch":["{}".format(x["batch_size"]) for x in params],
                                })
    
    df_results = df_results.append(df_partials)
    global_logger.SaveDataframe(df_results, fn='py_cv_res')
    # end for each field selection tests
  
  pd.set_option('display.max_rows', 1000)
  pd.set_option('display.max_columns', 500)
  pd.set_option('display.width', 1000)
  global_logger.VerboseLog("\nFinal results:\n{}".format(df_results.sort_values(by="mean_sc")))
  
  ChurnThreshold = 0.4
  
  for fields_list, c_best_params,c_best_clf in zip(f_sels, best_params,best_clfs):
    global_logger.VerboseLog("Full pred for {} fields w best: {}".format(len(fields_list),c_best_params))
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
