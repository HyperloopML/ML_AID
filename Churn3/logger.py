# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 15:21:30 2017

@module:  Utility

@description: 
    utility module

"""

from datetime import datetime as dt
import matplotlib.pyplot as plt
import sys
import os
import socket

from scipy.misc import imsave

from io import TextIOWrapper, BytesIO
import numpy as np
import itertools


class Logger:
  def __init__(self, lib_name = "LOGR", base_folder = "", SHOW_TIME = True):
    self.app_log = list()
    self.results = list()
    self.printed = list()
    self.MACHINE_NAME = self.GetMachineName()
    self.__version__ = "3.2_tfg_pd_ker"
    self.SHOW_TIME = SHOW_TIME
    self.file_prefix = dt.now().strftime("%Y%m%d_%H%M%S") 
    self.log_file = self.file_prefix + '_log.txt'
    self.log_results_file = self.file_prefix + "_RESULTS.txt"
    self.__lib__= lib_name
    self._base_folder  = base_folder

    self._logs_dir = os.path.join(self._base_folder,"_logs")
    self._outp_dir = os.path.join(self._base_folder,"_output")

    self._setup_folders([self._outp_dir,self._logs_dir])
    
    self.log_file = os.path.join(self._logs_dir, self.log_file)
    self.log_results_file = os.path.join(self._logs_dir, self.log_results_file)
    
    self.VerboseLog("Library [{}] initialized on machine [{}]".format(
                    self.__lib__, self.MACHINE_NAME))
    self.VerboseLog("Logger ver: {}".format(self.__version__))
    self.CheckTF()
    
    return

  def _setup_folders(self,folder_list):
    for folder in folder_list:
      if not os.path.isdir(folder):
        print("Creating folder [{}]".format(folder))
        os.makedirs(folder)
    return

  def ShowNotPrinted(self):
    nr_log = len(self.app_log)
    for i in range(nr_log):
      if not self.printed[i]:
        print(self.app_log[i], flush = True)
        self.printed[i] = True
    return
  
  def SaveDataframe(self, df, fn = ''):
    file_prefix = self.file_prefix + "_"
    csvfile = os.path.join(self._outp_dir,file_prefix+fn+'.csv')
    df.to_csv(csvfile)
    return
  
  def ShowResults(self):
    for res in self.results:
      self._logger(res, show = True, noprefix = True)
    return
  
  def _logger(self, logstr, show = True, results = False, noprefix = False):
    """ 
    log processing method 
    """
    nowtime = dt.now()
    prefix = ""
    strnowtime = nowtime.strftime("[{}][%Y-%m-%d %H:%M:%S] ".format(self.__lib__))
    if self.SHOW_TIME and (not noprefix):
      prefix = strnowtime
    if logstr[0]=="\n":
      logstr = logstr[1:]
      prefix = "\n"+prefix
    logstr = prefix + logstr
    self.app_log.append(logstr)
    if show:
      print(logstr, flush = True)
      self.printed.append(True)
    else:
      self.printed.append(False)    
    if results:
      self.results.append(logstr)
    try:
      log_output = open(self.log_file, 'w')
      for log_item in self.app_log:
        log_output.write("%s\n" % log_item)
      log_output.close()
    except:
      print(strnowtime+"Log write error !", flush = True)
    return

  def OutputImage(self,arr, label=''):
    """
    saves array to a file as image
    """
    label = label.replace(">","_")
    file_prefix = dt.now().strftime("%Y%m%d_%H%M%S_") 
    file_name = os.path.join(self._outp_dir,file_prefix+label+".png")
    self.Log("Saving figure [{}]".format(file_name))
    if os.path.isfile(file_name):
      self.Log("Aborting image saving. File already exists.")
    else:
      imsave(file_name, arr)
    return

  def OutputPyplotImage(self, label=''):
    """
    saves current figure to a file
    """
    file_prefix = dt.now().strftime("%Y%m%d_%H%M%S") 
    file_name = os.path.join(self._outp_dir,file_prefix+label+".png")
    self.Log("Saving figure [{}]".format(file_name))
    plt.savefig(file_name)
  
  def VerboseLog(self,str_msg, results = False):
    self._logger(str_msg, show = True, results = results)
    return
  
  def Log(self,str_msg, show = False, results = False):
    self._logger(str_msg, show = show, results = results)
    return
    
  def GetKerasModelSummary(self, model, full_info = False):
    if not full_info:
      # setup the environment
      old_stdout = sys.stdout
      sys.stdout = TextIOWrapper(BytesIO(), sys.stdout.encoding)      
      # write to stdout or stdout.buffer
      model.summary()      
      # get output
      sys.stdout.seek(0)      # jump to the start
      out = sys.stdout.read() # read output      
      # restore stdout
      sys.stdout.close()
      sys.stdout = old_stdout
    else:
      out = model.to_yaml()
    
    str_result = "Keras Neural Network Layout\n"+out
    return str_result   
  
  def GetKerasModelDesc(self, model):
    """
    gets keras model short description
    """
    short_name = ""
    nr_l = len(model.layers)
    for i in range(nr_l):
      layer = model.layers[i]
      s_layer = "{}".format(layer.name)      
      c_layer = s_layer.upper()[0]
      if c_layer == "D":
        if s_layer.upper()[0:2] == "DE":
          c_layer = "D"
        else:
          c_layer = "d"
      if (c_layer in ["A","d"]):
        c_layer += "_"
      if (c_layer == "D") and (i < (nr_l-1)):
        if (model.layers[i+1].name.upper()[0:2]) != "DR":
          c_layer += "_"
      short_name += c_layer
      
    return short_name
  
  def SaveKerasModel(self, model, label=""):
    """
    saves keras model to a file
    """
    label = label.replace(">","_")
    file_prefix = dt.now().strftime("%Y%m%d_%H%M%S_") 
    file_name = os.path.join(self._outp_dir,file_prefix+label+".h5")    
    model.save(file_name)
    return
  
  def LogKerasModel(self, model):
    self.VerboseLog(self.GetKerasModelSummary(model))
    return
  
  def GetMachineName(self):
    if socket.gethostname().find('.')>=0:
        name=socket.gethostname()
    else:
        name=socket.gethostbyaddr(socket.gethostname())[0]
    self.MACHINE_NAME = name
    return name
  
  def _check_tf_avail(self):
    import imp
    try:
        imp.find_module('tensorflow')
        found = True
    except ImportError:
        found = False    
    return found
  
  def CheckTF(self):
    ret = 0
    if self._check_tf_avail():
      from tensorflow.python.client import device_lib
      local_device_protos = device_lib.list_local_devices()
      types = [x.device_type for x in local_device_protos]
      if 'GPU' in types:
          ret = 2
          self._logger("Found TF running on GPU")
      else:
          self._logger("Found TF running on CPU")
          ret = 1
    else:
      self._logger("TF not found")
    return ret

  def LogConfusionMatrix(self,cm, labels=["0","1"], hide_zeroes=False, 
                         hide_diagonal=False, hide_threshold=None,
                         show = False):    
    """pretty print for confusion matrixes"""
    columnwidth = max([len(x) for x in labels] + [8])  # 5 is value length
    empty_cell = " " * columnwidth
    full_str = "         " + empty_cell +"Preds\n"
    # Print header
    full_str += "    " + empty_cell+ " "
    for label in labels:
      full_str += "%{0}s".format(columnwidth) % label+" "
    full_str+="\n"
    # Print rows
    for i, label1 in enumerate(labels):
      if i==0:
        full_str+="GT  %{0}s".format(columnwidth) % label1 +" "
      else:
        full_str+="    %{0}s".format(columnwidth) % label1 +" "
      for j in range(len(labels)):
        cell = '{num:{fill}{width}}'.format(num=cm[i, j], fill=' ', width=columnwidth)
                #"%{0}.0f".format(columnwidth) % cm[i, j]
        if hide_zeroes:
            cell = cell if float(cm[i, j]) != 0 else empty_cell
        if hide_diagonal:
            cell = cell if i != j else empty_cell
        if hide_threshold:
            cell = cell if cm[i, j] > hide_threshold else empty_cell
        full_str += cell + " "
      full_str +="\n"    
    self._logger("Confusion Matrix:\n{}".format(full_str), show = show)
    
  def PlotConfusionMatrix(self,cm, classes=["0","1"],
                            normalize=False,
                            title='Confusion matrix',
                            cmap=plt.cm.Blues):
    """
    This function prints and plots the confusion matrix.
    Normalization can be applied by setting `normalize=True`.
    """
    plt.imshow(cm, interpolation='nearest', cmap=cmap)
    #plt.title(title)
    plt.colorbar()
    tick_marks = np.arange(len(classes))
    plt.xticks(tick_marks, classes, rotation=45)
    plt.yticks(tick_marks, classes)

    if normalize:
        cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
        s_title = "[Normalized]" + title 
    else:
        s_title = "[Standard]" + title

    plt.title(s_title)

    thresh = cm.max() / 2.
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        plt.text(j, i, cm[i, j],
                 horizontalalignment="center",
                 color="white" if cm[i, j] > thresh else "black")

    plt.tight_layout()
    plt.ylabel('True label')
    plt.xlabel('Predicted label')    
    plt.show()