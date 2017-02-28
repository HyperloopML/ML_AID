# -*- coding: utf-8 -*-
"""
Created on Wed Dec  7 12:01:34 2016

@credits after imports

"""

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.tree import DecisionTreeClassifier, export_graphviz
from sklearn import preprocessing
from sklearn.model_selection import train_test_split
from bokeh.io import curdoc, output_file, show
from bokeh.plotting import figure, ColumnDataSource
from bokeh.layouts import column, row, widgetbox 
from bokeh.models import Button,  HoverTool, widgets , Slider 
from bokeh.models.widgets import DataTable,TableColumn,StringFormatter
from bokeh.models.widgets import NumberFormatter,Div,Select,TextInput
from bokeh.models.widgets import CheckboxGroup
from bokeh.models import CategoricalColorMapper
from bokeh.palettes import Set1_8 as pal
from datetime import datetime as dt
import matplotlib.image as mat_Image
import os
from sql_helper import MSSQLHelper
from cluster_repository import ClusterRepository
import json
import time as tm


__author__     = "Andrei Ionut DAMIAN"
__copyright__  = "Copyright 2017, HTSS"
__credits__    = ["Ionut Canavea","Ionut Muraru"]
__license__    = "GPL"
__version__    = "1.0.3"
__maintainer__ = "Andrei Ionut DAMIAN"
__email__      = "ionut.damian@htss.ro"
__status__     = "Production"
__library__    = "HYPERLOOP CLUSTERING MAIN"
__created__    = "2016-12-07"
__modified__   = "2017-02-27"

class HyperloopClusteringServer:
    
    def __init__(self):

        self.MODULE = "{} v{}".format(__library__,__version__)
        self._logger("INIT "+self.MODULE)

        pd.options.display.float_format = '{:,.3f}'.format
        pd.set_option('expand_frame_repr', False)
        
        self.FULL_DEBUG = False
        self.s_prefix = dt.strftime(dt.now(),'%Y%m%d')
        self.s_prefix+= "_"
        self.s_prefix+=dt.strftime(dt.now(),'%H%M')
        self.s_prefix+= "_"
        self.save_folder = "saved_data/"
        self.base_tree_file = "_tree.png"
        self.tree_file = ""
        self.current_X = 'N/A'
        self.current_Y = 'N/A'
        self.default_cluster_view = "F1/F2"
        self.nr_shown_records = 10
        
        self.ClusterDescription = ''
        self.text_ClusterDesc = None
        self.cds_select = None
        
        
        self.dot_alpha = 0.5
        self.dot_size  = 4
        self.nr_downsample = 1000
                
        self.TableViewText = None
        
        self.nr_inits = 30
        

        self.cds = None
        
        self._logger(self.s_prefix)
        self._logger("__name__: {}".format(__name__))
        self._logger("__file__: {}".format(__file__))
        self.current_cluster_glph_renderer = None
        self.initialize_data()
        self.initialize_layout()
        return
    
    def upper_config_str(self, mydict):
        
        newdict = dict()
        
        for k,v in mydict.items():
            newdict[k.upper()] = v
        
        return newdict

    def _logger(self, logstr, show = True):
        if not hasattr(self, 'log'):        
            self.log = list()
        nowtime = dt.now()
        strnowtime = nowtime.strftime("[HCSVR][%Y-%m-%d %H:%M:%S] ")
        logstr = strnowtime + logstr
        self.log.append(logstr)
        if show:
            print(logstr, flush = True)
        return
        
    def ClusterDownSampler(self, source_df, 
                           label_field, 
                           cluster_fields,
                           nr_samples):
        if self.DownSample:
            t0=tm.time()
            self._logger("DOWNSAMPLING ...")
            labels = list(source_df[label_field].unique())
            if label_field in cluster_fields:
                cluster_fields.remove(label_field)
            all_fields = list(cluster_fields) 
            all_fields.append(label_field)
            downsampled = pd.DataFrame()
            for label in labels:
                cluster_df = pd.DataFrame(source_df[source_df[label_field] == label])
                if cluster_df.shape[0]>nr_samples:
                    nr_cl = nr_samples
                else:
                    nr_cl = cluster_df.shape[0]
                self._logger("Downsampling {} in {} points".format(label,nr_samples))
                clf = KMeans(n_clusters = nr_cl,
                             n_jobs = -1,
                             n_init = 5)
                clf.fit(np.array(cluster_df[cluster_fields]))
                cluster_df['DownCluster'] = clf.labels_
                down_df = pd.DataFrame()
                i=0
                for fld in cluster_fields:
                    down_df[fld] = clf.cluster_centers_[:,i]
                    i += 1
                down_df[label_field] = label
                self._logger("Downsampled data {}\n{}".format(down_df.shape,down_df.head(3)))
                downsampled = downsampled.append(down_df)
            t1 = tm.time()
            self._logger('Done in {:.1f}s downsampling {}\n{}'.format(t1-t0,
                                                             downsampled.shape,
                                                             downsampled.head(3)))  
            df_result = downsampled
        else:
            self._logger("NO DOWNSAMPLE !!!")
            df_result = source_df
        return df_result
        
    
    def UploadCluster(self,df, cluster_dict):
        clRep = ClusterRepository()
        clRep.UploadCluster(df, cluster_dict, self.sql)
        return
        
        
        
        
    def _upload_cluster(self, sClusterName, sClusterObs, sClusterGrade,
                        nCentroidNo,
                        nCustomerNo, sClusterAuthor,
                        sClusterAlgorithm,
                        sF1_Obs, sF2_Obs, sF3_Obs, sF4_Obs, sF5_Obs, 
                        df_cluster, df_desc):

        
        cConfig = self.cluster_config
        clRep = ClusterRepository()
        clRep.UploadClusterDetail(sClusterName, sClusterObs, sClusterGrade,
                                  nCentroidNo,
                                  nCustomerNo, sClusterAuthor,
                                  sClusterAlgorithm,
                                  sF1_Obs, sF2_Obs, sF3_Obs, sF4_Obs, sF5_Obs, 
                                  df_cluster,
                                  df_desc,
                                  cConfig,
                                  self.sql)
        
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
                self._logger("  ID: {} / Name: {} / Fields: {}".format(cluster_ID,
                                                                       cluster_name,
                                                                       cluster_fields))
            cluster_def["DATA"] = None
            self.cluster_config_list.append(cluster_def)
            
        return
    
    def _setup_fields(self, cluster_dict):
        
        self.cID = cluster_dict["ID"]  #'PartnerId'      #'CustID'
        self.cf = list()
        for i in range(5):
            if cluster_dict["Fields"][i] != '':
                self.cf.append(cluster_dict["Fields"][i])

        #self.cf1 = cluster_dict["Fields"][0]   #'RecencyScore'     #R
        #self.cf2 = cluster_dict["Fields"][1]   #'NrTransactions'   #F
        #self.cf3 = cluster_dict["Fields"][2]   # 'TotalAmount'    #'TotalValue'       #M
        
        self.nr_fields = len(self.cf)
        if self.nr_fields == 3:
            self.scale_cf = [1,2]  #[0,1,2] #log-scale fields by nr
        else:
            self.scale_cf = [0,1]
        
        
        self.cfc = 'Segment'            # cluster label column
        self.AssignmentID = "CentroidID"
        if "SEGMENTS" in cluster_dict.keys():
            self.nr_clusters = int(cluster_dict["SEGMENTS"])
            self._logger("Starting with {} clusters".format(self.nr_clusters))
        else:
            self.nr_clusters = 4
            self._logger("Defaulting to {} clusters".format(self.nr_clusters))
            
        self.nr_tree_lvl = 3
        self.hover_fields = dict()
        self.hover_fields['Tranzactii (avg)'] = "TranCount"
        self.hover_fields['Valoare totala (avg)'] = "TotalAmount"  
        self.scaling_method = 'MinMax'
        self.refresh_tooltips(self.hover_fields)
        return

    
    def _load_db_cluster(self, strqry):
        df_temp = self.sql.Select(strqry)
        self.df_rfm = pd.DataFrame(df_temp[df_temp[self.cID] != -1])
        self._logger("Loaded dataset {} rows with head(3) \n{}".format(self.df_rfm.shape[0],
                                                                self.df_rfm.head(3)))
        self.origin_fields = list()
        self.cluster_fields = list()
        for i in range(len(self.cf)):
            if self.cf[i] != '':
                self.origin_fields.append(self.cf[i])
                self.cluster_fields.append('F'+str(i+1))                      
        return
        
    def LoadDBClusterByID(self,sID):
                
        self._logger("Loading cluster by ID: {}".format(sID))
        strqry = self.config_data[sID]["SQL"]
        self.cluster_config = self.config_data[sID]
        self.FastSave = False
        self.DownSample = True
        if "DOWNSAMPLE" in self.cluster_config.keys():
            self.DownSample = int(self.cluster_config["DOWNSAMPLE"])
            if not self.DownSample:
                self._logger("DOWNSAMPLING DISABLED !")
        if "FASTSAVE" in self.cluster_config.keys():
            self.FastSave = int(self.cluster_config["FASTSAVE"])
            if self.FastSave:
                self._logger("FASTSAVE ENABLED !")
        self._setup_fields(self.cluster_config)
        self._load_db_cluster(strqry)
        
        return
    
    def LoadDBClusterByName(self, sName):

        strqry = None  
        cluster_dict = None
        
        for i in range(len(self.cluster_config_list)):
            if sName == self.cluster_config_list[i]["Name"]:
                strqry = self.cluster_config_list[i]["SQL"] 
                cluster_dict = self.cluster_config_list[i]
                
        if strqry != None:    
            self.cluster_config = cluster_dict            
            self._logger("Loading cluster by Name: {}".format(sName))
            self._setup_fields(self.cluster_config)
            self._load_db_cluster(strqry)
        else:
            self._logger("\nERROR LOADING CLUSTER CONFIGURATION FILE\n")
            
        return
    

    def refresh_tooltips(self, dict_tooltips):
        
        self.hover_fields = dict_tooltips
        self.tooltip1 = list(self.hover_fields.keys())[0]
        self.tooltip2 = list(self.hover_fields.keys())[1]
        self.tip1_fld = self.hover_fields[self.tooltip1]        
        self.tip2_fld = self.hover_fields[self.tooltip2]        
        return
        
    def refresh_cds(self, cluster = False, x_col = None, y_col = None):
        self._logger("Refreshing ColumnDataSource ...")   
        df_downsamp = self.df_downrfm
        if x_col != None and y_col != None:
            if self.FULL_DEBUG:
                self._logger("Old X: {} Y: {}  / New X: {} Y: {}".format(self.current_X,
                                                                   self.current_Y,
                                                                   x_col,
                                                                   y_col))
            self.current_X = x_col
            self.current_Y = y_col
            
        if cluster and (x_col == None and y_col == None):
            if self.FULL_DEBUG:
                self._logger(" refreshing labels field '{}' only".format(self.cfc))
            self.cds.data[self.cfc] = np.array(df_downsamp[self.cfc]) 
        else:

            data_dict = dict(X = np.array(df_downsamp[self.current_X]),
                             Y = np.array(df_downsamp[self.current_Y]))
            
            data_dict[self.cfc] = np.array(df_downsamp[self.cfc]) 
            if False:
                data_dict[self.tip1_fld] = np.array(self.df_rfm[self.tip1_fld])
                data_dict[self.tip2_fld] = np.array(self.df_rfm[self.tip2_fld])
                data_dict[self.cID] = np.array(self.df_rfm[self.cID])

            if self.FULL_DEBUG:
                self._logger(" refreshing full cds: {}".format(list(data_dict.keys())))
                
            if self.cds == None:
                self.cds = ColumnDataSource(data_dict)
            else:
                self.cds.data = data_dict

        if self.FULL_DEBUG:
            self._logger("Done refreshing ColumnDataSource.")
        return
        
        

    def analyze_segments(self):
        
        self.segment_id_fld = "Cluster"
        self.segment_samples_fld = "Clienti"
        self.segment_value_fld = "Scor"
        self.segment_name_fld = self.cfc # default always
        
        self.df_clusters = pd.DataFrame()
        cluster_norm = np.zeros(self.nr_clusters)
        cluster_vals = np.zeros(self.nr_clusters)
        for i in range(self.nr_clusters):
            cluster_norm[i] = np.mean(self.cluster_centers[i,:])
            if self.FULL_DEBUG:
                self._logger(" cluster {} vals {}".format(i,(self.df_rfm[self.cfc] == i).head(3)))
            cluster_vals[i] = (self.df_rfm[self.cfc] == i).sum()
        
        self.df_clusters[self.segment_id_fld] = np.arange(0,self.nr_clusters,1)
        self.df_clusters[self.segment_value_fld] = cluster_norm
        self.df_clusters[self.segment_samples_fld] = cluster_vals
        
        self.df_clusters.sort_values(by=self.segment_value_fld, ascending = False, inplace = True)
        self.df_clusters = self.df_clusters.reset_index()
        clfields = [self.segment_id_fld,self.segment_value_fld, self.segment_samples_fld]
        self.df_clusters = self.df_clusters[clfields]

        lev_2 = ["MOST VALUED CUSTOMERS","LOWER RANGE CUSTOMERS"]
        lev_3 = ["MOST VALUED CUSTOMERS","AVERAGE CUSTOMERS","LOWER RANGE CUSTOMERS"]
        lev_4 = ["MOST VALUED CUSTOMERS",
                 "GOOD CUSTOMERS",
                 "AVERAGE CUSTOMERS",
                 "LOWER RANGE CUSTOMERS",
                 "BOTTOM CUSTOMERS",
                 "WORST CUSTOMERS"]
        new_ids = list()
        for i in range(self.nr_clusters):
            new_ids.append(100*(i+1))
            
        if self.nr_clusters == 2:
            self.levels = lev_2
        elif self.nr_clusters == 3:
            self.levels = lev_3
        else:            
            self.levels = list()
            for i in range(self.nr_clusters):
                self.levels.append(lev_4[i])

        self.df_clusters[self.cfc] = self.levels
        self.df_clusters['NewID'] = new_ids
        
        self._logger("Customer segmentation structure:\n {}".format(self.df_clusters))

                
        self.clusters_dict = dict()
        for i in range(self.nr_clusters):
            self.clusters_dict[self.df_clusters.loc[i,self.segment_id_fld]] = self.df_clusters.loc[i,self.cfc]
        if self.FULL_DEBUG:
            self._logger("Cluster dict: {}".format(self.clusters_dict))
        
        self.class_names = list()
        for i in range(self.nr_clusters):
            self.class_names.append(self.clusters_dict[i])
        if self.FULL_DEBUG:
            self._logger("Class names: {}".format(self.class_names))
        
            

        # df_rfm[cfc] must be working
        #self.df_rfm["TEMP_SEGMENT"] = self.df_rfm[self.cfc].astype(str)
        #self.df_rfm["TEMP_SEGMENT"] = self.df_rfm["TEMP_SEGMENT"].map(self.map_func)
        self.df_rfm[self.AssignmentID] = self.df_rfm[self.cfc]
        self.df_rfm[self.cfc] = self.df_rfm[self.cfc].map(self.map_func)
            
        old_clusters_list = self.df_rfm[self.AssignmentID].unique()
        strDesc = ''
        for i in range(self.nr_clusters):
            old_cluster = old_clusters_list[i]
            pos = self.df_clusters[self.segment_id_fld] == old_cluster
            new_cluster = self.df_clusters[pos]["NewID"].values[0]
            sNrClients = str(self.df_clusters[pos]['Clienti'].values[0])
            sLabel = self.df_clusters[pos]['Segment'].values[0]
            strDesc += " "+sNrClients+" clients in segment "+sLabel+","
            self._logger('Replacing ClusterID {} with {}'.format(old_cluster,new_cluster))
            self.df_rfm[self.AssignmentID].replace(old_cluster,new_cluster, inplace = True)
        
        self._logger('Found: {}'.format(strDesc))
        self.ClusterDescription = self.cluster_config['Description'] + strDesc
        if self.text_ClusterDesc != None:
            self.text_ClusterDesc.value = self.ClusterDescription
        self._logger("Top and bottom 3:\nTop 3:\n{}\nBottom 3:\n{}\n".format(self.df_rfm.head(3),
                                                       self.df_rfm.tail(3)))
        
        return
            
    def map_func(self,val):        
        vvv = int(val)
        lvl = self.clusters_dict[vvv]
        return lvl
    

    def _deprecated_update_tree_figure(self,s_png):
        img = mat_Image.imread(s_png)
        img = (img*255).astype(np.uint8)
        N, M, _ = img.shape
        img = img.view(dtype=np.uint32).reshape((N, M)) #convert image NxM dtype=uint32
        
        img = img[::-1]
        self._logger('IMG SHAPE {}'.format(img.shape))
        y_rng = 10
        xfact = float(M/N)
        x_rng = int(y_rng * xfact)
        self._logger("X:{} Y:{}".format(x_rng,y_rng))    
        #       fig = self.tab2_layout.children[1]
        fig = figure(x_range=(0,x_rng), y_range=(0,y_rng))
        # prepare tree display tab
    
        fig.plot_width = 900
        fig.plot_height = int(fig.plot_width / xfact)
        # done tree display tab
        fig.image_rgba(image=[img], x=0, y=0, dw=x_rng, dh=y_rng)
        self.tab2_layout.children[1] = fig
        
        #self.tab2_empty_tree_fig.plot_width = 900
        #self.tab2_empty_tree_fig.plot_height = int(900 / xfact)
        #self.tab2_empty_tree_fig.image_rgba(image=[img], 
        #                                    x=0, y=0, 
        #                                    dw=x_rng, dh=y_rng)

        self.update_cluster_image(self.current_cluster_view)        
        
        return img
        
        
        
    def update_png(self):
        s_file = self.tree_file
        self.DivText3.text = self.DivText3.text +" {}".format(self.class_names)
        self._logger("Update tree view {} ...".format(s_file))
        #img = self.update_tree_figure(s_png)        
        image_url = "http://localhost:80/"+s_file
        #image_url = "/hcsvr/static/"+s_file
        div_img_html = "<img src='"+image_url+"'>"
        self.DivTreeImage.text = div_img_html
        #div_img = Div(text = div_img_html)
        #self.tab3_layout.children[1] = div_img

        return

        
        
        
    def save_tree(self,tree_clf):
        tree_fields = self.origin_fields

        new_prefix = dt.strftime(dt.now(),'%Y%m%d')
        new_prefix+= "_"
        new_prefix+=dt.strftime(dt.now(),'%H%M%S')
        
        self.tree_file = new_prefix+self.base_tree_file
        
        self._logger("Saving tree {}...".format(self.tree_file))
        dot_data = export_graphviz(tree_clf, 
                                    out_file=None, 
                                    feature_names=tree_fields,  
                                    class_names=self.class_names,  
                                    filled=True, 
                                    rounded=True, 
                                    impurity = False,
                                    special_characters=True)  
        
        import importlib
        pydot_test = importlib.util.find_spec('pydotplus')
        if pydot_test != None:
            import pydotplus 
            graph = pydotplus.graph_from_dot_data(dot_data)
            if not os.path.exists('static'):
                os.makedirs('static')
            graph.write_png('static/'+self.tree_file)
        else:
            graph = None
            
        return graph

        
        
    def train_tree(self, predictors, label_column, nr_lvl):
        
        self._logger("Training tree with {} lvl and preds: {}...".format(nr_lvl,predictors))
        X = self.df_rfm.loc[:,predictors]
        
        if self.FULL_DEBUG:
            self._logger(" Tree dataset columns: {}".format(self.df_rfm.columns))
            
        y = self.df_rfm[label_column]
        X_train, X_test, y_train, y_test = train_test_split(X,y,test_size=0.15)
        
        # now train decision tree    
        clf_tree = DecisionTreeClassifier(max_depth = nr_lvl)
        clf_tree.fit(X_train,y_train)
        
        if self.FULL_DEBUG:
            self._logger(" Tree: {}".format(clf_tree))
        
        # predict 
        y_hat = clf_tree.predict(X_test)
        self.last_tree_accuracy = np.sum(y_hat == y_test) / (float(y_test.size)) * 100
        self.last_tree_clf = clf_tree
        s_acc = " Tested prediction on {} model unseen examples: {:.2f}%".format(X_test.shape[0],
                                                                                self.last_tree_accuracy)   
        self._logger(s_acc)    
        
        # now save constructed tree
        self.save_tree(clf_tree)
        if self.FULL_DEBUG:
            self._logger("Done training tree.")
        return 

        
        
    def generate_clusters(self, nr_clusters, 
                          predictors, cluster_column):

        self.nr_clusters = nr_clusters

        self.sBaseAlgorithm = "{}({} seg, {} inits):".format(self.cluster_config["Algorithm"],
                                          self.nr_clusters,
                                          self.nr_inits)
        self.sAlgorithm = self.sBaseAlgorithm + self.sAlgorithmOptions

        self._logger("Clustering for {}...".format(self.sAlgorithm))
        X = np.array(self.df_rfm[predictors])
        # unsupervised classification
        t0=tm.time()
        clf_km = KMeans(n_clusters = self.nr_clusters,
                        n_init = self.nr_inits, 
                        n_jobs = -1)
        clf_km.fit(X)
        # attach labels to model data and prepare DecisionTree train/test
        self.df_rfm[cluster_column]= clf_km.labels_
        self.cluster_centers = clf_km.cluster_centers_
        self.last_clustering_error = clf_km.inertia_
        self.last_cluster_mode = clf_km
        t1=tm.time()
        self._logger("Done clustering {:.1f}s. Clusters discovered:\n {}. Clusters inertia: {:.1f}".format(
                                         t1-t0,
                                         self.cluster_centers,
                                         self.last_clustering_error))
        
        self.analyze_segments()

        self.df_downrfm = self.ClusterDownSampler(source_df = self.df_rfm,
                                                  label_field = self.cfc,
                                                  cluster_fields = self.cluster_fields,
                                                  nr_samples = self.nr_downsample)
        if True:
            fld_x = self.cluster_fields[0]
            fld_y = self.cluster_fields[1]
            if self.current_X != 'N/A':
                fld_x = self.current_X
                fld_y = self.current_Y                
            self.refresh_cds(cluster = True,x_col = fld_x,y_col = fld_y)
        else:       
            # now update ONLY cluster column in ColumnDataSource
            self.refresh_cds(cluster = True)
        
        temp_list = list(self.df_rfm[cluster_column].unique())
        self.segments = [str(x) for x in temp_list]
        self.current_segment = self.segments[0]
        self.current_segment_clients = (self.df_rfm[self.cfc] == self.current_segment).sum()

        
        if self.FULL_DEBUG:
            self._logger("Done clustering.")
        
        return

    
        
        
    def update_texts(self, acc = None, inert = None):
        if acc!= None and inert != None:
            t1 = "<strong>Acuratetea: {:.2f}%</strong>".format(acc)
            t1+= "<br><i>Acuratetea de identifiare a clusterului pentru clientii noi cu"
            t1+= " ajutorul unui arbore de decizie calculata pentru un esantion"
            t1+= " out-of-sample de {} clienti </i>".format(self.nr_sample)
            t2 = "<strong>Eroarea: {:.1f} </strong>".format(inert)
            t2+= "<br><i>Inertia totala clusterelor (eroarea de clusterizare) "
            t2+= "pentru cazul cu {} clustere</i> ".format(self.nr_clusters)    
            
            self.DivText1.text = t1
            self.DivText2.text = t2
        
        if self.TableViewText != None:
            self.TableViewText.text = "<b>{}</b> clienti in segmentul<BR> {}".format(self.current_segment_clients,
                                                                 self.current_segment)
        return
    
        
    def save_rfm_clustering(self):
        # save
        sFile = "saved_data/"+self.s_prefix+'rfm_data_clusters.csv'
        self.df_rfm.to_csv(sFile,
                           sep = '|',
                           index = False)
                           
        # done saving   
        return

        
        
    # Define the callback function for cluster Slider
    def on_cluster_change(self, attr, old, new):
        c_clust = new
        self._logger("From cluster:{} to cluster:{} ...".format(old,new))
        # frop cluster columns
        # now generate new cluster
        self.generate_clusters(nr_clusters = c_clust,
                               predictors = self.cluster_fields,
                               cluster_column = self.cfc)
        
        self.train_tree(predictors = self.origin_fields, 
                        label_column = self.cfc, 
                        nr_lvl = self.nr_tree_lvl)  
            
        self.update_texts(self.last_tree_accuracy, 
                          self.last_clustering_error)
        
        self.update_png()
        
        self.on_update_table(0,0,self.nr_shown_records)
        
        self.cb_select_k.options = self.segments

        self.text_Algorithm.value = self.sAlgorithm

        self.update_cluster_image(self.current_cluster_view)
        
    
        if self.FULL_DEBUG:
            self._logger("Done from cluster:{} to cluster:{}".format(old,new))
        return        
    
        
        
    def on_update_tree_lvl(self, attr, old, new):
        self._logger("Updating tree level...")
        self.nr_tree_lvl = new
        self.train_tree(predictors = self.origin_fields, 
                        label_column = self.cfc,
                        nr_lvl = self.nr_tree_lvl)
        self.update_texts(self.last_tree_accuracy, self.last_clustering_error)
        self.update_png()    
        if self.FULL_DEBUG:
            self._logger("Done updating tree level.")
        return

        
        
    def on_update_table(self, attr, old, new):
        self.nr_shown_records = new
        self._logger("Refreshing data for table view: {}".format(
                              self.df_rfm.head(self.nr_shown_records).shape))
        self.cds_select.data = ColumnDataSource.from_df(
                                self.df_rfm.head(self.nr_shown_records))
        return    
    
        
        
    def update_cluster_image(self, new_option):
        
        if new_option=="F1/F2":
            fi1 = 0
            fi2 = 1
        elif new_option == "F1/F3":
            fi1 = 0
            fi2 = 2    
        else:
            fi1 = 1
            fi2 = 2
    
        fldX = self.cluster_fields[fi1]
        fldY = self.cluster_fields[fi2]                
    
        sTitle = 'Graficul clusterizare cu {} vs. {} pentru {} clustere '.format(fldX,
                                                                    fldY,
                                                                    self.nr_clusters)
        sTitle+= ' ({} clienti)'.format(self.df_rfm.shape[0])
        self._logger(sTitle)
        if self.FULL_DEBUG:
            self._logger(" CDS keys: {}".format(self.cds.data.keys()))
        self.cluster_figure.xaxis.axis_label = fldX
        self.cluster_figure.yaxis.axis_label = fldY
        self.cluster_figure.title.text = sTitle
        
        #if self.current_cluster_glph_renderer != None:
        #    self.current_cluster_glph_renderer.glyph.line_alpha = 0
        #    self.current_cluster_glph_renderer.glyph.fill_alpha = 0
        #    self.current_cluster_glph_renderer.glyph.visible = False

        #g_render=self.cluster_figure.circle(fldX,
        #                                fldY,
        #                                source = self.cds, 
        #                                alpha = 0.5, size=2, 
        #                                color=dict(field=self.cfc,transform=self.color_mapper),
        #                                selection_color = 'gray', 
        #                                nonselection_alpha = 0.1, 
        #                                legend = self.cfc)   
        
        self.color_mapper = CategoricalColorMapper(factors = list(self.levels),
                                                   palette = pal)
        if self.current_cluster_glph_renderer == None:
            self._logger("Redrawing with levels: {}".format(self.levels))
            self.color_mapper = CategoricalColorMapper(factors = list(self.levels),
                                                       palette = pal)
            g_render=self.cluster_figure.circle(x = 'X',
                                                y = 'Y',
                                                source = self.cds, 
                                                alpha = self.dot_alpha, 
                                                size= self.dot_size, 
                                                color=dict(field=self.cfc,transform=self.color_mapper),
                                                selection_color = 'gray', 
                                                nonselection_alpha = 0.1,
                                                legend=self.cfc)     
            
            #legend = Legend(legends=self.cfc, location=(0, -30))
            #self.cluster_figure.add_layout(legend, 'right')
            #self.cluster_figure.legend.orientation = "horizontal"
            self.cluster_figure.legend.location = "top_left"
            #self.cluster_figure.legend.background_fill_color = "navy"
            self.cluster_figure.legend.background_fill_alpha = 0.25   
            
            
            self.current_cluster_glph_renderer = g_render
        else:
            
            prevX = self.current_X #self.current_cluster_glph_renderer.glyph.x 
            prevY = self.current_Y #self.current_cluster_glph_renderer.glyph.y
            #self.current_cluster_glph_renderer.glyph.color = dict(field=self.cfc,transform=self.color_mapper)
            self._logger("Changing columns from {},{} to {},{}...".format(
                                                      prevX,
                                                      prevY,
                                                      fldX,
                                                      fldY))
            #self.current_cluster_glph_renderer.glyph.x = fldX
            #self.current_cluster_glph_renderer.glyph.y = fldY
            self.refresh_cds(x_col = fldX, y_col = fldY)
            if self.FULL_DEBUG:
                self._logger("Done changing columns.")

        if self.FULL_DEBUG:
            self._logger("  Cluster Glyph {}".format(self.current_cluster_glph_renderer))
        return

    


    def on_sel_img_change(self, attr,old,new):  
        self._logger("Select change axes {} ...".format(new))
        self.current_cluster_view = new
        self.update_cluster_image(self.current_cluster_view)     
        if self.FULL_DEBUG:
            self._logger("Done select change axes.")
        return 
        
    def on_reset_selection(self):
        self.cds.selected = self.empty_selection
        return
    
    def on_save(self):
        
        sLabel = self.btn_save.label
        self.btn_save.label = 'Saving data...'        
        self.btn_save.disabled = True
        
        self._logger("Saving data ...")
        self.save_rfm_clustering()
        
        sClusterName = self.text_ClusterName.value
        sClusterObs = self.text_ClusterDesc.value
        centroids = self.df_rfm['CentroidID'].unique()
        nCentroidNo = centroids.shape[0]                      
        nCustomerNo = self.df_rfm.shape[0] 
        sClusterAuthor = self.text_Author.value
        sClusterAlgorithm = self.text_Algorithm.value
        sF1_Obs = self.text_F1.value
        sF2_Obs = self.text_F2.value
        sF3_Obs = self.text_F3.value
        sF4_Obs = self.cluster_config['Fields'][3]
        sF5_Obs = self.cluster_config['Fields'][4]
        sClusterGrade = self.cb_ClusterGrade.value

        self._upload_cluster(sClusterName, sClusterObs, sClusterGrade, 
                             nCentroidNo,
                             nCustomerNo, sClusterAuthor,
                             sClusterAlgorithm,
                             sF1_Obs, sF2_Obs, sF3_Obs, sF4_Obs, sF5_Obs, 
                             self.df_rfm,
                             self.df_clusters)
        
        self.btn_save.label = sLabel
        self.btn_save.disabled = False        
        self._logger("Done saving data.")
        return

    def on_save_local(self):
        
        sLabel = self.btn_save_local.label
        self.btn_save_local.label = 'Saving data...'        
        self.btn_save_local.disabled = True
        
        self._logger("Saving data locally ...")
        self.save_rfm_clustering()
        self._logger("Done saving data.")
        self.btn_save_local.label = sLabel
        self.btn_save_local.disabled = False
        return
        
    def on_view_cluster(self, attr, old, new):
        self.current_segment  = new
        self.current_segment_clients = (self.df_rfm[self.cfc] == self.current_segment).sum()
        
        cluster_column_kind = self.df_rfm[self.cfc].dtype.kind
        if  cluster_column_kind == 'i':
            sel_val = int(self.current_segment)
        elif cluster_column_kind == 'f':
            sel_val = float(self.current_segment)
        else:
            sel_val = self.current_segment

        self._logger("Viewing cluster selection {} (kind:{}) prep...".format(sel_val,
                                                      cluster_column_kind))
            
        temp_df = self.df_rfm[self.df_rfm[self.cfc] == sel_val].head(self.nr_shown_records)
        self._logger("Viewed table head/tail:\n{}\n{}".format(temp_df.head(3),
                                                              temp_df.tail(3)))
        self.cds_select.data = ColumnDataSource.from_df(temp_df)
        self.update_texts()
        if self.FULL_DEBUG:
            self._logger("Done view cluster prep.")
        return
        
    def on_cb_scaling(self, attr, old, new):
        self.scaling_method = new
        self.prepare_data()
        self.text_Algorithm.value = self.sAlgorithm
        return
    
    def on_log_check(self, new):
        
        self.scale_cf =self.ckbox_transf.active

        self.prepare_data()
        self.text_Algorithm.value = self.sAlgorithm
        return
        
        
    
    def prepare_data(self):
        self._logger(" ")
        self.sBaseAlgorithm = "{}({} seg, {} inits):".format(self.cluster_config["Algorithm"],
                                          self.nr_clusters,
                                          self.nr_inits)

        self._logger("!!! (RE)INITIALIZING DATA: Alg={}".format(self.sBaseAlgorithm))

        # log transform values
        self.sAlgorithmOptions = ''
        self.scale_fields = list()
        for i in range(len(self.cf)):
            sScaled = self.cf[i]

            if i in self.scale_cf: # if must be log-scaled
                sField = self.cf[i]
                sScaled = 'F'+str(i)+'LOG'
                self._logger('Transform {}=LOG({})'.format(sField,sField))
                self.sAlgorithmOptions += ' +LOG('+sField+')'
                self.df_rfm.loc[self.df_rfm[sField]<=0, sField] = 0.01
                self.df_rfm[sScaled] = np.log(self.df_rfm[sField])

            self.scale_fields.append(sScaled)
            

        self.sAlgorithmOptions +=' +scale('+self.scaling_method+')'
                               
        if self.scaling_method == 'MinMax':
            # use MinMax scaler
            self._logger("Preprocessing MinMax {}...".format(self.scale_fields))        
            minmaxScaler = preprocessing.MinMaxScaler()
            np_arr = minmaxScaler.fit_transform(self.df_rfm[self.scale_fields])
        else:
            # user Standardization scaler (X-mean)/std
            self._logger("Preprocessing ZSCore {}...".format(self.scale_fields))        
            np_arr = preprocessing.scale(self.df_rfm[self.scale_fields])
        
        
        self._logger("  Initial object shape: {}".format(self.df_rfm.shape))
        if self.FULL_DEBUG:
            self._logger("  Tail nparr:\n{}".format(np_arr[-3:,:]))
            self._logger("  Tail 1 rfm:\n{}".format(self.df_rfm.tail(3)))
        for i in range(np_arr.shape[1]):
            self.df_rfm[self.cluster_fields[i]] = np_arr[:,i]
        #self.df_rfm = pd.concat([self.df_rfm, 
        #                         pd.DataFrame(data = np_arr, 
        #                                      columns = cluster_fields )
        #                         ],
        #                         axis = 1)
        self._logger("  Final object shape: {} ".format(self.df_rfm.shape))
        if self.FULL_DEBUG:
            self._logger("  Tail 2 rfm:\n{}".format(self.df_rfm.tail(3)))


        self.df_rfm[self.cfc] = np.zeros(self.df_rfm.shape[0])
        
        
        self._logger("Initial clustering ...")
        self.generate_clusters(nr_clusters =self.nr_clusters,
                               predictors = self.cluster_fields,
                               cluster_column = self.cfc)
        if self.FULL_DEBUG:
            self._logger("Done initial clustering.")

        self._logger("Initial tree growing...")
        self.train_tree(predictors = self.origin_fields, 
                        label_column = self.cfc, 
                        nr_lvl = self.nr_tree_lvl) 
        if self.FULL_DEBUG:
            self._logger("Done initial tree growing.")

        self.nr_customers = self.df_rfm.shape[0]
        self.nr_sample = int(self.nr_customers * 0.15)     
        
        if self.cds_select == None:
            self.cds_select = ColumnDataSource(self.df_rfm.head(self.nr_shown_records))            
        self.current_cluster_view = self.default_cluster_view
        self.on_view_cluster(0,0,self.current_segment)
        self._logger("Using segment {} data for table: {}".format(self.current_segment,
              self.df_rfm.head(self.nr_shown_records).shape))

        self._logger("!!! DONE DATA INITIALIZATION.\n")
        
        return
    
    def initialize_data(self):
        
        self._loadconfig()
        
        self.sql = MSSQLHelper()
        
        self.LoadDBClusterByID("1")
        
        self.prepare_data()   
        
        return

        
        
    def initialize_layout(self):
        
        self._logger("!!! LAYOUT PREPARATION...")
    
        #labels = list(np.unique(self.df_rfm[self.cfc]))
    
        self.color_mapper = CategoricalColorMapper(
                                                   factors = list(self.levels),
                                                   palette = pal)




   

        # Make a slider object: slider
        self.slider = Slider(start=2, end=6,step=1,value=self.nr_clusters, 
                             title='Numarul de clustere',
                             width = 200)
        # Attach the callback to the 'value' property of slider
        self.slider.on_change('value',self.on_cluster_change)

        self.DivText1 = Div(text = "")
        self.DivText2 = Div(text = "")
        
        

        self.slider_tree = Slider(start=2, end=4,step=1,value=self.nr_tree_lvl, 
                                  title='Numarul de nivele arbore',
                                  width = 200)
        self.slider_tree.on_change('value',self.on_update_tree_lvl)

    
        self.slider_table = Slider(start=10, end=10000,
                                   step=100,value=self.nr_shown_records, 
                                   title='Numarul de inregistrari afisate',
                                   width = 200)
        self.slider_table.on_change('value',self.on_update_table)
        
        if self.nr_fields >2:
            opt = ["F1/F2", "F1/F3", "F2/F3"]
        else:
            opt = ["F1/F2"]
        self.cb_select_rfm = Select(title="Option:", 
                                    value=self.default_cluster_view, 
                                    options=opt)

        self.cb_select_rfm.on_change('value',self.on_sel_img_change)



        self.cluster_figure = figure(webgl=True,
                                     tools='box_select',
                                     plot_width=500, 
                                     plot_height=500)
        tip1 = self.tooltip1
        tip2 = self.tooltip2
        self.hover = HoverTool(
                tooltips=[
                          ("Segment", "@"+self.cfc),
                          ("Client", "@"+self.cID),
                          (tip1, "@"+self.hover_fields[tip1]),
                          (tip2, "@"+self.hover_fields[tip2]),
                          ]
                          )
        if self.FULL_DEBUG:
            self._logger(" Tooltips: {}".format(self.hover.tooltips))

        self.cluster_figure.add_tools(self.hover)        


        self.update_cluster_image(self.current_cluster_view)

        self.empty_selection = self.current_cluster_glph_renderer.data_source.selected
        
        self.btn_reset = Button(label="Deselectare", width=20)
        self.btn_reset.on_click(self.on_reset_selection)
        
        self.btn_save = Button(label="Salvare", width=50)
        self.btn_save.on_click(self.on_save)

        self.btn_save_local = Button(label="Salvare doar local", width=50)
        self.btn_save_local.on_click(self.on_save_local)

        columns = list()
        for col in self.df_rfm.columns:
            if col == self.cfc:
                tblcol = TableColumn(field = col, title = col,
                                     width = 200,
                                     formatter=StringFormatter(font_style="bold")
                                     )
            else:
                tblcol = TableColumn(field = col, title = col,
                                     width = 50,
                                     formatter=NumberFormatter(format="0[.]00")
                                     )
            columns.append(tblcol)


        self.data_table = DataTable(source=self.cds_select, 
                                    columns = columns, 
                                    width=1200, height=500,
                                    fit_columns = False)
        
        
        self.cb_select_k = Select(title="Vizualizare segment:", 
                                    value=self.current_segment, 
                                    options=self.segments)
        self.cb_select_k.on_change("value",self.on_view_cluster)


        self.cb_scaling = Select(title="Metoda de scalare/normalizare:", 
                                    value="MinMax", 
                                    options=['MinMax','ZScore'])
        self.cb_scaling.on_change("value",self.on_cb_scaling)

        
        self.TableViewText = Div(text = "N/A")
        
        self.ckbox_transf = CheckboxGroup(labels = ["F1 LogTransform",
                                                    "F2 LogTransform",
                                                    "F3 LogTransform"],
                                          active = self.scale_cf)
        self.ckbox_transf.on_click(self.on_log_check)



        self.text_ClusterName = TextInput(value=self.cluster_config['Name']+
                                     " ({} segments)".format(self.nr_clusters),
                                     title="Nume model:"
                                     ,width = 400)

        self.cb_ClusterGrade= Select(title="Calitatea output:", 
                                    value='PRODUCTION', 
                                    width = 300,
                                    options=['TESTS','PRODUCTION'])
        
        self.text_ClusterDesc = TextInput(value=self.ClusterDescription,
                                     title="Descriere:"
                                     ,width = 700)
        
        
        
        self.text_Author = TextInput(value='Andrei Ionut Damian',
                                     title="Autor:")

        self.text_Algorithm = TextInput(value=self.sAlgorithm,
                                     title="Algoritm:", width = 700)
        
        self.text_F1 = TextInput(value=self.cluster_config['Fields'][0],
                                     title="Descriere camp F1:")
        
        self.text_F2 = TextInput(value=self.cluster_config['Fields'][1],
                                     title="Descriere camp F2:")

        self.text_F3 = TextInput(value=self.cluster_config['Fields'][2],
                                     title="Descriere camp F3:")
        
        ##
        ## done with the controls
        ##
        
        ##
        ## now to layout preparation
        ##
        self.row_btns = row(self.btn_reset, self.btn_save)
        
        self.mytabs = list()
        
        # tab1: clusters
        
        self.tab1_col1_controls = column(widgetbox(self.slider),
                                   widgetbox(self.cb_select_rfm),
                                   widgetbox(self.slider_tree),    
                                   widgetbox(self.cb_scaling),
                                   widgetbox(self.ckbox_transf),
                                   self.DivText1, 
                                   self.DivText2, 
                                   self.btn_reset,
                                   self.text_ClusterName,
                                   widgetbox(self.cb_ClusterGrade),
                                   self.text_Author
                                   )
        
        self.tab1_col2_controls = column(self.cluster_figure,
                                         self.text_ClusterDesc,
                                         self.text_Algorithm,
                                         self.text_F1,
                                         self.text_F2,
                                         self.text_F3,
                                         self.btn_save,
                                         self.btn_save_local)

        self.tab1_layout = row(self.tab1_col1_controls,self.tab1_col2_controls)
        
        self.tab1 = widgets.Panel(child=self.tab1_layout, 
                                  title='Segmentare')
        self.mytabs.append(self.tab1)

        #tab 2 table view
        self.tab2_controls = row(widgetbox(self.slider_table),
                                 widgetbox(self.cb_select_k),
                                 self.TableViewText)
        self.tab2_layout = column(self.tab2_controls,
                                  self.data_table) 
        
        self.tab2 = widgets.Panel(child=self.tab2_layout, 
                                  title='Vizualizare date')
        self.mytabs.append(self.tab2)


        # tab 3 tree view
        
        #self.tab3_empty_tree_fig =  figure(x_range=(0,40), y_range=(0,10))

        self.DivTreeImage = Div(text = "") 
        dt3 = "Arborele de decizie al segmentarii clientilor."
        #dt3+= " 'Value' reprezinta numarul de elemente din clasele {}".format(self.class_names)
        self.DivText3 = Div(text =  dt3, width=1000, height=50)
        self.tab3_layout = column(self.DivText3,
                                  self.DivTreeImage)
        
        self.tab3 = widgets.Panel(child=self.tab3_layout, 
                                  title='Vizualizare arbore')
        self.mytabs.append(self.tab3)
        
        
        
        # finalizare layout
        self.update_png()
        self.update_texts(self.last_tree_accuracy, 
                          self.last_clustering_error)


        self.final_layout = widgets.Tabs(tabs = self.mytabs)  
        
        self._logger("!!! DONE LAYOUT PREPARATION.\n")
        
        if self.FastSave:
            self.on_save()        
            self._logger("SHUTTING DOWN ...")
            os.kill(os.getpid(),9)
        
        return


if __name__ == '__main__':
    engine = HyperloopClusteringServer()
    html_output = os.path.basename(engine.s_prefix+__file__+'.html')
    output_file(html_output)
    show(engine.final_layout)
else:
    engine = HyperloopClusteringServer()
    document = curdoc()
    document.add_root(engine.final_layout)   
    document.title = 'Hyperloop Cluster Server'
    


    
    
    
    