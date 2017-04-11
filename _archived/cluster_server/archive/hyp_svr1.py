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
from sklearn.model_selection import train_test_split

from bokeh.io import curdoc, output_file, show
from bokeh.plotting import figure
from bokeh.plotting import ColumnDataSource
from bokeh.layouts import column
from bokeh.layouts import row


from bokeh.models import Button
from bokeh.models import OpenURL
from bokeh.models import HoverTool


from bokeh.models.glyphs import ImageURL
from bokeh.models.widgets import DataTable
from bokeh.models.widgets import TableColumn
from bokeh.models.widgets import StringFormatter
from bokeh.models.widgets import NumberFormatter
from bokeh.models.widgets import Div
from bokeh.models.widgets import Select

from bokeh.models import widgets 

from bokeh.layouts import gridplot

from bokeh.layouts import widgetbox 
from bokeh.models import Slider 


from bokeh.models import CategoricalColorMapper

from bokeh.palettes import Set1_8 as pal

from datetime import datetime as dt

from skimage.io import imread as SKImage
import matplotlib.image as mat_Image

import pydotplus 

import os

class HyperloopClusteringServer:
    def __init__(self):
        
        self.s_prefix = dt.strftime(dt.now(),'%Y%m%d')
        self.s_prefix+= "_"
        self.s_prefix+=dt.strftime(dt.now(),'%H%M')
        self.s_prefix+= "_"
        print(self.s_prefix)
        print("__name__: {}".format(__name__))
        print("__file__: {}".format(__file__))

    


    def update_png(self, s_png):
        print("Update tree view...")
        
        
        img = mat_Image.imread(s_png)
        img = (img*255).astype(np.uint8)
        N, M, _ = img.shape
        img = img.view(dtype=np.uint32).reshape((N, M)) #convert image NxM dtype=uint32
        
        img = img[::-1]
        print('IMG SHAPE {}'.format(img.shape))
        y_rng = 10
        xfact = float(M/N)
        x_rng = int(y_rng * xfact)
        print("X:{} Y:{}".format(x_rng,y_rng))    
        fig = figure(x_range=(0,x_rng), y_range=(0,y_rng))
        # prepare tree display tab
    
        fig.plot_width = 900
        fig.plot_height = int(fig.plot_width / xfact)
        # done tree display tab
        fig.image_rgba(image=[img], x=0, y=0, dw=x_rng, dh=y_rng)
        self.tab2_layout.children[1] = fig
        update_cluster_image(current_cluster_view)
        return img

    def save_tree(self,tree_clf):
        tree_fields = self.origin_fields
        print("Saving tree...")
        dot_data = export_graphviz(tree_clf, 
                                    out_file=None, 
                                    feature_names=tree_fields,  
                                    class_names=True,  
                                    filled=True, 
                                    rounded=True,  
                                    special_characters=True)  
        graph = pydotplus.graph_from_dot_data(dot_data)
        graph.write_png('tree.png')
        return graph

    def train_tree(self,df_tree_data, predictors, label_column, nr_lvl):
        
        print("Training tree with {} levels...".format(nr_lvl))
        X = df_tree_data.loc[:,predictors]
        print(" Tree dataset columns: {}".format(df_tree_data.columns))
        y = df_tree_data[label_column]
        X_train, X_test, y_train, y_test = train_test_split(X,y,test_size=0.15)
        
        # now train decision tree    
        clf_tree = DecisionTreeClassifier(max_depth = nr_lvl)
        clf_tree.fit(X_train,y_train)
        print(" Tree: {}".format(clf_tree))
        
        # predict 
        y_hat = clf_tree.predict(X_test)
        acc = np.sum(y_hat == y_test) / (float(y_test.size)) * 100
        s_acc = " Tested prediction on model unseen examples: {:.2f}%".format(acc)   
        print(s_acc)    
        
        # now save constructed tree
        self.save_tree(clf_tree)
        return clf_tree, acc

    def generate_clusters(self, df_data, nr_clusters, 
                          predictors, cluster_column):
        print("Clustering...")
        X = df_data.loc[:,predictors]
        # unsupervised classification
        clf = KMeans(n_clusters = nr_clusters, n_jobs=-1)
        clf.fit(X)
        # attach labels to model data and prepare DecisionTree train/test
        df_data[cluster_column]= clf.labels_
    
        print(" Clusters discovered: {}".format(np.unique(df_data[cluster_column])))
        print(" Clusters inertia: {:.1f}".format(clf.inertia_))
        
        
        return df_data, clf

    
    def update_texts(self, txt1,txt2, acc, nr_s, nr_cl, inert):
        t1 = "<strong>Acuratetea: {:.2f}%</strong>".format(acc)
        t1+= "<br><i>Acuratetea de identifiare a clusterului pentru clientii noi cu"
        t1+= " ajutorul unui arbore de decizie calculata pentru un esantion"
        t1+= " out-of-sample de {} clienti </i>".format(nr_s)
        t2 = "<strong>Eroarea: {:.1f} </strong>".format(inert)
        t2+= "<br><i>Inertia totala clusterelor (eroarea de clusterizare) "
        t2+= "pentru cazul cu {} clustere</i> ".format(nr_cl)    
        
        txt1.text = t1
        txt2.text = t2
        return
    
    def initialize_data(self):

        self.df_rfm = pd.read_csv('rfm_data.csv')
        #self.df_rfm.sort_values('M_TotSum')
        
        self.cID = 'CustID'
        
        self.cf1 = 'RecencyScore'
        self.cf2 = 'NrTransactions'
        self.cf3 = 'TotalValue'
        self.cf3_log = 'LogTotalValue'
        self.cfc = 'Segment'
        self.nr_cl = 4
        self.nr_tree_lvl = 3
        self.levels = ['AVERAGE','GOOD','VIP','LOW'] 
        
        
                
        self.origin_fields = [self.cf1,self.cf2,self.cf3]
        self.scale_fields  = [self.cf1,self.cf2,self.cf3_log]
        self.cluster_fields = ['R_Score','F_Score','M_Score']
        
        
        
        # log transform total value
        self.df_rfm[self.cf3_log] = np.log(self.df_rfm[self.cf3])
        
        if True:
            # use MinMax scaler
            minmaxScaler = preprocessing.MinMaxScaler()
            np_arr = minmaxScaler.fit_transform(self.df_rfm[self.scale_fields])
        else:
            # user Standardization scaler (X-mean)/std
            np_arr = preprocessing.scale(self.df_rfm[self.scale_fields])
        
        print("Initial object shape: {}".format(self.df_rfm.shape))
        self.df_rfm = pd.concat([self.df_rfm, 
                                 pd.DataFrame(data = np_arr, 
                                              columns = self.cluster_fields )
                                 ],
                                 axis = 1)
        print("Final object shape: {} \n columns:{}".format(self.df_rfm.shape, 
                                                            self.df_rfm.columns))



        res_set = self.generate_clusters(self.df_rfm,
                                         nr_clusters =self.nr_cl,
                                         predictors = self.cluster_fields,
                                         cluster_column = self.cfc)

        self.df_rfm, clf = res_set

        clf_tree, acc = self.train_tree(df_rfm, 
                                       predictors = self.origin_fields, 
                                       label_column = self.cfc, 
                                       nr_lvl = self.nr_tree_lvl)  
    
        labels = list(np.unique(df_rfm[self.cfc]))
    
        self.color_mapper = CategoricalColorMapper(
                                                   factors = list(range(8)),
                                                   palette = pal)

# save
df_rfm.to_csv(s_prefix+'rfm_data_clusters.csv')
# done saving    
   
cds = ColumnDataSource(df_rfm)

# Make a slider object: slider
slider = Slider(start=2, end=8,step=1,value=nr_cl, 
                title='Numarul de clustere:',
                width = 200)


nr_customers = df_rfm.shape[0]
nr_sample = int(nr_customers * 0.15)
c_clust = nr_cl

text1 = Div(text = "")
text2 = Div(text = "")

update_texts(text1,text2,
             acc, nr_sample,
             c_clust, clf.inertia_)


# Define the callback function: update_plot
def update_plot(attr, old, new):
    c_clust = new
    print("From cluster:{} to cluster:{} ...".format(old,new))
    # frop cluster columns
    df_new = cds.to_df()
    # now generate new cluster
    res_set = generate_clusters_and_test(df_new,
                                          nr_clusters = c_clust,
                                          predictors = cluster_fields,
                                          cluster_column = cfc)
    


    df_rfm, clf, clf_tree, c_acc = res_set
    # now update only cluster column
    cds.data[cfc] = np.array(df_rfm[cfc])
    
    update_texts(text1,text2,
                 c_acc, nr_sample,
                 c_clust, clf.inertia_)
    
    update_png('tree.png')

    print("Done from cluster:{} to cluster:{}".format(old,new))
    
# Attach the callback to the 'value' property of slider
slider.on_change('value',update_plot)

slider_tree = Slider(start=2, end=4,step=1,value=nr_tree_lvl, 
                     title='Numarul de nivele arbore:',
                     width = 200)

def update_tree_lvl(attr, old, new):
    nr_tree_lvl = new
    train_tree(df_rfm, 
               predictors = origin_fields, 
               label_column = cfc,
               nr_lvl = nr_tree_lvl)
    
    update_png('tree.png')    
    return

slider_tree.on_change('value',update_tree_lvl)

cds_select = ColumnDataSource(df_rfm.head(10))
print("Using data: {}".format(df_rfm.head(10).shape))
    
slider_table = Slider(start=10, end=1000,step=10,value=10, 
                      title='Numarul de inregistrari',
                      width = 200)

def update_table(attr, old, new):
    print("Using data: {}".format(df_rfm.head(new).shape))
    cds_select.data = cds_select.from_df(df_rfm.head(new))
    return

def update_cluster_image(new_option):
    
    if new_option=="R/F":
        fi1 = 0
        fi2 = 1
    elif new_option == "R/M":
        fi1 = 0
        fi2 = 2    
    else:
        fi1 = 1
        fi2 = 2

    fldX = cluster_fields[fi1]
    fldY = cluster_fields[fi2]                

    sTitle = 'Graficul RFM cu {} vs. {} pentru {} clustere '.format(fldX,
                                                                fldY,nr_cl)
    sTitle+= ' ({} clienti)'.format(df_rfm.shape[0])
    print(sTitle)
    cluster_figure.xaxis.axis_label = fldX
    cluster_figure.yaxis.axis_label = fldY
    cluster_figure.title.text = sTitle
    
    glph=cluster_figure.circle(fldX,fldY,source = cds, alpha = 0.5, size=2, 
              color=dict(field=cfc,transform=color_mapper),
              selection_color = 'gray', nonselection_alpha = 0.1, 
              legend = cfc)   
    return glph

    
slider_table.on_change('value',update_table)

def select_callback(attr,old,new):
    
    current_cluster_view = new
    update_cluster_image(current_cluster_view)
    
    return
    

cb_select = Select(title="Option:", value="R/F", options=["R/F", "R/M", "F/M"])

cb_select.on_change('value',select_callback)





cluster_figure = figure(tools='box_select',
                           plot_width=400, plot_height=400)

current_cluster_view = "R/F"

glph = update_cluster_image(current_cluster_view)

empty_selection = glph.data_source.selected

def btn_callback():
    cds.selected = empty_selection
    
print("   {}".format(cluster_figure))

columns = list()
for col in df_rfm.columns:
    if col == cfc:
        tblcol = TableColumn(field = col, title = col,
                             formatter=StringFormatter(font_style="bold")
                             )
    else:
        tblcol = TableColumn(field = col, title = col,
                             formatter=NumberFormatter(format="0[.]00")
                             )
    columns.append(tblcol)


data_table = DataTable(source=cds_select, 
                       columns=columns, 
                       width=900, height=200,
                       fit_columns = True)

btn_reset = Button(label="Deselectare", width=20)


btn_reset.on_click(btn_callback)


hover = HoverTool(
        tooltips=[
                  ("Segment", "@"+cfc),
                  ("Client", "@"+cID),
                  ("Tranzactii", "@NrTransactions"),
                  ("Valoare Totala", "@TotalValue"),                  
                  ]
                  )

cluster_figure.add_tools(hover)

## layout preparation

col_controls = column(widgetbox(slider),
                      widgetbox(cb_select),
                      widgetbox(slider_tree), 
                      widgetbox(slider_table),
                      text1, 
                      text2, 
                      btn_reset)

upper_layout = row(col_controls,cluster_figure)

tab1_layout = column(upper_layout, data_table) # , img)



mytabs = list()
tab1 = widgets.Panel(child=tab1_layout, title='Procesare')
mytabs.append(tab1)

tab2_empty_tree_fig = figure(x_range=(0,40), y_range=(0,10))
text3 = Div(text = "Arborele de decizie al segmentarii clientilor")
tab2_layout = column(text3,tab2_empty_tree_fig)
tab2 = widgets.Panel(child=tab2_layout, title='Vizualizare arbore')
mytabs.append(tab2)
update_png('tree.png')

final_layout = widgets.Tabs(tabs = mytabs)  


if __name__ == '__main__':
    html_output = os.path.basename(s_prefix+__file__+'.html')
    output_file(html_output)
    show(final_layout)
else:
    document = curdoc()
    document.add_root(final_layout)   
    document.title = 'RFM Server'
    


    
    
    
    