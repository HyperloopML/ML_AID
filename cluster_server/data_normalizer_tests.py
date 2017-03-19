# -*- coding: utf-8 -*-
"""
Created on Sat Mar 11 22:22:57 2017

@author: Andrei
"""

from sklearn.preprocessing import normalize
import numpy as np

np.set_printoptions(suppress = True, precision = 2)

a=np.array([
            [1.0,0.0,2.0,3.0],
            [0.1,0.2,3.0,0.0],
            [0.1,0.0,0.0,0.3],       
            [0.1,0.0,0.0,0.0]
            ])

an = normalize(a)