# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 21:25:35 2017

@author: Andrei Ionut DAMIAN
@project: HyperLoop RECOMMENDATION MATRIX
"""

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(x, *args, **kwargs):
        return x

import pandas as pd
import numpy as np
from sql_helper import MSSQLHelper, start_timer, end_timer, print_progress


if __name__=="__main__":
    
    attr_list = ['L1_BABY',
                 'L1_COSMETICE',
                 'L1_DERMOCOSMETICE',
                 'L1_PHARMA',
                 'L2_SPECIALE',
                 'L2_OTC',
                 'L2_MEDICATIE_ALTERNATIVA',
                 'L2_COSMETICE',
                 'L2_DERMOCOSMETICE',
                 'L2_RX',
                 'L2_BABY',
                 'L3_ALTELE',
                 'L3_BABY',
                 'L3_BARBATI',
                 'L3_BATISTE',
                 'L3_CADOURI',
                 'L3_COPII',
                 'L3_COSMETICE',
                 'L3_FRUMUSETE',
                 'L3_FRUMUSETE_PRE_NATAL',
                 'L3_GENERICE',
                 'L3_GENERICE_OTC',
                 'L3_HOMEOPATIE',
                 'L3_N_SISTEMUL_NERVOS',
                 'L3_OFERTE',
                 'L3_ORFAN',
                 'L3_ORIGINALE',
                 'L3_PROMO',
                 'L3_R_APARAT_RESPIRATOR',
                 'L3_S_ORGANE_SENZITIVE',
                 'L3_SANATATE',
                 'L3_SPECIALE',
                 'L3_V_VARIA',
                 'L3_X_ALTELE',
                 'L4_PENTRU_EA',
                 'L4_SAMPON_USCAT',
                 'L4_APIVITA',
                 'L4_OFERTE',
                 'L4_CADOURI',
                 'L4_MELVITA',
                 'L4_V07_NETERAPEUTICE',
                 'L4_STREPSILS_LEMON_24CPR___valid',
                 'L4_S01_PRODUSE_OFTALMOLOGICE',
                 'L4_INGRIJIRE_PERSONALA',
                 'L4_B_SANGE_SI_ORGANE_HEMATOPOETICE',
                 'L4_CADOURI_MIXTE',
                 'L4_N06_PSIHOANALEPTICE',
                 'L4_BARBATI',
                 'L4_R06_ANTIHISTAMINICE_DE_UZ_SISTEMIC',
                 'L4_TESTERE',
                 'L4_STYLIST',
                 'L4_S_ORGANE_SENZITIVE',
                 'L4_INGRIJIRE_CORP',
                 'L4_V_VARIA',
                 'L4_V07AY_ACCESORII_MEDICALE',
                 'L4_R05_PREP_PT_TRAT_TUSEI',
                 'L4_INGRIJJIRE_FATA',
                 'L4_N05_PSIHOLEPTICE',
                 'L4_XRNT_TINCTURI_HOMEOPATE',
                 'L4_HRANA',
                 'L4_BAIE',
                 'L4_D_PREPARATE_DERMATOLOGICE',
                 'L4_TESTE',
                 'L4_C_SISTEM_CARDIOVASCULAR',
                 'L4_M_SISTEMUL_MUSCULO_SCHELETIC',
                 'L4_INGRIJIRE_INTIMA',
                 'L4_COPII',
                 'L4_JUCARII',
                 'L4_A_TRACTUL_DIGESTIV__SI_METABOLISM',
                 'L4_CASETE',
                 'L4_CD_URI',
                 'L4_PARFUMERIE',
                 'L4_INGRIJIRE_PICIOARE',
                 'L4_N02_ANALGEZICE',
                 'L4_INGRIJIREA_FETEI',
                 'L4_ACCESORII_CASA',
                 'L4_INCALTAMINTE_SI_ACCESORII',
                 'L4_PROTECTIE_SEXUALA',
                 'L4_A_TRACTUL_DIGESTIV_SI_METABOLISM',
                 'L4_BARBIERIT',
                 'L4_INGRIJIRE_SOLARA',
                 'L4_DISPOZITIVE_MEDICALE',
                 'L4_PENTRU_COPII',
                 'L4_B_SANGE_SI_ORG_HEMATOPOETICE',
                 'L4_XRNP_HOMEOPATE_PICATURI',
                 'L4_MOSTRE',
                 'L4_OCHELARI',
                 'L4_N_SISTEMUL_NERVOS',
                 'L4_HYLO_COMOD_10ML_(CROMA)___inactiv',
                 'L4_PROMO_LS___inactiv',
                 'L4_J_ANTIINFECTIOASE_DE_UZ_SISTEMIC',
                 'L4_FRUMUSETE',
                 'L4_STREPSILS_MENTOL_12___valid',
                 'L4_R_APARATUL_RESPIRATOR',
                 'L4_ACCESORII',
                 'L4_V03_HOMEOPATIE',
                 'L4_MAKE_UP',
                 'L4_CETEBE_500MG_60CPS_(GLAXO)___valid',
                 'L4_HISTAMINUM_5CH_(BOIRON)___inactiv',
                 'L4_R_APARAT_RESPIRATOR',
                 'L4_INGRIJIREA_PARULUI',
                 'L4_POS',
                 'L4_INGRIJIRE_PAR',
                 'L4_INGRIJIRE_DENTARA',
                 'L4_P_PRODUSE_ANTIPARAZITARE',
                 'L4_V06_DIETETICE',
                 'L4_R02_PREP_PTR_ZONA_ORO_FARINGIANA',
                 'L4_S02_PREPARATE_PTR_URECHI',
                 'L4_CARTI',
                 'L4_INGRIJIREA_CORPULUI',
                 'L4_M_SIST_MUSCULO_SCHELETIC',
                 'L4_INGRIJIRE_FATA',
                 'L4_N01_ANESTEZICE',
                 'L4_R01_PREP_NAZALE',
                 'L4_TOALETA_ZILNICA',
                 'L4_ASPABLOD_30CPR_(BIOFARM)___inactiv',
                 'L4_ALTELE',
                 'L4_PUNGI_DE_CADOU',
                 'BRD_BrandGeneral',
                 'BRD_DR__HART',
                 'BRD_NUROFEN',
                 'BRD_APIVITA',
                 'BRD_AVENE',
                 'BRD_L_OCCITANE',
                 'BRD_VICHY',
                 'BRD_BIODERMA',
                 'BRD_LA_ROCHE___POSAY',
                 'BRD_L_ERBOLARIO',
                 'BRD_PARASINUS',
                 'BRD_NESTLE',
                 'BRD_OXYANCE',
                 'BRD_DOPPELHERZ_SYSTEM',
                 'BRD_TRUSSA',
                 'BRD_STREPSILS',
                 'BRD_ASPENTER',
                 'BRD_DUCRAY',
                 'BRD_BATISTE',
                 'BRD_Scholl']
            
    pd.set_option('expand_frame_repr', False)
    np.set_printoptions(edgeitems = 4, 
                        precision = 1,
                        formatter={'float': '{: 0.1f}'.format})
    
    
    p1 = '20160101'
    p2 = '20161230'
    p3 = 1048608
    
    strqry = """
            exec 
                [uspGetPartnerXItemByPeriod] 
                '"""+p1+"""',
                '"""+p2+"""',
                null,null,null,
                """+str(p3)+"""
            """
    strqry = "exec [uspGetPartnerXItemByPeriod] '20160101','20161230',null,null,null,1048608"

    ttt=start_timer()
    LIVE_LOAD = False
    if LIVE_LOAD:
        sql = MSSQLHelper()
        df_tran = sql.Select(strqry)
    else:
        if ('df_tran' in locals()) or ('df_tran' in globals()):
            if df_tran.shape[0]<100:
                df_tran = pd.read_csv('test.csv')
        else:
            df_tran = pd.read_csv('test.csv')

    print("Load: {:.2f}s".format(end_timer(ttt)))
    x = np.array(df_tran[attr_list])
    
    clients = list(df_tran.PartnerId.unique())
    df = pd.DataFrame()
    trans = list()
    
    ttt=start_timer()
    theta_list = list()
    nr_clients = len(clients)
    label_val = 1
    i = 0
    if 0:
        for client in clients:
            i += 1
            df_tran2 = df_tran[df_tran.PartnerId == client]
            trans.append(df_tran2.shape[0])
            theta = np.zeros(len(attr_list))
            for index, row in df_tran2.iterrows():
                arr = np.array(row[attr_list])
                res = (theta.dot(arr) - label_val)*arr
                update = 0.5* res
                theta = theta - update
            theta_list.append(theta)
            if (i%100)==0:
                print_progress("Processed {:.1f}%".format(i/float(nr_clients)*100))
        df['PartnerId'] = clients
        df['Transactions'] = trans

    print("Online rec-by-rec")
    df_tran2 = df_tran[df_tran.PartnerId == clients[0]]
    npdata = np.array(df_tran2[attr_list])
    theta = np.zeros(len(attr_list))
    epochs = df_tran2.shape[0]
    #for i in tqdm(range(epochs), ascii=True):
    for index, row in df_tran2.iterrows():
        arr = np.array(row[attr_list]).astype(float)
        err = (theta.dot(arr) - label_val)**2
        res = (theta.dot(arr) - label_val)*arr
        update = 0.1* res
        print("Error: {:.2f}".format(err))
        print("Theta0: {}".format(theta))
        theta = theta - update
        print("Data:   {}".format(arr))
        print("Update: {}".format(update))
        print("Theta1: {}".format(theta))
        print(" ")
    
    print("Batch")
    for epoch in range(5):
        #train each epoch
        print("Training epoch {}".format(epoch))

        
    print("Train: {:.2f}s".format(end_timer(ttt)))

    

    
    
    