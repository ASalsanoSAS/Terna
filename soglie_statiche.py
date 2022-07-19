import pandas as pd
import numpy as np

def event_to_process_generation(event, start_time, end_time, custom_period=True, last_recorded_datetime=None):
    #last_recorded_datetime = event_alg_soglie_dinamiche['end_date'].max()
    if custom_period == True:
        print('Using a custom period...')
        end_time_selected = end_time
        start_time_selected = start_time
        event_to_process = event[(event["receivedOn"]>start_time_selected)&(event["receivedOn"]<end_time_selected)]
    else:
        event_to_process = event[event["receivedOn"]>last_recorded_datetime]   
        end_time_selected = event["receivedOn"].max()
        start_time_selected = last_recorded_datetime
    return event_to_process, last_recorded_datetime, start_time_selected, end_time_selected

def preprocessing(event_to_process, master):
    df_meteo = event_to_process[['posId','TIMESTAMP.value','TEMPERATURE_value.value', 'TEMPERATURE_value.qc','HUMIDITY_value.value','HUMIDITY_value.qc']] 
    #df_meteo = df_meteo.to_frame()
    #df_meteo = pd.DataFrame(df_meteo)
    df_meteo = df_meteo.sort_values(by=['TIMESTAMP.value'])
    df_meteo = df_meteo.dropna(subset=['TEMPERATURE_value.value','HUMIDITY_value.value'], how='all')
    event_ts = event_to_process[['posId','TIMESTAMP.value','receivedOn','clientId','account', 'LOAD_04_A_avg.value', 'LOAD_08_A_avg.value', 'LOAD_12_A_avg.value','LOAD_04_B_avg.value', 'LOAD_08_B_avg.value',
                                 'LOAD_12_B_avg.value', 'LOAD_04_A_avg.qc', 'LOAD_08_A_avg.qc', 'LOAD_12_A_avg.qc', 'LOAD_04_B_avg.qc', 'LOAD_08_B_avg.qc', 'LOAD_12_B_avg.qc','NQC.value']]   
    #xt_ts = event_ts.to_frame()
    #xt_ts = pd.DataFrame(xt_ts) #converto la CASTable_event della terna semplice in DF
    xt_ts = event_ts
    load_ts_list = ['LOAD_04_A_avg.value', 'LOAD_08_A_avg.value', 'LOAD_12_A_avg.value', 'LOAD_04_B_avg.value', 'LOAD_08_B_avg.value', 'LOAD_12_B_avg.value']
    load_ts_qc_list = ['LOAD_04_A_avg.qc', 'LOAD_08_A_avg.qc', 'LOAD_12_A_avg.qc', 'LOAD_04_B_avg.qc', 'LOAD_08_B_avg.qc', 'LOAD_12_B_avg.qc', 'NQC.value','HUMIDITY_value.qc','TEMPERATURE_value.qc']
    xt_ts=xt_ts.sort_values(by=['TIMESTAMP.value'])
    df_meteo=df_meteo.sort_values(by=['TIMESTAMP.value'])
    xt_ts = xt_ts.merge(master, on='posId',how='left')# merge event TS con master con meteo
    xt_ts = pd.merge_asof(xt_ts, df_meteo, on=['TIMESTAMP.value'], direction="nearest",by='posId',allow_exact_matches=True)
    xt_ts[load_ts_qc_list] = np.nan_to_num(xt_ts[load_ts_qc_list] ) 
    
    ###### Preprocessing DOPPIA TERNA ######         
    event_dt = event_to_process[['posId','TIMESTAMP.value','receivedOn','clientId','account','LOAD_04_A_L1_avg.value', 'LOAD_08_A_L1_avg.value', 'LOAD_12_A_L1_avg.value','LOAD_04_B_L1_avg.value',
                                 'LOAD_08_B_L1_avg.value', 'LOAD_12_B_L1_avg.value', 'LOAD_04_A_L2_avg.value', 'LOAD_08_A_L2_avg.value', 'LOAD_12_A_L2_avg.value','LOAD_04_B_L2_avg.value', 
                                 'LOAD_08_B_L2_avg.value', 'LOAD_12_B_L2_avg.value','LOAD_04_A_L1_avg.qc', 'LOAD_08_A_L1_avg.qc', 'LOAD_12_A_L1_avg.qc','LOAD_04_B_L1_avg.qc', 'LOAD_08_B_L1_avg.qc', 'LOAD_12_B_L1_avg.qc',
                                 'LOAD_04_A_L2_avg.qc', 'LOAD_08_A_L2_avg.qc', 'LOAD_12_A_L2_avg.qc','LOAD_04_B_L2_avg.qc', 'LOAD_08_B_L2_avg.qc', 'LOAD_12_B_L2_avg.qc','NQC.value']]
    #xt_dt = event_dt.to_frame()
    #xt_dt = pd.DataFrame(xt_dt) #converto la CASTable_event della terna doppia in DF
    xt_dt = event_dt
    load_dt_list = ['LOAD_04_A_L1_avg.value', 'LOAD_08_A_L1_avg.value','LOAD_12_A_L1_avg.value', 'LOAD_04_B_L1_avg.value', 'LOAD_08_B_L1_avg.value', 'LOAD_12_B_L1_avg.value', 'LOAD_04_A_L2_avg.value', 'LOAD_08_A_L2_avg.value', 
                    'LOAD_12_A_L2_avg.value', 'LOAD_04_B_L2_avg.value','LOAD_08_B_L2_avg.value', 'LOAD_12_B_L2_avg.value']
    load_dt_qc_list = ['LOAD_04_A_L1_avg.qc', 'LOAD_08_A_L1_avg.qc','LOAD_12_A_L1_avg.qc', 'LOAD_04_B_L1_avg.qc', 'LOAD_08_B_L1_avg.qc', 'LOAD_12_B_L1_avg.qc', 'LOAD_04_A_L2_avg.qc', 'LOAD_08_A_L2_avg.qc', 
                       'LOAD_12_A_L2_avg.qc', 'LOAD_04_B_L2_avg.qc','LOAD_08_B_L2_avg.qc', 'LOAD_12_B_L2_avg.qc', 'NQC.value','HUMIDITY_value.qc','TEMPERATURE_value.qc']
    df_meteo=df_meteo.sort_values(by=['TIMESTAMP.value'])                                                       
    xt_dt = xt_dt.sort_values(by=['TIMESTAMP.value'])
    xt_dt = xt_dt.merge(master, on='posId',how='left')
    xt_dt = pd.merge_asof(xt_dt, df_meteo, on=['TIMESTAMP.value'], direction="nearest",by='posId',allow_exact_matches=True)
    xt_dt[load_dt_qc_list] = np.nan_to_num(xt_dt[load_dt_qc_list] ) # converto qc value in zero
    return xt_ts, xt_dt

def anomaly_detection(event_to_process, xt_ts, xt_dt, soglie_statiche_TS, soglie_statiche_DT, anomalia_ts, anomalia_dt):
    if len(xt_ts)> 0: 
        df_ts_processed = pd.merge(xt_ts, soglie_statiche_TS, on='posId', how='left')
        for anomalia in anomalia_ts: 
            df_ts_processed[anomalia] = np.full(shape=len(df_ts_processed), fill_value=13,dtype=object)
            posId = df_ts_processed['posId']
            LOAD_fase = 'LOAD'+anomalia[-5:]+'_avg.value'
            LOAD = df_ts_processed[LOAD_fase]
            LOAD_qc_fase = 'LOAD'+anomalia[-5:]+'_avg.qc'
            LOAD_qc = df_ts_processed[LOAD_qc_fase]
            val_min = df_ts_processed['Val_min']/10
            val_max = df_ts_processed['Val_max']/10
            soglia_max_fase = 'soglia_max'+anomalia[-5:]
            soglia_max = df_ts_processed[soglia_max_fase]
            soglia_min_fase = 'soglia_min'+anomalia[-5:]
            soglia_min = df_ts_processed[soglia_min_fase]
            df_ts_processed[anomalia]= np.where(((LOAD < df_ts_processed['Val_min']/10)|(LOAD > df_ts_processed['Val_max']/10)), 5, 0) #verifico che il tiro sia all'interno delle soglie
            df_ts_processed[anomalia]= np.where((df_ts_processed[anomalia]==0)&(LOAD_qc==0), 0, 5) #verifico che il tiro sia all'interno delle soglie
            df_ts_processed[anomalia]= np.where((df_ts_processed[anomalia]==0)&((LOAD > soglia_max)|(LOAD < soglia_min)), 1, 0) #verifico che il tiro sia all'interno delle soglie
            df_ts_processed["flag"]=np.where(df_ts_processed[LOAD_fase]>1.2*df_ts_processed[LOAD_fase].shift(1),1,0) 
            df_ts_processed["day"]=df_ts_processed['TIMESTAMP.value']
            df_ts_processed["controllo"]=df_ts_processed.groupby(['day','posId']).flag.transform(np.sum)
            df_ts_processed[anomalia]= np.where((df_ts_processed[anomalia]==1)&(df_ts_processed["controllo"]>1), 0, df_ts_processed[anomalia]) #controllo del gradente
            condition = ((df_ts_processed[anomalia]==1)&(df_ts_processed['TEMPERATURE_value.qc']==0)&(df_ts_processed['HUMIDITY_value.qc']==0)&
                        (df_ts_processed['TEMPERATURE_value.value']>-1.5)&(df_ts_processed['TEMPERATURE_value.value']<2)&(df_ts_processed['HUMIDITY_value.value']>90))
            df_ts_processed[anomalia]= np.where(condition, 2, df_ts_processed[anomalia]) # anomalia = manicotto (2)
    else:
        print('Il DF terna semplice e vuoto!')
        for anomalia in anomalia_ts:
            df_ts_processed[anomalia]= np.full(shape=len(df_ts_processed),fill_value = 0, dtype=object)
    if len(xt_dt)>0:
        df_dt_processed = pd.merge(xt_dt, soglie_statiche_DT, on='posId', how='inner')
        for anomalia in anomalia_dt: 
            df_dt_processed[anomalia] = np.full(shape=len(df_dt_processed), fill_value=13,dtype=object)
            posId = df_dt_processed['posId']
            LOAD_fase = 'LOAD'+anomalia[-8:]+'_avg.value'
            LOAD = df_dt_processed[LOAD_fase]
            LOAD_qc_fase = 'LOAD'+anomalia[-8:]+'_avg.qc'
            LOAD_qc = df_dt_processed[LOAD_qc_fase]
            val_min = df_dt_processed['Val_min']/10
            val_max = df_dt_processed['Val_max']/10
            soglia_max_fase = 'soglia_Max'+anomalia[-8:]
            soglia_max = df_dt_processed[soglia_max_fase]
            soglia_min_fase = 'soglia_Min'+anomalia[-8:]
            soglia_min = df_dt_processed[soglia_min_fase]
            df_dt_processed[anomalia]= np.where(((LOAD < df_dt_processed['Val_min']/10)|(LOAD > df_dt_processed['Val_max']/10)), 5, 0) #verifico che il tiro sia all'interno delle soglie
            df_dt_processed[anomalia]= np.where((df_dt_processed[anomalia]==0)&(LOAD_qc==0), 0, 5) #verifico che il tiro sia all'interno delle soglie
            df_dt_processed[anomalia]= np.where((df_dt_processed[anomalia]==0)&((LOAD > soglia_max)|(LOAD < soglia_min)), 1, 0) #verifico che il tiro sia all'interno delle soglie
            df_dt_processed["flag"]=np.where(df_dt_processed[LOAD_fase]>1.2*df_dt_processed[LOAD_fase].shift(1),1,0) 
            df_dt_processed["day"]=df_dt_processed['TIMESTAMP.value']
            df_dt_processed["controllo"]=df_dt_processed.groupby(['day','posId']).flag.transform(np.sum)
            df_dt_processed[anomalia]= np.where((df_dt_processed[anomalia]==1)&(df_dt_processed["controllo"]>1), 0, df_dt_processed[anomalia]) #controllo del gradente
            condition = ((df_dt_processed[anomalia]==1)&(df_dt_processed['TEMPERATURE_value.qc']==0)&(df_dt_processed['HUMIDITY_value.qc']==0)&
                        (df_dt_processed['TEMPERATURE_value.value']>-1.5)&(df_dt_processed['TEMPERATURE_value.value']<2)&(df_dt_processed['HUMIDITY_value.value']>90))
            df_dt_processed[anomalia]= np.where(condition, 2, df_dt_processed[anomalia]) # anomalia = manicotto (2)
    else:
        print('Il DF terna semplice e vuoto!')
        for anomalia in anomalia_ts:
            df_dt_processed[anomalia]= np.full(shape=len(df_ts_processed),fill_value = 0, dtype=object)
    return df_ts_processed, df_dt_processed

def final_output(event_to_process, df_ts_processed, df_dt_processed, anomalia_list_global):                       
    event_processed_ts = df_ts_processed[['posId','TIMESTAMP.value','receivedOn','clientId','account','Descrizione Codice Esterno Linea', 'anomalia_04_A','anomalia_08_A','anomalia_12_A','anomalia_04_B','anomalia_08_B',
                                          'anomalia_12_B', 'LOAD_04_A_avg.value', 'LOAD_08_A_avg.value', 'LOAD_12_A_avg.value', 'LOAD_04_B_avg.value', 'LOAD_08_B_avg.value', 'LOAD_12_B_avg.value']]
    event_processed_dt = df_dt_processed[['posId','TIMESTAMP.value','receivedOn','clientId','account','Descrizione Codice Esterno Linea','anomalia_04_A_L1','anomalia_08_A_L1','anomalia_12_A_L1','anomalia_04_B_L1',
                                          'anomalia_08_B_L1','anomalia_12_B_L1','anomalia_04_A_L2','anomalia_08_A_L2', 'anomalia_12_A_L2','anomalia_04_B_L2','anomalia_08_B_L2','anomalia_12_B_L2',
                                          'LOAD_04_A_L1_avg.value', 'LOAD_08_A_L1_avg.value', 'LOAD_12_A_L1_avg.value','LOAD_04_B_L1_avg.value', 'LOAD_08_B_L1_avg.value', 'LOAD_12_B_L1_avg.value',
                                          'LOAD_04_A_L2_avg.value', 'LOAD_08_A_L2_avg.value', 'LOAD_12_A_L2_avg.value', 'LOAD_04_B_L2_avg.value','LOAD_08_B_L2_avg.value', 'LOAD_12_B_L2_avg.value']]
    event_processed = event_processed_ts.append(event_processed_dt)
    print(len(event_processed))
    event_processed = pd.melt(event_processed,id_vars=['posId','TIMESTAMP.value','receivedOn','clientId','account','Descrizione Codice Esterno Linea'], var_name='anomalia').dropna()
    event_processed = event_processed[(event_processed.anomalia.isin(anomalia_list_global))]
    for anomalia in anomalia_list_global:
        metrica_fase = 'LOAD_'+anomalia[9:]+'_avg.value'
        event_processed['anomalia'] = event_processed['anomalia'].replace({anomalia:metrica_fase})
    event_processed = event_processed.rename(columns = {'anomalia': 'metrica','value':'anomalia'}, inplace = False)
    conteggio_sensori_analizzati = event_processed[['posId','metrica']].drop_duplicates(subset=['posId','metrica']).metrica.count()
    event_processed = event_processed[(event_processed.anomalia == 1)|(event_processed.anomalia == 2)]
    numero_anomalie = event_processed.anomalia.count()
    numero_anomalie_1 = event_processed[(event_processed.anomalia == 1)].anomalia.count()
    print(numero_anomalie_1)
    numero_anomalie_2 = event_processed[(event_processed.anomalia == 2)].anomalia.count()
    return event_processed