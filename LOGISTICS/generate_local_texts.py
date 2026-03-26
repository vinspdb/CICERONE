from train_test_split import TrainTestBuilder
import pandas as pd
import re
import sys
from tqdm import tqdm
from logistics_template import log_template
import re
import os
from jinja2 import Template
import pickle
import numpy as np

def clean_text(text):
    lines = text.strip().split("\n")
    last_occ = {}
    for line in lines:
        key = line.split()[0]
        last_occ[key] = line
    result = "\n".join(list(last_occ.values()))
    return result

def generate_prefix(timestamp, df_active_order):
    lista_prefix_finale = []
    for x in timestamp:
        df_active_order['start'] = pd.to_datetime(df_active_order['start']).dt.tz_localize(None)
        df_active_order['end'] = pd.to_datetime(df_active_order['end']).dt.tz_localize(None)
        active_orders = df_active_order[(df_active_order['start'] <= x) & (df_active_order['end'] >= x)]['order']

        for order in active_orders:
            filtered = [
                lp for lp, end, idx in zip(lista_prefix_def, end_timestamp, list_idx)
                if end <= x and idx == order
            ]
            if filtered:
                lista_prefix_finale.append(filtered[-1])
    return lista_prefix_finale


if __name__ == '__main__':

    thr = sys.argv[1]
    path_ocel = f'ocel/final_logistics_{thr}.csv'
    ts = TrainTestBuilder(path_ocel, 2000, 1380, .39, 200)

    ocel_log = pd.read_csv(path_ocel)
    train_sampled_timestamps, val_sampled_timestamps, test_sampled_timestamps = ts.timestamps_generator()
    print(len(train_sampled_timestamps), len(val_sampled_timestamps), len(test_sampled_timestamps))
    ocel_log.columns = ocel_log.columns.str.replace(":", "_")
    ocel_log['ocel_timestamp'] = pd.to_datetime(ocel_log['ocel_timestamp'])

    ocel_log.rename(columns={'ocel_type_Customer Order':'ocel_type_Customer_Order', 'ocel_type_Transport Document':'ocel_type_Transport_Document', 'ocel_type_Handling Unit':'ocel_type_Handling_Unit'}, inplace=True)

    df_group = ocel_log.groupby('3', sort=False)
    list_tmp = []
    list_prefix = []
    list_prefix_case_att = []
    all_timestamps = []
    end_timestamp = []
    list_idx = []
    z = 1
    filtered_columns = ['ocel_activity','ocel_type_Customer_Order','ocel_type_Transport_Document','ocel_type_Vehicle','ocel_type_Container','ocel_type_Handling_Unit','ocel_type_Truck','ocel_type_Forklift']
    filtered_columns2 = ['Amount of Goods', 'Amount of Containers', 'Status', 'Amount of Handling Units', 'Weight']

    active_orders_dict = {}
    active_items = []
    file_names = ['object_CustomerOrder','object_Container', 'object_Truck', 'object_TransportDocument', 
                'object_Forklift', 'object_HandlingUnit', 'object_Vehicle']

    folder_path = f"LOGISTICS_CPN/LOGISTICS_{thr}/"

    info_objs = {}
    for file_name in file_names:
        file_path = os.path.join(folder_path, f"{file_name}.csv")
        if os.path.exists(file_path):
            obj_name = file_name.replace('object_', '').lower()
            df_csv = pd.read_csv(file_path, sep=';')
            df_csv.columns = df_csv.columns.str.strip()
            df_csv['ocel_time'] = pd.to_datetime(df_csv['ocel_time'])
            df_csv['ocel_id'] = df_csv['ocel_id'].astype(str).str.strip()
            info_objs[obj_name] = df_csv
        else:
            print(f"Attenzione: {file_path} non trovato.")


    for name, groups in tqdm(df_group, desc="Processing all groups"):
        list_prefix_temp = []
        index_event = 0
        event_past = ''
        event_text = ''
        trace_text = ''

        for i, row in groups.iterrows():
            riga_filtrata = row[filtered_columns]
            celle_valide = riga_filtrata[riga_filtrata.notna() & (riga_filtrata != '')]
            row_as_text = []

            for key, value in celle_valide.items():
                if key not in ['ocel_activity', '0']:
                    obj_name = key.replace('ocel_type_', '').lower().replace('_','')
                    
                    if obj_name not in info_objs:
                        continue

                    info_obj = info_objs[obj_name]

                    search_obj = [s.strip() for s in value.replace('[','').replace(']','').replace("'",'').split(',')]

                    t = pd.Timestamp(row["2"])
                    info_obj['ocel_time'] = pd.to_datetime(info_obj['ocel_time'])

                    risultato_vicino = (
                    info_obj[info_obj['ocel_id'].isin(search_obj)]
                    .assign(delta=lambda x: (x['ocel_time'] - t).abs())
                    .loc[lambda x: x.groupby('ocel_id')['delta'].idxmin()]
                    .drop(columns='delta')
                    )

                    if not risultato_vicino.empty:
                        filtered_cols = [col for col in filtered_columns2 if col in risultato_vicino.columns]
                        if filtered_cols:
                            for m, r in risultato_vicino.iterrows():
                                row_dict = r[filtered_cols].to_dict()
                                ocel_id = r['ocel_id']
                                row_as_text.append(ocel_id + ' ' + ' '.join([f"{k}: {v}" for k, v in row_dict.items()]) + '\n')

            current_activity = row['ocel_activity']
            event_template = Template(log_template[current_activity]['event_template'])
            event_dict_hist = {v: row[v] for v in log_template[current_activity]['event_attribute']}
            
            event_text += event_template.render(event_dict_hist) + '\n'
            trace_text += "".join(list(dict.fromkeys(row_as_text)))
            event_text = re.sub(r"['\"\[\]]", '', event_text)

            list_prefix.append(event_text.rstrip("\n"))
            list_prefix_case_att.append('\n'+trace_text.rstrip("\n"))
            list_tmp.append(row['ocel_timestamp'])
            list_idx.append(z)
            end_timestamp.append(pd.to_datetime(row['ocel_timestamp']))

            index_event += 1
            active_orders_dict[z] = groups.tail(1)['ocel_timestamp'].values[0]

        all_timestamps.append({
            'order': z,
            'start': groups.head(1)['ocel_timestamp'].values[0],
            'end': groups.tail(1)['ocel_timestamp'].values[0]
        })
        z += 1

    list_case_new = []
    for l in list_prefix_case_att:
        righe = l.strip().split('\n')
        righe_uniche = list(dict.fromkeys(righe))
        value = '\n'.join(righe_uniche).replace('nan', 'NA')
        value = clean_text(value)
        list_case_new.append(value)

    list_prefix_new = [] 
    for l in list_prefix:
        list_prefix_new.append("".join(l))

    lista_prefix_def = []
    for x, y in zip(list_prefix_new,list_case_new):
        lista_prefix_def.append(x+'\n'+y)

    df_active_order = pd.DataFrame(all_timestamps).sort_values('end', ascending=True)

    end_timestamp = pd.to_datetime(end_timestamp).tz_localize(None)
    end_timestamp = np.array(end_timestamp.astype(str).tolist())
    list_idx = np.array(list_idx)

    train = generate_prefix(train_sampled_timestamps, df_active_order)
    val = generate_prefix(val_sampled_timestamps, df_active_order)
    test = generate_prefix(test_sampled_timestamps, df_active_order)

    with open(f'ocel/logistics_{thr}/logistics_{thr}_train_local', 'wb') as f:
        pickle.dump(train,f)

    with open(f'ocel/logistics_{thr}/logistics_{thr}_val_local', 'wb') as f:
        pickle.dump(val,f)

    with open(f'ocel/logistics_{thr}/logistics_{thr}_test_local', 'wb') as f:
        pickle.dump(test,f)