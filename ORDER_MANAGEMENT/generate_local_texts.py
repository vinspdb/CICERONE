from train_test_split import TrainTestBuilder
import pandas as pd
import re
import pm4py
import sqlite3
from tqdm import tqdm
from order_template import log_template
import os
from jinja2 import Template
import re
import sys
import ast
import numpy as np
import pickle

def process_row(row):
    ids = ast.literal_eval(row["ocel_type_items"])
    names = ast.literal_eval(row["ocel_type_products"])
    
    pairs = list(zip(ids, names))
    
    pairs_sorted = sorted(pairs, key=lambda x: int(x[0].split("-")[1]))

    final_string = ", ".join([f"{new_id} - {name}" for new_id, name in pairs_sorted])
    
    return final_string

def col_names(table_name, cursor):
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns_info = cursor.fetchall()
        column_names = [column[1] for column in columns_info]
        return column_names

def response_generation():
        ocel = pm4py.read_ocel2_sqlite(f'ocel/order_management_{thr}.sqlite')
        packages_flat = pm4py.ocel_flattening(ocel, 'packages')

        conn = sqlite3.connect(f'ocel/order_management_{thr}.sqlite')
        cursor = conn.cursor()

        cursor.execute('SELECT ocel_id FROM object_Items')
        all_items = [a[0] for a in cursor.fetchall()]

        cursor.execute(f"SELECT * FROM object_Packages")
        all_packages = [a[0] for a in cursor.fetchall()]

        active_packages_dict = {}
        for package in all_packages:
            temp = packages_flat[packages_flat['case:concept:name'] == package]
            active_packages_dict[package] = temp.iloc[-1,1]

        items_flat = pm4py.ocel_flattening(ocel, 'items')

        active_item_dict = {}
        for item in all_items:
            temp = items_flat[items_flat['case:concept:name'] == item]
            active_item_dict[item] = temp[temp["concept:name"] == "package delivered"].iloc[-1,1]
    
        cursor.execute(f"SELECT * FROM object_Packages")
        all_packages = [a[0] for a in cursor.fetchall()]

        package_items_match = {package : [] for package in all_packages}

        table = 'object_object' 
        cols = col_names(table, cursor)
        for package in all_packages:
            cursor.execute(f"SELECT {cols[1]} FROM {table}  WHERE {cols[0]} = '{package}' AND {cols[-1]} = 'contains'")
            package_items_match[package] = [a[0] for a in cursor.fetchall()]

        table = "object_Items"
        cols = col_names(table, cursor)
        cursor.execute(f"SELECT {cols[0]} FROM {table}")
        item_to_order_package = {a[0] : 0 for a in cursor.fetchall()}
        for item in all_items:
            table = "object_object"
            cols = col_names(table, cursor)
            cursor.execute(f"SELECT {cols[0]},{cols[-1]} FROM {table} WHERE {cols[1]} = '{item}'")
            temp = cursor.fetchall()
            item_to_order_package[item] = [int(temp[0][0][-4:]), temp[1][0]]

        packages_time_table = []
        table = "event_CreatePackage"
        cols = col_names(table, cursor)

        package_to_orders = {package : [] for package in all_packages}
        for package in all_packages:
            items = package_items_match[package]
            orders = [item_to_order_package[item][0] for item in items]
            temp = []
            for order in orders:
                temp.append(ocel_log[ocel_log['3'] == order].iloc[-1,2])
        
            package_to_orders[package] = max(temp)

        for package in all_packages:
            cursor.execute(f"SELECT {cols[1]} FROM {table}  WHERE {cols[0]} LIKE '%{package}'")
            temp = cursor.fetchall()[0][0]
            packages_time_table.append([package, temp, package_to_orders[package]])

        packages_time_table_df = pd.DataFrame(packages_time_table)

        return active_item_dict, active_packages_dict, packages_time_table_df


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
    path_ocel = f'ocel/final_order_management_{thr}_filtered.csv'
    ts = TrainTestBuilder(path_ocel, 2000, 1220, .45, 200)

    ocel_log = pd.read_csv(path_ocel)
    train_sampled_timestamps, val_sampled_timestamps, test_sampled_timestamps = ts.timestamps_generator()

    print(len(train_sampled_timestamps), len(val_sampled_timestamps), len(test_sampled_timestamps))
    ocel_log.columns = ocel_log.columns.str.replace(":", "_")

    ocel_log["item_product"] = ocel_log.apply(process_row, axis=1)

    df_group = ocel_log.groupby('3', sort=False)
    list_tmp = []
    list_prefix = []
    list_prefix_case_att = []
    all_timestamps = []
    end_timestamp = []
    list_idx = []
    z = 1
    filtered_columns = ['ocel_type_orders', 'ocel_activity','ocel_type_products','ocel_type_employees','ocel_type_packages','ocel_type_customers']
    filtered_columns2 = ['weight', 'price', 'role']


    event_object_df = pd.read_csv(f'ORDER_MANAGEMENT_CPN/ORDER_MANAGEMENT_{thr}/event_object.csv', sep=';')
    product_object_df = pd.read_csv(f'ORDER_MANAGEMENT_CPN/ORDER_MANAGEMENT_{thr}/object_Products.csv', sep=';')
    dist_prod = product_object_df['ocel_id'].unique().tolist()


    event_object_df.columns = event_object_df.columns.str.replace(":", "_")

    active_orders_dict = {}
    active_items = []
    file_names = ['object_Orders', 'object_Items', 'object_Employees', 
                'object_Customers', 'object_Packages', 'object_Products']

    folder_path = f"ORDER_MANAGEMENT_CPN/ORDER_MANAGEMENT_{thr}/"

    info_objs = {}
    for file_name in file_names:
        file_path = os.path.join(folder_path, f"{file_name}.csv")
        if os.path.exists(file_path):
            obj_name = file_name.replace('object_', '').lower()
            df_csv = pd.read_csv(file_path, sep=';')
            df_csv.columns = df_csv.columns.str.strip()
            df_csv['ocel_id'] = df_csv['ocel_id'].astype(str).str.strip()
            info_objs[obj_name] = df_csv
        else:
            print(f"{file_path} not found.")

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
                if key not in ['ocel_activity', '0', 'ocel_type_customers']:
                    obj_name = key.replace('ocel_type_', '').lower()
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
            event_dict_hist = {}
            
            for v in log_template[current_activity]['event_attribute']:
                            value = row[v]
                            if isinstance(value, str):
                                value = re.sub(r"p-\w+", "package", value)
                                event_dict_hist[v] = value.replace(',','')
                            else:
                                event_dict_hist[v] = value
            event_text = event_text + event_template.render(event_dict_hist) + '\n'
            trace_text = trace_text + "".join(list(dict.fromkeys(row_as_text)))
            event_text = re.sub(r"['\"\[\]]", '', event_text)
            list_prefix.append(event_text.rstrip("\n") )
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
        value = '\n'.join(righe_uniche)
        value = re.sub(r"o-\w+", "order", value)
        value = re.sub(r"p-\w+", "package", value)
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

    active_item_dict, active_packages_dict, packages_time_table_df = response_generation()

    train  = generate_prefix(train_sampled_timestamps, df_active_order)
    val  = generate_prefix(val_sampled_timestamps, df_active_order)
    test = generate_prefix(test_sampled_timestamps, df_active_order)


    with open(f'ocel/order_management_{thr}/order_management_{thr}_train_local', 'wb') as f:
        pickle.dump(train,f)

    with open(f'ocel/order_management_{thr}/order_management_{thr}_val_local', 'wb') as f:
        pickle.dump(val,f)

    with open(f'ocel/order_management_{thr}/order_management_{thr}_test_local', 'wb') as f:
        pickle.dump(test,f)

