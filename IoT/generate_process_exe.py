import pandas as pd
import numpy as np
import sqlite3
from collections import defaultdict
import copy
import re
import sys
import pandas as pd
import pm4py
import sys

class SingleGraphsGenerator():
    def __init__(self, noise):
        self.noise = noise
        self.ocel_path = f"ocel/CargoPickup_IoT_{noise}.sqlite"
        conn = sqlite3.connect(self.ocel_path)
        self.cursor = conn.cursor()
        self.num_vp_obj = 2000
    
    def preprocessing_steps(self):
        table = 'event_map_type'
        self.cursor.execute(f"SELECT * FROM {table}")
        self.all_events = {a[1] : i for a,i in zip(self.cursor.fetchall(), range(14))}

        self.cursor.execute("SELECT * FROM object_Pickupplan")
        self.all_pickups = sorted([a[0] for a in self.cursor.fetchall()])
        self.all_pickups = sorted(self.all_pickups, key=lambda x: int(x[3:]))

    def col_names(self, table_name, cursor):

        cursor.execute(f"PRAGMA table_info({table_name});")
        columns_info = self.cursor.fetchall()
        column_names = [column[1] for column in columns_info]

        return column_names
    def is_date_in_interval(self, date, intervals):
        for interval in intervals:
            start_date = interval[0]
            end_date = interval[1]
            if start_date <= date <= end_date:
                return True
        return False
    def query_fort_trucks(self, pickup, truck):    
        query = f"""WITH PickupRows AS (
            SELECT *
            FROM object_Truck
            WHERE ocel_id = '{truck}' AND `Pickup Plan ID` = '{pickup}'
        ),
        ClosestAvailable AS (
            SELECT
                t1.ocel_time AS pickup_time,
                t1.`Pickup Plan ID`,
                (SELECT t2.ocel_time
                FROM object_Truck t2
                WHERE t2.ocel_id = t1.ocel_id
                AND t2.`Truck Status` = 'Available'
                AND t2.ocel_time > t1.ocel_time
                ORDER BY t2.ocel_time ASC
                LIMIT 1) AS available_time,
                (SELECT t2.`Truck Status`
                FROM object_Truck t2
                WHERE t2.ocel_id = t1.ocel_id
                AND t2.`Truck Status` = 'Available'
                AND t2.ocel_time > t1.ocel_time
                ORDER BY t2.ocel_time ASC
                LIMIT 1) AS available_status
            FROM PickupRows t1
        )
        SELECT *
        FROM ClosestAvailable
        ORDER BY pickup_time"""
        return query
    
    def generate_events(self, pickup):
        self.cursor.execute(f"SELECT * FROM object_object WHERE ocel_source_id = '{pickup}'")
        cargo = self.cursor.fetchall()[0][1]

        self.cursor.execute(f"SELECT * FROM event_object WHERE ocel_event_id = 'assign_trs_{pickup}'")
        trucks = [a[1] for a in self.cursor.fetchall()]
        self.cursor.execute(f"SELECT * FROM object_Truck WHERE ocel_id IN {tuple(trucks)} AND [Pickup Plan ID] = '{pickup}'")
        first_last_truck = {tr : [] for tr in trucks}
        for truck in set(trucks):
            self.cursor.execute(self.query_fort_trucks(pickup, truck))
            temp = self.cursor.fetchall()
            for row in temp:
                first_last_truck[truck].append([row[0],row[2]])
        first_last_truck = {k : list(set(map(tuple, v))) for k,v in first_last_truck.items()}

        self.cursor.execute(f"SELECT * FROM event_LodgePickupPlan WHERE ocel_id = 'Lodge_{pickup}'")
        events = [list(self.cursor.fetchall()[0]) + ['LodgePickupPlan']]
        self.cursor.execute(f"SELECT * FROM event_AssignTruck where ocel_id = 'assign_trs_{pickup}'")
        AT = [list(a) for a in self.cursor.fetchall()]

        for row in AT:
            for k,v in first_last_truck.items():
                for b in v:
                    if row[1] == b[0]:
                        row[0] += k
        events.extend([a + ['AssignTruck'] for a in AT])

        for truck in set(trucks):
            already_seen = []
            
            self.cursor.execute(f"SELECT * FROM event_object WHERE ocel_object_id = '{truck}' AND ocel_event_id NOT LIKE 'assign%'")
            temp = [a[0] for a in self.cursor.fetchall()]
            table_1 = "event"
            table_2 = "event_map_type"
            cols_1 = self.col_names(table_1, self.cursor)
            cols_2 = self.col_names(table_2, self.cursor)
            
            self.cursor.execute(f"SELECT {table_1}.{cols_1[0]}, {table_2}.{cols_2[1]} FROM {table_1}\
                            LEFT JOIN {table_2} ON {table_1}.{cols_1[1]} = {table_2}.{cols_2[0]}\
                            WHERE {table_1}.{cols_1[0]} IN {tuple(temp)}")
            tab = self.cursor.fetchall()
            
            for idx,row in enumerate(tab):
                if row[0] not in already_seen:
                    temp = "event_" + row[1]
                    self.cursor.execute(f"SELECT * FROM {temp} WHERE {self.col_names(temp,self.cursor)[0]} = '{row[0]}'")
                    table = self.cursor.fetchall()
                    filtered_events = [list(event) + [row[1]] for event in table if self.is_date_in_interval(event[1], first_last_truck[truck])]
                    events.extend(filtered_events)
                    already_seen.append(row[0])
            
            
            

        events = sorted(events, key=lambda x: x[1])
        return events
    
    def events_to_objects_generator(self,events):
        table = 'event_object'
        cols = self.col_names(table, self.cursor)
        tot_objects = []
        for row in events:
            self.cursor.execute(f"SELECT {cols[1]} FROM {table} WHERE {cols[0]} = '{row[0]}'")
            objects = tuple(set([a[0] for a in self.cursor.fetchall()]))
            tot_objects.append(objects)

        events_by_objects = {}
        for idx, row in enumerate(tot_objects):
            for obj in row:
                if obj not in events_by_objects:
                    events_by_objects[obj] = [idx]
                else:
                    events_by_objects[obj].append(idx)
        return events_by_objects
    
    def generate_adjacency_list_with_k(self, events_by_objects_copy, par):
        """
        Generate an adjacency list from events_by_objects, linking events up to the K-th event.

        Parameters:
        - events_by_objects: dict, mapping objects to their respective events.
        - K: int, maximum index of events to include in the adjacency list.

        Returns:
        - adjacency_list: list of two lists [source_nodes, target_nodes].
        """
    
    
        def generate_consecutive_pairs(events):
            """Generate pairs of consecutive events."""
            pairs = []
            n = len(events)
            for i in range(n - 1):
                pairs.append((events[i], events[i + 1]))
            return pairs
    
        events_by_objects = {}
        for k,v in events_by_objects_copy.items():
            events_by_objects[k] = [a for a in v if a <= par]
        subsequences_by_object = {}
        for obj, events in events_by_objects.items():
            subsequences_by_object[obj] = generate_consecutive_pairs(events)

        event_links = defaultdict(set)
        for subseq in subsequences_by_object.values():
            for e1, e2 in subseq:
                event_links[e1].add(e2)

        source_nodes = []
        target_nodes = []

        # Iterate over the event links and create the source-target pairs
        for source, targets in event_links.items():
            for target in targets:
                source_nodes.append(source)
                target_nodes.append(target)

        return [source_nodes, target_nodes]
    
    def extract_truck_id(self, row):
    # Use a regular expression to find the pattern "tr<number>"
        match = re.search(r"tr(\d+)", row)
        if match:
            return match.group(0)  # Return the full match, e.g., "tr32"
        return None  # Return None if no truck ID is found

    def extract_pcp_id(self, row):
        # Use a regular expression to find the pattern "tr<number>"
        match = re.search(r"Pcp(\d+)", row)
        if match:
            return match.group(0)  # Return the full match, e.g., "tr32"
        return None  # Return None if no truck ID is found

    def extract_silo_id(self, row):
        # Use a regular expression to find the pattern "tr<number>"
        match = re.search(r"Silo(\d+)", row)
        if match:
            return match.group(0)  # Return the full match, e.g., "tr32"
        return None  # Return None if no truck ID is found
    
    def generate_graphs(self, pickup):
        self.cursor.execute(f"SELECT * FROM object_object WHERE ocel_source_id = '{pickup}'")
        cargo = self.cursor.fetchall()[0][1]

        events = self.generate_events(pickup)
        events_by_objects_copy  = copy.deepcopy(self.events_to_objects_generator(events))
        all_graphs = []
        graph = {}
        self.cursor.execute(f"SELECT [Num of trucks], [Total pickup weight] FROM object_Pickupplan WHERE ocel_id = '{pickup}'")
        graph['Pickupplan'] = list(self.cursor.fetchall()[0]) + [pickup]
        graph['Pickupplan_to_Cargo'] = [[0],[0]]

        graph['Pickupplan_to_event'] = [[],[]]
        graph['Cargo_to_event'] = [[0],[0]]

        timestamps = [a[1] for a in events]
        self.cursor.execute(f"SELECT [Cargo Type] FROM object_Cargo WHERE ocel_id = '{cargo}'")
        type = self.cursor.fetchall()[0][0]
        types = ['Wheat','Corn','Potato','Other',"Rice"]
        graph['Cargo'] = [[0] * 6  + [cargo]]
        graph['Cargo'][0][types.index(type)] = 1
        #cursor.execute(f"SELECT [Cargo stock weight(scheduled)] FROM object_Cargo WHERE ocel_id = '{cargo}'")
        #graph['Cargo'][0][-2] = cursor.fetchall()[0][0]
        graph['event'] = []
        for idx,row in enumerate(events):
            temp_ev = [1 if i == self.all_events[row[2]] else 0 for i in range(len(self.all_events))] + [row[0], row[1]]
            graph['event'].append(temp_ev)
            graph['event_to_event'] = self.generate_adjacency_list_with_k(events_by_objects_copy, idx)
            pcp = self.extract_pcp_id(row[0]) 
            if pcp:
                graph['Pickupplan_to_event'][0].append(0)
                graph['Pickupplan_to_event'][1].append(idx)
            self.cursor.execute(f"SELECT [Cargo stock weight(scheduled)] FROM object_Cargo WHERE ocel_id = '{cargo}' AND ocel_time <= '{row[1]}'")
            ws = self.cursor.fetchall()[-1][0]
            graph['Cargo'][0][-2] = ws
            #graph['Cargo'].append(copy.deepcopy(graph['Cargo'][idx]))
            all_graphs.append(graph)
            graph = copy.deepcopy(graph)
        #for e in events:
            #print(e[0])
            #exit()
        return all_graphs, timestamps
    

    def generate_all_graphs(self):
        self.preprocessing_steps()
        all_graphs = []
        all_timestamps = []
        pickup_idx = []
        active_pickups = []
        list_eventi = []
        print("Generating single graphs...")
        for idx,pickup in enumerate(self.all_pickups):
            #print(idx)
            #exit()
            if idx % 200 == 0:
                print(round(idx * 100 / self.num_vp_obj), '%')

            temp = self.generate_graphs(pickup)
            iii = 0
            for t in temp[0]:
                list_eventi.append(t['event'][iii][14])
                iii = iii +1


            all_timestamps.extend(temp[-1])
            
            all_graphs.extend(temp[0])
            
            active_pickups.append([pickup, temp[-1][0], temp[-1][-1]])
            pickup_idx.extend([pickup] * len(temp[-1]))

        all_graphs = np.array(all_graphs)
        all_timestamps = np.array(all_timestamps)
        pickup_idx = np.array(pickup_idx)
        print("Done!")
        return list_eventi, all_timestamps, pickup_idx, active_pickups
    
def merge_ocel(thr):
    ocel_name = f'CargoPickup_IoT_{thr}'
    ocel_sql = pm4py.read.read_ocel2_sqlite(f"ocel/{ocel_name}.sqlite")
    ocel_sql.relations = ocel_sql.relations.drop_duplicates()
    log_logidx = pd.read_csv(f'ocel/processes_{ocel_name}.csv')
    pm4py.write_ocel(ocel_sql, f'ocel/{ocel_name}.csv')
    log_sql_csv = pd.read_csv(f'ocel/{ocel_name}.csv')
    log_logidx['2'] = pd.to_datetime(log_logidx['2'], errors='coerce')
    log_sql_csv['ocel:timestamp'] = pd.to_datetime(log_sql_csv['ocel:timestamp'], errors='coerce').dt.tz_localize(None)
    df_new = pd.merge(
        log_logidx,
        log_sql_csv,
        left_on=['0', '2'],
        right_on=['ocel:eid', 'ocel:timestamp'],
        how='left'
    )
    df_new.to_csv(f'ocel/final_{ocel_name}.csv', index=False)
if __name__ == '__main__':
    noise = sys.argv[1]
    sge = SingleGraphsGenerator(noise)
    list_eventi, all_timestamps, pickup_idx, active_pickups = sge.generate_all_graphs()
    d = {'0': list_eventi, '1': list_eventi,'2': all_timestamps, '3': pickup_idx}
    df = pd.DataFrame(data=d)
    df.to_csv(f'ocel/processes_CargoPickup_IoT_{noise}.csv', index=False)
    import pickle
    with open(f'ocel/active_pickup_CargoPickup_IoT_{noise}', 'wb') as f:
        pickle.dump(active_pickups,f)