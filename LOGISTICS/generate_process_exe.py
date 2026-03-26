import sqlite3
import pandas as pd
import yaml
import sys
import pandas as pd
import pm4py
import sys

class OCELProcessExtractor:
    def __init__(self, db_path, output_path, config_path):
        self.db_path = db_path
        self.output_path = output_path
        self.config = self._load_config(config_path)
        self.viewpoint_obj = self.config["viewpoint"]
        self.relations = self.config["relations"]
        self.include_objects = set(self.config.get("include_objects", []))

    def _load_config(self, config_path):
        with open(config_path, "r") as f:
            return yaml.safe_load(f)

    def col_names(self, table):
        self.cursor.execute(f"PRAGMA table_info({table});")
        return [c[1] for c in self.cursor.fetchall()]

    def _query_related(self, sources, qualifier, direction):
        """Genera query dinamica forward/backward"""
        if not sources:
            return []
        placeholders = ",".join(["?"] * len(sources))
        if direction == "forward":
            query = f"""
                SELECT ocel_target_id FROM object_object 
                WHERE ocel_source_id IN ({placeholders}) 
                AND ocel_qualifier = ?
            """
        else:  # backward
            query = f"""
                SELECT ocel_source_id FROM object_object 
                WHERE ocel_target_id IN ({placeholders}) 
                AND ocel_qualifier = ?
            """
        self.cursor.execute(query, (*sources, qualifier))
        return [r[0] for r in self.cursor.fetchall()]

    def _collect_objects(self, start_obj):
        """Percorre le relazioni definite nel file di configurazione"""
        collected = {self.viewpoint_obj: [start_obj]}

        for rel in self.relations:
            src_type, tgt_type = rel["from"], rel["to"]
            qualifier, direction = rel["qualifier"], rel.get("direction", "forward")

            if src_type not in collected:
                continue
            src_objects = collected[src_type]
            related = self._query_related(src_objects, qualifier, direction)
            if related:
                collected.setdefault(tgt_type, []).extend(related)
        return collected

    def _get_events(self, object_dict):
        """Estrae tutti gli eventi relativi agli oggetti inclusi"""
        all_objects = []
        for obj_type, ids in object_dict.items():
            if not self.include_objects or obj_type in self.include_objects:
                all_objects.extend(ids)
        if not all_objects:
            return []

        placeholders = ",".join(["?"] * len(all_objects))
        cols_eo = self.col_names("event_object")
        self.cursor.execute(f"""
            SELECT * FROM event_object 
            WHERE {cols_eo[1]} IN ({placeholders})
        """, all_objects)
        links = self.cursor.fetchall()
        events = tuple([row[0] for row in links])
        if not events:
            return []

        placeholders = ",".join(["?"] * len(events))
        cols_e = self.col_names("event")
        cols_m = self.col_names("event_map_type")
        self.cursor.execute(f"""
            SELECT e.{cols_e[0]}, m.{cols_m[1]}
            FROM event e
            LEFT JOIN event_map_type m
            ON e.{cols_e[1]} = m.{cols_m[0]}
            WHERE e.{cols_e[0]} IN ({placeholders})
        """, events)
        base = self.cursor.fetchall()

        result = []
        for ev_id, ev_type in base:
            temp_table = f"event_{ev_type}"
            cols_temp = self.col_names(temp_table)
            self.cursor.execute(f"SELECT * FROM {temp_table} WHERE {cols_temp[0]} = ?", (ev_id,))
            res = self.cursor.fetchall()
            if res:
                result.append((ev_id, ev_type, res[0][1]))

        result = sorted(result, key=lambda x: x[2])
        return result

    def generate_process_executions(self):
        """Ciclo principale"""
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

        print("Generating process executions...")
        vp_table = f"object_{self.viewpoint_obj}"
        self.cursor.execute(f"SELECT ocel_id FROM {vp_table}")
        all_vp = [r[0] for r in self.cursor.fetchall()]

        all_rows, order_idx = [], []

        for idx, vp in enumerate(all_vp):
            if idx % 200 == 0:
                print(f"{int(idx / len(all_vp) * 100)}%")
            objects = self._collect_objects(vp)
            events = self._get_events(objects)
            all_rows.extend(events)
            order_idx.extend([idx + 1] * len(events))

        df = pd.DataFrame(all_rows)
        df[3] = order_idx
        df.sort_values(by=[3,2,0], ascending=True).to_csv(self.output_path, index=False)
        print("Done!")

def merge_ocel(thr):
    ocel_name = f'logistics_{thr}'
    ocel_sql = pm4py.read.read_ocel2_sqlite(f"ocel/{ocel_name}.sqlite")
    log_logidx = pd.read_csv(f'ocel/processes_{ocel_name}.csv')
    pm4py.write_ocel(ocel_sql, f'ocel/{ocel_name}.csv')
    log_sql_csv = pd.read_csv(f'ocel/{ocel_name}.csv', sep=',')
    log_logidx['2'] = pd.to_datetime(log_logidx['2'])
    log_sql_csv['ocel:timestamp'] = pd.to_datetime(log_sql_csv['ocel:timestamp'])

    log_sql_csv['ocel:timestamp'] = log_sql_csv['ocel:timestamp'].dt.tz_localize(None)
    df_new = pd.merge(log_logidx, log_sql_csv, left_on=['0', '2'], right_on=['ocel:eid','ocel:timestamp'], how='inner')
    df_new.to_csv(f'ocel/final_{ocel_name}.csv', index=False)

if __name__ == '__main__':

    thr = sys.argv[1]
    extractor = OCELProcessExtractor(
        db_path=f"ocel/logistics_{thr}.sqlite",
        output_path=f"ocel/processes_logistics_{thr}.csv",
        config_path="ocel/logistics.yaml"
    )
    extractor.generate_process_executions()
    merge_ocel(thr)