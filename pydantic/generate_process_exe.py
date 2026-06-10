import sqlite3
import pandas as pd
import yaml

DB_PATH     = "ocel/pydantic.sqlite"
PROC_PATH   = "ocel/processes_pydantic2500.csv"
FINAL_PATH  = "ocel/final_pydantic.csv"
CONFIG_PATH = "ocel/pydantic.yaml"

with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

viewpoint_obj  = config["viewpoint"]
relations      = config["relations"]
include_objects = set(config.get("include_objects", []))

conn   = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

def col_names(table):
    cursor.execute(f'PRAGMA table_info("{table}");')
    return [c[1] for c in cursor.fetchall()]

def query_related(sources, qualifier, direction):
    if not sources:
        return []
    ph = ",".join(["?"] * len(sources))
    if direction == "forward":
        q = f"SELECT ocel_target_id FROM object_object WHERE ocel_source_id IN ({ph}) AND ocel_qualifier = ?"
    else:
        q = f"SELECT ocel_source_id FROM object_object WHERE ocel_target_id IN ({ph}) AND ocel_qualifier = ?"
    cursor.execute(q, (*sources, qualifier))
    return [r[0] for r in cursor.fetchall()]

def collect_objects(start_obj):
    collected = {viewpoint_obj: [start_obj]}
    for rel in relations:
        src_type, tgt_type = rel["from"], rel["to"]
        qualifier, direction = rel["qualifier"], rel.get("direction", "forward")
        if src_type not in collected:
            continue
        related = query_related(collected[src_type], qualifier, direction)
        if related:
            collected.setdefault(tgt_type, []).extend(related)
    return collected

def get_timestamp(ev_id, ev_type):
    try:
        cursor.execute(f'SELECT ocel_time FROM "event_{ev_type}" WHERE ocel_id = ?', (ev_id,))
        row = cursor.fetchone()
        return row[0] if row else None
    except Exception:
        return None

def get_events(object_dict):
    all_objects = []
    for obj_type, ids in object_dict.items():
        if not include_objects or obj_type in include_objects:
            all_objects.extend(ids)
    if not all_objects:
        return []
    ph = ",".join(["?"] * len(all_objects))
    cursor.execute(f"""
        SELECT DISTINCT eo.ocel_event_id, e.ocel_type
        FROM event_object eo
        JOIN event e ON eo.ocel_event_id = e.ocel_id
        WHERE eo.ocel_object_id IN ({ph})
        AND eo.ocel_qualifier = 'timeline_event'
    """, all_objects)
    rows = cursor.fetchall()
    result = []
    for ev_id, ev_type in rows:
        ts = get_timestamp(ev_id, ev_type)
        if ts:
            result.append((ev_id, ev_type, ts))
    return sorted(result, key=lambda x: x[2])

print("Step 1: estrazione process executions...")
cursor.execute(f'SELECT ocel_id FROM "object_{viewpoint_obj}" WHERE number IS NOT NULL')
all_vp = [r[0] for r in cursor.fetchall()]
print(f"  Issue reali: {len(all_vp)}")

all_rows, order_idx = [], []
for idx, vp in enumerate(all_vp):
    if idx % 250 == 0:
        print(f"  {int(idx/len(all_vp)*100)}%")
    objects  = collect_objects(vp)
    events   = get_events(objects)
    all_rows.extend(events)
    order_idx.extend([idx + 1] * len(events))

proc_df = pd.DataFrame(all_rows, columns=[0, 1, 2])
proc_df[3] = order_idx
proc_df = proc_df.sort_values(by=[3, 2, 0], ascending=True)
proc_df.to_csv(PROC_PATH, index=False)
print(f"  Salvato: {PROC_PATH}  ({len(proc_df)} righe)")

print("\nStep 2: costruzione flat OCEL...")
cursor.execute("SELECT ocel_type, ocel_type_map FROM event_map_type")
event_types = cursor.fetchall()

flat_rows = []
for ev_type, ev_type_map in event_types:
    table = f"event_{ev_type_map}"
    cols  = col_names(table)
    cursor.execute(f'SELECT * FROM "{table}"')
    for row in cursor.fetchall():
        d = dict(zip(cols, row))
        d["ocel:activity"] = ev_type
        flat_rows.append(d)

flat_df = pd.DataFrame(flat_rows)
flat_df.rename(columns={"ocel_id": "ocel:eid", "ocel_time": "ocel:timestamp"}, inplace=True)
flat_df["ocel:eid"] = flat_df["ocel:eid"].astype(str)
flat_df["ocel:timestamp"] = pd.to_datetime(flat_df["ocel:timestamp"])

print("Step 3: merge...")

proc_df[0]  = proc_df[0].astype(str)
proc_df[2]  = pd.to_datetime(proc_df[2])

final_df = pd.merge(
    proc_df,
    flat_df,
    left_on=[0, 2],
    right_on=["ocel:eid", "ocel:timestamp"],
    how="inner"
)
final_df.to_csv(FINAL_PATH, index=False)

print(f"\nDone!")
print(f"  Righe final CSV : {len(final_df)}")
print(f"  Colonne         : {final_df.columns.tolist()}")
print(f"  Salvato in      : {FINAL_PATH}")
print()
print("Sample:")
print(final_df.head(3).to_string())