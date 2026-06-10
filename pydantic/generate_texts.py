from train_test_split import TrainTestBuilder
from pydantic_template import log_template
from jinja2 import Template
import pandas as pd
import sqlite3
from tqdm import tqdm
import numpy as np
import pickle
import os
import re

DB_PATH    = "ocel/pydantic.sqlite"
FINAL_PATH = "ocel/final_pydantic.csv"

def clean_message(msg):
    if not msg:
        return ""
    msg = re.sub(r'\s+', ' ', msg)
    msg = re.sub(r'https?://\S+', '', msg)
    msg = re.sub(r'#[0-9]+', '', msg)
    msg = re.sub(r'[^\w\s\-\.:,/]', ' ', msg)
    msg = re.sub(r'\s+', ' ', msg)
    return msg.strip()


def build_lookup_tables(cursor):
    cursor.execute("SELECT ocel_id, type, login FROM object_user")
    user_login, user_type = {}, {}
    for ocel_id, utype, login in cursor.fetchall():
        if login and ocel_id not in user_login:
            user_login[ocel_id] = login
        if utype and ocel_id not in user_type:
            user_type[ocel_id] = utype

    cursor.execute("SELECT ocel_id, number, title FROM object_issue WHERE number IS NOT NULL")
    issue_info = {}
    for ocel_id, number, title in cursor.fetchall():
        if number and ocel_id not in issue_info:
            issue_info[ocel_id] = {"number": number, "title": (title or '')[:40]}

    cursor.execute("SELECT ocel_id, name, slug FROM object_team")
    team_info = {}
    for ocel_id, name, slug in cursor.fetchall():
        team_info[ocel_id] = name or slug or ""

    cursor.execute("SELECT ocel_id, sha, commit_message FROM object_commit")
    commit_info = {}
    for ocel_id, sha, msg in cursor.fetchall():
        commit_info[ocel_id] = {
            "sha":     sha[:7] if sha else "",
            "message": clean_message(msg) if msg else "",
        }

    cursor.execute("""
        SELECT eo.ocel_event_id, eo.ocel_qualifier, o.ocel_type, eo.ocel_object_id
        FROM event_object eo
        JOIN object o ON eo.ocel_object_id = o.ocel_id
    """)
    event_objects = {}
    for ev_id, qual, obj_type, obj_id in cursor.fetchall():
        event_objects.setdefault(str(ev_id), []).append((qual, obj_type, obj_id))

    assoc_map = {}
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'event_%'")
    for (table,) in cursor.fetchall():
        cursor.execute(f'PRAGMA table_info("{table}")')
        cols = [c[1] for c in cursor.fetchall()]
        if 'author_association' in cols:
            cursor.execute(
                f'SELECT ocel_id, author_association FROM "{table}" '
                f'WHERE author_association IS NOT NULL'
            )
            for ev_id, assoc in cursor.fetchall():
                assoc_map[str(ev_id)] = assoc

    return user_login, user_type, issue_info, team_info, commit_info, event_objects, assoc_map

def build_event_row(ev_id, ev_type, event_objects,
                    user_login, issue_info, team_info, commit_info, assoc_map):
    objs  = event_objects.get(str(ev_id), [])
    assoc = assoc_map.get(str(ev_id), "")

    by_qual = {}
    for qual, obj_type, obj_id in objs:
        by_qual.setdefault(qual, []).append((obj_type, obj_id))

    def first_login(qual):
        for obj_type, obj_id in by_qual.get(qual, []):
            if obj_type == 'user':
                return user_login.get(obj_id, "")
        return ""

    actor = first_login('actor')
    if not actor:
        for obj_type, obj_id in by_qual.get('created', []):
            if obj_type == 'user':
                actor = user_login.get(obj_id, "")
                break

    issue_number, issue_title = "", ""
    for obj_type, obj_id in by_qual.get('created', []):
        if obj_type == 'issue':
            info = issue_info.get(obj_id, {})
            issue_number = str(info.get('number', ''))
            issue_title  = info.get('title', '')
            break

    assignee = first_login('assignee')

    reviewer = first_login('requested_reviewer')
    if not reviewer:
        for obj_type, obj_id in by_qual.get('requested_reviewer', []):
            if obj_type == 'team':
                reviewer = team_info.get(obj_id, "")
                break

    committer, commit_sha = "", ""
    for obj_type, obj_id in by_qual.get('committer', []):
        if obj_type == 'user':
            committer = user_login.get(obj_id, "")
            break
    for qual, obj_type, obj_id in objs:
        if qual == 'timeline_event' and obj_type == 'commit':
            info = commit_info.get(obj_id, {})
            commit_sha = info.get('sha', '')
            break

    return {
        'ocel_activity':      ev_type,
        'actor_login':        actor,
        'assignee_login':     assignee,
        'reviewer_login':     reviewer,
        'committer_login':    committer,
        'commit_sha':         commit_sha,
        'issue_number':       issue_number,
        'issue_title':        issue_title,
        'author_association': assoc,
    }


def build_global_dict(timestamp_list, df_active_exec_full,
                      lista_prefix_def, end_timestamp, list_idx,
                      active_issues_dict):
    
    df = df_active_exec_full.copy()
    df["start"] = pd.to_datetime(df["start"]).dt.tz_localize(None)
    df["end"]   = pd.to_datetime(df["end"]).dt.tz_localize(None)
    df = df.sort_values("end").reset_index(drop=True)  # ← aggiunto

    global_batches = []

    for x in timestamp_list:
        x_ts = pd.to_datetime(x)

        active_execs = df[
            (df["start"] <= x_ts) & (df["end"] >= x_ts)
        ]["exec_idx"].values

        texts, targets, masks = [], [], []

        for exec_idx in active_execs:
            filtered = [
                (lp, end)
                for lp, end, idx in zip(lista_prefix_def, end_timestamp, list_idx)
                if idx == exec_idx and end <= x
            ]
            if not filtered:
                continue

            prefix_text, _ = filtered[-1]

            closed_ts = active_issues_dict.get(exec_idx)
            if closed_ts:
                remaining = (pd.to_datetime(closed_ts) - x_ts).total_seconds()
            else:
                remaining = 0.0

            texts.append([prefix_text])
            targets.append(np.array([[remaining]], dtype=np.float64))
            masks.append(np.array([[remaining > 0]], dtype=bool))

        if texts:
            global_batches.append({
                'local_texts': texts,
                'targets':     {'issue': targets},
                'masks':       {'issue': masks},
            })

    return global_batches

def extract_local_texts(global_batches):
    """
    Appiattisce i global batches in una lista semplice di testi prefix,
    compatibile con la baseline che non usa il global encoder.
    """
    return [item[0] for batch in global_batches for item in batch['local_texts']]


def compute_normalization(train_dict):
    y_all = np.concatenate([
        t.flatten()
        for batch in train_dict
        for t, m in zip(batch['targets']['issue'], batch['masks']['issue'])
        if m.flatten()[0]
    ])
    mean_y = float(np.mean(y_all))
    std_y  = float(np.std(y_all))
    std_y  = std_y if std_y > 0 else 1.0
    return mean_y, std_y


def normalize_dict(global_dict, mean_y, std_y):
    for batch in global_dict:
        batch['targets']['issue'] = [
            (t.astype(np.float64) - mean_y) / std_y for t in batch['targets']['issue']
        ]
    return global_dict


if __name__ == "__main__":

    ts_builder = TrainTestBuilder(FINAL_PATH, 2500, 1600, .45, 200)
    ocel_log   = pd.read_csv(FINAL_PATH)
    ocel_log.columns = [str(c) for c in ocel_log.columns]

    train_ts, val_ts, test_ts = ts_builder.timestamps_generator()
    

    print(f"Timestamps — train: {len(train_ts)}, val: {len(val_ts)}, test: {len(test_ts)}")

    conn   = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("Building lookup tables...")
    (user_login, user_type, issue_info, team_info,
     commit_info, event_objects, assoc_map) = build_lookup_tables(cursor)

    active_issues_dict = {}
    for exec_idx, group in ocel_log.groupby("3", sort=False):
        closed = group[group["1"] == "closed"]
        if not closed.empty:
            active_issues_dict[exec_idx] = closed["2"].iloc[0]
        else:
            if len(group) > 0:
                active_issues_dict[exec_idx] = group["2"].iloc[-1]  # fallback ultimo evento

    df_group = ocel_log.groupby("3", sort=False)

    lista_prefix_def = []   
    end_timestamp    = []   
    list_idx         = []   
    all_timestamps   = []

    default_tmpl = {
        'event_attribute': ['ocel_activity', 'actor_login'],
        'event_template':  '{{ocel_activity}} {{actor_login}}',
    }

    for exec_idx, group in tqdm(df_group, desc="Building prefixes"):

        event_text   = ""
        seen_users   = {}
        seen_commits = {}

        for _, row in group.iterrows():
            ev_id    = str(row["0"])
            ev_type  = row["1"]
            ts_event = pd.to_datetime(row["2"])

            ev_row = build_event_row(
                ev_id, ev_type, event_objects,
                user_login, issue_info, team_info, commit_info, assoc_map
            )

            tmpl_info   = log_template.get(ev_type, default_tmpl)
            render_dict = {k: ev_row.get(k, "") for k in tmpl_info['event_attribute']}
            line        = Template(tmpl_info['event_template']).render(render_dict).strip()
            event_text  = event_text + line + "\n"

            assoc = assoc_map.get(ev_id, "")
            for qual, obj_type, obj_id in event_objects.get(ev_id, []):
                if obj_type == 'user':
                    login = user_login.get(obj_id)
                    if login:
                        if login not in seen_users:
                            seen_users[login] = {
                                "type":        user_type.get(obj_id, ""),
                                "association": assoc,
                            }
                        elif assoc:
                            seen_users[login]["association"] = assoc
                elif obj_type == 'commit' and qual == 'timeline_event':
                    if obj_id not in seen_commits:
                        seen_commits[obj_id] = commit_info.get(obj_id, {})

            trace_lines = []
            for login, attrs in seen_users.items():
                parts = [f"user:{login}"]
                if attrs.get("type"):
                    parts.append(f"type: {attrs['type']}")
                if attrs.get("association"):
                    parts.append(f"association: {attrs['association']}")
                trace_lines.append(" ".join(parts))
            for obj_id, info in seen_commits.items():
                parts = [f"commit:{info['sha']}" if info.get('sha') else "commit"]
                if info.get('message'):
                    parts.append(f"\"{info['message']}\"")
                trace_lines.append(" ".join(parts))

            trace_text = "\n".join(trace_lines)
            prefix     = event_text.rstrip("\n") + ("\n" + trace_text if trace_text else "")

            lista_prefix_def.append(prefix)
            end_timestamp.append(ts_event)
            list_idx.append(exec_idx)

        all_timestamps.append({
            "exec_idx": exec_idx,
            "start":    group["2"].iloc[0],
            "end":      group["2"].iloc[-1],
        })

    df_active_exec_full = pd.DataFrame(all_timestamps)
    end_timestamp = np.array(
        pd.to_datetime(end_timestamp).tz_localize(None).astype(str).tolist()
    )
    list_idx = np.array(list_idx)

    print("Generating global dicts...")
    train_dict_raw = build_global_dict(
        train_ts, df_active_exec_full,
        lista_prefix_def, end_timestamp, list_idx,
        active_issues_dict 
    )
    val_dict_raw = build_global_dict(
        val_ts, df_active_exec_full,
        lista_prefix_def, end_timestamp, list_idx,
        active_issues_dict 
    )
    test_dict_raw = build_global_dict(
        test_ts, df_active_exec_full,
        lista_prefix_def, end_timestamp, list_idx,
        active_issues_dict 
    )
    print(f"Raw batches — train: {len(train_dict_raw)}, val: {len(val_dict_raw)}, test: {len(test_dict_raw)}")

    mean_y, std_y = compute_normalization(train_dict_raw)

    train_dict = normalize_dict(train_dict_raw, mean_y, std_y)
    val_dict   = normalize_dict(val_dict_raw,   mean_y, std_y)
    test_dict  = normalize_dict(test_dict_raw,  mean_y, std_y)

    train_text = extract_local_texts(train_dict)
    val_text   = extract_local_texts(val_dict)
    test_text  = extract_local_texts(test_dict)
    print(f"Local texts — train: {len(train_text)}, val: {len(val_text)}, test: {len(test_text)}")

    out_dir = "ocel/pydantic"
    os.makedirs(out_dir, exist_ok=True)

    with open(f"{out_dir}/pydantic_train_local", "wb") as f:
        pickle.dump(train_text, f)
    with open(f"{out_dir}/pydantic_val_local", "wb") as f:
        pickle.dump(val_text, f)
    with open(f"{out_dir}/pydantic_test_local", "wb") as f:
        pickle.dump(test_text, f)

    with open(f"{out_dir}/pydantic_train_dict", "wb") as f:
        pickle.dump(train_dict, f)
    with open(f"{out_dir}/pydantic_val_dict", "wb") as f:
        pickle.dump(val_dict, f)
    with open(f"{out_dir}/pydantic_test_dict", "wb") as f:
        pickle.dump(test_dict, f)

    conn.close()