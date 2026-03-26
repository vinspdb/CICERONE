import sys
import ast
import pandas as pd
from pathlib import Path

COLUMN_NAMES = [
    'event_id_0', 'activity_type', 'timestamp_0', 'case_id',
    'ocel_eid', 'ocel_timestamp', 'ocel_activity',
    'ocel_orders', 'ocel_customers', 'ocel_items',
    'ocel_products', 'ocel_employees', 'ocel_packages'
]

REFERENCE_ACTIVITY = 'PlaceOrder'

def parse_list(val) -> list:
    """Converte una stringa tipo \"['a', 'b']\" in lista Python."""
    if pd.isna(val) or str(val).strip() == '':
        return []
    try:
        return ast.literal_eval(str(val))
    except Exception:
        return []


def list_to_str(lst: list) -> str:
    """Riconverte una lista in stringa, oppure stringa vuota se vuota."""
    return str(lst) if lst else ''


def build_case_references(df: pd.DataFrame) -> dict:
    """
    Per ogni case_id, estrae dal PlaceOrder:
      - orders    : set degli order ID validi
      - customers : set dei customer ID validi
      - items     : set degli item ID validi
      - item_to_product : dict {item_id -> product_name}
    """
    refs = {}
    place_orders = df[df['activity_type'] == REFERENCE_ACTIVITY]

    for _, row in place_orders.iterrows():
        case_id = row['case_id']
        if case_id in refs:
            continue 

        items    = parse_list(row['ocel_items'])
        products = parse_list(row['ocel_products'])

        refs[case_id] = {
            'orders':           set(parse_list(row['ocel_orders'])),
            'customers':        set(parse_list(row['ocel_customers'])),
            'items':            set(items),
            'item_to_product':  dict(zip(items, products)),
        }

    return refs


def filter_row(row: pd.Series, refs: dict) -> pd.Series:
    """
    Filtra orders, customers, items e products di una riga
    mantenendo solo gli oggetti presenti nel PlaceOrder del suo caso.
    """
    case_id = row['case_id']
    if case_id not in refs:
        return row

    ref = refs[case_id]
    row = row.copy()

    orders = parse_list(row['ocel_orders'])
    row['ocel_orders'] = list_to_str([o for o in orders if o in ref['orders']])

    customers = parse_list(row['ocel_customers'])
    row['ocel_customers'] = list_to_str([c for c in customers if c in ref['customers']])

    items = parse_list(row['ocel_items'])
    filtered_items    = [i for i in items if i in ref['items']]
    filtered_products = [ref['item_to_product'][i] for i in filtered_items
                         if i in ref['item_to_product']]

    row['ocel_items']    = list_to_str(filtered_items)
    row['ocel_products'] = list_to_str(filtered_products)

    return row

def main_filter(thr):

    input_path = f"ocel/final_order_management_{thr}.csv"

    output_path = f"ocel/final_order_management_{thr}_filtered.csv"

    df = pd.read_csv(input_path)
    df.columns = COLUMN_NAMES
    print(f"  {len(df)} righe, {df['case_id'].nunique()} case_id")

    refs = build_case_references(df)

    df_filtered = df.apply(lambda row: filter_row(row, refs), axis=1)

    original_columns = [
        '0', '1', '2', '3',
        'ocel:eid', 'ocel:timestamp', 'ocel:activity',
        'ocel:type:orders', 'ocel:type:customers', 'ocel:type:items',
        'ocel:type:products', 'ocel:type:employees', 'ocel:type:packages'
    ]
    df_filtered.columns = original_columns

    df_filtered.to_csv(output_path, index=False)
    print(f"Saved: {output_path}")

    total_items_before = df['ocel_items'].apply(lambda x: len(parse_list(x))).sum()
    total_items_after  = df_filtered['ocel:type:items'].apply(lambda x: len(parse_list(x))).sum()
    removed = total_items_before - total_items_after


