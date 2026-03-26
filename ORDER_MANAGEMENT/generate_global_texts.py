import torch
import sys
import pickle
import numpy as np

def convert_example(sg_graphs, text, len_global):
    global_new = []
    index = 0
    for s in len_global:
        chunk = [[x] for x in text[index : index + s]]
        chunk_order = [np.array(x['order'].y.tolist()) for x in sg_graphs[index : index + s]]
        chunk_item = [np.array(x['item'].y.tolist()) for x in sg_graphs[index : index + s]]
        chunk_package = [np.array(x['package'].y.tolist()) for x in sg_graphs[index : index + s]]

        chunk_item_x = [np.array(x['item'].x.tolist()) for x in sg_graphs[index : index + s]]
        chunk_package_x = [np.array(x['package'].x.tolist()) for x in sg_graphs[index : index + s]]
        
        lista_item = []
        for ci in chunk_item_x:
            lista_temp = []
            i = 0
            for c in ci:
                lista_temp.append('item'+str(i)+" weight " + str(round(c[0],3)) + " price " +str(round(c[1],3)))
                i = i+1
            lista_item.append(lista_temp)

        lista_package = []
        for ci in chunk_package_x:
            lista_temp = []
            i = 0
            for c in ci:
                lista_temp.append("package weight " + str(round(c[0],3)))
                i = i+1
            lista_package.append(lista_temp)
        
        mask_order = [np.array(x['order'].mask.tolist()) for x in sg_graphs[index : index + s]]
        mask_item = [np.array(x['item'].mask.tolist()) for x in sg_graphs[index : index + s]]
        mask_package = [np.array(x['package'].mask.tolist()) for x in sg_graphs[index : index + s]]

        ex = {
            'local_texts': chunk,
            'targets': {
                'order': chunk_order,
                'item': chunk_item,
                'package': chunk_package,
                'list_item': lista_item,
                'list_package': lista_package
            },
            'masks': {
                'order': mask_order,
                'item': mask_item,
                'package': mask_package,
            }
        }
        global_new.append(ex)
        index += s
    return global_new

if __name__ == '__main__':
    thr = sys.argv[1]

    fg_graphs_train_new = torch.load(f"ocel/order_management_{thr}/train_graphs_fg.pt",weights_only=False)
    fg_graphs_val_new = torch.load(f"ocel/order_management_{thr}/val_graphs_fg.pt",weights_only=False)
    fg_graphs_test_new = torch.load(f"ocel/order_management_{thr}/test_graphs_fg.pt",weights_only=False)

    len_global_train =[]
    len_global_val =[]
    len_global_test =[]

    for l in fg_graphs_train_new:
        len_global_train.append(len(l['order'].x))

    for l in fg_graphs_val_new:
        len_global_val.append(len(l['order'].x))

    for l in fg_graphs_test_new:
        len_global_test.append(len(l['order'].x))

    sg_graphs_train_new = torch.load(f"ocel/order_management_{thr}/train_graphs_sg.pt",weights_only=False)
    sg_graphs_val_new = torch.load(f"ocel/order_management_{thr}/val_graphs_sg.pt",weights_only=False)
    sg_graphs_test_new = torch.load(f"ocel/order_management_{thr}/test_graphs_sg.pt",weights_only=False)


    ocel_name = f'order_management_{thr}'
    with open(f'ocel/order_management_{thr}/{ocel_name}_train_local', 'rb') as f:
            train_text = pickle.load(f)
    with open(f'ocel/order_management_{thr}/{ocel_name}_val_local', 'rb') as f:
            val_text = pickle.load(f)
    with open(f'ocel/order_management_{thr}/{ocel_name}_test_local', 'rb') as f:
            test_text = pickle.load(f)

    train = convert_example(sg_graphs_train_new, train_text, len_global_train)
    val = convert_example(sg_graphs_val_new, val_text, len_global_val)
    test = convert_example(sg_graphs_test_new, test_text, len_global_test)

    with open(f'ocel/order_management_{thr}/{ocel_name}_train_dict', 'wb') as f:
        pickle.dump(train,f)

    with open(f'ocel/order_management_{thr}/{ocel_name}_val_dict', 'wb') as f:
        pickle.dump(val,f)

    with open(f'ocel/order_management_{thr}/{ocel_name}_test_dict', 'wb') as f:
        pickle.dump(test,f)