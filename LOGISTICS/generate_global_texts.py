import sys
import numpy as np
import pickle
import torch

def convert_example(sg_graphs, text, len_global):
    global_new = []
    index = 0
    for s in len_global:
        chunk = [[x] for x in text[index : index + s]]
        chunk_order = [np.array(x['order'].y.tolist()) for x in sg_graphs[index : index + s]]
        chunk_item = [np.array(x['container'].y.tolist()) for x in sg_graphs[index : index + s]]
        chunk_package = [np.array(x['td'].y.tolist()) for x in sg_graphs[index : index + s]]

        mask_order = [np.array(x['order'].mask.tolist()) for x in sg_graphs[index : index + s]]
        mask_item = [np.array(x['container'].mask.tolist()) for x in sg_graphs[index : index + s]]
        mask_package = [np.array(x['td'].mask.tolist()) for x in sg_graphs[index : index + s]]
        ex = {
            'local_texts': chunk,
            'targets': {
                'order': chunk_order,
                'container': chunk_item,
                'td': chunk_package,
            },
            'masks': {
                'order': mask_order,
                'container': mask_item,
                'td': mask_package,
            }
        }
        global_new.append(ex)
        index += s
    return global_new


if __name__ == '__main__':
    NOISE = sys.argv[1]
    fg_graphs_train_new = torch.load(f"ocel/logistics_{NOISE}/train_graphs_fg.pt",weights_only=False)
    fg_graphs_val_new = torch.load(f"ocel/logistics_{NOISE}/val_graphs_fg.pt",weights_only=False)
    fg_graphs_test_new = torch.load(f"ocel/logistics_{NOISE}/test_graphs_fg.pt",weights_only=False)

    len_global_train =[]
    len_global_val =[]
    len_global_test =[]

    for l in fg_graphs_train_new:
        len_global_train.append(len(l['order'].x))

    for l in fg_graphs_val_new:
        len_global_val.append(len(l['order'].x))

    for l in fg_graphs_test_new:
        len_global_test.append(len(l['order'].x))

    sg_graphs_train_new = torch.load(f"ocel/logistics_{NOISE}/train_graphs_sg.pt",weights_only=False)
    sg_graphs_val_new = torch.load(f"ocel/logistics_{NOISE}/val_graphs_sg.pt",weights_only=False)
    sg_graphs_test_new = torch.load(f"ocel/logistics_{NOISE}/test_graphs_sg.pt",weights_only=False)

    with open(f'ocel/logistics_{NOISE}/logistics_{NOISE}_train_local', 'rb') as f:
            train_text = pickle.load(f)
    with open(f'ocel/logistics_{NOISE}/logistics_{NOISE}_val_local', 'rb') as f:
            val_text = pickle.load(f)
    with open(f'ocel/logistics_{NOISE}/logistics_{NOISE}_test_local', 'rb') as f:
            test_text = pickle.load(f)

    train = convert_example(sg_graphs_train_new, train_text, len_global_train)
    val = convert_example(sg_graphs_val_new, val_text, len_global_val)
    test = convert_example(sg_graphs_test_new, test_text, len_global_test)

    with open(f'ocel/logistics_{NOISE}/logistics_{NOISE}_train_dict', 'wb') as f:
        pickle.dump(train,f)

    with open(f'ocel/logistics_{NOISE}/logistics_{NOISE}_val_dict', 'wb') as f:
        pickle.dump(val,f)

    with open(f'ocel/logistics_{NOISE}/logistics_{NOISE}_test_dict', 'wb') as f:
        pickle.dump(test,f)