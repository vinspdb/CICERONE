import torch
import torch.nn as nn
import torch.nn.functional as F
from transformers import AutoModel, AutoTokenizer
import numpy as np
from typing import List, Dict
from tqdm import tqdm
import pickle

class PositionalEmbedding(nn.Module):
    def __init__(self, hidden_dim, max_seq_length=1000):
        super().__init__()
        self.embedding = nn.Embedding(max_seq_length, hidden_dim)
    
    def forward(self, x, positions):
        pos_emb = self.embedding(positions)
        return x + pos_emb


class BertTextPredictor(nn.Module):
    def __init__(self, encoder_name='bert-base-uncased', hidden_dim=768, freeze_encoder=False):
        super().__init__()
        self.encoder = AutoModel.from_pretrained(encoder_name, local_files_only=True)
        self.tokenizer = AutoTokenizer.from_pretrained(encoder_name, truncation_side='left', local_files_only=True)

        if freeze_encoder:
            for param in self.encoder.parameters():
                param.requires_grad = False

        encoder_hidden_size = self.encoder.config.hidden_size
        
        self.projection = nn.Linear(encoder_hidden_size, hidden_dim)
        self.layer_norm = nn.LayerNorm(hidden_dim)

        self.container_pos_embedding = PositionalEmbedding(hidden_dim, max_seq_length=1000)

        self.order_predictor = nn.Sequential(
            nn.Linear(hidden_dim, 256), nn.GELU(), nn.Dropout(0.1),
            nn.Linear(256, 64), nn.GELU(), nn.Linear(64, 1)
        )
        self.container_predictor = nn.Sequential(
            nn.Linear(hidden_dim, 256), nn.GELU(), nn.Dropout(0.1),
            nn.Linear(256, 64), nn.GELU(), nn.Linear(64, 1)
        )
        self.td_predictor = nn.Sequential(
            nn.Linear(hidden_dim, 256), nn.GELU(), nn.Dropout(0.1),
            nn.Linear(256, 64), nn.GELU(), nn.Linear(64, 1)
        )

    def encode_text(self, text: str) -> torch.Tensor:
        inputs = self.tokenizer(
            text, return_tensors='pt', truncation=True,
            max_length=512, padding='max_length'
        )
        inputs = {k: v.to(next(self.encoder.parameters()).device) for k, v in inputs.items()}
        
        with torch.set_grad_enabled(self.training):
            outputs = self.encoder(**inputs)
        
        cls_embedding = outputs.last_hidden_state[:, 0, :]
        projected = self.projection(cls_embedding.squeeze(0)) 
        normalized = self.layer_norm(projected)
        
        return normalized

    def forward(self, text: str) -> torch.Tensor:
        return self.encode_text(text)

    def predict(self, local_texts: List[str], num_items_per_instance: List[int], training=True) -> Dict[str, List]:
        predictions = {'order': [], 'container': [], 'td': []}
        N = len(local_texts)
        for i in range(N):
            text_emb = self.forward(local_texts[i])

            order_pred = self.order_predictor(text_emb)
            predictions['order'].append(order_pred if training else order_pred.detach().cpu().numpy())

            num_items = num_items_per_instance[i]
            if num_items > 0:
                container_embeds = text_emb.unsqueeze(0).expand(num_items, -1)
                
                positions = torch.arange(num_items, device=device, dtype=torch.long)
                container_emb_with_pos = self.container_pos_embedding(container_embeds, positions)
               
                container_preds = self.container_predictor(container_emb_with_pos)
                predictions['container'].append(container_preds if training else container_preds.detach().cpu().numpy())
            else:
                predictions['container'].append(np.zeros((0, 1)))

            td_pred = self.td_predictor(text_emb)
            predictions['td'].append(td_pred if training else td_pred.detach().cpu().numpy())

        return predictions

def evaluate_model(model, test_data, device):
    model.eval()
    l1 = nn.L1Loss()
    
    errors = {'order': torch.tensor(0.0, device=device),
              'container': torch.tensor(0.0, device=device),
              'td': torch.tensor(0.0, device=device)}

    counts = {'order': torch.tensor(0.0, device=device),
              'container': torch.tensor(0.0, device=device),
              'td': torch.tensor(0.0, device=device)}

    with torch.no_grad():
        for sample in tqdm(test_data, desc="Evaluating"):
            local_texts = sample['local_texts']
            targets = sample['targets']
            masks = sample['masks']

            num_containers = [len(t) for t in targets['container']]

            preds = model.predict(local_texts, num_containers, training=False)

            for key in ['order', 'container', 'td']:
                for pred, target, mask in zip(preds[key], targets[key], masks[key]):

                    target_tensor = torch.tensor(target, dtype=torch.float32, device=device).view(-1)
                    mask_tensor = torch.tensor(mask, dtype=torch.bool, device=device).view(-1)

                    if isinstance(pred, np.ndarray):
                        pred_tensor = torch.tensor(pred, dtype=torch.float32, device=device).view(-1)
                    else:
                        pred_tensor = pred.view(-1)

                    if mask_tensor.sum() == 0:
                        continue

                    loss_masked = l1(pred_tensor[mask_tensor], target_tensor[mask_tensor])
                    errors[key] += loss_masked * mask_tensor.sum()
                    counts[key] += mask_tensor.sum()

    mae_dict = {}
    for key in ['order', 'container', 'td']:
        if counts[key].item() > 0:
            mae_dict[key] = (errors[key] / counts[key]).item()
        else:
            mae_dict[key] = 0.0

    total_err = sum(errors.values())
    total_cnt = sum(counts.values())
    mae_dict['overall'] = (total_err / total_cnt).item() if total_cnt.item() > 0 else 0.0

    print("\n" + "="*60)
    print("TEST RESULTS")
    print("="*60)
    print(f"Order MAE:   {mae_dict['order']:.4f}")
    print(f"container MAE:    {mae_dict['container']:.4f}")
    print(f"td MAE: {mae_dict['td']:.4f}")
    print("="*60)

    return mae_dict


if __name__ == "__main__":
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")
    import sys
    ocel_name = sys.argv[1]
    encoder_name = sys.argv[2]
    
    with open(f'ocel/{ocel_name}/{ocel_name}_train_dict', 'rb') as f:
        train_samples = pickle.load(f)
    with open(f'ocel/{ocel_name}/{ocel_name}_val_dict', 'rb') as f:
        val_samples = pickle.load(f)
    with open(f'ocel/{ocel_name}/{ocel_name}_test_dict', 'rb') as f:
        test_samples = pickle.load(f)

    model = BertTextPredictor(
        encoder_name=encoder_name,
        hidden_dim=512,
        freeze_encoder=False
    )

    print(f"Total parameters: {sum(p.numel() for p in model.parameters()):,}")
    print(f"Trainable parameters: {sum(p.numel() for p in model.parameters() if p.requires_grad):,}")

    if encoder_name == 'cross-encoder/ms-marco-TinyBERT-L2-v2':
            encoder_name = 'TinyBert'
    elif encoder_name =='prajjwal1/bert-medium':
            encoder_name = 'bertmedium'

    state_dict = torch.load(f"neural_network/model/local/best_model_local_{ocel_name}_{encoder_name}.pt", map_location=device)
    model.load_state_dict(state_dict)
    model.to(device)
    model.eval()

    mae_results = evaluate_model(model, test_samples, device)