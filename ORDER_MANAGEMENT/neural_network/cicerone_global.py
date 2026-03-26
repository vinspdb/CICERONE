import torch
import torch.nn as nn
from transformers import AutoModel, AutoTokenizer
from tqdm import tqdm
import numpy as np
from typing import List
import pickle
import random
import sys
import os

class ResidualConv1DBlock(nn.Module):
    def __init__(self, hidden_dim, kernel_size, dropout=0.1):
        super().__init__()
        self.conv = nn.Conv1d(hidden_dim, hidden_dim, kernel_size, padding=kernel_size//2)
        self.norm = nn.LayerNorm(hidden_dim)
        self.dropout = nn.Dropout(dropout)
        self.activation = nn.GELU()
        
    def forward(self, x):
        residual = x
        x = self.conv(x)
        x = x.transpose(1, 2)
        x = self.norm(x)
        x = x.transpose(1, 2)
        x = self.activation(x)
        x = self.dropout(x)
        return x + residual

class GlobalBiLSTMEncoder(nn.Module):
    def __init__(self, hidden_dim, num_layers=2, dropout=0.3):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=hidden_dim,
            hidden_size=hidden_dim,
            num_layers=num_layers,
            batch_first=True,
            bidirectional=False,
            dropout=dropout if num_layers > 1 else 0.0
        )
        self.norm = nn.LayerNorm(hidden_dim)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x):
        x = x.squeeze(0).transpose(0, 1).unsqueeze(0)
        out, _ = self.lstm(x)
        out = self.dropout(out)
        out = self.norm(out)
        return out.squeeze(0).transpose(0, 1).unsqueeze(0)

class GlobalRNNEncoder(nn.Module):
    def __init__(self, hidden_dim, num_layers=2, dropout=0.3):
        super().__init__()
        self.lstm = nn.GRU(
            input_size=hidden_dim,
            hidden_size=hidden_dim,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0
        )
        self.norm = nn.LayerNorm(hidden_dim)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x):
        x = x.squeeze(0).transpose(0, 1).unsqueeze(0)
        out, _ = self.lstm(x) 
        out = self.dropout(out)
        out = self.norm(out)
        return out.squeeze(0).transpose(0, 1).unsqueeze(0)


class PositionalEmbedding(nn.Module):
    def __init__(self, hidden_dim, max_seq_length=1000):
        super().__init__()
        self.embedding = nn.Embedding(max_seq_length, hidden_dim)

    def forward(self, x, positions):
        pos_emb = self.embedding(positions)
        return x + pos_emb

class SimpleGlobalTextPredictor(nn.Module):
    def __init__(
        self,
        encoder_name='bert-base-uncased',
        hidden_dim=768,
        freeze_encoder=False,
        global_encoder_type='conv1d',   # 'conv1d' | 'lstm' | 'rnn'
        num_global_layers=4,
    ):
        super().__init__()
        assert global_encoder_type in ('conv1d', 'rnn', 'lstm'), \
            f"global_encoder_type deve essere 'conv1d', 'rnn' o 'lstm', ricevuto: {global_encoder_type}"

        self.encoder = AutoModel.from_pretrained(encoder_name, local_files_only=True)
        self.tokenizer = AutoTokenizer.from_pretrained(encoder_name, truncation_side='left', local_files_only=True)
        self.global_encoder_type = global_encoder_type

        if freeze_encoder:
            for param in self.encoder.parameters():
                param.requires_grad = False

        encoder_hidden_size = self.encoder.config.hidden_size
        self.instance_projection = nn.Linear(encoder_hidden_size, hidden_dim)

        # ---- Global encoder ----
        if global_encoder_type == 'conv1d':
            self.global_transformer = nn.Sequential(
                ResidualConv1DBlock(hidden_dim, 3),
                ResidualConv1DBlock(hidden_dim, 5),
                ResidualConv1DBlock(hidden_dim, 7),
                ResidualConv1DBlock(hidden_dim, 9),
            )
        elif global_encoder_type == 'rnn':
            self.global_transformer = GlobalRNNEncoder(
                hidden_dim, num_layers=num_global_layers
            )
        elif global_encoder_type == 'lstm':
            self.global_transformer = GlobalBiLSTMEncoder(
                hidden_dim, num_layers=num_global_layers
            )

        self.layer_norm = nn.LayerNorm(hidden_dim)

        # Positional embeddings for item and package
        self.item_pos_embedding = PositionalEmbedding(hidden_dim, max_seq_length=1000)
        self.package_pos_embedding = PositionalEmbedding(hidden_dim, max_seq_length=1000)

        # Prediction heads
        self.order_predictor = nn.Sequential(
            nn.Linear(hidden_dim, 256), nn.GELU(), nn.Dropout(0.1),
            nn.Linear(256, 64), nn.GELU(), nn.Linear(64, 1)
        )
        self.item_predictor = nn.Sequential(
            nn.Linear(hidden_dim, 256), nn.GELU(), nn.Dropout(0.1),
            nn.Linear(256, 64), nn.GELU(), nn.Linear(64, 1)
        )
        self.package_predictor = nn.Sequential(
            nn.Linear(hidden_dim, 256), nn.GELU(), nn.Dropout(0.1),
            nn.Linear(256, 64), nn.GELU(), nn.Linear(64, 1)
        )

    def encode_instances(self, local_texts: List[List[str]]) -> torch.Tensor:
        device = next(self.encoder.parameters()).device
        local_texts_flat = [
            "\n".join([str(t) for t in texts if t is not None])
            for texts in local_texts
        ]
        inputs = self.tokenizer(
            local_texts_flat,
            return_tensors='pt',
            truncation=True,
            padding='max_length',
            max_length=512
        )
        inputs = {k: v.to(device) for k, v in inputs.items()}
        with torch.set_grad_enabled(self.training):
            outputs = self.encoder(**inputs)
        cls_embeddings = outputs.last_hidden_state[:, 0, :]
        projected = self.instance_projection(cls_embeddings)
        return projected

    def forward(self, local_texts: List[str]) -> torch.Tensor:
        instance_embeds = self.encode_instances(local_texts)
        x = instance_embeds.unsqueeze(0).transpose(1, 2)
        x = self.global_transformer(x)
        x = x.transpose(1, 2).squeeze(0)
        return self.layer_norm(x)

    def predict(self, local_texts, num_items_per_instance, num_packages_per_instance, training=True):
        device = next(self.encoder.parameters()).device
        global_embeds = self.forward(local_texts)
        N = len(local_texts)

        order_preds = self.order_predictor(global_embeds)
        order_preds_out = order_preds if training else order_preds.detach().cpu().numpy()

        item_preds_list = []
        for i in range(N):
            num_items = num_items_per_instance[i]
            if num_items > 0:
                inst_emb = global_embeds[i].unsqueeze(0).expand(num_items, -1)
                positions = torch.arange(num_items, device=device, dtype=torch.long)
                inst_emb_with_pos = self.item_pos_embedding(inst_emb, positions)
                preds = self.item_predictor(inst_emb_with_pos)
                item_preds_list.append(preds if training else preds.detach().cpu().numpy())
            else:
                item_preds_list.append(np.zeros((0, 1)))

        package_preds_list = []
        for i in range(N):
            num_packages = num_packages_per_instance[i]
            if num_packages > 0:
                inst_emb = global_embeds[i].unsqueeze(0).expand(num_packages, -1)
                positions = torch.arange(num_packages, device=device, dtype=torch.long)
                inst_emb_with_pos = self.package_pos_embedding(inst_emb, positions)
                preds = self.package_predictor(inst_emb_with_pos)
                package_preds_list.append(preds if training else preds.detach().cpu().numpy())
            else:
                package_preds_list.append(np.zeros((0, 1)))

        return {
            'order': list(order_preds_out),
            'item': item_preds_list,
            'package': package_preds_list
        }


def compute_loss(predictions, targets, masks, device):
    l1 = nn.L1Loss()
    total_loss = torch.tensor(0.0, dtype=torch.float32, device=device)
    total_count = torch.tensor(0.0, dtype=torch.float32, device=device)

    for key in ['order', 'item', 'package']:
        for pred, target, mask in zip(predictions[key], targets[key], masks[key]):
            target_tensor = torch.tensor(target, dtype=torch.float32, device=device).view(-1)
            mask_tensor   = torch.tensor(mask,   dtype=torch.bool,    device=device).view(-1)
            pred_tensor   = (pred if isinstance(pred, torch.Tensor)
                             else torch.tensor(pred, dtype=torch.float32, device=device)).view(-1)
            if mask_tensor.sum() == 0:
                continue
            total_loss  += l1(pred_tensor[mask_tensor], target_tensor[mask_tensor]) * mask_tensor.sum()
            total_count += mask_tensor.sum()

    return total_loss / total_count if total_count.item() > 0 else torch.tensor(0.0, device=device)


def train_epoch(model, train_data, optimizer, device):
    model.train()
    losses = []
    for sample in tqdm(train_data, desc="Training"):
        optimizer.zero_grad()
        local_texts  = sample['local_texts']
        targets      = sample['targets']
        masks        = sample['masks']
        num_items    = [len(t) for t in targets['item']]
        num_packages = [len(t) for t in targets['package']]
        preds = model.predict(local_texts, num_items, num_packages)
        loss  = compute_loss(preds, targets, masks, device)
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()
        losses.append(loss.item())
    return np.mean(losses)


def validate(model, val_data, device):
    model.eval()
    losses = []
    with torch.no_grad():
        for sample in tqdm(val_data, desc="Validation"):
            local_texts  = sample['local_texts']
            targets      = sample['targets']
            masks        = sample['masks']
            num_items    = [len(t) for t in targets['item']]
            num_packages = [len(t) for t in targets['package']]
            preds = model.predict(local_texts, num_items, num_packages)
            loss  = compute_loss(preds, targets, masks, device)
            losses.append(loss.item())
    return np.mean(losses)

def train_model(model, train_data, val_data, epochs=50, lr=2e-5, device=None, save_path=''):
    if device is None:
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model.to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=0.01)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, epochs)
    best_val_loss = float('inf')
    patience_counter = 0
    patience = 5
    import time
    start = time.time()

    for epoch in range(epochs):
        print(f"\n{'='*60}\nEpoch {epoch+1}/{epochs}\n{'='*60}")
        train_loss = train_epoch(model, train_data, optimizer, device)
        val_loss   = validate(model, val_data, device)
        print(f"Train Loss: {train_loss:.4f} | Val Loss: {val_loss:.4f} | LR: {optimizer.param_groups[0]['lr']:.2e}")
        if val_loss < best_val_loss:
            best_val_loss    = val_loss
            patience_counter = 0
            torch.save(model.state_dict(), save_path)
            print("✓ Model saved!")
        else:
            patience_counter += 1
            print("No improvement ({patience_counter}/{patience})")
            if patience_counter >= patience:
                print("Early stopping triggered!")
                break
        scheduler.step()
    print('training time', time.time() - start)
    return model



def set_seed(seed):
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    np.random.seed(seed)
    random.seed(seed)

if __name__ == "__main__":
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")

    ocel_name          = sys.argv[1]
    encoder_name       = sys.argv[2]
    global_encoder_type = sys.argv[3] if len(sys.argv) > 3 else 'conv1d'  # 'conv1d' | 'mlp' | 'transformer'

    with open(f'ocel/{ocel_name}/{ocel_name}_train_dict', 'rb') as f:
        train_samples = pickle.load(f)
    with open(f'ocel/{ocel_name}/{ocel_name}_val_dict', 'rb') as f:
        val_samples = pickle.load(f)
    with open(f'ocel/{ocel_name}/{ocel_name}_test_dict', 'rb') as f:
        test_samples = pickle.load(f)

    seed = 42
    set_seed(seed)
    os.environ['PYTHONHASHSEED']        = str(seed)
    os.environ['CUBLAS_WORKSPACE_CONFIG'] = ':4096:8'
    r = random.Random(seed)
    shuffled_train = r.sample(train_samples, k=len(train_samples))

    model = SimpleGlobalTextPredictor(
        encoder_name=encoder_name,
        hidden_dim=512,
        freeze_encoder=False,
        global_encoder_type=global_encoder_type,
        num_global_layers=4,
    )

    print(f"Global encoder type: {global_encoder_type}")
    print(f"Total parameters:     {sum(p.numel() for p in model.parameters()):,}")
    print(f"Trainable parameters: {sum(p.numel() for p in model.parameters() if p.requires_grad):,}")

    if encoder_name == 'cross-encoder/ms-marco-TinyBERT-L2-v2':
            encoder_name = 'TinyBert'
    elif encoder_name =='prajjwal1/bert-medium':
            encoder_name = 'bertmedium'

    trained_model = train_model(
        model, shuffled_train, val_samples,
        epochs=20, lr=2e-5, device=device,
        save_path=(
            f'neural_network/model/global/'
            f'best_model_{global_encoder_type}_{ocel_name}_{encoder_name}.pt'
        )
    )
    print("\nTraining complete!")