import torch
import torch.nn as nn
from transformers import AutoModel, AutoTokenizer
from tqdm import tqdm
import numpy as np
from typing import List, Dict
import pickle
import random
import sys
import os
import time
import json

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

class SimpleGlobalTextPredictor(nn.Module):
    def __init__(
        self,
        encoder_name='bert-base-uncased',
        hidden_dim=768,
        freeze_encoder=False,
        global_encoder_type='conv1d',   # 'conv1d' | 'rnn' | 'lstm'
        num_global_layers=4,
    ):
        super().__init__()
        assert global_encoder_type in ('conv1d', 'rnn', 'lstm'), \
            f"global_encoder_type deve essere 'conv1d', 'rnn' o 'lstm', ricevuto: {global_encoder_type}"

        self.encoder = AutoModel.from_pretrained(encoder_name, local_files_only=True)
        self.tokenizer = AutoTokenizer.from_pretrained(encoder_name, truncation_side='left', local_files_only=True)

        if freeze_encoder:
            for param in self.encoder.parameters():
                param.requires_grad = False

        encoder_hidden_size = self.encoder.config.hidden_size
        self.instance_projection = nn.Linear(encoder_hidden_size, hidden_dim)

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

        self.order_predictor = nn.Sequential(
            nn.Linear(hidden_dim, 256), nn.GELU(), nn.Dropout(0.1),
            nn.Linear(256, 64), nn.GELU(), nn.Linear(64, 1)
        )

    def encode_instances(self, texts: List[List[str]]) -> torch.Tensor:
        device = next(self.encoder.parameters()).device
        embeddings = []
        batch_size = 1

        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i:i + batch_size]
            batch_texts = [
                "\n".join([str(t) for t in item if t is not None])
                if isinstance(item, list) else item
                for item in batch_texts
            ]

            inputs = self.tokenizer(
                batch_texts,
                return_tensors="pt",
                truncation=True,
                padding='longest',
                max_length=512
            )
            inputs = {k: v.to(device) for k, v in inputs.items()}

            with torch.set_grad_enabled(self.training):
                outputs = self.encoder(**inputs)

            cls_embeddings = outputs.last_hidden_state[:, 0, :]
            projected = self.instance_projection(cls_embeddings)
            embeddings.append(projected)

        return torch.cat(embeddings, dim=0)

    def forward(self, local_texts: List[str]) -> torch.Tensor:
        instance_embeds = self.encode_instances(local_texts)
        x = instance_embeds.unsqueeze(0)
        x = x.transpose(1, 2)  
        x = self.global_transformer(x) 
        x = x.transpose(1, 2) 
        x = x.squeeze(0)
        return self.layer_norm(x)

    def predict(self, local_texts):
        global_embeds = self.forward(local_texts)
        predictions = []
        
        for i in range(len(local_texts)):
            inst_emb = global_embeds[i]
            order_pred = self.order_predictor(inst_emb)
            predictions.append(order_pred)

        return predictions


def compute_loss(predictions: List, targets: List, masks: List, device: torch.device) -> torch.Tensor:

    total_loss = torch.tensor(0.0, dtype=torch.float32, device=device) 
    total_count = torch.tensor(0.0, dtype=torch.float32, device=device)    
    l1 = nn.L1Loss()

    for pred, target, mask in zip(predictions, targets, masks):

        pred = pred.to(device).float().view(-1)
        
        target = torch.as_tensor(target, dtype=torch.float32, device=device).view(-1)
        mask = torch.as_tensor(mask, dtype=torch.bool, device=device).view(-1)
        
        if mask.sum() == 0:
            continue
        
        loss_masked = l1(pred[mask], target[mask])

        total_loss += loss_masked * mask.sum()
        total_count += mask.sum()
    
    if total_count.item() == 0:
                return sum(p.sum() for p in predictions) * 0  
    return total_loss / total_count

def train_epoch(model, train_data, optimizer, device):
    model.train()
    losses = []
    for sample in tqdm(train_data, desc="Training"):
        optimizer.zero_grad()
        local_texts = sample['local_texts']
        targets = sample['targets']['issue']
        masks = sample['masks']['issue']

        preds = model.predict(local_texts)
        loss = compute_loss(preds, targets, masks, device)
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
            local_texts = sample['local_texts']
            targets = sample['targets']['issue']
            masks = sample['masks']['issue']

            preds = model.predict(local_texts)
            loss = compute_loss(preds, targets, masks, device)
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

    for epoch in range(epochs):
        print(f"\n{'='*60}\nEpoch {epoch+1}/{epochs}\n{'='*60}")
        train_loss = train_epoch(model, train_data, optimizer, device)
        val_loss = validate(model, val_data, device)
        print(f"Train Loss: {train_loss:.4f} | Val Loss: {val_loss:.4f} | LR: {optimizer.param_groups[0]['lr']:.2e}")

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            patience_counter = 0
            torch.save(model.state_dict(), save_path)
            print("Model saved!")
        else:
            patience_counter += 1
            print(f"No improvement ({patience_counter}/{patience})")
            if patience_counter >= patience:
                print("Early stopping triggered!")
                break

        scheduler.step()
    return model


def set_seed(seed):
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    np.random.seed(seed)
    random.seed(seed)



if __name__ == "__main__":
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")
    ocel_name = sys.argv[1]
    encoder_name = sys.argv[2]
    global_encoder_type = sys.argv[3] if len(sys.argv) > 3 else 'conv1d'  # 'conv1d' | 'rnn' | 'lstm'

    with open(f'ocel/{ocel_name}/{ocel_name}_train_dict', 'rb') as f:
        train_samples = pickle.load(f)
    with open(f'ocel/{ocel_name}/{ocel_name}_val_dict', 'rb') as f:
        val_samples = pickle.load(f)
    with open(f'ocel/{ocel_name}/{ocel_name}_test_dict', 'rb') as f:
        test_samples = pickle.load(f)
    
    seed = 123
    set_seed(seed)
    os.environ['PYTHONHASHSEED'] = str(seed)
    os.environ['CUBLAS_WORKSPACE_CONFIG'] = ':4096:8'
    r = random.Random(seed)
    shuffled_train = r.sample(train_samples, k=len(train_samples))
    
    model = SimpleGlobalTextPredictor(
        encoder_name=encoder_name,
        hidden_dim=512,
        freeze_encoder=False,
        global_encoder_type=global_encoder_type
    )

    total_params    = sum(p.numel() for p in model.parameters())
    trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)

    print(f"Global encoder type: {global_encoder_type}")
    print(f"Total parameters:     {sum(p.numel() for p in model.parameters()):,}")
    print(f"Trainable parameters: {sum(p.numel() for p in model.parameters() if p.requires_grad):,}")

    if device.type == 'cuda':
        torch.cuda.reset_peak_memory_stats(device)
        torch.cuda.synchronize(device)

    wall_start = time.time()

    enc_short = encoder_name
    if encoder_name == 'cross-encoder/ms-marco-TinyBERT-L2-v2':
        enc_short = 'TinyBert'
    elif encoder_name == 'prajjwal1/bert-medium':
        enc_short = 'bertmedium'

    trained_model = train_model(
        model, shuffled_train, val_samples,
        epochs=20, lr=2e-5, device=device,
        save_path=(
            f'neural_network/model/global/'
            f'best_model_{global_encoder_type}_{ocel_name}_{enc_short}.pt'
        )
    )

    wall_end = time.time()
    training_time_s = wall_end - wall_start

    if device.type == 'cuda':
        torch.cuda.synchronize(device)
        peak_gpu_train_mb = torch.cuda.max_memory_allocated(device) / 1024**2
        gpu_name          = torch.cuda.get_device_name(device)
    else:
        peak_gpu_train_mb = 0.0
        gpu_name          = 'cpu'

    results = {
        "ocel":               ocel_name,
        "encoder":            encoder_name,
        "global_encoder_type": global_encoder_type,
        "total_params":       total_params,
        "trainable_params":   trainable_params,
        "training_time_s":    round(training_time_s, 2),
        "training_time_min":  round(training_time_s / 60, 3),
        "peak_gpu_train_mb":  round(peak_gpu_train_mb, 1),
        "device":             gpu_name,
    }

    out_dir  = 'neural_network/benchmarks'
    os.makedirs(out_dir, exist_ok=True)
    out_file = f'{out_dir}/benchmark_{global_encoder_type}_{ocel_name}_{enc_short}.json'

    with open(out_file, 'w') as f:
        json.dump(results, f, indent=2)

    print("\n" + "="*60)
    print("BENCHMARK RESULTS")
    print("="*60)
    for k, v in results.items():
        print(f"  {k:<28} {v}")
    print(f"\nSaved → {out_file}")