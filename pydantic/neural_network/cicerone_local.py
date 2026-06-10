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

        self.order_predictor = nn.Sequential(
            nn.Linear(hidden_dim, 256), nn.GELU(), nn.Dropout(0.1),
            nn.Linear(256, 64), nn.GELU(), nn.Linear(64, 1)
        )
     
    def encode_text(self, texts):
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
            projected = self.projection(cls_embeddings)
            embeddings.append(projected)

        return torch.cat(embeddings, dim=0)


    def forward(self, text: str) -> torch.Tensor:
        return self.encode_text(text)

    def predict(self, local_texts: List[str]) -> List[torch.Tensor]:
        predictions = []
        
        for i in range(len(local_texts)):
            text_emb = self.forward(local_texts[i])
            order_pred = self.order_predictor(text_emb)
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

def evaluate_model(model, test_data, device):
    model.eval()
    l1 = nn.L1Loss()
    
    error = torch.tensor(0.0, device=device)
    count = torch.tensor(0.0, device=device)

    with torch.no_grad():
        for sample in tqdm(test_data, desc="Evaluating"):
            local_texts = sample['local_texts']
            targets = sample['targets']['issue']
            masks = sample['masks']['issue']

            preds = model.predict(local_texts)

            for pred, target, mask in zip(preds, targets, masks):
                target_tensor = torch.tensor(target, dtype=torch.float32, device=device).view(-1)
                mask_tensor = torch.tensor(mask, dtype=torch.bool, device=device).view(-1)

                if isinstance(pred, np.ndarray):
                    pred_tensor = torch.tensor(pred, dtype=torch.float32, device=device).view(-1)
                else:
                    pred_tensor = pred.view(-1)

                if mask_tensor.sum() == 0:
                    continue

                loss_masked = l1(pred_tensor[mask_tensor], target_tensor[mask_tensor])
                error += loss_masked * mask_tensor.sum()
                count += mask_tensor.sum()

    mae = (error / count).item() if count.item() > 0 else 0.0

    print("\n" + "="*60)
    print("TEST RESULTS")
    print("="*60)
    print(f"issue MAE: {mae:.4f}")
    print("="*60)

    return {'issue': mae}

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
    trained_model = train_model(
        model, shuffled_train, val_samples,
        epochs=20, lr=2e-5, device=device,
        save_path=f'neural_network/model/local/best_model_local_{ocel_name}_{encoder_name}.pt'
    )