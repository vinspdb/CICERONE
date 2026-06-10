import torch
import torch.nn as nn
from transformers import AutoModel, AutoTokenizer
import numpy as np
from typing import List
from tqdm import tqdm
import pickle
import time
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

        self.order_predictor = nn.Sequential(
            nn.Linear(hidden_dim, 256), nn.GELU(), nn.Dropout(0.1),
            nn.Linear(256, 64), nn.GELU(), nn.Linear(64, 1)
        )

    def encode_instances(self, texts: List[List[str]]) -> torch.Tensor:
        device = next(self.encoder.parameters()).device
        embeddings = []
        batch_size = 32

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

def evaluate_model(model, test_data, device):
    model.eval()
    torch.cuda.reset_peak_memory_stats(device)
    torch.cuda.synchronize()
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
    print(count)
    print("\n" + "="*60)
    print("TEST RESULTS")
    print("="*60)
    print(f"Order MAE: {mae:.4f}")
    print("="*60)
    peak_memory_mb = torch.cuda.max_memory_allocated(device) / 1024**2
    
    print(f"Peak GPU allocated: {peak_memory_mb:.2f} MB")

    return {'issue': mae}


if __name__ == "__main__":
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")
    import sys
    ocel_name = sys.argv[1]
    encoder_name = sys.argv[2]
    global_encoder_type = sys.argv[3] if len(sys.argv) > 3 else 'conv1d'  # 'conv1d' | 'rnn' | 'lstm'

    
    with open(f'ocel/{ocel_name}/{ocel_name}_train_dict', 'rb') as f:
        train_samples = pickle.load(f)
    with open(f'ocel/{ocel_name}/{ocel_name}_val_dict', 'rb') as f:
        val_samples = pickle.load(f)
    with open(f'ocel/{ocel_name}/{ocel_name}_test_dict', 'rb') as f:
        test_samples = pickle.load(f)

    model = SimpleGlobalTextPredictor(
        encoder_name=encoder_name,
        hidden_dim=512,
        freeze_encoder=False,
        global_encoder_type=global_encoder_type
    )
    print(model)

    print(f"Total parameters: {sum(p.numel() for p in model.parameters()):,}")
    print(f"Trainable parameters: {sum(p.numel() for p in model.parameters() if p.requires_grad):,}")
    if encoder_name == 'cross-encoder/ms-marco-TinyBERT-L2-v2':
                encoder_name = 'TinyBert'
    elif encoder_name == 'prajjwal1/bert-medium':
                encoder_name = 'bertmedium'
    state_dict = torch.load(f"neural_network/model/global/best_model_{global_encoder_type}_{ocel_name}_{encoder_name}.pt", map_location=device)
    model.load_state_dict(state_dict)

    model.to(device)
    model.eval()

    wall_start = time.time()
    mae_results = evaluate_model(model, test_samples, device)
    wall_end = time.time()
    training_time_s = wall_end - wall_start
    print("Evaluation time-->", training_time_s)