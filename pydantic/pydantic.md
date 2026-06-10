# How to Use

## Step 1: Downnlod the Dataset via PyStack
To download the dataset, PyStack requires the GITHUB_ACCESS_TOKEN
```
python -m extract_ocel
```
## Step 2: Generate the process execution
```
python -m generate_process_exe
```
## Step 3: Generate the global batches of the object-centric process executions
```
python -m generate_texts
```
## Step 4: CICERONE Training

Once the labeled examples have been generated, you can train CICERONE.

### CICERONE Global
```
python -m neural_network.cicerone_global pydantic prajjwal1/bert-medium
```
### CICERONE Local
```
python -m neural_network.cicerone_local pydantic prajjwal1/bert-medium
```
