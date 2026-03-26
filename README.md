<table border="0">
  <tr>
    <td valign="middle" width="200">
<img width="491" height="509" alt="cicerone" src="https://github.com/user-attachments/assets/0b60aa2f-9d41-4feb-aa73-99cff6810637" />
    </td>
    <td align="right" valign="middle">
      <h1>CICERONE: A Natural Language-based Global Approach for Object-Centric Predictive Process Monitoring</h1>
    </td>
  </tr>
</table>

**The repository contains code referred to the work:**

*Vincenzo Pasquadibisceglie, Annalisa Appice, Donato Malerba*


[*CICERONE: A Natural Language-based Global Approach for Object-Centric Predictive Process Monitoring*]

# How to Use

## Step 1: Prepare the Dataset
Move to the specific OCEL directory (e.g., ORDER_MANAGEMENT).
Then generate the process execution, specifying the alpha value (0, 0.1, or 0.2).
```
python -m generate_process_exe 0
```
## Step 2: Generate the semantic stories of the object-centric process executions
```
python -m generate_local_texts 0
```
## Step 3: Generate the global batches of the object-centric process executions
```
python -m generate_global_batches 0
```
## Step 4: CICERONE Training

Once the labeled examples have been generated, you can train CICERONE.

### CICERONE Global
```
python -m neural_network.cicerone_global order_management_0 prajjwal1/bert-medium
```
### CICERONE Local
```
python -m neural_network.cicerone_local order_management_0 prajjwal1/bert-medium
```
