# Report Grain

- One row = one task
    

---

# Inputs

- date range
    
- service id's
    
- loadData flag
    

---

# Output

## rows[]

- work_order_id
    
- service_name
    
- service_status
    
- task_id
    
- task_date
    
- task_labor_hours
    
- task_material
    
- est_material
    
- target_rev_uom
    
- actual_rev_uom
    
- ns_invoiced
    

## message

---

# Main Query `__invoke`

## Responsibilities

- checks users permission
    
- checks loaddata flag
    
- calls `getFilteredReportRows($request)`
    

## Returns

- rows[]
    
- message
    

---

# Main Data Builder

## `getFilteredReportRows($request)`

### Purpose

Builds the final report rows for the frontend/export.

### Grain

- One row = one task
    

### Flow

1. Parse date range
    
2. Get matching tasks
    
3. Extract work order IDs from tasks
    
4. Load work order + service info
    
5. Build lookup arrays
    
    - labor by task
        
    - actual material by task
        
    - actual material by work order
        
    - estimated material by work order
        
6. Load NetSuite invoice/sales order amounts by work order
    
7. Calculate:
    
    - target rev / UOM
        
    - actual rev / UOM
        
8. Map each task into one final report row
    

### Key Formulas

- target rev / UOM = sales order amount / estimated material
    
- actual rev / UOM = invoice amount / actual material
    

---

# NetSuite

## Inputs

- job ids
    

## Returns

```php
[
    'pep_job_id' => 1001,
    'record_type' => 'invoice',
    'workorder_id' => 555,
    'line_amount' => 3200.50,
]
```

---

# Functions

## `actualLaborHoursBytask(request, $daterange)`

Returns actual labor hours.

### Formula

```sql
SUM((timeclock.out - timeclock.in) / 60)
```

^actual-labor-hours

---

## `actualTaskMaterialBytaskId(request, $daterange)`

Uses `task_material` table.

### Formula

```sql
SUM(task_material.actual)
```

^actual-task-material

---

## `computeTargetRevUomByWorkOrderId`

### Formula

```text
target rev / UOM = sales order amount / estimated material
```

^targetRevUOM

---

## `computeActualRevUomByWorkOrderId`

### Logic

```text
if service.act_matieral > service.est_matieral
OR service.status = complete

    actual rev / UOM =
        sales order amount / actual material

else
    null
```

^actualRevUOM

---

## `tasks -> daystouched()`

### Inputs

- users_count
    
- total_labor_hours
    

### Logic

```text
adj_value =
    (total_labor_hours / user_count) * .105

if adj_value > 1
    return 1
else
    return adj_value
```

^task-daystouched

---

# Data Columns

## Task Labor

- Calculated → [[#^actual-labor-hours]]
    
- actual labor hours
    

---

## Labor Rate

Pulled from cost override table.

### Source

```text
Labor -> Labor std / Labor Override
```

### Tied By

- region id
    

---

## Labor Cost

### Formula

```text
Task_labor * Labor Rate
```

---

## Task Material

- Calculated → [[#^actual-task-material]]
    

---

## Mat Rate

### Source

Pulled from cost override table (front-end).

```text
Cost -> Mat std / Mat Override
```

---

## Mat Cost

### Formula

```text
Mat_Rate * Task_Material
```

---

## Task Equip

- Calculated → [[#^task-daystouched]]
    

---

## Equip Rate

### Source

Pulled from cost override table.

```text
Fleet Cost Tab -> Cost / Cost Override
```

---

## Equip Cost

### Formula

```text
Task Equip * Equip Rate
```

---

## Target Rev / UOM

- Calculated → [[#^targetRevUOM]]
    

### Formula

```text
target rev / UOM =
    sales order amount / estimated material
```

---

## Actual Rev / UOM

- Calculated → [[#^actualRevUOM]]
    

### Formula

```text
actual rev / UOM =
    sales order amount / actual material
```

### Condition

```text
if actual material > estimated material
OR service.status = complete
```

---

## Earned Revenue

### Logic

```text
if service.act_matieral > service.est_matieral
OR service.status = complete

    Actual_material * (Actual Rev / UOM)

else

    Actual_material * (Target Rev / UOM)
```

---

## Revenue

---

## NS Invoiced

### Source

Pulled from NetSuite API.

```text
Proposal and sales order by service
```

---

## Total Direct Costs

---

## DM

---

## DM%