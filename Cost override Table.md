
# Modal Structure

| Tab | Purpose |
|---|---|
| Cost | Material cost + MPLH overrides |
| Fleet | Fleet category crew-day overrides |
| Fleet Cost | Service-level equipment overrides |
| Labor | Region labor rate overrides |

---

# Cost Tab

| Column | Meaning | Source |
|---|---|---|
| Id | material id | DB |
| Mat | material name | DB |
| Target MPLH | baseline MPLH | DB (`materials.target_mplh`) |
| MPLH Override | user override | localStorage |
| Mat Std | standard material cost | DB (`materials.cost`) |
| Mat Override | override cost | localStorage |

---

# Fleet Tab

| Column | Meaning | Source |
|---|---|---|
| Fleet | fleet category id | Hardcoded PHP |
| Category | equipment label | Hardcoded PHP |
| Per | unit label (`HR`) | Hardcoded PHP |
| Hr Cost | hourly equipment cost | Hardcoded PHP |
| Crew Day Cost | hr cost × 10 | Derived PHP |
| Crew Day Override | override value | localStorage |

---

# Fleet Cost Tab

| Column | Meaning | Source |
|---|---|---|
| Service ID | service id | DB |
| Service Name | service abbreviation | DB |
| Cost | standard equipment cost | Hardcoded Vue |
| Cost Override | override value | localStorage |
| Target MPEG | default MPEG | Hardcoded Vue |
| MPEG Override | override value | localStorage |

---

# Labor Tab

| Column | Meaning | Source |
|---|---|---|
| Region ID | region id | DB |
| Region | region name | DB |
| Labor Std | default labor rate | DB |
| Labor Override | override value | localStorage |

---
# Simplified Architecture

## Pulled From DB

- materials
- regions
- services
- work orders

---

## Hardcoded

- Fleet categories
- Fleet costs
- MPEG defaults
- Service lists

---

## Stored In localStorage

- labor overrides
- material overrides
- MPLH overrides
- fleet overrides
- MPEG overrides
