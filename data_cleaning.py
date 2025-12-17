import pandas as pd


df = pd.read_csv("part_level_tariff.csv")

id_vars = [
    'ROW ID #', 'Division', 'Part #', 'Item Description', 'Material',
    'Manufacturing class id', 'Average copper content (%)',
    'Base copper tariff \n(applicable to brass, copper and bronze dependent on average copper content %)',
    'Brass, copper, bronze tariff', 'Steel and iron tariff',
    'Aluminum tariff'
]

# Melt the dataframe
df_long = df.melt(
    id_vars=id_vars, 
    var_name="country", 
    value_name="tariff_value"
)

import numpy as np
import pandas as pd

# Define material groups
brass_group = [
    "Brass", "Brass/plastic", "Bronze", "Copper ",
    "Lead-free brass", "Lead-free bronze"
]
steel_group = ["Steel", "Stainless Steel ", "Iron"]

def safe_numeric(val):
    """Convert to float if possible, else 0"""
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0

def get_tariff(row):
    mat = row["Material"].strip() if isinstance(row["Material"], str) else row["Material"]

    if mat in brass_group:
        return safe_numeric(row["Brass, copper, bronze tariff"])
    elif mat in steel_group:
        return safe_numeric(row["Steel and iron tariff"])
    elif mat == "Aluminum":
        return safe_numeric(row["Aluminum tariff"])
    elif mat == "Zinc":
        return 0
    else:
        return 0   # fallback if unexpected material

# Apply function
df_long["Metal Tariff"] = df_long.apply(get_tariff, axis=1)

# Final sub dataframe
sub_df = df_long[["ROW ID #", "Material", "country", "tariff_value", "Metal Tariff"]]
sub_df["Material"] = sub_df["Material"].astype(str).str.strip()

print(sub_df.head(20))

sub_df.to_csv("tariff_part_level_cleaned 2.csv", index=False)
