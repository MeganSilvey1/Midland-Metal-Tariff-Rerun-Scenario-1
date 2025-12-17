import os
import numpy as np
import pandas as pd

scenario_file = 'scenario_outputs/scenario 3 12052025 2.xlsx'
bidsheet_file = 'new/Bidsheet Master Consolidate Landed 12052025.csv'
output_path = os.path.join('scenario_outputs', 'scenario 3 12052025 added columns 2.xlsx')

port_country_map = {
    'DALIAN': 'China', 
    'NINGBO': 'China', 
    'QINGDAO': 'China', 
    'QINGDAO2': 'China', 
    'SHANGHAI': 'China',
    'SHENZHEN': 'China', 
    'TIANJIN': 'China', 
    'XINGANG': 'China', 
    'XIAMEN': 'China',
    'AHMEDABAD': 'India', 
    'CHENNAI': 'India', 
    'DADRI': 'India', 
    'MUMBAI': 'India',
    'MUNDRA': 'India', 
    'NHAVA SHEVA': 'India',
    'SURABAYA': 'Indonesia',
    'PORT KLANG': 'Malaysia', 
    'PASIR GUDANG': 'Malaysia', 
    'TANJUNG PELAPAS': 'Malaysia',
    'BUSAN': 'South Korea',
    'KAOHSIUNG': 'Taiwan', 
    'KEELUNG': 'Taiwan', 
    'TAICHUNG': 'Taiwan', 
    'TAIPEI': 'Taiwan',
    'BANGKOK': 'Thailand',
    'LAEM CHABANG': 'Thailand',
    'HO CHI MINH CITY': 'Vietnam', 
    'VUNG TAU': 'Vietnam', 
    'HAI PHONG': 'Vietnam',
    'VIRGINIA': 'India'
}

# Read first 13 rows to preserve them in output
header_rows_df = pd.read_excel(scenario_file, nrows=13, header=None)

# Read scenario and bidsheet files (skip first 13 rows for processing)
scenario_df = pd.read_excel(scenario_file, skiprows=13)
bidsheet_df = pd.read_csv(bidsheet_file)

supplier_port_file = "Supplier Port per Part table 070925.csv"
freight_file = "Freight cost mutipliers table 071025v2.csv"

freight_df = pd.read_csv(freight_file) 
supplier_port_df = pd.read_csv(supplier_port_file)
tariff_df_2 = pd.read_csv("tariff_part_level_cleaned.csv")


# Set indices for fast lookup
tariff_df_2.set_index('ROW ID #', inplace=True)
supplier_port_df.set_index('ROW ID #', inplace=True)
freight_df.set_index('Reference', inplace=True)



def get_supplier_info(row_id, supplier):
    """
    Returns a dict with:
    - Division
    - Port
    - FreightMultiplier
    - Tariff values (tariff_value, Metal Tariff, Metal Type, Country)
    
    Returns None if any info is missing.
    """
    try:
        # 1️⃣ Get supplier port and division from supplier_port_df
        supplier_row = supplier_port_df.loc[row_id]
        division = supplier_row['Division']
        port = supplier_row[supplier]
        country = port_country_map[port]
        # 2️⃣ Get freight multiplier from freight_df
        freight_multiplier = freight_df.loc[port, division]

        # 3️⃣ Get tariff info from tariff_df_2
        tariff_rows = tariff_df_2.loc[row_id]
        if isinstance(tariff_rows, pd.Series):
            tariff_row = tariff_rows
        else:
            # Filter to match the country
            tariff_row = tariff_rows[tariff_rows['Country'] == country]
            if tariff_row.empty:
                return None
            tariff_row = tariff_row.iloc[0]

        tariff_value = float(tariff_row['tariff_value'])
        metal_tariff = float(tariff_row['Metal Tariff'])
        metal_type = tariff_row['Metal Type']
        
        return {
            'Division': division,
            'Port': port,
            'FreightMultiplier': float(freight_multiplier),
            'tariff_value': float(tariff_value),
            'Metal Tariff': float(metal_tariff),
            'Metal Type': metal_type,
            'Country': country
        }

    except KeyError:
        # Any missing row or column returns None
        return None
    

# You may need to adjust the key column to match your data
key_col = 'ROW ID #'  # Change this to your actual key column

# Set bidsheet index for fast lookup
bidsheet_map = bidsheet_df.set_index(key_col)

def get_bidsheet_value(row, col_name):
    key = row.get(key_col)
    try:
        return bidsheet_map.loc[key, col_name]
    except Exception:
        return np.nan

def get_bidsheet_value2(row_id, col_name):
    key = row_id
    try:
        return bidsheet_map.loc[key, col_name]
    except Exception:
        return np.nan

# Add new columns next to Annual Volume (per UOM)
scenario_df['Wapp FOB'] = scenario_df.apply(lambda row: get_bidsheet_value(row, 'Volume-banded WAPP'), axis=1)
scenario_df['Wapp landed from July'] = scenario_df.apply(lambda row: get_bidsheet_value(row, 'Volume-banded WAPP Landed Cost'), axis=1)


for idx, row in scenario_df.iterrows():
    incumbent = row['Incumbent Supplier']
    selected = row['Selected Supplier']
    row_id = row['ROW ID #']

    # Only act if incumbent and selected supplier are the same
    if incumbent == selected:
        # Column to check in df
        cost_col = f'{incumbent} - R2 - Total Cost Per UOM FOB Port of Origin/Departure (USD)'

        # Check if row exists in df
        matching_row = bidsheet_df[bidsheet_df['ROW ID #'] == row_id]
        if not matching_row.empty:
            matching_row = matching_row.iloc[0]  # take first matching row
            # Check if cost_col exists and has valid value
            if cost_col in matching_row and pd.notna(matching_row[cost_col]) and matching_row[cost_col] not in [0, '-', '']:
                scenario_df.at[idx, 'Final quote per each FOB Port of Departure (USD)'] = matching_row[cost_col]
            

            else:
                # fallback to Volume-banded WAPP
                scenario_df.at[idx, 'Final quote per each FOB Port of Departure (USD)'] = matching_row['Volume-banded WAPP']


for idx, row in scenario_df.iterrows():
    selected_supplier = row['Selected Supplier']
    row_id = row['ROW ID #']

    if selected_supplier == '-':
        scenario_df.at[idx, 'Final quote per each landed (USD)'] = np.nan
        continue

    cost_col = f'{selected_supplier} - R2 - Total landed cost per UOM (USD)'

    matching_row = bidsheet_df[bidsheet_df['ROW ID #'] == row_id]
    if not matching_row.empty:
        matching_row = matching_row.iloc[0]
        if cost_col in matching_row and pd.notna(matching_row[cost_col]) and matching_row[cost_col] not in [0, '-', '']:
            scenario_df.at[idx, 'Final quote per each landed (USD)'] = matching_row[cost_col]
        else:
            scenario_df.at[idx, 'Final quote per each landed (USD)'] = matching_row['Volume-banded WAPP Landed Cost']
    else:
        scenario_df.at[idx, 'Final quote per each landed (USD)'] = matching_row['Volume-banded WAPP Landed Cost']


scenario_df['Final quote per each FOB Port of Departure (USD)'] = pd.to_numeric(scenario_df['Final quote per each FOB Port of Departure (USD)'], errors='coerce')
scenario_df['Annual Volume (per UOM)'] = pd.to_numeric(scenario_df['Annual Volume (per UOM)'], errors='coerce')
scenario_df['FOB Extended Cost (USD)'] = scenario_df['Final quote per each FOB Port of Departure (USD)'] * scenario_df['Annual Volume (per UOM)']

def incumbent_fob(row):
    incumbent_supplier = get_bidsheet_value(row, 'Normalized incumbent supplier')
    val = get_bidsheet_value(row, f'{incumbent_supplier} - R2 - Total Cost Per UOM FOB Port of Origin/Departure (USD)')
    wapp_fob = row['Wapp FOB']
    return wapp_fob if val == 0 or pd.isna(val) else val

def incumbent_landed(row):
    row_id = row.get('ROW ID #')
    incumbent_supplier = get_bidsheet_value(row, 'Normalized incumbent supplier')
    val = get_bidsheet_value(row, f'{incumbent_supplier} - R2 - Total landed cost per UOM (USD)')

    wapp_landed_cost = 0
    if incumbent_supplier != '-':
        wapp_price = get_bidsheet_value(row, 'Volume-banded WAPP')
        multiplier_info = get_supplier_info(row_id, incumbent_supplier)
        wapp_landed_cost = wapp_price * multiplier_info['FreightMultiplier'] + wapp_price * (multiplier_info['tariff_value'] + multiplier_info['Metal Tariff'])

    return wapp_landed_cost if val == 0 or pd.isna(val) else val

scenario_df['Incumbent bid or WAPP in case no bid FOB'] = scenario_df.apply(incumbent_fob, axis=1)
scenario_df['Incumbent bid or WAPP in case no bid landed'] = scenario_df.apply(incumbent_landed, axis=1)

def final_quote_landed(row):
    selected_supplier = row.get('Selected Supplier')
    key = row.get(key_col)
    col_name = f'{selected_supplier} - R2 - Total landed cost per UOM (USD)'

    incumbent = row.get('Incumbent Supplier')
    if key in [12700]:
        is_kg=True
    wapp_price = get_bidsheet_value(row, 'Volume-banded WAPP')
    multiplier_info = get_supplier_info(key, incumbent)
    wapp_landed_cost = '9090'
    if incumbent!= '-':
        wapp_landed_cost = wapp_price * multiplier_info['FreightMultiplier'] + wapp_price * (multiplier_info['tariff_value'] + multiplier_info['Metal Tariff'])
        print('----***---')
        print(wapp_price, multiplier_info)
    try:
        landed_cost = bidsheet_map.loc[key, col_name] 
        return  landed_cost if landed_cost not in [0, '', '-'] else wapp_landed_cost
    except Exception:
        return wapp_landed_cost

scenario_df['Final quote per each landed (USD)'] = scenario_df.apply(final_quote_landed, axis=1)


# --- Add columns for 2nd best, 3rd best, ... supplier bids ---
# Load supplier port file and port-country mapping
supplier_port_file = "Supplier Port per Part table 070925.csv"
port_country_map = {
    'DALIAN': 'China', 
    'NINGBO': 'China', 
    'QINGDAO': 'China', 
    'QINGDAO2': 'China', 
    'SHANGHAI': 'China',
    'SHENZHEN': 'China', 
    'TIANJIN': 'China', 
    'XINGANG': 'China', 
    'XIAMEN': 'China',
    'AHMEDABAD': 'India', 
    'CHENNAI': 'India', 
    'DADRI': 'India', 
    'MUMBAI': 'India',
    'MUNDRA': 'India', 
    'NHAVA SHEVA': 'India',
    'SURABAYA': 'Indonesia',
    'PORT KLANG': 'Malaysia', 
    'PASIR GUDANG': 'Malaysia', 
    'TANJUNG PELAPAS': 'Malaysia',
    'BUSAN': 'South Korea',
    'KAOHSIUNG': 'Taiwan', 
    'KEELUNG': 'Taiwan', 
    'TAICHUNG': 'Taiwan', 
    'TAIPEI': 'Taiwan',
    'BANGKOK': 'Thailand',
    'LAEM CHABANG': 'Thailand',
    'HO CHI MINH CITY': 'Vietnam', 
    'VUNG TAU': 'Vietnam', 
    'HAI PHONG': 'Vietnam',
    'VIRGINIA': 'India'
}
supplier_port_df = pd.read_csv(supplier_port_file)
supplier_port_df = supplier_port_df.set_index('ROW ID #')

# Helper to get supplier country from port mapping
def get_supplier_country(row_id, supplier_name):
    try:
        port = supplier_port_df.loc[row_id, supplier_name] if supplier_name in supplier_port_df.columns else np.nan
        if pd.isna(port):
            return np.nan
        return port_country_map.get(str(port).strip().upper(), np.nan)
    except Exception:
        return np.nan

CODA_NOT_SUPPLY = ["1163", "1164", "1165", "1166", "1167", "1173", "1176", "1177", "1178", "1179", "1180", "1181", "1182", "1183", "1184", "1185", "1186", "1187", "1188", "1190", "1213", "1277", "1288", "1289", "1290", "1305", "1306", "1308", "1309", "1310", "1311", "1312", "1318", "1319", "1320", "1321", "1322", "1323", "1327", "1328", "1333", "1335", "1341", "1342", "1346", "1347", "1348", "1352", "1358", "1359", "1360", "1361", "1362", "1364", "1365", "1366", "1367", "1368", "1369", "1370", "1372", "1374", "1379", "1386", "1387", "1388", "1389", "1390", "1393", "1394", "1395", "1396", "1397", "1398", "1399", "1400", "1405", "1406", "1407", "1408", "1409", "1410", "1411", "1412", "1413", "1414", "1415", "1416", "1417", "1418", "1425", "1429", "1430", "1439", "1441", "1445", "1446", "1448", "1489", "1490", "1498", "1499", "1510", "1511", "1516", "1520", "6813", "6815", "6825", "6838", "6839", "6844", "6851", "6852", "6864", "6866", "6890", "6893", "6909", "6910", "6911", "6912", "6917", "6918", "6919", "6927", "6928", "6929", "6930", "6932", "6933", "6934", "6939", "7060", "7071", "7072", "7073", "7076", "7089", "7090", "7102", "7111", "7117", "7119", "7125", "7126", "7136", "7145", "7185", "7186", "7187", "7188", "7189", "7197", "7207", "7209", "7210", "7211", "7212", "7213", "7214", "7215", "7254", "7256", "7300", "7301", "7306", "7331", "7332", "7826", "7919", "8742", "8772", "8915", "9994", "13613"]
ZHEJIANG_WANDEKAI_NOT_SUPPLY = [ "1578", "1793", "1794", "1896", "1899", "3005", "4377", "4381", "4382", "4383", "4406", "4407", "4408", "4413", "4414", "4415", "4416", "4417", "4421", "4423", "4425", "4454", "4455", "4456", "4458", "4744", "4749", "4754", "4787", "4797", "4800", "4809", "4810", "4821", "5853", "5854", "7904", "8411", "8412", "8413", "8432", "8433", "8434", "8435", "8521", "8522", "8539", "8540", "8541", "9448", "9695", "13160", "13161", "13162" ]
OSTON_INDUSTRIAL_NOT_SUPPLY = ["11","15","276","277","4703","4704","4937","9619","9709","11151"]

# For each row, get all suppliers and their costs
def get_sorted_suppliers(row):
    key = row.get(key_col)
    suppliers = []

    for col in bidsheet_df.columns:
        if col.endswith(' - R2 - Total landed cost per UOM (USD)'):
            supplier_name = col.split(' - R2 - ')[0]
            if str(row['ROW ID #']) in CODA_NOT_SUPPLY and supplier_name.strip() == 'Coda':
                continue
            if str(row['ROW ID #']) in ZHEJIANG_WANDEKAI_NOT_SUPPLY and supplier_name.strip() == 'ZHEJIANG WANDEKAI':
                continue
            if str(row['ROW ID #']) in OSTON_INDUSTRIAL_NOT_SUPPLY and supplier_name.strip() == 'Oston Industrial':
                continue
            try:
                landed = bidsheet_map.loc[key, col]
                fob_col = f'{supplier_name} - R2 - Total Cost Per UOM FOB Port of Origin/Departure (USD)'
                fob = bidsheet_map.loc[key, fob_col] if fob_col in bidsheet_map.columns else np.nan
                country = get_supplier_country(key, supplier_name)
                if not pd.isna(landed) and landed != 0:
                    suppliers.append({
                        'supplier': supplier_name,
                        'landed': landed,
                        'fob': fob,
                        'country': country
                    })
            except Exception:
                continue
    # Sort by landed cost ascending
    incumbent = row['Incumbent Supplier']
    print('+-+-+-+-+-+-')
    print(incumbent)
    if incumbent not in ['-'] + [x['supplier'] for x in suppliers]:
        wapp_price = get_bidsheet_value2(key,'Volume-banded WAPP')
        print('wapp_price', wapp_price)
        multiplier_info = get_supplier_info(key, incumbent)
        wapp_landed_cost = wapp_price * multiplier_info['FreightMultiplier'] + wapp_price * (multiplier_info['tariff_value'] + multiplier_info['Metal Tariff'])
        suppliers.append(
            {
                'supplier': incumbent,
                'landed': wapp_landed_cost,
                'fob': wapp_price,
                'country': get_supplier_country(key, incumbent)
            }
        )

    suppliers = sorted(suppliers, key=lambda x: x['landed'] if not pd.isna(x['landed']) else np.inf)
    return suppliers

# Find max number of suppliers for any row
max_suppliers = scenario_df.apply(get_sorted_suppliers, axis=1).map(len).max()

# Build all new columns in a single DataFrame for performance
def build_best_supplier_columns(scenario_df, max_suppliers):
    new_cols = {}
    for rank in range(2, max_suppliers+1):
        new_cols[f'{rank}nd best FOB bid (USD)'] = []
        new_cols[f'{rank}nd best landed bid (USD)'] = []
        new_cols[f'{rank}nd best supplier name'] = []
        new_cols[f'{rank}nd best supplier country/supply location'] = []
    for _, row in scenario_df.iterrows():
        suppliers = get_sorted_suppliers(row)
        for rank in range(2, max_suppliers+1):
            idx = rank - 1
            if len(suppliers) > idx:
                sup = suppliers[idx]
                new_cols[f'{rank}nd best FOB bid (USD)'].append(sup['fob'])
                new_cols[f'{rank}nd best landed bid (USD)'].append(sup['landed'])
                new_cols[f'{rank}nd best supplier name'].append(sup['supplier'])
                new_cols[f'{rank}nd best supplier country/supply location'].append(sup['country'])
            else:
                new_cols[f'{rank}nd best FOB bid (USD)'].append(np.nan)
                new_cols[f'{rank}nd best landed bid (USD)'].append(np.nan)
                new_cols[f'{rank}nd best supplier name'].append(np.nan)
                new_cols[f'{rank}nd best supplier country/supply location'].append(np.nan)
    return pd.DataFrame(new_cols)

best_supplier_df = build_best_supplier_columns(scenario_df, max_suppliers)
scenario_df = pd.concat([scenario_df.reset_index(drop=True), best_supplier_df], axis=1)
# Save to scenario_outputs/test_scenario_results.xlsx

from part_reference import part_reference
part_map = dict(part_reference)
# Ensure ROW ID is string
scenario_df['ROW ID #'] = scenario_df['ROW ID #'].astype(str)
# Update 'Part #' using the in-memory map
scenario_df['Part #'] = scenario_df.apply(
    lambda row: part_map.get(row['ROW ID #'], row['Part #']),
    axis=1
)

# Write output with first 13 rows preserved at the top
with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    # Write the processed data
    scenario_df.to_excel(writer, sheet_name='Sheet1', index=False, startrow=13, header=True)
    
    # Get the workbook and worksheet to write header rows
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    
    # Write the first 13 rows at the top
    for row_idx, row_data in enumerate(header_rows_df.values, start=1):
        for col_idx, value in enumerate(row_data, start=1):
            worksheet.cell(row=row_idx, column=col_idx, value=value)