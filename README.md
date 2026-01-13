Midland Metal Tariff Rerun
==========================

Runbook
-------
- Update tariff inputs (only if tariffs change): add or replace the part-level tariff file in the same format as `part_level_tariff.csv`, then run `data_cleaning.py` to regenerate `tariff_part_level_cleaned.csv`.
- Prepare bidsheet landed costs: run `landed_consolidate_2.py` using `new/bidsheet_master_consolidate 141025.csv` as input; it produces `new/Bidsheet Master Consolidate Landed 12052025.xlsx`. Convert that file to CSV with `excel_to_csv.py`. You will need to update the input and output file names in it.
- Compute scenario results: run `scenario_scripts/scenario_3.py` to create `scenario_outputs/scenario 3 12052025.xlsx`.
- Add reporting columns: run `add_columns_in_scenario.py` to produce `scenario_outputs/scenario 3 12052025 added columns.xlsx`.

Script purposes
---------------
- `data_cleaning.py` flattens `part_level_tariff.csv`, normalizes country names, derives a metal tariff per material group, and writes `tariff_part_level_cleaned.csv`. It then appends zero-tariff rows for specific `ROW ID #` values across selected Asian countries.
- `landed_consolidate_2.py` consolidates bidsheet data and outputs landed-cost workbook; pair with `excel_to_csv.py` to emit CSV.
- `scenario_scripts/scenario_3.py` ingests the cleaned tariff table, supplier-port map, freight multipliers, and the bidsheet to assign suppliers. It keeps incumbents when they are the lowest-cost or absent, otherwise chooses the lowest bid while trying to keep new awards at ~65% of total landed cost, then exports `scenario_outputs/scenario 3 12052025.xlsx`.
- `add_columns_in_scenario.py` enriches the scenario output with bidsheet cost columns, recalculates landed/FOB figures, recomputes savings and supplier-mix summaries, and rewrites `scenario 3 12052025 added columns.xlsx` with a summary header.
