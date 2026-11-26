# Offline manual | Data Studio

Development source only. Launch the GUI after compiling. Choose an operation, enter local input path and a new output path where needed, set the parameter as described under the operation selector, then Run. Status and errors are shown below. Cancel requests cooperative cancellation. The draft editor accepts transcript or plain text. Choose a theme to change workbench colors and control borders. UI scale adjusts the controls from 80% to 300% for the current session. Paths are manually typed in this increment; native dialogs and full accessibility remain pending.

- `select-columns`: Enter exact comma-separated column names in output order. Optionally save to a new CSV or Parquet file. Use the first sheet for XLSX input.
- `counts`: Counts exact cell values, including blank text. The preview reports the 30 most frequent values and the total number of distinct values.
- `preview`: Preview CSV, XLSX or Parquet as text. For XLSX preview and export, enter the exact worksheet name or leave the parameter blank to use the first sheet.
- `filter`: Exact-match filter; CSV or string Parquet.
- `summary`: Numeric column n, missing, mean, sample SD, min/max.
- `export`: CSV formula guard; Parquet UTF-8 columns.
- `scatter`: Monochrome scatter SVG, 800 x 600.

Limitations: In-memory text table only; no inferred types, large-data virtualization, formulas, rich statistics, project pipelines, other formats, authoritative palettes or full scientific export. XLSX ZIP expansion and range allocation occur in dependency before cell limit; hostile workbooks not safe for production. CSV guard alters potentially formula-like cells, including negative numbers; not a proof of spreadsheet safety. No exact type preservation.

No output overwrite is allowed. No accounts, ads, telemetry or runtime downloads are intended. Do not use this unverified snapshot with valuable or hostile input. Citation and donations are voluntary.

Parquet import supports flat, uncompressed UTF-8 columns. Nulls become blank text. Numeric, nested, and compressed input returns an error. Preview, filter, numeric summary, scatter, and export all accept this input through the same table loader.
