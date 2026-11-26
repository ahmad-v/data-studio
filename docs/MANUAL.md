# Offline manual | Data Studio

Development source only. Launch the GUI after compiling. Choose an operation, enter local input path and a new output path where needed, set the parameter as described under the operation selector, then Run. Status and errors are shown below. Cancel requests cooperative cancellation. The draft editor accepts transcript or plain text. Theme selection changes client colors only. Paths are manually typed in this increment; native dialogs and full accessibility remain pending.

- `preview`: CSV / first XLSX sheet text preview.
- `filter`: Exact-match filter; CSV or string Parquet.
- `summary`: Numeric column n, missing, mean, sample SD, min/max.
- `export`: CSV formula guard; Parquet UTF-8 columns.
- `scatter`: Monochrome scatter SVG, 800 x 600.

Limitations: In-memory text table only; no inferred types, worksheet selector, large-data virtualization, formulas, rich statistics, project pipelines, other formats, authoritative palettes or full scientific export. XLSX ZIP expansion and range allocation occur in dependency before cell limit; hostile workbooks not safe for production. CSV guard alters potentially formula-like cells, including negative numbers; not a proof of spreadsheet safety. No exact type preservation.

No output overwrite is allowed. No accounts, ads, telemetry or runtime downloads are intended. Do not use this unverified snapshot with valuable or hostile input. Citation and donations are voluntary.
