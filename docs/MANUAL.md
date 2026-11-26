# Offline manual | Data Studio

Development source only. Launch the GUI after compiling. Choose an operation, enter local input path and a new output path where needed, set the parameter as described under the operation selector, then Run. Status and errors are shown below. Cancel requests cooperative cancellation. The draft editor accepts transcript or plain text. Choose a theme to change workbench colors and control borders. UI scale adjusts the controls from 80% to 300% for the current session. Paths are manually typed in this increment; native dialogs and full accessibility remain pending.

- `select-columns`: Enter exact comma-separated column names in output order. Optionally save to a new CSV or Parquet file. Use the first sheet for XLSX input.
- `counts`: Counts exact cell values, including blank text. The preview reports the 30 most frequent values and the total number of distinct values.
- `preview`: Preview CSV, XLSX or Parquet as text. For XLSX preview and export, enter the exact worksheet name or leave the parameter blank to use the first sheet.
- `filter`: Exact-match filter; CSV or string Parquet.
- `sort`: Enter the exact column name. Rows with equal values retain their order. Values sort as text, so `10` precedes `2`.
- `deduplicate`: Keep the first occurrence of each full row. Leave the parameter blank. Both operations can preview rows or export to a new CSV, TSV or Parquet file.
- `mask-column`: Enter the exact column name to replace its nonempty values with `[MASKED]`. Preview the result or export a new CSV, TSV or Parquet file. Review other columns and blank patterns before sharing masked data.
- `summary`: Numeric column n, missing, mean, sample SD, min/max.
- `export`: CSV formula guard; Parquet UTF-8 columns.
- `linear-regression`: Enter exact predictor and response column names separated by a comma. The preview reports the least-squares intercept, slope, R² and complete and excluded pair counts. It uses finite numeric complete pairs. A constant response yields an undefined R². Inferential statistics are outside this route.
- `correlation`: Enter two exact column names separated by a comma. The preview reports Pearson's r and the complete and excluded pair counts. Blank pairs are excluded. Both columns must vary, and every complete pair must be finite numeric data.
- `line-chart`: Enter two numeric column names separated by a comma. Write a new monochrome SVG. Complete pairs are connected in table order, including across missing rows.
- `scatter`: Monochrome scatter SVG, 800 x 600.

Limitations: In-memory text table only; no inferred types, large-data virtualization, formulas, advanced statistics, project editing, undo, batch runs, other formats, authoritative palettes or full scientific export. XLSX ZIP expansion and range allocation occur in dependency before cell limit; hostile workbooks not safe for production. CSV guard alters potentially formula-like cells, including negative numbers; not a proof of spreadsheet safety. No exact type preservation.

No output overwrite is allowed. No accounts, ads, telemetry or runtime downloads are intended. Do not use this unverified snapshot with valuable or hostile input. Citation and donations are voluntary.

Parquet import supports flat, uncompressed UTF-8 columns. Nulls become blank text. Numeric, nested, and compressed input returns an error. Preview, filter, numeric summary, scatter, and export all accept this input through the same table loader.

For tab-separated text, use the .tsv extension on input or output. Quoted fields can contain tabs. The same size, header and formula-guard rules apply as for CSV. TSV passed the Linux round-trip fixture.

## Save and replay a table project

Create a UTF-8 JSON file with schema version 1 and up to 32 ordered steps. The example filters rows, removes exact duplicates, sorts by text ID, and projects the named columns.

```json
{
  "schema_version": 1,
  "steps": [
    {"operation": "filter", "column": "group", "value": "sample"},
    {"operation": "deduplicate"},
    {"operation": "sort", "column": "id"},
    {"operation": "select-columns", "columns": ["id", "group"]}
  ]
}
```

Use `mask-column` with a `column` field to include a fixed mask step. The optional top-level `worksheet` selects an exact XLSX sheet name. Set the table as Input and the project path as Parameter, or use `--cli run-project input.csv result.tsv project.json`. An empty output path previews the result. An invalid step reports its number and leaves the output absent. Keep project files local if filter values contain sensitive information. The project engine interprets only these operations and uses the same bounded text-table loader as individual routes.
