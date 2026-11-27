# Offline manual | Data Studio

Development source only. Launch the GUI after compiling. Choose an operation, enter local input path and a new output path where needed, set the parameter as described under the operation selector, then Run. Status and errors are shown below. Cancel requests cooperative cancellation. The draft editor accepts transcript or plain text. Choose a theme to change workbench colors and control borders. UI scale adjusts the controls from 80% to 300% and remembers your appearance choices when you reopen the app. Paths are manually typed in this increment; native dialogs and full accessibility remain pending.

- `select-columns`: Enter exact comma-separated column names in output order. Optionally save to a new CSV or Parquet file. Use the first sheet for XLSX input.
- `counts`: Counts exact cell values, including blank text. The preview reports the 30 most frequent values and the total number of distinct values.
- `preview`: Preview CSV, XLSX or Parquet as text. For XLSX preview and export, enter the exact worksheet name or leave the parameter blank to use the first sheet.
- `filter`: Exact-match filter; CSV or string Parquet.
- `filter-contains` and `filter-not-contains`: JSON parameters name a column and nonempty literal substring. Matching is case-sensitive and does not interpret the search text as a pattern.
- `head-rows` and `tail-rows`: JSON parameter `{"rows":N}` keeps the first or last N rows, preserving source order. N is limited to 500,000.
- `drop-duplicates-by-columns`: JSON parameter `{"columns":["email"]}` compares exact cell text in the chosen key columns and keeps the first occurrence.
- `add-index-column`: JSON parameter `{"name":"row_number","start":1}` adds sequential signed 64-bit values in current row order and rejects overflow.
- `add-constant-column`: JSON parameter `{"name":"cohort","value":"pilot"}` copies the literal value into a new column.
- `normalize-text`: JSON parameter `{"column":"name","mode":"trim"}` supports `trim`, Unicode lowercase, Unicode uppercase, and `collapse-whitespace` (Unicode whitespace collapsed to one ASCII space).
- `rename-columns`: JSON parameter `{"mapping":{"old_a":"new_a","old_b":"new_b"}}` applies all changes atomically; final names must be unique.
- `drop-columns`: JSON parameter `{"columns":["temporary"]}` removes selected columns, retains input order for the rest, and requires at least one remaining column.
- `filter-not-equal`: JSON parameter `{"column":"status","value":"closed"}` retains rows that differ by exact text.
- `filter-contains-insensitive`: JSON parameter `{"column":"name","value":"ada"}` compares Unicode lowercase text; it is not locale-specific collation or full linguistic case-folding.
- `filter-starts-with` and `filter-ends-with`: JSON parameters name a column and a nonempty literal prefix or suffix. Matching is case-sensitive.
- `filter-number-greater`, `filter-number-greater-equal`, `filter-number-less`, and `filter-number-less-equal`: `{"column":"score","value":10}` keeps finite numeric values by the named strict or inclusive comparison. Blank cells are excluded; malformed nonblank values fail the operation.
- `filter-number-between`: `{"column":"score","lower":10,"upper":20}` keeps finite numeric values within inclusive bounds. Bounds must be finite and ordered; blank cells are excluded and malformed nonblank values fail.
- `replace-literal`: JSON parameter `{"column":"notes","find":"old","replace":"new"}` replaces all non-overlapping literal matches in one column. Empty find strings are rejected and the 32 MiB transformed-text bound applies before replacement.
- `drop-rows-any-missing`: Removes each row containing any empty or whitespace-only cell.
- `fill-forward` and `fill-backward`: JSON parameter `{"column":"group"}` fills blanks from the nearest preceding or following nonblank cell. Leading or trailing blanks without a source value remain blank.
- `drop-empty-columns`: Removes columns that contain only blank or whitespace-only cells. It errors if no nonempty column remains.
- `reverse-rows`: Reverses row order and preserves headers and cell text.
- `sort`: Enter the exact column name. Rows with equal values retain their order. Values sort as text, so `10` precedes `2`.
- `deduplicate`: Keep the first occurrence of each full row. Leave the parameter blank. Both operations can preview rows or export to a new CSV, TSV or Parquet file.
- `mask-column`: Enter the exact column name to replace its nonempty values with `[MASKED]`. Preview the result or export a new CSV, TSV or Parquet file. Review other columns and blank patterns before sharing masked data.
- `summary`: Numeric column n, missing, mean, sample SD, min/max.
- Statistical reports can optionally be saved to a new `.json` output. They do not alter the input table.
- `export`: CSV formula guard; Parquet UTF-8 columns.
- `linear-regression`: Enter exact predictor and response column names separated by a comma. The preview reports the least-squares intercept, slope, R² and complete and excluded pair counts. It uses finite numeric complete pairs. A constant response yields an undefined R². Inferential statistics are outside this route.
- `correlation`: Enter two exact column names separated by a comma. The preview reports Pearson's r and the complete and excluded pair counts. Blank pairs are excluded. Both columns must vary, and every complete pair must be finite numeric data.
- `line-chart`: Enter two numeric column names separated by a comma. Write a new monochrome SVG. Complete pairs are connected in table order, including across missing rows.
- `scatter`: Monochrome scatter SVG, 800 x 600.

Limitations: In-memory text table only; no inferred types, large-data virtualization, formulas, project editing, undo, batch runs, other formats, authoritative palettes or full scientific export. Inferential routes are limited to the methods documented below; assumptions are not diagnosed, p-values are unadjusted, and no multiple-testing correction or causal inference is provided. XLSX ZIP expansion and range allocation occur in dependency before cell limit; hostile workbooks not safe for production. CSV guard alters potentially formula-like cells, including negative numbers; not a proof of spreadsheet safety. No exact type preservation.

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

## Join two tables

Choose `inner-join` with the left table as Input. The operation parameter is JSON, for example `{"right_path":"lookup.tsv","left_key":"id","right_key":"code"}`. Keys are exact nonempty text. Unmatched and blank keys are omitted. Duplicate keys produce every matching pair in input order. The right key column is omitted and other columns from the right table use the `right.` prefix. Name collisions, more than 256 columns or 500,000 cells and joined text over 32 MiB cause the operation to fail before saving. Use a new CSV, TSV or Parquet output or leave it blank to preview. XLSX joins use the first sheet.

Use `left-join`, `right-join` or `full-join` with the same JSON parameters. Matching keys are exact nonblank text and duplicate keys produce all pairs. Blank keys do not match: left and full joins retain unmatched left rows; right and full joins retain unmatched right rows, including blank-key rows. Left joins follow left input order; right joins follow right input order; full joins list left rows first and then unmatched right rows. Missing-side columns are empty strings. The right key column is omitted and other right-side headers use the `right.` prefix. The same column, row and output-text bounds apply.

Use `group-summary` with `group-column,value-column`. One row per exact group appears in text sort order. Blank numeric values count as missing. Sample SD uses n−1 and is blank below two observations. Groups with no numeric values retain their counts and blank statistics. Preview the result or write a new CSV, TSV or Parquet. The usual delimited-text formula guard applies to exported text cells.

## Query a table with SQL

Choose `select-sql` and enter one SQLite SELECT. The imported table is named `input`. For example, `SELECT category, SUM(CAST(amount AS REAL)) AS total FROM input GROUP BY category ORDER BY category`. Wrap column names with spaces in double quotes. The table starts with text columns, so use explicit casts and review invalid numeric text before casting. NULL results are exported as empty text and BLOB results are rejected. Result names must be unique.

The bundled engine uses an in-memory database and limits statement size, output cells and execution work. It permits SELECT and a limited list of text and numeric functions. The route refuses database attachment, extensions, PRAGMAs, writes and multiple statements. Use a new CSV, TSV or Parquet output or leave it blank to preview.

## Inferential statistics and intervals

- `quantiles` reports Q1, median, Q3 and IQR using Hyndman-Fan type 7 linear interpolation.
- `standard-error` reports sample SD divided by the square root of n. At least two finite observations are required.
- `mean-ci` reports a two-sided 95% Student-t interval. It assumes independent observations and uses the sample variance.
- `bootstrap-mean-ci` and `bootstrap-median-ci` use 2,000 deterministic percentile-bootstrap resamples with replacement. They require 2 to 5,000 finite observations. The intervals are not bias-corrected; resampling is reproducible from the values.
- `welch-t-test` takes `group-column,value-column`. It requires exactly two nonblank groups with at least two finite values each; blank group labels and blank numeric values are omitted. Group order is text order, and the reported difference is first group minus second. The test uses the Welch-Satterthwaite approximation and returns a two-sided p-value and 95% difference interval.
- `paired-t-test` takes `first-column,second-column`; incomplete pairs are omitted. It tests the mean first-minus-second difference with a two-sided Student-t test and 95% interval.
- `one-way-anova` takes `group-column,value-column`. It reports an ordinary fixed-effects one-way ANOVA for independent groups. Blank group labels and numeric values are omitted. It does not test normality or equal variance assumptions.
- `spearman` takes `x-column,y-column`, computes average ranks for ties, and reports rho. Its two-sided p-value uses a t approximation, which can be inaccurate for small samples.
- `correlation-interval` returns a 95% Fisher-z interval for Pearson's r when n > 3 and |r| < 1. It assumes independent bivariate-normal pairs.

All numeric routes reject nonfinite or malformed observed values, state their method in the JSON report and do not imply causation. P-values are unadjusted for multiple testing. Review assumptions and the input table before drawing conclusions. Reports appear in the preview and can be saved to a new JSON file without overwriting an existing one.

## Additional operations

- `drop-missing`. Enter the exact column name to remove rows whose selected cell is empty or contains only whitespace.
- `fill-missing`. Enter `column=value` to replace empty and whitespace-only cells with literal text.
- `rename-column`. Enter `old=new`. The new name must contain text and be unique.

All three operations can be used in a project. Use a `column` field and, where applicable, `value` for fill or `name` for rename.

## Recode, convert and reshape

The ten bounded transforms below are available as individual operations and as ordered version 1 project steps. Enter each parameter as JSON (without an `operation` field); examples use these exact field names:

- `recode-values`: `{"column":"status","mapping":{"old":"new"}}`. Mapping is exact and case-sensitive; unmapped cells stay unchanged.
- `convert-number`: `{"column":"amount"}` validates finite numeric values and writes normalized numeric text. `convert-integer` requires exact signed 64-bit integers. `convert-boolean` accepts `true`/`false`, `yes`/`no` or `1`/`0` (case-insensitive) and writes `true` or `false`. All three preserve blank cells and reject malformed nonblank cells.
- `split-column`: `{"column":"name","delimiter":" ","new_columns":["first","last"]}` replaces the source column with the declared outputs. The delimiter is literal and each nonblank row must produce exactly the requested number of parts.
- `merge-columns`: `{"columns":["first","last"],"name":"full","delimiter":" "}` appends a new column; source columns remain.
- `pivot`: `{"id_column":"id","pivot_column":"year","value_column":"value"}` creates sorted category columns. Blank pivot categories and duplicate identifier/category pairs are rejected; aggregate duplicates first.
- `unpivot`: `{"id_columns":["id"],"value_columns":["2024","2025"],"variable_name":"year","value_name":"value"}` emits one output row per source row and selected value column, including blank values.
- `group-sum` and `group-mean`: `{"group_column":"site","value_column":"score"}` produce exact text groups in sorted order. Blank numeric values are omitted, blank group labels remain groups, and malformed nonblank numbers fail the operation.

These operations keep the table text-based and enforce a 128 KiB parameter limit, the existing row and column limits, and a 32 MiB transformed-text limit. A failed transform produces no partial output file. Conversion validates and normalizes cell text; it does not create typed columns. Split and merge are literal operations, not regular-expression tools. Project steps are bounded to 32 and failed steps are reported by position.
