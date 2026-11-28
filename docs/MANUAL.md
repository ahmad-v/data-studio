# Offline manual | Data Studio

Development source only. Launch the GUI after compiling. Choose an operation, select a local input and a new output path where needed, set the parameter as described under the operation selector, then Run. Native file dialogs are available. Status and errors are shown below. Cancel requests cooperative cancellation. The draft editor accepts transcript or plain text. Choose a theme to change workbench colors and control borders. UI scale adjusts the controls from 80% to 300% and remembers your appearance choices when you reopen the app. Broader accessibility work remains pending.

Choose `Inspect typed table` to open the native Data table tab for CSV, TSV, XLSX or supported scalar Parquet input. For XLSX, Parameter may name an exact worksheet; leave it blank for the first sheet. The view shows one-based row indices, typed cells and nulls, sortable headers, bounded pages, and per-column type, missing-count, label and unit information. The preview includes at most 10,000 cells and clips cell text to 256 characters; sorting changes the preview order only, not the source dataset. The operation writes no output and does not modify the source file.

- `select-columns`: Enter exact comma-separated column names in output order. For CSV, TSV or supported flat Parquet input, previews and supported outputs use typed columns. Delimited exports preserve original cell spellings where possible, including leading zeroes; true nulls serialize as empty fields and cannot be distinguished from empty strings on reimport. Use the first sheet for XLSX input.
- `counts`: Counts exact cell values, including blank text. The preview reports the 30 most frequent values and the total number of distinct values.
- `preview`: Preview CSV, XLSX or Parquet as text. For XLSX preview and export, enter the exact worksheet name or leave the parameter blank to use the first sheet.
- `inspect-schema`: Leave Parameter blank to infer column types, or pass JSON such as `{"overrides":{"score":"float64"},"metadata":{"score":{"label":"Response","unit":"points"}}}`. It reports one type, missing-cell count and example value per column, plus category count for categorical columns. Override types are `boolean`, `int64`, `uint64`, `float64`, `date`, `time`, `datetime`, `categorical`, `string`, or `null`. Null means an empty or whitespace-only cell; strings such as `NA` are ordinary text. Invalid overrides fail without changing the input. An optional `.json` output writes a no-overwrite schema report. The typed path is used by schema-aware basic operations and typed projects; many other transformations and statistical routes still use the legacy text table.
- `export-typed-parquet`: Use the same optional schema JSON to validate or override types, then choose a new `.parquet` output. It writes Arrow booleans, signed/unsigned integers, finite floats, dates, nanosecond times and timestamps, UTF-8 strings and dictionary-encoded categorical columns. Empty/whitespace-only values become null. Labels and units are saved as Arrow field metadata. Offset-aware datetimes are normalized to UTC; mixed aware and naive values in one column are rejected. The report preview describes the schema written. The source file remains unchanged.
- `filter`: Enter `column=value`. With CSV, TSV or supported flat Parquet input, previews and supported outputs use the inferred column type, so numeric `010` matches numeric 10 and blank/whitespace values are null. Delimited output preserves original spellings for retained rows, including leading zeroes; true nulls serialize as empty fields and cannot be distinguished from empty strings on reimport.
- `filter-contains` and `filter-not-contains`: JSON parameters name a column and nonempty literal substring. Matching is case-sensitive and does not interpret the search text as a pattern. CSV, TSV and supported scalar Parquet inputs use the typed route, which preserves the column's type and nulls for retained records.
- `head-rows` and `tail-rows`: JSON parameter `{"rows":N}` keeps the first or last N rows, preserving source order. N is limited to 500,000.
- `drop-duplicates-by-columns`: JSON parameter `{"columns":["email"]}` keeps the first occurrence. CSV, TSV and supported scalar Parquet inputs compare typed values, treating null keys as equal while preserving the original values and schema.
- `add-index-column`: JSON parameter `{"name":"row_number","start":1}` adds sequential signed 64-bit values in current row order and rejects overflow. CSV, TSV, XLSX, and supported scalar Parquet inputs use typed columns; Parquet output retains the integer type.
- `add-constant-column`: JSON parameter `{"name":"cohort","value":"pilot"}` copies a literal into a new column. Optional `value_type` can explicitly select `boolean`, `int64`, `uint64`, `float64`, `date`, `time`, `datetime`, `categorical`, `string`, or `null`; without it the value is inferred. Typed CSV, TSV, XLSX, and supported scalar Parquet inputs preserve the other columns' types; invalid values and overflow fail before output is written.
- `normalize-text`: JSON parameter `{"column":"name","mode":"trim"}` supports `trim`, Unicode lowercase, Unicode uppercase, and `collapse-whitespace` (Unicode whitespace collapsed to one ASCII space).
- `rename-columns`: JSON parameter `{"mapping":{"old_a":"new_a","old_b":"new_b"}}` applies all changes atomically; final names must be unique. Typed CSV, TSV, XLSX, and supported scalar Parquet inputs preserve each column's values and types.
- `drop-columns`: JSON parameter `{"columns":["temporary"]}` removes selected columns, retains input order for the rest, and requires at least one remaining column. Typed CSV, TSV, XLSX, and supported scalar Parquet inputs preserve remaining values and types.
- `filter-not-equal`: JSON parameter `{"column":"status","value":"closed"}` retains rows that differ. Ordinary CSV/TSV and supported scalar Parquet routes compare using inferred types; legacy projects keep their existing text semantics.
- `filter-contains-insensitive`: JSON parameter `{"column":"name","value":"ada"}` compares Unicode lowercase text; it is not locale-specific collation or full linguistic case-folding. CSV, TSV and supported scalar Parquet inputs use the typed route, preserving types and null semantics.
- `filter-starts-with` and `filter-ends-with`: JSON parameters name a column and a nonempty literal prefix or suffix. Matching is case-sensitive.
- `filter-number-greater`, `filter-number-greater-equal`, `filter-number-less`, and `filter-number-less-equal`: `{"column":"score","value":10}` keeps finite numeric values by the named strict or inclusive comparison. In ordinary CSV/TSV and supported scalar Parquet routes, nulls are excluded and integer comparisons outside the exact float range fail instead of rounding; legacy routes retain their documented numeric-text behavior.
- `filter-number-between`: `{"column":"score","lower":10,"upper":20}` keeps finite numeric values within inclusive bounds. Bounds must be finite and ordered; typed routes exclude nulls and reject integer values outside the exact float comparison range.
- Numeric derivations add a new text column using exact JSON parameters: `add-numeric-columns` and `subtract-numeric-columns` use `{"left":"a","right":"b","name":"result"}`; `multiply-numeric-columns` and `divide-numeric-columns` use the same shape; `percent-change-column` uses `{"current":"after","baseline":"before","name":"change_pct"}`. Blank inputs produce blank output cells. Division and percent change reject zero divisors/baselines.
- Unary numeric derivations use `{"column":"amount","name":"result"}` for `absolute-value-column`, `square-root-column` and `log10-column`. Square root requires nonnegative values; logarithm requires positive values. `round-numeric-column` adds `decimal_places` from -12 to 12. `clip-numeric-column` uses `{"column":"score","name":"bounded","minimum":0,"maximum":100}` with finite ordered inclusive bounds.
- Each numeric derivation is an ordered version 1 project step too. Malformed values, domain errors, non-finite results, duplicate output names, dimension overflow, or the 32 MiB text limit fail without writing partial output. These routes add calculated text columns; they do not create typed cells or a general formula language.
- `replace-literal`: JSON parameter `{"column":"notes","find":"old","replace":"new"}` replaces all non-overlapping literal matches in one column. Empty find strings are rejected and the 32 MiB transformed-text bound applies before replacement.
- `drop-rows-any-missing`: Removes each row containing any empty or whitespace-only cell.
- `fill-forward` and `fill-backward`: JSON parameter `{"column":"group"}` fills blanks from the nearest preceding or following nonblank cell. Leading or trailing blanks without a source value remain blank.
- `drop-empty-columns`: Removes columns that contain only blank or whitespace-only cells. It errors if no nonempty column remains.
- `reverse-rows`: Reverses row order and preserves headers and cell text.
- `sort`: Enter the exact column name. CSV, TSV and supported flat Parquet inputs use stable typed order for previews and supported outputs, with nulls last.
- `deduplicate`: Keep the first occurrence of each full row. Leave the parameter blank. Both operations can preview rows or export to a new CSV, TSV or Parquet file. CSV, TSV and supported scalar Parquet inputs use type-aware equality and keep nulls distinct from non-null values.
- `mask-column`: Enter the exact column name to replace its nonempty values with `[MASKED]`. Preview the result or export a new CSV, TSV or Parquet file. Review other columns and blank patterns before sharing masked data.
- `summary`: Numeric column n, missing, mean, sample SD, min/max.
- Statistical reports can optionally be saved to a new `.json` output. They do not alter the input table.
- `export`: CSV formula guard; Parquet UTF-8 columns.
- `linear-regression`: Enter exact predictor and response column names separated by a comma. The preview reports the least-squares intercept, slope, R² and complete and excluded pair counts. It uses finite numeric complete pairs. A constant response yields an undefined R². Inferential statistics are outside this route.
- `correlation`: Enter two exact column names separated by a comma. The preview reports Pearson's r and the complete and excluded pair counts. Blank pairs are excluded. Both columns must vary, and every complete pair must be finite numeric data.
- `line-chart`: Enter two numeric column names separated by a comma, or JSON such as `{"x":"time","y":"signal","title":"Signal over time","x_axis":"Time (s)","y_axis":"Amplitude"}`. Writes a new monochrome SVG with visible title and axis labels. Complete pairs are connected in table order, including across missing rows. Existing `x,y` parameters remain supported and use the column names as axis labels. For multiple series, pass JSON such as `{"x":"time","series":["control","treatment"],"palette":"batlowS","title":"Response","x_axis":"Time (s)","y_axis":"Value"}`. The new figure uses a qualitative palette, distinct markers, and alternating line patterns; it supports 2–12 series.
- `scatter`: Monochrome scatter SVG, 800 x 600, with a visible title and axis labels. Accepts the same comma or JSON parameter forms as `line-chart`; multi-series JSON uses the same palette and marker options.

Limitations: Most standalone general-purpose operations still use the legacy in-memory string table. CSV/TSV and supported scalar Parquet `filter`, `filter-not-equal`, numeric filters, sort, select-columns, rename-column, drop-missing, drop-rows-any-missing, head-rows, tail-rows and reverse-rows use typed values for previews and supported outputs. Numeric filters reject integer values outside the exact float comparison range instead of rounding them. Version 1 projects retain text behavior unless they include `typed_schema`; version 2 projects infer typed columns by default and can supply `typed_schema` overrides and metadata. The bounded typed Parquet pipeline supports schema inference, comparisons, numeric expressions, renaming, typed inner joins, aggregation and row/column selection. A virtualized editable table, general expression language, project editor, undo, batch runs, broader format coverage, authoritative palettes and full scientific export remain open. Inferential routes are limited to the methods documented below; assumptions are not diagnosed, p-values are unadjusted, and no multiple-testing correction or causal inference is provided. XLSX ZIP expansion and range allocation occur in the dependency before the cell limit; hostile workbooks are not safe for production. CSV formula mitigation modifies potentially formula-like strings; it is not proof of spreadsheet safety.

No output overwrite is allowed. No accounts, ads, telemetry or runtime downloads are intended. Do not use this unverified snapshot with valuable or hostile input. Citation and donations are voluntary.

Parquet import supports flat, uncompressed scalar columns, including booleans, numeric primitives, dates, times, timestamps and UTF-8 dictionary values. Nulls become blank text in the legacy transformation table. Nested and binary columns, compressed Parquet pages and files beyond the declared/decoded byte limits are rejected. Typed CSV/TSV inspection, export and typed projects parse delimited records directly into columnar typed values in two bounded passes; they do not first construct the legacy row-oriented string table. XLSX and Parquet typed inputs still use their existing decoder before typed conversion. `export-typed-parquet` writes the validated schema as native Arrow physical types. Typed projects support case-sensitive/insensitive substring, prefix and suffix filters while retaining source types and null semantics, plus type-checked `fill-missing`, `fill-forward`, and `fill-backward` operations. Constant replacement text must parse as the declared or inferred column type; propagation copies typed scalars, leaves leading/trailing gaps when no neighbor exists, and preserves the source file. Version 1 projects without `typed_schema` retain their text behavior. Version 2 projects infer a typed schema; unported transforms remain unsupported in the typed pipeline.

For tab-separated text, use the .tsv extension on input or output. Quoted fields can contain tabs. The same size, header and formula-guard rules apply as for CSV. TSV passed the Linux round-trip fixture.

## Save and replay a table project

Create a UTF-8 JSON file with schema version 1 or 2 and up to 32 ordered steps. Version 1 keeps the existing text semantics. Version 2 infers column types and executes through the typed pipeline; use `typed_schema` to override inference or add labels and units. The example below uses version 1, filters rows, removes exact duplicates, sorts by text ID, and projects the named columns.

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

Use `mask-column` with a `column` field to include a fixed mask step. The optional top-level `worksheet` selects an exact XLSX sheet name. Set the table as Input and the project path as Parameter, or use `--cli run-project input.csv result.tsv project.json`. An empty output path previews the result. An invalid step reports its number and leaves the output absent. Keep project files local if filter values contain sensitive information. Projects without `typed_schema` use the existing text-table pipeline.

Version 1 projects can opt into typed execution by including `typed_schema`. New projects can use version 2 to infer the input schema automatically. Save typed results as Parquet to keep physical types and nulls:

```json
{
  "schema_version": 2,
  "typed_schema": {
    "overrides": {"id": "int64", "group": "categorical"},
    "metadata": {"score": {"label": "Response", "unit": "points"}}
  },
  "steps": [
    {"operation": "filter", "column": "score", "value": "10"},
    {"operation": "sort", "column": "id"},
    {"operation": "select-columns", "columns": ["id", "group", "score"]},
    {"operation": "drop-rows-any-missing"}
  ]
}
```

The typed subset supports `filter`, `filter-compare`, `filter-number-greater`, `filter-number-greater-equal`, `filter-number-less`, `filter-number-less-equal`, `filter-number-between`, the bounded numeric column operations (`add-numeric-columns`, `subtract-numeric-columns`, `multiply-numeric-columns`, `divide-numeric-columns`, `percent-change-column`, `absolute-value-column`, `square-root-column`, `log10-column`, `round-numeric-column`, and `clip-numeric-column`), `derive-numeric-column`, `rename-column`, `typed-inner-join`, `group-sum`, `group-mean`, `sort`, `select-columns`, `drop-missing`, `drop-rows-any-missing`, `head-rows`, `tail-rows` and `reverse-rows`. Numeric column operations are compiled to the same bounded expression evaluator used by `derive-numeric-column`; nulls propagate, invalid domains, division by zero and non-finite values fail with a row-specific error, and integer inputs must be exactly representable before conversion. Numeric filters compare against parsed typed values and exclude nulls. `filter-compare` accepts `eq`, `ne`, `lt`, `lte`, `gt` and `gte`. Null equals a missing literal only with `eq`; other comparisons exclude null rows. Sorts use typed ordering with nulls last and preserve input order for ties. Categorical values sort by their text labels. Renaming preserves type and metadata. A typed inner join reads a second supported table input and requires `right_path`, `left_key`, `right_key` and `right_schema`; relative paths resolve from the project JSON file. The right key must have the same declared/inferred type as the left key, null keys never match, categorical keys compare by label, duplicate keys expand to all matching pairs, and right-side non-key columns get a `right.` prefix. `group-sum` and `group-mean` take `group_column` and `value_column`, output the key and a `sum_<value_column>` or `mean_<value_column>` float64 column, and preserve key type and metadata plus the value unit. Null keys form one group; null numeric values are skipped; groups with no numeric values have a null aggregate. Integer inputs outside the exact float64 range and non-finite aggregate results fail. Groups retain first-seen order. Incompatible or colliding columns fail before output is written. An incompatible filter or mixed timezone-aware and naive datetime sort fails the project without writing a partial output.

A numeric expression can reference numeric columns and finite literals:

```json
{
  "operation": "derive-numeric-column",
  "name": "total",
  "label": "Total response",
  "unit": "points",
  "expression": {
    "op": "add",
    "left": {"op": "column", "name": "score"},
    "right": {"op": "literal", "value": 2.0}
  }
}
```

Expression nodes support `add`, `subtract`, `multiply`, `divide`, `negate`, `absolute`, `square_root`, `log10`, `round` and `clip`. `round` takes `decimal_places` from -12 through 12; `clip` takes finite ordered `minimum` and `maximum` bounds. Expressions are limited to depth 16 and 64 nodes. Results are float64, arithmetic nulls propagate, and division by zero, invalid domains, overflow and non-finite values fail with the source row number. Integer inputs must be within the exactly representable float64 range (absolute value at most 2^53); this prevents silently rounded integer values. Output is written only after the whole project succeeds. Other transformations currently return a step-specific unsupported-operation error in typed mode. Typed Parquet output retains Arrow types, nulls, categorical dictionaries, labels and units; the source remains untouched.

## Join two tables

Choose `inner-join` with the left table as Input. The operation parameter is JSON, for example `{"right_path":"lookup.tsv","left_key":"id","right_key":"code"}`. For CSV, TSV and supported scalar Parquet inputs, both keys are inferred or decoded as typed values and must have matching types; null keys never match, categorical values compare by label, and numeric strings such as `02` and `2` can match. Other supported formats retain exact nonempty text key matching. Unmatched keys are omitted. Duplicate keys produce every matching pair in input order. The right key column is omitted and other columns from the right table use the `right.` prefix. Name collisions, more than 256 columns or 500,000 cells and joined text over 32 MiB cause the operation to fail before saving. Use a new CSV, TSV or Parquet output or leave it blank to preview. XLSX joins use the first sheet.

Use `left-join`, `right-join` or `full-join` with the same JSON parameters. Matching keys are exact nonblank text and duplicate keys produce all pairs. Blank keys do not match: left and full joins retain unmatched left rows; right and full joins retain unmatched right rows, including blank-key rows. Left joins follow left input order; right joins follow right input order; full joins list left rows first and then unmatched right rows. Missing-side columns are empty strings. The right key column is omitted and other right-side headers use the `right.` prefix. The same column, row and output-text bounds apply.

Use `group-summary` with `group-column,value-column`. One row per exact group appears in text sort order. Blank numeric values count as missing. Sample SD uses n−1 and is blank below two observations. Groups with no numeric values retain their counts and blank statistics. Preview the result or write a new CSV, TSV or Parquet. The usual delimited-text formula guard applies to exported text cells.

## Query a table with SQL

Choose `select-sql` and enter one SQLite SELECT. The imported table is named `input`. For example, `SELECT category, SUM(CAST(amount AS REAL)) AS total FROM input GROUP BY category ORDER BY category`. Wrap column names with spaces in double quotes. The table starts with text columns, so use explicit casts and review invalid numeric text before casting. NULL results are exported as empty text and BLOB results are rejected. Result names must be unique.

The bundled engine uses an in-memory database and limits statement size, output cells and execution work. It permits SELECT and a limited list of text and numeric functions. The route refuses database attachment, extensions, PRAGMAs, writes and multiple statements. Use a new CSV, TSV or Parquet output or leave it blank to preview.

## Inferential statistics and intervals

Inferential operations (`quantiles`, `standard-error`, `mean-ci`, both bootstrap intervals, `welch-t-test`, `one-way-anova`, `paired-t-test`, `spearman` and `correlation-interval`) read CSV, TSV, the first XLSX worksheet, and supported scalar Parquet through the typed data reader. Values must infer as numeric columns; null values are excluded, and paired tests retain row alignment while excluding incomplete pairs. Integer inputs above the exact IEEE-754 `f64` integer range are rejected instead of being rounded silently. The source is unchanged.

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
- `rename-column`: Enter `old=new`. The new name must contain text and be unique. CSV, TSV and supported flat Parquet inputs use typed columns for previews and supported outputs. Delimited output preserves original cell spellings where possible; true nulls serialize as empty fields.

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
