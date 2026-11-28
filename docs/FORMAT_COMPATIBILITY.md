# Format and operation compatibility

No capability is marked device-verified or released. The merged typed-processing baseline passed the five-target native matrix and Integration. The typed CSV/TSV route expansion in this development change adds numeric comparisons and null-aware row selection; its hosted verification is pending. The machine-readable registry is a development inventory, not an advertised capability list.

| Operation | Route | Evidence status |
|---|---|---|
| drop-missing | Drop rows missing a selected value; CSV/TSV and supported scalar Parquet use typed nulls | Typed route regression added; hosted verification pending |
| fill-missing | Fill blanks in a selected column | Linux fixture-tested |
| rename-column | Typed CSV/TSV and supported flat Parquet rename with preview and supported output | New regression route; hosted verification pending |
| preview | CSV / first XLSX sheet text preview | implemented-unverified |
| inspect-schema | Infer or validate scalar types and metadata | Linux fixture-tested; native matrix pending |
| export-typed-parquet | Arrow scalar, temporal and categorical Parquet export with import round-trip | Linux fixture-tested; native matrix pending |
| filter | Type-aware equality and inequality for supported CSV/TSV and scalar Parquet inputs, with preview and delimited or Parquet output | Regression route added; hosted verification pending |
| filter-number-* | Typed finite numeric comparisons and inclusive ranges; nulls are excluded, integer comparisons outside the exact float range fail explicitly | Regression route added; hosted verification pending |
| sort | Stable typed ascending order with nulls last for supported CSV/TSV and scalar Parquet inputs | Regression route added; hosted verification pending |
| drop-rows-any-missing | Drop rows with one or more typed null cells | Typed route regression added; hosted verification pending |
| head-rows / tail-rows / reverse-rows | Preserve typed values while selecting or reordering row windows | Typed route regression added; hosted verification pending |
| deduplicate | Keep the first occurrence of each exact row | Linux fixture-tested |
| mask-column | Replace nonempty cells in one selected column | Linux fixture and CLI-tested |
| inner-join | Typed join for CSV/TSV and supported scalar Parquet; text fallback for other supported inputs | Typed regression route added; hosted verification pending |
| left-join | Exact text key left outer join; duplicate matches expand; blank keys do not match | Linux fixture-tested |
| right-join | Exact text key right outer join; right row order and unmatched blank-key rows retained | Linux fixture-tested |
| full-join | Exact text full outer join; unmatched rows from both sides retained | Linux fixture-tested |
| run-project | Legacy text replay; typed comparisons, bounded numeric expressions, rename, type-preserving inner join, typed group sum/mean, sort, selection, row windows and Parquet output | Linux schema, null, metadata, join, aggregation and failure fixtures added; hosted verification pending |
| select-sql | One read-only SQLite SELECT over the imported text table | Linux fixture and reference CLI-tested |
| group-summary | Typed CSV/TSV, first-sheet XLSX and supported scalar Parquet keys and numeric columns; missing counts stay distinct; integer counts and floating-point statistics are preserved on Parquet output | Linux regression fixtures passed; hosted checks pending |
| summary | Numeric column n, missing, mean, sample SD, min/max | implemented-unverified |
| export | CSV formula guard; Parquet UTF-8 columns | implemented-unverified |
| linear-regression | Ordinary least squares with intercept and complete numeric pairs | Linux fixture and CLI-tested |
| correlation | Pearson coefficient on complete numeric pairs | Linux fixture-tested |
| line-chart | Monochrome labeled vector lines in table order; legacy and JSON parameters | Linux fixture-tested |
| scatter | Monochrome labeled scatter SVG, 800 x 600; legacy and JSON parameters | Linux fixture-tested |

See section 04-07 and 10 of the master specification for complete targets.
