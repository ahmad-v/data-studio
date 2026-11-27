# Status | 0.1.0-alpha.1

Alpha source is on GitHub. The initial Rust integration checks ran on Linux x86_64.

| Operation | Scope | Status |
|---|---|---|
| drop-missing | Drop rows missing a selected value | Linux fixture-tested |
| fill-missing | Fill blanks in a selected column | Linux fixture-tested |
| rename-column | Rename a table header | Linux fixture-tested |
| select-columns | Select and reorder table columns | Linux fixture-tested |
| counts | Exact value counts for a column | Linux fixture-tested |
| preview | CSV, TSV, named or first XLSX sheet, or supported flat scalar Parquet cast to text | Linux fixture-tested |
| inspect-schema | Infer or explicitly validate a columnar schema; report types, nulls, categorical dictionary counts, labels and units without modifying input; CSV/TSV parses directly to typed columns | Linux unit and route fixtures passed; five-target hosted verification pending |
| export-typed-parquet | Write a validated or inferred typed Arrow schema, including nulls, temporal types, category dictionaries and label/unit metadata | Linux round-trip/rejection fixtures passed; hosted native matrix pending |
| filter | Exact-match filter; CSV or string Parquet | Linux fixture-tested |
| sort | Stable ascending text sort by one column | Linux fixture-tested |
| deduplicate | Keep the first occurrence of each exact row | Linux fixture-tested |
| mask-column | Replace nonempty cells in one selected column | Linux fixture and CLI-tested |
| inner-join | Exact nonblank key inner join with bounded result | Linux fixture and CLI-tested |
| left-join | Left outer join with duplicate-key expansion and blank-key nonmatching | Linux fixture-tested |
| right-join | Right outer join preserving right input order and every unmatched row | Linux fixture-tested |
| full-join | Full outer join preserving unmatched rows from both tables | Linux fixture-tested |
| run-project | Replay text projects unchanged; optional `typed_schema` enables typed comparisons, bounded numeric expressions, rename, type-preserving inner join, typed group sum/mean, sorting, selection and row windows while preserving Parquet schema, nulls, categories and metadata; CSV/TSV inputs parse directly to typed columns | Tests cover comparisons, expression errors, metadata, schema-preserving rename/join/aggregation, null semantics and failure isolation; hosted verification pending |
| select-sql | One read-only SQLite SELECT over the imported text table | Linux fixture and reference CLI-tested |
| group-summary | Grouped finite numeric descriptive statistics | Linux unit fixture and CLI-tested |
| summary | Numeric column n, missing, mean, sample SD, min/max | Linux fixture-tested |
| quantiles | R7 quartiles, median and IQR | Linux interpolation fixture-tested |
| standard-error | Sample standard error | Linux sample-SD reference-tested |
| mean-ci | Two-sided 95% Student-t mean interval | Linux critical-value and interval reference-tested |
| bootstrap-mean-ci | Deterministic percentile bootstrap mean interval, 2,000 resamples | Linux deterministic fixture-tested, 5,000-row cap |
| bootstrap-median-ci | Deterministic percentile bootstrap median interval, 2,000 resamples | Linux deterministic fixture-tested, 5,000-row cap |
| welch-t-test | Unequal-variance two-sample t test, interval and p value | Linux t-distribution reference-tested |
| paired-t-test | Paired two-sided Student-t test and interval | Linux difference-vector fixture-tested |
| one-way-anova | Independent-groups one-way ANOVA | Linux reference fixture-tested |
| spearman | Rank correlation with average ties and approximate p value | Linux tied-rank fixture-tested |
| correlation-interval | Pearson Fisher-z interval | Linux bounded-interval fixture-tested |
| recode-values | Exact cell mapping with unchanged fallback values | Linux route and project fixtures |
| convert-number | Validate and normalize finite numeric text | Linux route and project fixtures |
| convert-integer | Validate exact signed 64-bit integer text | Linux route and project fixtures |
| convert-boolean | Normalize accepted boolean spellings to true/false | Linux route and project fixtures |
| split-column | Literal exact-arity split into named columns | Linux route and project fixtures |
| merge-columns | Append a delimited column while retaining source columns | Linux route and project fixtures |
| pivot | Long-to-wide reshape with duplicate-pair rejection | Linux route and project fixtures |
| unpivot | Wide-to-long reshape with selected id/value columns | Linux route and project fixtures |
| group-sum | Grouped finite numeric sum | Linux route and project fixtures |
| group-mean | Grouped finite numeric mean | Linux route and project fixtures |
| filter-contains | Case-sensitive literal substring inclusion | Linux route and versioned-project fixtures passed; hosted matrix pending |
| filter-not-contains | Case-sensitive literal substring exclusion | Linux route and versioned-project fixtures passed; hosted matrix pending |
| head-rows | Keep first N rows in source order | Linux route and versioned-project fixtures passed; hosted matrix pending |
| tail-rows | Keep last N rows in source order | Linux route and versioned-project fixtures passed; hosted matrix pending |
| drop-duplicates-by-columns | Keep first exact key occurrence in selected columns | Linux route and versioned-project fixtures passed; hosted matrix pending |
| add-index-column | Add sequential signed 64-bit row index | Linux route and versioned-project fixtures passed; hosted matrix pending |
| add-constant-column | Add literal text to every row | Linux route and versioned-project fixtures passed; hosted matrix pending |
| normalize-text | Trim, Unicode case change, or collapse whitespace | Linux route and versioned-project fixtures passed; hosted matrix pending |
| rename-columns | Atomically rename multiple columns | Linux route and versioned-project fixtures passed; hosted matrix pending |
| drop-columns | Drop selected columns while retaining at least one | Linux route and versioned-project fixtures passed; hosted matrix pending |
| filter-not-equal | Exact-text inequality filter | Linux route and project fixtures passed; hosted matrix pending |
| filter-contains-insensitive | Unicode lowercase substring filter | Linux route and project fixtures passed; hosted matrix pending |
| filter-starts-with | Case-sensitive literal prefix filter | Linux route and project fixtures passed; hosted matrix pending |
| filter-ends-with | Case-sensitive literal suffix filter | Linux route and project fixtures passed; hosted matrix pending |
| replace-literal | Bounded non-overlapping literal text replacement | Linux route and project fixtures passed; hosted matrix pending |
| drop-rows-any-missing | Remove rows with any blank or whitespace-only cell | Linux route and project fixtures passed; hosted matrix pending |
| fill-forward | Fill blanks from nearest preceding value | Linux route and project fixtures passed; hosted matrix pending |
| fill-backward | Fill blanks from nearest following value | Linux route and project fixtures passed; hosted matrix pending |
| drop-empty-columns | Remove all-blank columns, retaining at least one | Linux route and project fixtures passed; hosted matrix pending |
| reverse-rows | Reverse table row order | Linux route and project fixtures passed; hosted matrix pending |
| filter-number-greater | Keep finite numeric values strictly above a threshold | Linux route and project fixtures passed; hosted matrix pending |
| filter-number-greater-equal | Keep finite numeric values at or above a threshold | Linux route and project fixtures passed; hosted matrix pending |
| filter-number-less | Keep finite numeric values strictly below a threshold | Linux route and project fixtures passed; hosted matrix pending |
| filter-number-less-equal | Keep finite numeric values at or below a threshold | Linux route and project fixtures passed; hosted matrix pending |
| filter-number-between | Keep finite numeric values within inclusive bounds | Linux route and project fixtures passed; hosted matrix pending |
| add-numeric-columns | Add finite numeric columns | Linux route and project fixtures passed; hosted matrix pending |
| subtract-numeric-columns | Subtract finite numeric columns | Linux route and project fixtures passed; hosted matrix pending |
| multiply-numeric-columns | Multiply finite numeric columns | Linux route and project fixtures passed; hosted matrix pending |
| divide-numeric-columns | Divide finite numeric columns; zero is rejected | Linux route and project fixtures passed; hosted matrix pending |
| percent-change-column | Compute percent change; zero baseline is rejected | Linux route and project fixtures passed; hosted matrix pending |
| absolute-value-column | Add absolute value column | Linux route and project fixtures passed; hosted matrix pending |
| square-root-column | Add square root column for nonnegative values | Linux route and project fixtures passed; hosted matrix pending |
| log10-column | Add base-10 logarithm column for positive values | Linux route and project fixtures passed; hosted matrix pending |
| round-numeric-column | Round numeric text at precision -12 through 12 | Linux route and project fixtures passed; hosted matrix pending |
| clip-numeric-column | Clamp finite numeric values to inclusive bounds | Linux route and project fixtures passed; hosted matrix pending |
| export | CSV formula guard; Parquet UTF-8 columns | Linux fixture-tested |
| linear-regression | Ordinary least squares with intercept and complete numeric pairs | Linux fixture and CLI-tested |
| correlation | Pearson coefficient on complete numeric pairs | Linux fixture-tested |
| line-chart | Monochrome vector lines in table order | Linux fixture-tested |
| scatter | Monochrome scatter SVG, 800 x 600 | implemented-unverified |

## Known gaps

Most transformations still run on the legacy in-memory string table. Versioned projects can opt into a typed columnar pipeline with schema-aware comparisons, bounded arithmetic expression columns, stable typed sorting, column selection and row windows/removal; the pipeline preserves Arrow values, nulls, categorical dictionaries, labels and units through typed Parquet output. Joins, aggregation, typed editing and broad standalone transformation migration remain open. Large-data virtualization, regex transforms, multi-key/duplicate-aggregating pivots, project editor, undo, batch runs, other formats, authoritative palettes and full scientific export remain open. XLSX ZIP expansion and range allocation occur in the dependency before cell limit; hostile workbooks are not safe for production. CSV guard alters potentially formula-like cells, including negative numbers; it is not a proof of spreadsheet safety.

Hosted matrix, device, and packaging verification remain open. See [verification evidence](VERIFICATION.md) and [dependency review](DEPENDENCIES.md).
