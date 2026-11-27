# Status | 0.1.0-alpha.1

Alpha source is on GitHub. The initial Rust integration checks ran on Linux x86_64.

| Operation | Scope | Status |
|---|---|---|
| drop-missing | Drop rows missing a selected value | Linux fixture-tested |
| fill-missing | Fill blanks in a selected column | Linux fixture-tested |
| rename-column | Rename a table header | Linux fixture-tested |
| select-columns | Select and reorder table columns | Linux fixture-tested |
| counts | Exact value counts for a column | Linux fixture-tested |
| preview | CSV, TSV, named or first XLSX sheet, or uncompressed UTF-8 Parquet text preview | Linux fixture-tested |
| filter | Exact-match filter; CSV or string Parquet | Linux fixture-tested |
| sort | Stable ascending text sort by one column | Linux fixture-tested |
| deduplicate | Keep the first occurrence of each exact row | Linux fixture-tested |
| mask-column | Replace nonempty cells in one selected column | Linux fixture and CLI-tested |
| inner-join | Exact nonblank key inner join with bounded result | Linux fixture and CLI-tested |
| left-join | Left outer join with duplicate-key expansion and blank-key nonmatching | Linux fixture-tested |
| right-join | Right outer join preserving right input order and every unmatched row | Linux fixture-tested |
| full-join | Full outer join preserving unmatched rows from both tables | Linux fixture-tested |
| run-project | Replay declared table transformations from a versioned project | Linux fixture and CLI-tested |
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
| export | CSV formula guard; Parquet UTF-8 columns | Linux fixture-tested |
| linear-regression | Ordinary least squares with intercept and complete numeric pairs | Linux fixture and CLI-tested |
| correlation | Pearson coefficient on complete numeric pairs | Linux fixture-tested |
| line-chart | Monochrome vector lines in table order | Linux fixture-tested |
| scatter | Monochrome scatter SVG, 800 x 600 | implemented-unverified |

## Known gaps

In-memory text table only; no inferred types, large-data virtualization, formulas, advanced statistics, project editing, undo, batch runs, other formats, authoritative palettes or full scientific export. XLSX ZIP expansion and range allocation occur in dependency before cell limit; hostile workbooks not safe for production. CSV guard alters potentially formula-like cells, including negative numbers; not a proof of spreadsheet safety. No exact type preservation.

Hosted matrix, device, and packaging verification remain open. See [verification evidence](VERIFICATION.md) and [dependency review](DEPENDENCIES.md).
