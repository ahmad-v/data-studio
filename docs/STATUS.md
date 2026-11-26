# Status | 0.1.0-alpha.1

Alpha source is on GitHub. The initial Rust integration checks ran on Linux x86_64.

| Operation | Scope | Status |
|---|---|---|
| select-columns | Select and reorder table columns | Linux fixture-tested |
| counts | Exact value counts for a column | Linux fixture-tested |
| preview | CSV, TSV, named or first XLSX sheet, or uncompressed UTF-8 Parquet text preview | Linux fixture-tested |
| filter | Exact-match filter; CSV or string Parquet | Linux fixture-tested |
| sort | Stable ascending text sort by one column | Linux fixture-tested |
| deduplicate | Keep the first occurrence of each exact row | Linux fixture-tested |
| mask-column | Replace nonempty cells in one selected column | Linux fixture and CLI-tested |
| summary | Numeric column n, missing, mean, sample SD, min/max | Linux fixture-tested |
| export | CSV formula guard; Parquet UTF-8 columns | Linux fixture-tested |
| linear-regression | Ordinary least squares with intercept and complete numeric pairs | Linux fixture and CLI-tested |
| correlation | Pearson coefficient on complete numeric pairs | Linux fixture-tested |
| line-chart | Monochrome vector lines in table order | Linux fixture-tested |
| scatter | Monochrome scatter SVG, 800 x 600 | implemented-unverified |

## Known gaps

In-memory text table only; no inferred types, large-data virtualization, formulas, rich statistics, project pipelines, other formats, authoritative palettes or full scientific export. XLSX ZIP expansion and range allocation occur in dependency before cell limit; hostile workbooks not safe for production. CSV guard alters potentially formula-like cells, including negative numbers; not a proof of spreadsheet safety. No exact type preservation.

Hosted matrix, device, and packaging verification remain open. See [verification evidence](VERIFICATION.md) and [dependency review](DEPENDENCIES.md).
