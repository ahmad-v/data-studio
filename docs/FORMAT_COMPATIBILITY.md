# Format and operation compatibility

Nothing is verified on a native target. The machine-readable registry is a development inventory, not an advertised capability list.

| Operation | Route | Evidence status |
|---|---|---|
| preview | CSV / first XLSX sheet text preview | implemented-unverified |
| filter | Exact-match filter; CSV or string Parquet | implemented-unverified |
| sort | Stable ascending text sort by one column | Linux fixture-tested |
| deduplicate | Keep the first occurrence of each exact row | Linux fixture-tested |
| summary | Numeric column n, missing, mean, sample SD, min/max | implemented-unverified |
| export | CSV formula guard; Parquet UTF-8 columns | implemented-unverified |
| linear-regression | Ordinary least squares with intercept and complete numeric pairs | Linux fixture and CLI-tested |
| correlation | Pearson coefficient on complete numeric pairs | Linux fixture-tested |
| scatter | Monochrome scatter SVG, 800 x 600 | implemented-unverified |

See section 04-07 and 10 of the master specification for complete targets.
