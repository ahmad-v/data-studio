# Format and operation compatibility

No capability is marked device-verified or released. Linux and Windows hosted checks passed on the previous pull request head; the current typed Parquet change needs its own five-target verification. The machine-readable registry is a development inventory, not an advertised capability list.

| Operation | Route | Evidence status |
|---|---|---|
| drop-missing | Drop rows missing a selected value | Linux fixture-tested |
| fill-missing | Fill blanks in a selected column | Linux fixture-tested |
| rename-column | Rename a table header | Linux fixture-tested |
| preview | CSV / first XLSX sheet text preview | implemented-unverified |
| inspect-schema | Infer or validate scalar types and metadata | Linux fixture-tested; native matrix pending |
| export-typed-parquet | Arrow scalar, temporal and categorical Parquet export with import round-trip | Linux fixture-tested; native matrix pending |
| filter | Exact-match filter; CSV or string Parquet | implemented-unverified |
| sort | Stable ascending text sort by one column | Linux fixture-tested |
| deduplicate | Keep the first occurrence of each exact row | Linux fixture-tested |
| mask-column | Replace nonempty cells in one selected column | Linux fixture and CLI-tested |
| inner-join | Exact nonblank key inner join with bounded result | Linux fixture and CLI-tested |
| left-join | Exact text key left outer join; duplicate matches expand; blank keys do not match | Linux fixture-tested |
| right-join | Exact text key right outer join; right row order and unmatched blank-key rows retained | Linux fixture-tested |
| full-join | Exact text full outer join; unmatched rows from both sides retained | Linux fixture-tested |
| run-project | Legacy text replay; typed schema-aware filter, sort, selection, missing-row removal, head/tail/reverse and Parquet output | Linux typed project fixture added; five-target verification pending |
| select-sql | One read-only SQLite SELECT over the imported text table | Linux fixture and reference CLI-tested |
| group-summary | Grouped finite numeric descriptive statistics | Linux unit fixture and CLI-tested |
| summary | Numeric column n, missing, mean, sample SD, min/max | implemented-unverified |
| export | CSV formula guard; Parquet UTF-8 columns | implemented-unverified |
| linear-regression | Ordinary least squares with intercept and complete numeric pairs | Linux fixture and CLI-tested |
| correlation | Pearson coefficient on complete numeric pairs | Linux fixture-tested |
| line-chart | Monochrome vector lines in table order | Linux fixture-tested |
| scatter | Monochrome scatter SVG, 800 x 600 | implemented-unverified |

See section 04-07 and 10 of the master specification for complete targets.
