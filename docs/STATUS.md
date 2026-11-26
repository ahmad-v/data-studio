# Status | 0.1.0-alpha.1

M0 complete as source planning; partial M1 implementation, not verified. All five repos created locally as source trees only. No remote resources created.

| Operation | Scope | Status |
|---|---|---|
| preview | CSV / first XLSX sheet text preview | implemented-unverified |
| filter | Exact-match filter; CSV or string Parquet | implemented-unverified |
| summary | Numeric column n, missing, mean, sample SD, min/max | implemented-unverified |
| export | CSV formula guard; Parquet UTF-8 columns | implemented-unverified |
| scatter | Monochrome scatter SVG, 800 x 600 | implemented-unverified |

## Known gaps

In-memory text table only; no inferred types, worksheet selector, large-data virtualization, formulas, rich statistics, project pipelines, other formats, authoritative palettes or full scientific export. XLSX ZIP expansion and range allocation occur in dependency before cell limit; hostile workbooks not safe for production. CSV guard alters potentially formula-like cells, including negative numbers; not a proof of spreadsheet safety. No exact type preservation.

Cargo and rustc absent; registry and distribution DNS failed. No lockfile, compilation, rustfmt, clippy, Rust tests, GUI/device tests, package builds or security audit performed. All five platform builds blocked pending CI. See MASTER_SPECIFICATION.md for authoritative acceptance gates.
