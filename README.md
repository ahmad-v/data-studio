# Data Studio

Local tabular data workbench. This is a development snapshot (0.1.0-alpha.1), not a verified release or installer.

This independent studio uses Localwright at source level. There is no separate mandatory Localwright installation.

## What is in this snapshot

- CSV / first XLSX sheet text preview (implemented-unverified).
- `inspect-schema` builds a typed columnar schema view with inference, explicit overrides, null counts, categorical dictionary counts, labels and units; it leaves source cells unchanged.
- `inspect-table` loads a bounded typed page (up to 100 rows; text capped at 256 characters) into the native Data table workspace. Optional JSON filters and a stable sort apply to the complete accepted dataset before slicing the page; use `offset` to request another page. The operation remains in-memory and subject to file and cell limits. Header sorting and table search apply only to the returned page. It does not write or modify files.
- Typed `run-project` results also open in the native Table tab. That view contains only the first 30 rows from the project result preview; table sorting and search apply to those displayed rows only. Add typed sort/filter steps to the project to transform all result rows. The preview payload cannot distinguish null from empty text in nullable string columns, and the Table summary discloses that limitation.
- `export-typed-parquet` validates that schema and writes typed Arrow/Parquet scalar columns, nulls, categories, and label/unit metadata; Parquet imports accept supported flat scalar types.
- Filter, sort, column selection, single and multi-column rename, column removal, and typed constant/index additions use typed CSV/TSV/XLSX/Parquet values for previews and CSV/TSV/Parquet outputs. Delimited exports preserve original cell spellings where possible (including leading zeroes). True nulls and empty strings both serialize as empty fields; typed Parquet preserves nulls separately.
- Exact-match filter over delimited text or supported scalar Parquet converted to text (implemented-unverified).
- Numeric column n, missing, mean, sample SD, min/max (implemented-unverified).
- CSV formula guard; Parquet UTF-8 columns (implemented-unverified).
- Scientific line and scatter figures export SVG or vector PDF with titles, axis labels and legends. Multi-series figures use accessible marker/dash styles and the `batlowS` qualitative palette. The pinned Localwright core also bundles sequential `batlow`, `viridis`, `cividis`, `plasma`, `inferno`, `magma` and diverging `vik` maps; Data Studio's current chart renderer does not yet expose those sequential/diverging maps. See the manual for palette names, attribution and the current selection limit. PDF exports accept `width_mm` and `height_mm` in JSON and default to 210 × 148 mm. Host font availability affects label outlines; generated plots are not rasterized.

The project is intended to remain free, local and offline, without tracking, advertising, subscriptions or premium features. See [the manifesto](MANIFESTO.md) for the motivation behind it.

## Development

See [build notes](docs/BUILD.md), [status](docs/STATUS.md), [architecture](docs/ARCHITECTURE.md) and [manual](docs/MANUAL.md). The complete [master specification](MASTER_SPECIFICATION.md) defines the remaining requirements. This development branch is being checked on Linux and hosted native targets; successful test and platform evidence is recorded in the handoff and CI runs. It is not a verified release or installer.

## Citation and contributions

If the software is useful in your research, please consider citing its version once a verified release and actual repository URL exist. See `CITATION.cff`. No DOI has been assigned. Code, testing, translations and documentation are welcome. No donation account has been configured.

Original code candidate license: MIT OR Apache-2.0, pending maintainer approval and dependency review. AI-assisted source requires human review before publication.
