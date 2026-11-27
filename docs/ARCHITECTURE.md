# Architecture

Data Studio: Local tabular data workbench.

Localwright core contains serde-based domain contracts, request/response, capability and engine descriptors, job cancellation, bounded reads, no-clobber output and settings. It has no Iced dependency. Localwright UI is an optional dependency of the studios and provides a generic native operation workbench. It dispatches a single job to a background thread and receives the structured outcome through a channel. The same handler runs through the CLI. Studio processing lives in its own crate. No local server, system engine lookup or runtime network client is intentionally implemented.

Source contracts version 0.1.0-alpha.1; pre-1.0 API is unstable. Use exact shared revision after import. New breaking changes require migration notes and all four consumer checks. The application uses Rust 1.95.0 for its bundled SQLite dependency. No promise of ABI stability. Current settings schema 1. Request and outcome are internal alpha interchange, not stable project file formats.

`inspect-schema` builds a typed columnar view over an imported text table, infers or validates scalar types, reports nulls and categorical dictionaries, and attaches optional labels and units. This is an initial architecture layer only: existing transformation operations still use the legacy string table, and typed values are not yet retained in project files, edits or Parquet output. The typed view is bounded by the current table cell limit.

Limitations: Existing processing is still an in-memory text table; no large-data virtualization, typed editing, project schema persistence, general formula language, full worksheet selection, other formats, authoritative palettes or full scientific export. XLSX ZIP expansion and range allocation occur in the dependency before the cell limit; hostile workbooks are not safe for production. CSV guard alters potentially formula-like cells, including negative numbers; it is not a proof of spreadsheet safety.
