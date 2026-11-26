# Verification evidence

The integration run used Rust 1.88.0 on Linux x86_64 with Localwright pinned to the revision in `Cargo.lock`. Formatting, locked all-target checking, default and headless tests, strict Clippy, and CLI help passed.

The XLSX, filter, summary, and CSV formula guard tests passed. Openpyxl and PyArrow confirmed that the CLI Parquet export preserves the XLSX fixture values as strings.

The GitHub workflow defines five platform checks. Device tests and installer verification remain open.
