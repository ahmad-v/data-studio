# Verification evidence

The integration run used Rust 1.88.0 on Linux x86_64 with Localwright pinned to the revision in `Cargo.lock`. Formatting, locked all-target checking, default and headless tests, strict Clippy, and CLI help passed.

The XLSX, filter, summary, and CSV formula guard tests passed. Openpyxl and PyArrow confirmed that the CLI Parquet export preserves the XLSX fixture values as strings.

The GitHub workflow defines five platform checks. Device tests and installer verification remain open.

The bounded SQL route was checked with Rust 1.95.0 and bundled SQLite 3.54.1. The fixture verifies grouped results and rejects writes, external attachments, extensions, inaccessible schema objects, multiple statements and unsupported result values. An independent Python SQLite run matched the CLI result. The authorizer and resource bounds narrow exposed SQL features and do not sandbox native code.

On 2026-11-27, the locked headless suite passed 18 tests and strict Clippy passed on Linux x86_64 with the declared Rust 1.95.0 toolchain. Left, right and full outer-join fixtures cover duplicate keys, unmatched rows, blank keys and empty-side columns. The capability registry JSON and whitespace checks passed. This does not verify installed GUI workflows or target-specific packages.
