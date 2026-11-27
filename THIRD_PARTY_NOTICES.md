# Third-party notices (incomplete)

No third-party engine binaries, models, icons or fonts are redistributed in this source snapshot. Cargo dependencies are declared in manifests. Their full transitive inventory, licenses, notices, advisories and redistribution obligations are unresolved until dependency resolution and review. Do not distribute binaries before completing these checks. See docs/DEPENDENCIES.md.

Portable CI previews include `DEPENDENCY_LICENSES.json` and collected Cargo dependency license and notice texts under `third-party/`. The inventory includes normal and build dependency edges. It is not a binary linkage audit, a choice among upstream license alternatives, or a review of separately embedded assets and platform libraries. Empty notice lists require follow-up against the upstream source.

The SQL operation includes SQLite 3.54.1, distributed in the public domain by the SQLite project. The `rusqlite` and `libsqlite3-sys` wrapper packages declare MIT licenses. See the [upstream SQLite copyright declaration](https://sqlite.org/copyright.html).
