# Dependencies

Cargo manifests list the exact direct requirements. The committed lockfile records the resolved versions and registry checksums. Builds use Rust 1.95.0 from `rust-toolchain.toml`. The CI checkout action is pinned to its upstream v4.2.2 commit.

A preliminary RustSec review used advisory database revision `c9f6c91a44dbad9d71742329e9df061279f9c4be`. The pinned Localwright revision uses Iced’s Tiny Skia software renderer and removes the affected `lru 0.12.5` dependency path (RUSTSEC-2026-0002 and RUSTSEC-2026-0253). Full license review, SBOM and native dependency review remain open.

The XLSX and Parquet stack uses Calamine 0.38.0, ZIP 9.0.5 and Arrow/Parquet 57.3.3. These pins address advisories in the previous lock candidates for Calamine, ZIP, Quick-XML, Arrow Data and Thrift. The updated spreadsheet stack requires Rust 1.88.

Read-only SQL uses the bundled SQLite 3.54.1 source from the pinned `libsqlite3-sys 0.40.0` lock entry and the MIT-licensed Rust wrapper `rusqlite 0.43.0`. The engine is compiled into Data Studio and does not require a host SQLite command or library. See the upstream [SQLite copyright and public-domain declaration](https://sqlite.org/copyright.html). The final release audit must separately review the native version.
