//! Preflight untrusted XLSX worksheet XML before handing it to Calamine.
//!
//! Calamine may allocate a rectangular range based on a worksheet's declared
//! dimension. Check that range from the bounded ZIP stream first so malformed
//! workbook metadata cannot trigger a very large allocation before our cell
//! limit is applied.
use crate::MAX_CELLS;
use localwright_core::{Error, Job, Result};
use quick_xml::{events::Event, Reader};
use std::io::{self, Cursor, Read};
use zip::ZipArchive;

const MAX_ARCHIVE_MEMBERS: usize = 4096;
const MAX_ARCHIVE_EXPANSION: u64 = 64 * 1024 * 1024;
const MAX_MEMBER_EXPANSION: u64 = 32 * 1024 * 1024;
const MAX_EXCEL_ROWS: u32 = 1_048_576;
const MAX_EXCEL_COLUMNS: u32 = 16_384;

/// Validate archive expansion and worksheet dimensions before a workbook is
/// opened by the spreadsheet engine. Every worksheet is checked because the
/// requested sheet name is resolved only after workbook parsing.
pub(crate) fn validate_workbook(bytes: &[u8], job: &Job) -> Result<()> {
    let mut archive = ZipArchive::new(Cursor::new(bytes))
        .map_err(|error| Error::input(format!("Invalid XLSX archive: {error}")))?;
    if archive.len() > MAX_ARCHIVE_MEMBERS {
        return Err(Error::input("Workbook archive member limit exceeded"));
    }

    let mut total_declared = 0u64;
    for index in 0..archive.len() {
        job.check()?;
        let entry = archive
            .by_index(index)
            .map_err(|error| Error::input(format!("Invalid XLSX archive entry: {error}")))?;
        total_declared = total_declared
            .checked_add(entry.size())
            .ok_or_else(|| Error::input("Workbook expansion overflow"))?;
        if total_declared > MAX_ARCHIVE_EXPANSION || entry.size() > MAX_MEMBER_EXPANSION {
            return Err(Error::input("Workbook declared expansion limit exceeded"));
        }
        let _name = entry
            .name()
            .map_err(|error| Error::input(format!("Invalid XLSX path: {error}")))?;
    }

    // Validate actual decompressed bytes too. Central-directory sizes can be
    // malformed, so never rely on those values as the only expansion bound.
    let mut total_expanded = 0u64;
    let mut worksheet_count = 0usize;
    for index in 0..archive.len() {
        job.check()?;
        let entry = archive
            .by_index(index)
            .map_err(|error| Error::input(format!("Invalid XLSX archive entry: {error}")))?;
        let name = entry
            .name()
            .map_err(|error| Error::input(format!("Invalid XLSX path: {error}")))?;
        let is_worksheet = is_worksheet_xml(&name);
        let allowed = MAX_MEMBER_EXPANSION.min(MAX_ARCHIVE_EXPANSION - total_expanded);
        let read_limit = allowed + 1;
        let expanded = if is_worksheet {
            let mut xml = Vec::with_capacity(entry.size().min(allowed) as usize);
            entry
                .take(read_limit)
                .read_to_end(&mut xml)
                .map_err(|error| Error::input(format!("Could not read XLSX entry: {error}")))?;
            if xml.len() as u64 > allowed {
                return Err(Error::input("Workbook actual expansion limit exceeded"));
            }
            total_expanded += xml.len() as u64;
            worksheet_count += 1;
            validate_worksheet_xml(&xml, job)?;
            xml.len() as u64
        } else {
            let mut limited = entry.take(read_limit);
            let expanded = io::copy(&mut limited, &mut io::sink())
                .map_err(|error| Error::input(format!("Could not read XLSX entry: {error}")))?;
            if expanded > allowed {
                return Err(Error::input("Workbook actual expansion limit exceeded"));
            }
            total_expanded += expanded;
            expanded
        };
        if expanded > MAX_MEMBER_EXPANSION || total_expanded > MAX_ARCHIVE_EXPANSION {
            return Err(Error::input("Workbook actual expansion limit exceeded"));
        }
    }
    if worksheet_count == 0 {
        return Err(Error::input("Workbook contains no worksheet XML"));
    }
    Ok(())
}

fn is_worksheet_xml(name: &str) -> bool {
    name.strip_prefix("xl/worksheets/")
        .is_some_and(|remainder| !remainder.contains('/') && remainder.ends_with(".xml"))
}

fn validate_worksheet_xml(xml: &[u8], job: &Job) -> Result<()> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);
    let mut buffer = Vec::new();
    let mut bounds: Option<(u32, u32, u32, u32)> = None;
    let mut cells = 0usize;

    loop {
        job.check()?;
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(element) | Event::Empty(element)) => {
                let local_name = element.name().local_name();
                let target_attribute = if local_name.into_inner() == "dimension" {
                    Some("ref")
                } else if local_name.into_inner() == "c" {
                    cells = cells
                        .checked_add(1)
                        .ok_or_else(|| Error::input("Worksheet cell count overflow"))?;
                    if cells > MAX_CELLS {
                        return Err(Error::input("Worksheet exceeds cell limit"));
                    }
                    Some("r")
                } else {
                    None
                };
                if let Some(target_attribute) = target_attribute {
                    for attribute in element.attributes() {
                        let attribute = attribute.map_err(|error| {
                            Error::input(format!("Malformed worksheet attribute: {error}"))
                        })?;
                        if attribute.key.local_name().into_inner() == target_attribute {
                            let reference = attribute
                                .normalized_value(quick_xml::XmlVersion::Implicit1_0)
                                .map_err(|error| {
                                    Error::input(format!("Malformed worksheet reference: {error}"))
                                })?;
                            let range = parse_reference(&reference)?;
                            include_range(&mut bounds, range);
                            ensure_cell_bound(bounds.expect("range was included"))?;
                            break;
                        }
                    }
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => {
                return Err(Error::input(format!("Malformed worksheet XML: {error}")));
            }
        }
        buffer.clear();
    }
    Ok(())
}

fn parse_reference(reference: &str) -> Result<(u32, u32, u32, u32)> {
    let mut areas = reference.split(':');
    let first = areas.next().unwrap_or_default();
    let last = areas.next().unwrap_or(first);
    if areas.next().is_some() {
        return Err(Error::input("Unsupported multi-area worksheet reference"));
    }
    let (first_column, first_row) = parse_cell(first)?;
    let (last_column, last_row) = parse_cell(last)?;
    Ok((
        first_column.min(last_column),
        first_row.min(last_row),
        first_column.max(last_column),
        first_row.max(last_row),
    ))
}

fn parse_cell(reference: &str) -> Result<(u32, u32)> {
    let reference = reference.strip_prefix('$').unwrap_or(reference);
    let split = reference
        .find(|character: char| !character.is_ascii_alphabetic())
        .ok_or_else(|| Error::input("Invalid worksheet cell reference"))?;
    let (column_text, row_text) = reference.split_at(split);
    let row_text = row_text.strip_prefix('$').unwrap_or(row_text);
    if column_text.is_empty()
        || row_text.is_empty()
        || !row_text.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(Error::input("Invalid worksheet cell reference"));
    }
    let mut column = 0u32;
    for byte in column_text.bytes() {
        let digit = byte.to_ascii_uppercase().checked_sub(b'A').unwrap_or(26) + 1;
        if digit > 26 {
            return Err(Error::input("Invalid worksheet column reference"));
        }
        column = column
            .checked_mul(26)
            .and_then(|value| value.checked_add(u32::from(digit)))
            .ok_or_else(|| Error::input("Worksheet column reference overflow"))?;
    }
    let row = row_text
        .parse::<u32>()
        .map_err(|_| Error::input("Invalid worksheet row reference"))?;
    if column == 0 || column > MAX_EXCEL_COLUMNS || row == 0 || row > MAX_EXCEL_ROWS {
        return Err(Error::input(
            "Worksheet cell reference is outside XLSX limits",
        ));
    }
    Ok((column, row))
}

fn include_range(bounds: &mut Option<(u32, u32, u32, u32)>, range: (u32, u32, u32, u32)) {
    *bounds = Some(match *bounds {
        Some((min_column, min_row, max_column, max_row)) => (
            min_column.min(range.0),
            min_row.min(range.1),
            max_column.max(range.2),
            max_row.max(range.3),
        ),
        None => range,
    });
}

fn ensure_cell_bound(bounds: (u32, u32, u32, u32)) -> Result<()> {
    let columns = u64::from(bounds.2 - bounds.0 + 1);
    let rows = u64::from(bounds.3 - bounds.1 + 1);
    if rows.saturating_mul(columns) > MAX_CELLS as u64 {
        return Err(Error::input("Worksheet exceeds cell limit"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use zip::{write::SimpleFileOptions, ZipWriter};

    fn workbook(sheet_xml: &str) -> Vec<u8> {
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        writer
            .start_file("xl/worksheets/sheet1.xml", SimpleFileOptions::default())
            .unwrap();
        writer.write_all(sheet_xml.as_bytes()).unwrap();
        writer.finish().unwrap().into_inner()
    }

    #[test]
    fn accepts_bounded_dimensions_and_real_cell_references() {
        let bytes = workbook(
            r#"<worksheet><dimension ref="A1:B3"/><sheetData><row r="1"><c r="A1"/><c r="B1"/></row><row r="3"><c r="B3"/></row></sheetData></worksheet>"#,
        );
        validate_workbook(&bytes, &Job::new()).unwrap();
    }

    #[test]
    fn rejects_huge_declared_ranges_before_spreadsheet_engine_open() {
        let bytes =
            workbook(r#"<worksheet><dimension ref="A1:XFD1048576"/><sheetData/></worksheet>"#);
        assert!(validate_workbook(&bytes, &Job::new()).is_err());
    }

    #[test]
    fn rejects_sparse_cells_whose_extent_exceeds_cell_budget() {
        let bytes = workbook(
            r#"<worksheet><sheetData><row r="1"><c r="A1"/></row><row r="1000000"><c r="A1000000"/></row></sheetData></worksheet>"#,
        );
        assert!(validate_workbook(&bytes, &Job::new()).is_err());
    }

    #[test]
    fn rejects_invalid_or_out_of_spec_cell_references() {
        for reference in ["A0", "XFE1", "A1048577", "A1:B2:C3"] {
            assert!(parse_reference(reference).is_err(), "{reference}");
        }
        assert_eq!(parse_reference("$A$1:$B$2").unwrap(), (1, 1, 2, 2));
    }

    #[test]
    fn legacy_and_typed_import_paths_reject_before_calamine_parsing() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("oversized.xlsx");
        std::fs::write(
            &path,
            workbook(r#"<worksheet><dimension ref="A1:XFD1048576"/><sheetData/></worksheet>"#),
        )
        .unwrap();

        assert!(crate::load(&path, None, &Job::new()).is_err());
        assert!(crate::typed::TypedTable::from_path(
            &path,
            None,
            crate::typed::SchemaRequest::default(),
            &Job::new(),
        )
        .is_err());
        assert!(path.exists());
    }
}
