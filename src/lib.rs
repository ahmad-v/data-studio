#![forbid(unsafe_code)]
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use calamine::{Reader, Xlsx};
use localwright_core::*;
use parquet::arrow::ArrowWriter;
use std::{io::Cursor, path::Path, sync::Arc};
pub const OPERATIONS:&[Operation]=&[
 Operation{id:"preview",label:"Preview",hint:"Import CSV or first XLSX worksheet as text; preview first 30 rows."},
 Operation{id:"filter",label:"Filter",hint:"Exact text match: parameter column=value. Optional .csv or .parquet output."},
 Operation{id:"summary",label:"Summary",hint:"Parameter: column name. Finite numeric values only; blanks excluded, invalid values rejected."},
 Operation{id:"scatter",label:"Scatter SVG",hint:"Parameter: x-column,y-column. Numeric complete rows, new .svg output."},
 Operation{id:"export",label:"Export",hint:"Export table to new .csv or .parquet. CSV guards formula-like cells; Parquet uses UTF-8 strings."}];
#[derive(Debug)]
struct Table {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}
const MAX_BYTES: u64 = 16 * 1024 * 1024;
const MAX_CELLS: usize = 500_000;
const MAX_COLS: usize = 256;
fn err(e: impl std::fmt::Display) -> Error {
    Error::input(e.to_string())
}
fn validate(headers: Vec<String>, rows: Vec<Vec<String>>) -> Result<Table> {
    let mut seen = std::collections::HashSet::new();
    if headers.is_empty()
        || headers.len() > MAX_COLS
        || headers
            .iter()
            .any(|h| h.is_empty() || !seen.insert(h.clone()))
    {
        return Err(Error::input(
            "Headers must be unique and nonempty (maximum 256)",
        ));
    }
    if rows.len().saturating_mul(headers.len()) > MAX_CELLS
        || rows.iter().any(|r| r.len() != headers.len())
    {
        return Err(Error::input("Cell limit or inconsistent row width"));
    }
    Ok(Table { headers, rows })
}
fn load(p: &Path, j: &Job) -> Result<Table> {
    let bytes = read_bounded(p, MAX_BYTES)?;
    j.check()?;
    match p
        .extension()
        .and_then(|s| s.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("csv") => {
            let mut r = csv::Reader::from_reader(bytes.as_slice());
            let headers = r
                .headers()
                .map_err(err)?
                .iter()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            if headers.is_empty() || headers.len() > MAX_COLS {
                return Err(Error::input("Invalid column count"));
            }
            let mut rows = vec![];
            for row in r.records() {
                j.check()?;
                if (rows.len() + 1) * headers.len() > MAX_CELLS {
                    return Err(Error::input("Cell limit exceeded"));
                }
                rows.push(row.map_err(err)?.iter().map(str::to_owned).collect());
            }
            validate(headers, rows)
        }
        Some("xlsx") => {
            let mut zip = zip::ZipArchive::new(Cursor::new(bytes.as_slice())).map_err(err)?;
            if zip.len() > 4096 {
                return Err(Error::input("Workbook archive member limit exceeded"));
            }
            let mut total = 0u64;
            for i in 0..zip.len() {
                j.check()?;
                let entry = zip.by_index(i).map_err(err)?;
                total = total
                    .checked_add(entry.size())
                    .ok_or_else(|| Error::input("Workbook expansion overflow"))?;
                if total > 64 * 1024 * 1024 || entry.size() > 32 * 1024 * 1024 {
                    return Err(Error::input("Workbook declared expansion limit exceeded"));
                }
            }
            drop(zip);
            let mut book: Xlsx<_> =
                calamine::open_workbook_from_rs(Cursor::new(bytes)).map_err(err)?;
            let names = book.sheet_names();
            let name = names.first().ok_or_else(|| Error::input("No worksheet"))?;
            let range = book.worksheet_range(name).map_err(err)?;
            let (h, w) = range.get_size();
            if w > MAX_COLS || h.saturating_mul(w) > MAX_CELLS {
                return Err(Error::input("Worksheet exceeds cell limit"));
            }
            let mut rows = range.rows();
            let headers = rows
                .next()
                .ok_or_else(|| Error::input("Empty worksheet"))?
                .iter()
                .map(ToString::to_string)
                .collect();
            let mut records = vec![];
            for row in rows {
                j.check()?;
                records.push(row.iter().map(ToString::to_string).collect());
            }
            validate(headers, records)
        }
        _ => Err(Error::input("Expected CSV or XLSX")),
    }
}
fn column(t: &Table, name: &str) -> Result<usize> {
    t.headers
        .iter()
        .position(|h| h == name)
        .ok_or_else(|| Error::input("Column not found"))
}
fn summary(t: &Table, i: usize, j: &Job) -> Result<String> {
    let (mut n, mut missing, mut mean, mut m2) = (0u64, 0u64, 0.0f64, 0.0f64);
    let (mut min, mut max) = (f64::INFINITY, f64::NEG_INFINITY);
    for row in &t.rows {
        j.check()?;
        let s = row[i].trim();
        if s.is_empty() {
            missing += 1;
            continue;
        }
        let x: f64 = s.parse().map_err(err)?;
        if !x.is_finite() {
            return Err(Error::input("Nonfinite numeric value"));
        }
        n += 1;
        let delta = x - mean;
        mean += delta / n as f64;
        m2 += delta * (x - mean);
        min = min.min(x);
        max = max.max(x);
        if !mean.is_finite() || !m2.is_finite() {
            return Err(Error::input("Numerical overflow"));
        }
    }
    if n == 0 {
        return Err(Error::input("No finite numeric observations"));
    }
    let sd = if n > 1 {
        Some((m2 / (n - 1) as f64).max(0.0).sqrt())
    } else {
        None
    };
    Ok(serde_json::to_string_pretty(
        &serde_json::json!({"n":n,"missing":missing,"mean":mean,"sample_sd":sd,"min":min,"max":max,"method":"Welford, sample denominator n-1"}),
    )?)
}
fn safe(s: &str) -> String {
    if s.trim_start().starts_with(['=', '+', '-', '@']) || s.starts_with(['\t', '\r', '\n']) {
        format!("'{s}")
    } else {
        s.into()
    }
}
fn export(t: &Table, p: &Path, j: &Job) -> Result<()> {
    match p.extension().and_then(|s| s.to_str()) {
        Some("csv") => write_new(p, |f| {
            let mut w = csv::Writer::from_writer(f);
            w.write_record(t.headers.iter().map(|s| safe(s)))
                .map_err(err)?;
            for row in &t.rows {
                j.check()?;
                w.write_record(row.iter().map(|s| safe(s))).map_err(err)?;
            }
            w.flush()?;
            Ok(())
        }),
        Some("parquet") => write_new(p, |f| {
            j.check()?;
            let schema = Arc::new(Schema::new(
                t.headers
                    .iter()
                    .map(|s| Field::new(s.as_str(), DataType::Utf8, false))
                    .collect::<Vec<_>>(),
            ));
            let arrays: Vec<ArrayRef> = (0..t.headers.len())
                .map(|i| {
                    Arc::new(StringArray::from_iter_values(
                        t.rows.iter().map(|r| r[i].as_str()),
                    )) as ArrayRef
                })
                .collect();
            let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(err)?;
            let mut writer = ArrowWriter::try_new(f, schema, None).map_err(err)?;
            writer.write(&batch).map_err(err)?;
            j.check()?;
            writer.close().map_err(err)?;
            Ok(())
        }),
        _ => Err(Error::input("Output must be .csv or .parquet")),
    }
}
pub fn handle(r: Request, j: Job) -> Result<Outcome> {
    let mut t = load(&r.input, &j)?;
    let mut written = None;
    let preview = match r.operation.as_str() {
        "summary" => summary(&t, column(&t, &r.parameter)?, &j)?,
        "scatter" => {
            let (a, b) = r
                .parameter
                .split_once(',')
                .ok_or_else(|| Error::input("Expected x-column,y-column"))?;
            let (a, b) = (column(&t, a)?, column(&t, b)?);
            if r.output.extension().and_then(|x| x.to_str()) != Some("svg") {
                return Err(Error::input("Expected .svg"));
            }
            let mut points = vec![];
            for row in &t.rows {
                j.check()?;
                if row[a].trim().is_empty() || row[b].trim().is_empty() {
                    continue;
                }
                if points.len() >= 10000 {
                    return Err(Error::input("Scatter point limit"));
                }
                points.push((
                    row[a].trim().parse().map_err(err)?,
                    row[b].trim().parse().map_err(err)?,
                ));
            }
            localwright_core::figure::export_scatter(&r.output, &points, 800, 600)?;
            written = Some(r.output);
            format!(
                "{} complete cases plotted; missing pairs excluded",
                points.len()
            )
        }
        "preview" | "filter" | "export" => {
            if r.operation == "filter" {
                let (name, value) = r
                    .parameter
                    .split_once('=')
                    .ok_or_else(|| Error::input("Expected column=value"))?;
                let i = column(&t, name)?;
                t.rows.retain(|row| row[i] == value);
            }
            if r.operation == "export"
                || (r.operation == "filter" && !r.output.as_os_str().is_empty())
            {
                j.check()?;
                export(&t, &r.output, &j)?;
                written = Some(r.output);
            }
            let s = serde_json::to_string_pretty(
                &serde_json::json!({"rows":t.rows.len(),"headers":t.headers,"preview":t.rows.iter().take(30).collect::<Vec<_>>() }),
            )?;
            s.chars().take(24000).collect()
        }
        _ => return Err(Error::input("Unknown operation")),
    };
    Ok(Outcome{preview,written,warnings:vec!["Text-only import: XLSX formatting, formulas, types and additional sheets are not preserved. CSV export prefixes formula-like cells; Parquet uses strings.".into()],..Outcome::default()})
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn filter_and_summary() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("input.csv");
        std::fs::write(&p, "group,x\na,1\nb,2\na,3\na,\n").unwrap();
        let t = load(&p, &Job::new()).unwrap();
        let v: serde_json::Value =
            serde_json::from_str(&summary(&t, 1, &Job::new()).unwrap()).unwrap();
        assert_eq!(v["mean"], 2.0);
        assert_eq!(v["sample_sd"], 1.0);
        assert_eq!(v["missing"], 1);
        let q = d.path().join("out.csv");
        handle(
            Request {
                operation: "filter".into(),
                input: p,
                output: q.clone(),
                parameter: "group=a".into(),
                ..Request::default()
            },
            Job::new(),
        )
        .unwrap();
        assert_eq!(load(&q, &Job::new()).unwrap().rows.len(), 3);
        let pq = d.path().join("out.parquet");
        export(&t, &pq, &Job::new()).unwrap();
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(pq).unwrap())
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 4);
    }
    #[test]
    fn formula_guard() {
        assert_eq!(safe(" =1+1"), "' =1+1");
    }
}
