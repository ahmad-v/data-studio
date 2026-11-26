#![forbid(unsafe_code)]
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use calamine::{Reader, Xlsx};
use localwright_core::*;
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::Compression,
};
use std::{io::Cursor, path::Path, sync::Arc};
pub const OPERATIONS:&[Operation]=&[
 Operation{id:"preview",label:"Preview",hint:"Preview CSV, TSV, XLSX or UTF-8 Parquet as text. For XLSX, enter a worksheet name or leave blank for the first sheet."},
 Operation{id:"filter",label:"Filter",hint:"Exact text match: parameter column=value. Optional .csv, .tsv or .parquet output."},
 Operation{id:"select-columns",label:"Select columns",hint:"Enter comma-separated column names in output order. Optional new CSV or Parquet output."},
 Operation{id:"sort",label:"Sort rows",hint:"Enter a column name. Stable ascending text order. Optional CSV, TSV or Parquet output."},
 Operation{id:"deduplicate",label:"Deduplicate rows",hint:"Keep the first occurrence of each exact row. Optional CSV, TSV or Parquet output."},
 Operation{id:"mask-column",label:"Mask a column",hint:"Enter the exact column name. Replace its nonempty cells with [MASKED]. Optional new CSV, TSV or Parquet output."},
 Operation{id:"counts",label:"Value counts",hint:"Enter a column name to count exact values, including blanks. Preview shows up to 30 groups."},
 Operation{id:"summary",label:"Summary",hint:"Parameter: column name. Finite numeric values only; blanks excluded, invalid values rejected."},
 Operation{id:"correlation",label:"Pearson correlation",hint:"Enter x-column,y-column. Blank pairs excluded; numeric values must be finite."},
 Operation{id:"linear-regression",label:"Simple linear regression",hint:"Enter predictor,response. Fits an intercept and slope to finite numeric complete pairs."},
 Operation{id:"line-chart",label:"Line chart SVG",hint:"Enter x-column,y-column. Complete numeric rows connect in table order. Choose a new .svg file."},
 Operation{id:"scatter",label:"Scatter SVG",hint:"Parameter: x-column,y-column. Numeric complete rows, new .svg output."},
 Operation{id:"export",label:"Export",hint:"Export to .csv, .tsv or .parquet as text. For XLSX, enter a worksheet name or leave blank for the first sheet."}];
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
fn load_parquet(bytes: Vec<u8>, job: &Job) -> Result<Table> {
    const MAX_ROW_GROUPS: usize = 256;
    const MAX_DECODED_BYTES: u64 = 32 * 1024 * 1024;

    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).map_err(err)?;
    let fields = builder.schema().fields();
    if fields.is_empty() || fields.len() > MAX_COLS {
        return Err(Error::input("Invalid Parquet column count"));
    }
    if fields
        .iter()
        .any(|field| field.data_type() != &DataType::Utf8)
    {
        return Err(Error::input(
            "Parquet import supports flat UTF-8 string columns only",
        ));
    }
    let headers = fields
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let max_rows = MAX_CELLS / headers.len();
    let metadata = builder.metadata();
    let expected_rows = usize::try_from(metadata.file_metadata().num_rows())
        .map_err(|_| Error::input("Invalid Parquet row count"))?;
    if expected_rows > max_rows || metadata.num_row_groups() > MAX_ROW_GROUPS {
        return Err(Error::new(
            ErrorKind::Limit,
            "Parquet cell or row group limit exceeded",
        ));
    }
    let mut declared_bytes = 0u64;
    for group in metadata.row_groups() {
        job.check()?;
        for column in group.columns() {
            if column.compression() != Compression::UNCOMPRESSED {
                return Err(Error::input(
                    "Parquet import currently supports uncompressed columns only",
                ));
            }
            declared_bytes = u64::try_from(column.uncompressed_size())
                .ok()
                .and_then(|size| declared_bytes.checked_add(size))
                .filter(|size| *size <= MAX_DECODED_BYTES)
                .ok_or_else(|| {
                    Error::new(ErrorKind::Limit, "Parquet declared byte limit exceeded")
                })?;
        }
    }

    let reader = builder.with_batch_size(1024).build().map_err(err)?;
    let mut rows = Vec::new();
    let mut decoded_bytes = 0u64;
    for batch in reader {
        job.check()?;
        let batch = batch.map_err(err)?;
        if rows.len().saturating_add(batch.num_rows()) > expected_rows {
            return Err(Error::input("Parquet data exceeds declared row count"));
        }
        let columns = batch
            .columns()
            .iter()
            .map(|array| {
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| Error::input("Unexpected Parquet column type"))
            })
            .collect::<Result<Vec<_>>>()?;
        for index in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(columns.len());
            for column in &columns {
                let value = if column.is_null(index) {
                    ""
                } else {
                    column.value(index)
                };
                decoded_bytes = decoded_bytes.saturating_add(value.len() as u64);
                if decoded_bytes > MAX_DECODED_BYTES {
                    return Err(Error::new(ErrorKind::Limit, "Parquet text limit exceeded"));
                }
                row.push(value.into());
            }
            rows.push(row);
        }
    }
    if rows.len() != expected_rows {
        return Err(Error::input("Parquet row count does not match metadata"));
    }
    validate(headers, rows)
}

fn load(p: &Path, sheet: Option<&str>, j: &Job) -> Result<Table> {
    let bytes = read_bounded(p, MAX_BYTES)?;
    j.check()?;
    match p
        .extension()
        .and_then(|s| s.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some(kind @ ("csv" | "tsv")) => {
            let delimiter = if kind == "tsv" { b'\t' } else { b',' };
            let mut r = csv::ReaderBuilder::new()
                .delimiter(delimiter)
                .from_reader(bytes.as_slice());
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
            let name = match sheet {
                Some(name) if names.iter().any(|item| item == name) => name,
                Some(name) => return Err(Error::input(format!("Worksheet not found: {name}"))),
                None => names.first().ok_or_else(|| Error::input("No worksheet"))?,
            };
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
        Some("parquet") => load_parquet(bytes, j),
        _ => Err(Error::input("Expected CSV, TSV, XLSX or Parquet")),
    }
}
fn column(t: &Table, name: &str) -> Result<usize> {
    t.headers
        .iter()
        .position(|h| h == name)
        .ok_or_else(|| Error::input("Column not found"))
}
fn select_columns(table: Table, parameter: &str, job: &Job) -> Result<Table> {
    let mut seen = std::collections::HashSet::new();
    let mut columns = Vec::new();
    for name in parameter.split(',') {
        let index = column(&table, name)?;
        if !seen.insert(index) {
            return Err(Error::input("A column was selected more than once"));
        }
        columns.push(index);
    }
    let headers = columns
        .iter()
        .map(|&index| table.headers[index].clone())
        .collect();
    let rows = table
        .rows
        .into_iter()
        .map(|row| {
            job.check()?;
            Ok(columns.iter().map(|&index| row[index].clone()).collect())
        })
        .collect::<Result<Vec<_>>>()?;
    validate(headers, rows)
}

fn deduplicate(rows: Vec<Vec<String>>, job: &Job) -> Result<Vec<Vec<String>>> {
    let mut seen = std::collections::HashSet::new();
    let mut keep = Vec::with_capacity(rows.len());
    for row in &rows {
        job.check()?;
        keep.push(seen.insert(row));
    }
    drop(seen);
    Ok(rows
        .into_iter()
        .zip(keep)
        .filter_map(|(row, keep)| keep.then_some(row))
        .collect())
}

fn value_counts(table: &Table, index: usize, job: &Job) -> Result<String> {
    let mut counts = std::collections::BTreeMap::<&str, usize>::new();
    for row in &table.rows {
        job.check()?;
        *counts.entry(&row[index]).or_default() += 1;
    }
    let groups = counts.len();
    let mut ranked = counts.into_iter().collect::<Vec<_>>();
    ranked.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    let preview = ranked
        .into_iter()
        .take(30)
        .map(|(value, count)| serde_json::json!({"value":value,"count":count}))
        .collect::<Vec<_>>();
    Ok(serde_json::to_string_pretty(&serde_json::json!({
        "rows": table.rows.len(),
        "distinct_values": groups,
        "counts": preview,
    }))?)
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
fn column_pair(table: &Table, parameter: &str) -> Result<(usize, usize)> {
    let (x, y) = parameter
        .split_once(',')
        .ok_or_else(|| Error::input("Expected x-column,y-column"))?;
    Ok((column(table, x)?, column(table, y)?))
}

struct PairMoments {
    n: usize,
    mean_x: f64,
    mean_y: f64,
    sum_xx: f64,
    sum_yy: f64,
    sum_xy: f64,
}

fn pair_moments(table: &Table, x: usize, y: usize, job: &Job) -> Result<PairMoments> {
    let mut n = 0;
    let (mut mean_x, mut mean_y) = (0.0, 0.0);
    let (mut sum_xx, mut sum_yy, mut sum_xy) = (0.0, 0.0, 0.0);
    for row in &table.rows {
        job.check()?;
        let (a, b) = (row[x].trim(), row[y].trim());
        if a.is_empty() || b.is_empty() {
            continue;
        }
        let (a, b): (f64, f64) = (a.parse().map_err(err)?, b.parse().map_err(err)?);
        if !a.is_finite() || !b.is_finite() {
            return Err(Error::input("Nonfinite numeric value in a complete pair"));
        }
        n += 1;
        let (dx, dy) = (a - mean_x, b - mean_y);
        mean_x += dx / n as f64;
        mean_y += dy / n as f64;
        sum_xx += dx * (a - mean_x);
        sum_yy += dy * (b - mean_y);
        sum_xy += dx * (b - mean_y);
        if !sum_xx.is_finite() || !sum_yy.is_finite() || !sum_xy.is_finite() {
            return Err(Error::input("Numerical overflow"));
        }
    }
    Ok(PairMoments {
        n,
        mean_x,
        mean_y,
        sum_xx,
        sum_yy,
        sum_xy,
    })
}

impl PairMoments {
    fn pearson_r(&self) -> Result<f64> {
        if self.n < 2 || self.sum_xx <= 0.0 || self.sum_yy <= 0.0 {
            return Err(Error::input(
                "Correlation requires at least two complete pairs and nonzero variance in both columns",
            ));
        }
        // Divide by the larger norm first to avoid an intermediate overflow.
        let (norm_x, norm_y) = (self.sum_xx.sqrt(), self.sum_yy.sqrt());
        Ok((self.sum_xy / norm_x.max(norm_y) / norm_x.min(norm_y)).clamp(-1.0, 1.0))
    }
}

fn correlation(table: &Table, x: usize, y: usize, job: &Job) -> Result<String> {
    let moments = pair_moments(table, x, y, job)?;
    Ok(serde_json::to_string_pretty(&serde_json::json!({
        "x": table.headers[x],
        "y": table.headers[y],
        "n": moments.n,
        "excluded_blank_pairs": table.rows.len() - moments.n,
        "r": moments.pearson_r()?,
        "method": "Pearson product-moment correlation, complete pairs",
    }))?)
}

fn linear_regression(table: &Table, x: usize, y: usize, job: &Job) -> Result<String> {
    let moments = pair_moments(table, x, y, job)?;
    if moments.n < 2 || moments.sum_xx <= 0.0 {
        return Err(Error::input(
            "Regression requires at least two complete pairs and nonzero predictor variance",
        ));
    }
    let slope = moments.sum_xy / moments.sum_xx;
    let intercept = (-slope).mul_add(moments.mean_x, moments.mean_y);
    if !slope.is_finite() || !intercept.is_finite() {
        return Err(Error::input(
            "Numerical overflow in regression coefficients",
        ));
    }
    let r_squared = if moments.sum_yy > 0.0 {
        Some(moments.pearson_r()?.powi(2))
    } else {
        None
    };
    Ok(serde_json::to_string_pretty(&serde_json::json!({
        "predictor": table.headers[x],
        "response": table.headers[y],
        "n": moments.n,
        "excluded_blank_pairs": table.rows.len() - moments.n,
        "intercept": intercept,
        "slope": slope,
        "r_squared": r_squared,
        "method": "Ordinary least squares with intercept, complete pairs",
    }))?)
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
        Some(kind @ ("csv" | "tsv")) => write_new(p, |f| {
            let delimiter = if kind == "tsv" { b'\t' } else { b',' };
            let mut w = csv::WriterBuilder::new()
                .delimiter(delimiter)
                .from_writer(f);
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
        _ => Err(Error::input("Output must be .csv, .tsv or .parquet")),
    }
}
pub fn handle(r: Request, j: Job) -> Result<Outcome> {
    let sheet = (matches!(r.operation.as_str(), "preview" | "export") && !r.parameter.is_empty())
        .then_some(r.parameter.as_str());
    let mut t = load(&r.input, sheet, &j)?;
    let mut written = None;
    let mut warnings = vec!["Text-only import: XLSX formatting, formulas, types and additional sheets are not preserved. Delimited-text export prefixes formula-like cells; Parquet import accepts flat uncompressed UTF-8 columns and treats nulls as blank text.".into()];
    let preview = match r.operation.as_str() {
        "counts" => value_counts(&t, column(&t, &r.parameter)?, &j)?,
        "summary" => summary(&t, column(&t, &r.parameter)?, &j)?,
        "correlation" => {
            let (x, y) = column_pair(&t, &r.parameter)?;
            correlation(&t, x, y, &j)?
        }
        "linear-regression" => {
            let (x, y) = column_pair(&t, &r.parameter)?;
            linear_regression(&t, x, y, &j)?
        }
        "scatter" | "line-chart" => {
            let (a, b) = column_pair(&t, &r.parameter)?;
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
                    return Err(Error::input("Figure point limit"));
                }
                points.push((
                    row[a].trim().parse().map_err(err)?,
                    row[b].trim().parse().map_err(err)?,
                ));
            }
            if r.operation == "line-chart" {
                localwright_core::figure::export_line(&r.output, &points, 800, 600)?;
            } else {
                localwright_core::figure::export_scatter(&r.output, &points, 800, 600)?;
            }
            written = Some(r.output);
            format!(
                "{} complete cases plotted; missing pairs excluded",
                points.len()
            )
        }
        "preview" | "filter" | "export" | "select-columns" | "sort" | "deduplicate"
        | "mask-column" => {
            match r.operation.as_str() {
                "select-columns" => t = select_columns(t, &r.parameter, &j)?,
                "filter" => {
                    let (name, value) = r
                        .parameter
                        .split_once('=')
                        .ok_or_else(|| Error::input("Expected column=value"))?;
                    let index = column(&t, name)?;
                    t.rows.retain(|row| row[index] == value);
                }
                "sort" => {
                    let index = column(&t, &r.parameter)?;
                    j.check()?;
                    t.rows.sort_by(|a, b| a[index].cmp(&b[index]));
                    j.check()?;
                }
                "mask-column" => {
                    let index = column(&t, &r.parameter)?;
                    for row in &mut t.rows {
                        j.check()?;
                        if !row[index].is_empty() {
                            row[index] = "[MASKED]".into();
                        }
                    }
                    warnings.push("Only the selected column's nonempty text cells were masked. Headers, blank patterns and other columns can still identify people or reveal sensitive information.".into());
                }
                "deduplicate" => t.rows = deduplicate(t.rows, &j)?,
                _ => {}
            }
            if r.operation == "export"
                || (r.operation != "preview" && !r.output.as_os_str().is_empty())
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
    Ok(Outcome {
        preview,
        written,
        warnings,
        ..Outcome::default()
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn filter_and_summary() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("input.csv");
        std::fs::write(&p, "group,x\na,1\nb,2\na,3\na,\n").unwrap();
        let t = load(&p, None, &Job::new()).unwrap();
        let v: serde_json::Value =
            serde_json::from_str(&summary(&t, 1, &Job::new()).unwrap()).unwrap();
        assert_eq!(v["mean"], 2.0);
        assert_eq!(v["sample_sd"], 1.0);
        assert_eq!(v["missing"], 1);
        let counts: serde_json::Value =
            serde_json::from_str(&value_counts(&t, 0, &Job::new()).unwrap()).unwrap();
        assert_eq!(counts["distinct_values"], 2);
        assert_eq!(
            counts["counts"][0],
            serde_json::json!({"value":"a","count":3})
        );
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
        assert_eq!(load(&q, None, &Job::new()).unwrap().rows.len(), 3);
        let pq = d.path().join("out.parquet");
        export(&t, &pq, &Job::new()).unwrap();
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let mut reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&pq).unwrap())
                .unwrap()
                .build()
                .unwrap();
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 4);
        let imported = load(&pq, None, &Job::new()).unwrap();
        assert_eq!(imported.headers, t.headers);
        assert_eq!(imported.rows, t.rows);
    }
    #[test]
    fn selects_columns_in_requested_order() {
        let table = validate(
            vec!["one".into(), "two".into()],
            vec![vec!["x".into(), "y".into()]],
        )
        .unwrap();
        let reordered = select_columns(table, "two,one", &Job::new()).unwrap();
        assert_eq!(reordered.headers, ["two", "one"]);
        assert_eq!(reordered.rows[0], ["y", "x"]);
        assert!(select_columns(reordered, "two,two", &Job::new()).is_err());
    }

    #[test]
    fn masks_only_nonempty_values_of_the_selected_column() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("input.csv");
        let output = dir.path().join("masked.csv");
        let original = "name,category\nAda,A\n,B\n ,C\n";
        std::fs::write(&input, original).unwrap();
        let request = Request {
            operation: "mask-column".into(),
            input: input.clone(),
            output: output.clone(),
            parameter: "name".into(),
            ..Request::default()
        };
        let result = handle(request.clone(), Job::new()).unwrap();
        assert!(result
            .warnings
            .iter()
            .any(|warning| warning.contains("blank patterns")));
        assert!(!result.preview.contains("Ada"));
        assert_eq!(
            std::fs::read_to_string(&output).unwrap(),
            "name,category\n[MASKED],A\n,B\n[MASKED],C\n"
        );
        assert_eq!(std::fs::read_to_string(&input).unwrap(), original);
        assert!(handle(request, Job::new()).is_err());
    }

    #[test]
    fn writes_a_vector_line_chart_in_table_order() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("series.csv");
        let output = dir.path().join("series.svg");
        std::fs::write(&input, "x,y\n3,0\n1,5\n2,2.5\n4,\n").unwrap();
        let result = handle(
            Request {
                operation: "line-chart".into(),
                input,
                output: output.clone(),
                parameter: "x,y".into(),
                ..Request::default()
            },
            Job::new(),
        )
        .unwrap();
        assert!(result.preview.contains("3 complete cases"));
        let chart = std::fs::read_to_string(output).unwrap();
        assert!(chart.contains(r#"points=" 780.000,550.000 60.000,20.000 420.000,285.000""#));
        assert!(!chart.contains("<image"));
    }

    #[test]
    fn correlation_uses_only_complete_pairs() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("pairs.csv");
        std::fs::write(&input, "x,y,constant\n1,6,1\n2,4,1\n3,2,1\n4,,1\n").unwrap();
        let request = |parameter: &str| Request {
            operation: "correlation".into(),
            input: input.clone(),
            parameter: parameter.into(),
            ..Request::default()
        };
        let outcome = handle(request("x,y"), Job::new()).unwrap();
        let report: serde_json::Value = serde_json::from_str(&outcome.preview).unwrap();
        assert_eq!(report["n"], 3);
        assert_eq!(report["excluded_blank_pairs"], 1);
        assert!((report["r"].as_f64().unwrap() + 1.0).abs() < 1e-12);
        assert!(handle(request("x,constant"), Job::new()).is_err());
    }

    #[test]
    fn fits_ordinary_least_squares_and_handles_constant_columns() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("fit.csv");
        std::fs::write(&input, "x,y,constant\n1,2,4\n2,4,4\n3,7,4\n4,,4\n").unwrap();
        let fit = |parameter: &str| {
            handle(
                Request {
                    operation: "linear-regression".into(),
                    input: input.clone(),
                    parameter: parameter.into(),
                    ..Request::default()
                },
                Job::new(),
            )
        };
        let report: serde_json::Value = serde_json::from_str(&fit("x,y").unwrap().preview).unwrap();
        assert_eq!(report["n"], 3);
        assert_eq!(report["excluded_blank_pairs"], 1);
        assert!((report["intercept"].as_f64().unwrap() + 2.0 / 3.0).abs() < 1e-12);
        assert!((report["slope"].as_f64().unwrap() - 2.5).abs() < 1e-12);
        assert!((report["r_squared"].as_f64().unwrap() - 75.0 / 76.0).abs() < 1e-12);
        let constant: serde_json::Value =
            serde_json::from_str(&fit("x,constant").unwrap().preview).unwrap();
        assert_eq!(constant["slope"], 0.0);
        assert_eq!(constant["intercept"], 4.0);
        assert!(constant["r_squared"].is_null());
        assert!(fit("constant,y").is_err());
    }

    #[test]
    fn sort_is_stable_and_deduplication_keeps_first_row() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("rows.csv");
        std::fs::write(&input, "key,name\nb,B\na,First\na,Second\na,First\n").unwrap();
        for (operation, parameter, expected) in [
            ("sort", "key", "key,name\na,First\na,Second\na,First\nb,B\n"),
            ("deduplicate", "", "key,name\nb,B\na,First\na,Second\n"),
        ] {
            let output = dir.path().join(format!("{operation}.csv"));
            let request = Request {
                operation: operation.into(),
                input: input.clone(),
                output: output.clone(),
                parameter: parameter.into(),
                ..Request::default()
            };
            handle(request.clone(), Job::new()).unwrap();
            assert_eq!(std::fs::read_to_string(&output).unwrap(), expected);
            assert!(handle(request, Job::new()).is_err());
        }
    }

    #[test]
    fn tsv_preserves_quoted_tabs_and_unicode() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("source.tsv");
        let output = dir.path().join("copy.tsv");
        std::fs::write(&input, "name\tvalue\nCafé\t\"a\tb\"\n").unwrap();
        let table = load(&input, None, &Job::new()).unwrap();
        assert_eq!(table.headers, ["name", "value"]);
        assert_eq!(table.rows[0], ["Café", "a\tb"]);
        export(&table, &output, &Job::new()).unwrap();
        let imported = load(&output, None, &Job::new()).unwrap();
        assert_eq!(imported.headers, table.headers);
        assert_eq!(imported.rows, table.rows);
    }

    #[test]
    fn formula_guard() {
        assert_eq!(safe(" =1+1"), "' =1+1");
    }
}
