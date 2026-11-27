#![forbid(unsafe_code)]
mod join;
mod project;
mod sql;
mod statistics;
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use calamine::{Reader, Xlsx};
use localwright_core::*;
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::Compression,
};
use project::{Project, Step};
use std::{io::Cursor, path::Path, sync::Arc};
pub const OPERATIONS:&[Operation]=&[
 Operation{id:"preview",label:"Preview",hint:"Preview CSV, TSV, XLSX or UTF-8 Parquet as text. For XLSX, enter a worksheet name or leave blank for the first sheet."},
 Operation{id:"filter",label:"Filter",hint:"Exact text match: parameter column=value. Optional .csv, .tsv or .parquet output."},
 Operation{id:"select-columns",label:"Select columns",hint:"Enter comma-separated column names in output order. Optional new CSV or Parquet output."},
 Operation{id:"sort",label:"Sort rows",hint:"Enter a column name. Stable ascending text order. Optional CSV, TSV or Parquet output."},
 Operation{id:"deduplicate",label:"Deduplicate rows",hint:"Keep the first occurrence of each exact row. Optional CSV, TSV or Parquet output."},
 Operation{id:"mask-column",label:"Mask a column",hint:"Enter the exact column name. Replace its nonempty cells with [MASKED]. Optional new CSV, TSV or Parquet output."},
 Operation{id:"inner-join",label:"Inner join tables",hint:r#"Enter JSON with right_path, left_key and right_key. Exact nonblank text keys. Optional new CSV, TSV or Parquet output."#},
 Operation{id:"left-join",label:"Left join tables",hint:r#"Enter JSON with right_path, left_key and right_key. Preserves unmatched left rows; nonmatching right columns are blank. Exact nonblank text keys."#},
 Operation{id:"right-join",label:"Right join tables",hint:r#"Enter JSON with right_path, left_key and right_key. Preserves unmatched right rows; nonmatching left columns are blank. Exact nonblank text keys."#},
 Operation{id:"full-join",label:"Full outer join tables",hint:r#"Enter JSON with right_path, left_key and right_key. Preserves unmatched rows from both tables; missing-side columns are blank."#},
 Operation{id:"drop-missing",label:"Drop rows with missing values",hint:"Enter a column name. Removes rows where that column is blank after trimming whitespace."},
 Operation{id:"fill-missing",label:"Fill missing values",hint:"Enter column=value. Replaces blank or whitespace-only cells in the selected column."},
 Operation{id:"rename-column",label:"Rename a column",hint:"Enter old=new. The new name must be unique and nonblank."},
 Operation{id:"run-project",label:"Run data project",hint:"Enter the project JSON path. Preview a local table or write it to a new CSV, TSV or Parquet file."},
 Operation{id:"counts",label:"Value counts",hint:"Enter a column name to count exact values, including blanks. Preview shows up to 30 groups."},
 Operation{id:"select-sql",label:"Run a local SQL query",hint:"Enter one SQLite SELECT over the input table. Use explicit numeric casts for imported text."},
 Operation{id:"group-summary",label:"Grouped numeric summary",hint:"Enter group-column,value-column. Computes counts, mean, sample SD and range for each exact group."},
 Operation{id:"summary",label:"Summary",hint:"Parameter: column name. Finite numeric values only; blanks excluded, invalid values rejected."},
 Operation{id:"quantiles",label:"Quantiles and IQR",hint:"Parameter: numeric column. Reports R7 quartiles, median and interquartile range as JSON."},
 Operation{id:"standard-error",label:"Standard error",hint:"Parameter: numeric column. Reports sample SD and standard error of the mean."},
 Operation{id:"mean-ci",label:"Mean confidence interval",hint:"Parameter: numeric column. Reports a two-sided 95% Student-t interval for the mean."},
 Operation{id:"bootstrap-mean-ci",label:"Bootstrap mean interval",hint:"Parameter: numeric column. Deterministic 95% percentile bootstrap; at least 2 and at most 5,000 values."},
 Operation{id:"bootstrap-median-ci",label:"Bootstrap median interval",hint:"Parameter: numeric column. Deterministic 95% percentile bootstrap; at least 2 and at most 5,000 values."},
 Operation{id:"welch-t-test",label:"Welch two-sample test",hint:"Enter group-column,value-column. Requires exactly two nonblank groups with at least two finite values each."},
 Operation{id:"paired-t-test",label:"Paired t test",hint:"Enter first-column,second-column. Blank pairs excluded; tests mean paired difference with a two-sided Student-t interval."},
 Operation{id:"one-way-anova",label:"One-way ANOVA",hint:"Enter group-column,value-column. Blank group/value rows excluded; at least two groups and positive within-group degrees of freedom required."},
 Operation{id:"spearman",label:"Spearman rank correlation",hint:"Enter x-column,y-column. Complete numeric pairs only; average ranks for ties and approximate p value."},
 Operation{id:"correlation-interval",label:"Pearson correlation interval",hint:"Enter x-column,y-column. Reports a two-sided 95% Fisher-z interval for n > 3 and |r| < 1."},
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
fn select_columns<'a>(
    table: Table,
    names: impl IntoIterator<Item = &'a str>,
    job: &Job,
) -> Result<Table> {
    let mut seen = std::collections::HashSet::new();
    let mut columns = Vec::new();
    for name in names {
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

const MASK_WARNING: &str = "Only selected columns' nonempty text cells were masked. Headers, blank patterns and other columns can still identify people or reveal sensitive information.";

fn apply_step(mut table: Table, step: Step, job: &Job) -> Result<Table> {
    match step {
        Step::SelectColumns { columns } => {
            return select_columns(table, columns.iter().map(String::as_str), job)
        }
        Step::Filter {
            column: name,
            value,
        } => {
            let index = column(&table, &name)?;
            let mut kept = Vec::new();
            for row in table.rows {
                job.check()?;
                if row[index] == value {
                    kept.push(row);
                }
            }
            table.rows = kept;
        }
        Step::Sort { column: name } => {
            let index = column(&table, &name)?;
            job.check()?;
            table.rows.sort_by(|a, b| a[index].cmp(&b[index]));
            job.check()?;
        }
        Step::Deduplicate {} => table.rows = deduplicate(table.rows, job)?,
        Step::DropMissing { column: name } => {
            let index = column(&table, &name)?;
            let mut kept = Vec::new();
            for row in table.rows {
                job.check()?;
                if !row[index].trim().is_empty() {
                    kept.push(row);
                }
            }
            table.rows = kept;
        }
        Step::FillMissing {
            column: name,
            value,
        } => {
            let index = column(&table, &name)?;
            if value.trim().is_empty() {
                return Err(Error::input(
                    "Replacement value must contain non-whitespace text",
                ));
            }
            let current_bytes = table.rows.iter().flatten().map(String::len).sum::<usize>();
            let mut total_bytes = current_bytes;
            for row in &mut table.rows {
                job.check()?;
                if row[index].trim().is_empty() {
                    total_bytes = total_bytes - row[index].len() + value.len();
                    if total_bytes > 32 * 1024 * 1024 {
                        return Err(Error::new(
                            ErrorKind::Limit,
                            "Filled table exceeds 32 MiB of cell text",
                        ));
                    }
                    row[index].clone_from(&value);
                }
            }
        }
        Step::RenameColumn { column: old, name } => {
            let index = column(&table, &old)?;
            if name.trim().is_empty() {
                return Err(Error::input("Column name must contain non-whitespace text"));
            }
            if table
                .headers
                .iter()
                .enumerate()
                .any(|(position, existing)| position != index && *existing == name)
            {
                return Err(Error::input("A column with this name already exists"));
            }
            table.headers[index] = name;
        }
        Step::MaskColumn { column: name } => {
            let index = column(&table, &name)?;
            for row in &mut table.rows {
                job.check()?;
                if !row[index].is_empty() {
                    row[index] = "[MASKED]".into();
                }
            }
        }
    }
    Ok(table)
}

fn request_step(operation: &str, parameter: &str) -> Result<Option<Step>> {
    let step = match operation {
        "select-columns" => Step::SelectColumns {
            columns: parameter.split(',').map(str::to_owned).collect(),
        },
        "filter" => {
            let (name, value) = parameter
                .split_once('=')
                .ok_or_else(|| Error::input("Expected column=value"))?;
            Step::Filter {
                column: name.into(),
                value: value.into(),
            }
        }
        "sort" => Step::Sort {
            column: parameter.into(),
        },
        "deduplicate" => Step::Deduplicate {},
        "drop-missing" => Step::DropMissing {
            column: parameter.into(),
        },
        "fill-missing" | "rename-column" => {
            let (column, value) = parameter
                .split_once('=')
                .ok_or_else(|| Error::input("Expected column=value"))?;
            if operation == "fill-missing" {
                Step::FillMissing {
                    column: column.into(),
                    value: value.into(),
                }
            } else {
                Step::RenameColumn {
                    column: column.into(),
                    name: value.into(),
                }
            }
        }
        "mask-column" => Step::MaskColumn {
            column: parameter.into(),
        },
        _ => return Ok(None),
    };
    Ok(Some(step))
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

struct NumericSummary {
    n: u64,
    missing: u64,
    mean: f64,
    m2: f64,
    min: f64,
    max: f64,
}

impl Default for NumericSummary {
    fn default() -> Self {
        Self {
            n: 0,
            missing: 0,
            mean: 0.0,
            m2: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }
}

impl NumericSummary {
    fn add(&mut self, value: &str) -> Result<()> {
        let value = value.trim();
        if value.is_empty() {
            self.missing += 1;
            return Ok(());
        }
        let x: f64 = value.parse().map_err(err)?;
        if !x.is_finite() {
            return Err(Error::input("Nonfinite numeric value"));
        }
        self.n += 1;
        let delta = x - self.mean;
        self.mean += delta / self.n as f64;
        self.m2 += delta * (x - self.mean);
        self.min = self.min.min(x);
        self.max = self.max.max(x);
        if !self.mean.is_finite() || !self.m2.is_finite() {
            return Err(Error::input("Numerical overflow"));
        }
        Ok(())
    }

    fn sample_sd(&self) -> Option<f64> {
        (self.n > 1).then(|| (self.m2 / (self.n - 1) as f64).max(0.0).sqrt())
    }
}

fn summary(table: &Table, index: usize, job: &Job) -> Result<String> {
    let mut stats = NumericSummary::default();
    for row in &table.rows {
        job.check()?;
        stats.add(&row[index])?;
    }
    if stats.n == 0 {
        return Err(Error::input("No finite numeric observations"));
    }
    Ok(serde_json::to_string_pretty(&serde_json::json!({
        "n":stats.n, "missing":stats.missing, "mean":stats.mean, "sample_sd":stats.sample_sd(),
        "min":stats.min, "max":stats.max, "method":"Welford, sample denominator n-1",
    }))?)
}

fn grouped_summary(table: &Table, group: usize, value: usize, job: &Job) -> Result<Table> {
    use std::collections::{btree_map::Entry, BTreeMap};
    let headers = ["group", "n", "missing", "mean", "sample_sd", "min", "max"];
    let mut budget = MAX_CELLS / headers.len();
    let mut groups = BTreeMap::<&str, NumericSummary>::new();
    for row in &table.rows {
        job.check()?;
        let stats = match groups.entry(row[group].as_str()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                budget = budget
                    .checked_sub(1)
                    .ok_or_else(|| Error::input("Grouped summary exceeds the cell limit"))?;
                entry.insert(NumericSummary::default())
            }
        };
        stats.add(&row[value])?;
    }
    let mut rows = Vec::with_capacity(groups.len());
    for (key, stats) in groups {
        job.check()?;
        let display = |value: Option<f64>| value.map(|value| value.to_string()).unwrap_or_default();
        let observed = stats.n != 0;
        rows.push(vec![
            key.into(),
            stats.n.to_string(),
            stats.missing.to_string(),
            display(observed.then_some(stats.mean)),
            display(stats.sample_sd()),
            display(observed.then_some(stats.min)),
            display(observed.then_some(stats.max)),
        ]);
    }
    validate(headers.into_iter().map(str::to_owned).collect(), rows)
}

fn column_pair(table: &Table, parameter: &str) -> Result<(usize, usize)> {
    let (x, y) = parameter
        .split_once(',')
        .ok_or_else(|| Error::input("Expected x-column,y-column"))?;
    Ok((column(table, x)?, column(table, y)?))
}

fn numeric_value(value: &str) -> Result<Option<f64>> {
    if value.trim().is_empty() {
        return Ok(None);
    }
    let number: f64 = value.trim().parse().map_err(err)?;
    if !number.is_finite() {
        return Err(Error::input("Nonfinite numeric value"));
    }
    Ok(Some(number))
}

fn numeric_values(table: &Table, index: usize, job: &Job) -> Result<Vec<f64>> {
    let mut values = Vec::new();
    for row in &table.rows {
        job.check()?;
        if let Some(value) = numeric_value(&row[index])? {
            values.push(value);
        }
    }
    Ok(values)
}

fn complete_numeric_pairs(
    table: &Table,
    first: usize,
    second: usize,
    job: &Job,
) -> Result<(Vec<f64>, Vec<f64>)> {
    let mut a = Vec::new();
    let mut b = Vec::new();
    for row in &table.rows {
        job.check()?;
        let (Some(left), Some(right)) = (numeric_value(&row[first])?, numeric_value(&row[second])?)
        else {
            continue;
        };
        a.push(left);
        b.push(right);
    }
    Ok((a, b))
}

fn grouped_numeric_values(
    table: &Table,
    group_index: usize,
    value_index: usize,
    job: &Job,
) -> Result<Vec<statistics::GroupSample>> {
    let mut groups = std::collections::BTreeMap::<String, Vec<f64>>::new();
    for row in &table.rows {
        job.check()?;
        let label = &row[group_index];
        if label.trim().is_empty() {
            continue;
        }
        if let Some(value) = numeric_value(&row[value_index])? {
            groups.entry(label.clone()).or_default().push(value);
        }
    }
    Ok(groups
        .into_iter()
        .map(|(label, values)| statistics::GroupSample { label, values })
        .collect())
}

fn statistical_report(
    operation: &str,
    table: &Table,
    parameter: &str,
    job: &Job,
) -> Result<serde_json::Value> {
    match operation {
        "quantiles"
        | "standard-error"
        | "mean-ci"
        | "bootstrap-mean-ci"
        | "bootstrap-median-ci" => {
            let values = numeric_values(table, column(table, parameter)?, job)?;
            match operation {
                "quantiles" => statistics::quantiles(&values),
                "standard-error" => statistics::standard_error(&values),
                "mean-ci" => statistics::mean_confidence_interval(&values),
                "bootstrap-mean-ci" => statistics::bootstrap_mean_interval(&values, job),
                _ => statistics::bootstrap_median_interval(&values, job),
            }
        }
        "welch-t-test" | "one-way-anova" => {
            let (group_name, value_name) = parameter
                .split_once(',')
                .ok_or_else(|| Error::input("Expected group-column,value-column"))?;
            let groups = grouped_numeric_values(
                table,
                column(table, group_name)?,
                column(table, value_name)?,
                job,
            )?;
            if operation == "welch-t-test" {
                if groups.len() != 2 {
                    return Err(Error::input(
                        "Welch test requires exactly two nonblank groups",
                    ));
                }
                statistics::welch_t_test(&groups[0].values, &groups[1].values)
            } else {
                statistics::one_way_anova(&groups)
            }
        }
        "paired-t-test" | "spearman" => {
            let (first, second) = column_pair(table, parameter)?;
            let (a, b) = complete_numeric_pairs(table, first, second, job)?;
            if operation == "paired-t-test" {
                statistics::paired_t_test(&a, &b)
            } else {
                statistics::spearman(&a, &b)
            }
        }
        "correlation-interval" => {
            let (first, second) = column_pair(table, parameter)?;
            let moments = pair_moments(table, first, second, job)?;
            statistics::pearson_interval(moments.pearson_r()?, moments.n)
        }
        _ => Err(Error::input("Unknown statistical route")),
    }
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
    let project = if r.operation == "run-project" {
        Some(Project::open(Path::new(&r.parameter))?)
    } else {
        None
    };
    let sheet = match &project {
        Some(project) => project.worksheet.as_deref(),
        None => (matches!(r.operation.as_str(), "preview" | "export") && !r.parameter.is_empty())
            .then_some(r.parameter.as_str()),
    };
    if project.is_some()
        && sheet.is_some()
        && !r
            .input
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("xlsx"))
    {
        return Err(Error::input(
            "Project worksheet selection requires XLSX input",
        ));
    }
    let mut t = load(&r.input, sheet, &j)?;
    let mut written = None;
    let mut warnings = vec!["Text-only import: XLSX formatting, formulas, types and additional sheets are not preserved. Delimited-text export prefixes formula-like cells; Parquet import accepts flat uncompressed UTF-8 columns and treats nulls as blank text.".into()];
    let preview = match r.operation.as_str() {
        "counts" => value_counts(&t, column(&t, &r.parameter)?, &j)?,
        "summary" => summary(&t, column(&t, &r.parameter)?, &j)?,
        "quantiles"
        | "standard-error"
        | "mean-ci"
        | "bootstrap-mean-ci"
        | "bootstrap-median-ci"
        | "welch-t-test"
        | "paired-t-test"
        | "one-way-anova"
        | "spearman"
        | "correlation-interval" => {
            let report = statistical_report(&r.operation, &t, &r.parameter, &j)?;
            let text = serde_json::to_string_pretty(&report)?;
            if !r.output.as_os_str().is_empty() {
                if r.output
                    .extension()
                    .and_then(|extension| extension.to_str())
                    != Some("json")
                {
                    return Err(Error::input("Statistical reports require a .json output"));
                }
                j.check()?;
                write_bytes(&r.output, text.as_bytes())?;
                written = Some(r.output);
            }
            text
        }
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
        | "mask-column" | "run-project" | "inner-join" | "left-join" | "right-join"
        | "full-join" | "group-summary" | "select-sql" | "drop-missing" | "fill-missing"
        | "rename-column" => {
            if let Some(project) = project {
                let (result, steps, masked) = project.run(t, &j)?;
                t = result;
                warnings.push(format!("Applied {steps} declarative project steps."));
                if masked {
                    warnings.push(MASK_WARNING.into());
                }
            } else if r.operation == "select-sql" {
                t = sql::query(t, &r.parameter, &j)?;
                warnings.push("The local SQLite input table contains text. Use explicit casts for numeric work. NULL results become blank cells.".into());
            } else if r.operation == "group-summary" {
                let (group, value) = column_pair(&t, &r.parameter)?;
                t = grouped_summary(&t, group, value, &j)?;
                warnings.push("Group labels are exact text, including blanks. Blank numeric cells are excluded, undefined statistics are blank, and sample SD uses n-1.".into());
            } else if matches!(
                r.operation.as_str(),
                "inner-join" | "left-join" | "right-join" | "full-join"
            ) {
                let (kind, warning) = match r.operation.as_str() {
                    "left-join" => (join::Kind::Left, "Left join uses exact text keys. Blank keys do not match; unmatched left rows are preserved with blank right columns. Duplicate matches produce every matching pair."),
                    "right-join" => (join::Kind::Right, "Right join uses exact text keys. Blank keys do not match; unmatched right rows are preserved with blank left columns. Results follow right input order, with duplicate left matches in left input order."),
                    "full-join" => (join::Kind::Full, "Full outer join uses exact text keys. Blank keys do not match; unmatched rows from both tables are preserved with blank columns on the missing side."),
                    _ => (join::Kind::Inner, "Inner join uses exact text keys. Blank and unmatched keys are excluded. Matching duplicate keys produce every matching pair."),
                };
                t = join::run(t, &r.parameter, kind, &j)?;
                warnings.push(warning.into());
            } else if let Some(step) = request_step(&r.operation, &r.parameter)? {
                if matches!(step, Step::MaskColumn { .. }) {
                    warnings.push(MASK_WARNING.into());
                }
                t = apply_step(t, step, &j)?;
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
        let reordered = select_columns(table, ["two", "one"], &Job::new()).unwrap();
        assert_eq!(reordered.headers, ["two", "one"]);
        assert_eq!(reordered.rows[0], ["y", "x"]);
        assert!(select_columns(reordered, ["two", "two"], &Job::new()).is_err());
    }

    #[test]
    fn select_sql_queries_the_table_and_denies_other_capabilities() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("input.csv");
        let output = dir.path().join("out.csv");
        std::fs::write(&input, "group,value\nA,2\nA,3\nB,4\n").unwrap();
        let request = |statement: &str| Request {
            operation: "select-sql".into(),
            parameter: statement.into(),
            input: input.clone(),
            output: output.clone(),
            ..Request::default()
        };
        for invalid in [
            "ATTACH DATABASE 'other.db' AS other",
            "DELETE FROM input",
            "SELECT * FROM sqlite_schema",
            "PRAGMA table_info(input)",
            "SELECT load_extension('forbidden')",
            "SELECT count(*) FROM input; SELECT 2",
            "SELECT random()",
            "SELECT X'00' AS binary",
        ] {
            assert!(handle(request(invalid), Job::new()).is_err(), "{invalid}");
            assert!(!output.exists());
        }
        handle(
            request("SELECT \"group\", SUM(CAST(value AS INTEGER)) AS total FROM input GROUP BY \"group\" ORDER BY \"group\""),
            Job::new(),
        ).unwrap();
        assert_eq!(
            std::fs::read_to_string(output).unwrap(),
            "group,total\nA,5\nB,4\n"
        );
    }

    #[test]
    fn grouped_summary_keeps_empty_groups_and_uses_sample_deviation() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("input.csv");
        let output = dir.path().join("out.tsv");
        std::fs::write(&input, "group,x\nA,1\nA,3\nA,\nB,2\nC,\n").unwrap();
        handle(
            Request {
                operation: "group-summary".into(),
                parameter: "group,x".into(),
                input,
                output: output.clone(),
                ..Request::default()
            },
            Job::new(),
        )
        .unwrap();
        let table = load(&output, None, &Job::new()).unwrap();
        assert_eq!(table.rows[0][..4], ["A", "2", "1", "2"]);
        let sample_sd = table.rows[0][4].parse::<f64>().unwrap();
        assert!((sample_sd - 2.0f64.sqrt()).abs() < 1e-12);
        assert_eq!(table.rows[1], ["B", "1", "0", "2", "", "2", "2"]);
        assert_eq!(table.rows[2], ["C", "0", "1", "", "", "", ""]);
    }

    #[test]
    fn ten_statistical_routes_return_reports_and_preserve_no_overwrite() {
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("measurements.csv");
        std::fs::write(
            &input,
            "group,x,y\nA,1,10\nA,2,9\nA,3,8\nB,5,5\nB,6,6\nB,7,3\n,8,4\n",
        )
        .unwrap();
        let operations = [
            ("quantiles", "x"),
            ("standard-error", "x"),
            ("mean-ci", "x"),
            ("bootstrap-mean-ci", "x"),
            ("bootstrap-median-ci", "x"),
            ("welch-t-test", "group,x"),
            ("paired-t-test", "x,y"),
            ("one-way-anova", "group,x"),
            ("spearman", "x,y"),
            ("correlation-interval", "x,y"),
        ];
        for (operation, parameter) in operations {
            let outcome = handle(
                Request {
                    operation: operation.into(),
                    input: input.clone(),
                    parameter: parameter.into(),
                    ..Request::default()
                },
                Job::new(),
            )
            .unwrap_or_else(|error| panic!("{operation}: {error}"));
            let report: serde_json::Value = serde_json::from_str(&outcome.preview)
                .unwrap_or_else(|error| panic!("{operation}: {error}"));
            assert!(report.is_object(), "{operation} report must be JSON object");
        }

        let output = directory.path().join("confidence.json");
        let report = handle(
            Request {
                operation: "mean-ci".into(),
                input: input.clone(),
                output: output.clone(),
                parameter: "x".into(),
                ..Request::default()
            },
            Job::new(),
        )
        .unwrap();
        assert_eq!(report.written, Some(output.clone()));
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
        assert_eq!(saved["confidence_level"], 0.95);
        assert!(handle(
            Request {
                operation: "mean-ci".into(),
                input,
                output,
                parameter: "x".into(),
                ..Request::default()
            },
            Job::new(),
        )
        .is_err());
    }

    #[test]
    fn inner_join_preserves_match_order_and_excludes_blank_keys() {
        let dir = tempfile::tempdir().unwrap();
        let left = dir.path().join("left.csv");
        let right = dir.path().join("right.tsv");
        let output = dir.path().join("joined.csv");
        std::fs::write(&left, "key,name\na,First\nb,Second\n,Blank\na,Third\n").unwrap();
        std::fs::write(&right, "id\tname\na\tOne\na\tTwo\n\tBlank\n").unwrap();
        let request = Request {
            operation: "inner-join".into(),
            input: left,
            output: output.clone(),
            parameter: serde_json::json!({
                "right_path": right, "left_key": "key", "right_key": "id"
            })
            .to_string(),
            ..Request::default()
        };
        handle(request.clone(), Job::new()).unwrap();
        assert_eq!(
            std::fs::read_to_string(&output).unwrap(),
            "key,name,right.name\na,First,One\na,First,Two\na,Third,One\na,Third,Two\n"
        );
        assert!(handle(request, Job::new()).is_err());
    }

    #[test]
    fn project_runs_ordered_steps_and_withholds_partial_output() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("input.csv");
        let output = dir.path().join("project.tsv");
        let path = dir.path().join("pipeline.json");
        std::fs::write(
            &input,
            "name,group,id\nAda,a,2\nRen,a,1\nAda,a,2\nKai,b,3\n",
        )
        .unwrap();
        let request = Request {
            operation: "run-project".into(),
            input: input.clone(),
            output: output.clone(),
            parameter: path.to_str().unwrap().into(),
            ..Request::default()
        };
        std::fs::write(
            &path,
            r#"{
            "schema_version": 1,
            "steps": [
                {"operation":"filter","column":"group","value":"a"},
                {"operation":"sort","column":"unknown"}
            ]
        }"#,
        )
        .unwrap();
        assert!(handle(request.clone(), Job::new())
            .unwrap_err()
            .message
            .contains("Project step 2"));
        assert!(!output.exists());
        std::fs::write(
            &path,
            r#"{
            "schema_version": 1,
            "steps": [
                {"operation":"filter","column":"group","value":"a"},
                {"operation":"deduplicate"},
                {"operation":"sort","column":"id"},
                {"operation":"mask-column","column":"name"},
                {"operation":"select-columns","columns":["id","name"]}
            ]
        }"#,
        )
        .unwrap();
        let result = handle(request, Job::new()).unwrap();
        assert!(result
            .warnings
            .iter()
            .any(|warning| warning.contains("Applied 5")));
        assert_eq!(
            std::fs::read_to_string(output).unwrap(),
            "id\tname\n1\t[MASKED]\n2\t[MASKED]\n"
        );
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
