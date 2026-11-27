use crate::{project::Step, Table, MAX_CELLS, MAX_COLS};
use localwright_core::{Error, Job, Result};
use std::collections::{BTreeMap, BTreeSet, HashSet};

const MAX_TEXT_BYTES: usize = 32 * 1024 * 1024;
const MAX_PARAMETER_BYTES: usize = 128 * 1024;

pub(crate) fn parse_step(operation: &str, parameter: &str) -> Result<Step> {
    if parameter.len() > MAX_PARAMETER_BYTES {
        return Err(Error::new(
            localwright_core::ErrorKind::Limit,
            "Transformation parameters are limited to 128 KiB",
        ));
    }
    let mut value: serde_json::Value = serde_json::from_str(parameter)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| Error::input("Transformation parameters must be a JSON object"))?;
    if object.contains_key("operation") {
        return Err(Error::input(
            "Do not include operation in the parameter object",
        ));
    }
    object.insert("operation".into(), operation.into());
    serde_json::from_value(value).map_err(Into::into)
}

fn text_bytes(table: &Table) -> Result<usize> {
    table
        .headers
        .iter()
        .chain(table.rows.iter().flatten())
        .try_fold(0usize, |total, value| total.checked_add(value.len()))
        .filter(|bytes| *bytes <= MAX_TEXT_BYTES)
        .ok_or_else(|| {
            Error::new(
                localwright_core::ErrorKind::Limit,
                "Transformed table exceeds 32 MiB of cell text",
            )
        })
}

fn unique_name(table: &Table, name: &str, allowed_existing: &HashSet<usize>) -> Result<()> {
    if name.trim().is_empty() {
        return Err(Error::input(
            "New column names must contain non-whitespace text",
        ));
    }
    if table
        .headers
        .iter()
        .enumerate()
        .any(|(i, existing)| !allowed_existing.contains(&i) && existing == name)
    {
        return Err(Error::input(format!("Column {name:?} already exists")));
    }
    Ok(())
}

fn numeric_cell(value: &str) -> Result<Option<f64>> {
    if value.trim().is_empty() {
        return Ok(None);
    }
    let number: f64 = value
        .trim()
        .parse()
        .map_err(|_| Error::input("Expected a finite numeric value"))?;
    if !number.is_finite() {
        return Err(Error::input("Expected a finite numeric value"));
    }
    Ok(Some(number))
}

fn convert(table: &mut Table, column: &str, kind: &str, job: &Job) -> Result<()> {
    let index = crate::column(table, column)?;
    for row in &mut table.rows {
        job.check()?;
        let value = row[index].trim();
        if value.is_empty() {
            continue;
        }
        row[index] = match kind {
            "number" => numeric_cell(value)?
                .ok_or_else(|| Error::input("Expected a finite numeric value"))?
                .to_string(),
            "integer" => value
                .parse::<i64>()
                .map_err(|_| Error::input("Expected an exact signed 64-bit integer"))?
                .to_string(),
            "boolean" => match value.to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" => "true".into(),
                "false" | "0" | "no" => "false".into(),
                _ => {
                    return Err(Error::input(
                        "Boolean conversion accepts true/false, yes/no or 1/0",
                    ))
                }
            },
            _ => return Err(Error::input("Unsupported conversion")),
        };
    }
    text_bytes(table)?;
    Ok(())
}

pub(crate) fn apply(mut table: Table, step: Step, job: &Job) -> Result<Table> {
    match step {
        Step::RecodeValues { column, mapping } => {
            let index = crate::column(&table, &column)?;
            let mut bytes = text_bytes(&table)?;
            for row in &mut table.rows {
                job.check()?;
                if let Some(value) = mapping.get(&row[index]) {
                    bytes = bytes
                        .checked_sub(row[index].len())
                        .and_then(|size| size.checked_add(value.len()))
                        .filter(|size| *size <= MAX_TEXT_BYTES)
                        .ok_or_else(|| {
                            Error::new(
                                localwright_core::ErrorKind::Limit,
                                "Recode output exceeds 32 MiB of cell text",
                            )
                        })?;
                    row[index].clone_from(value);
                }
            }
        }
        Step::ConvertNumber { column } => convert(&mut table, &column, "number", job)?,
        Step::ConvertInteger { column } => convert(&mut table, &column, "integer", job)?,
        Step::ConvertBoolean { column } => convert(&mut table, &column, "boolean", job)?,
        Step::SplitColumn {
            column,
            delimiter,
            new_columns,
        } => {
            if delimiter.is_empty() || !(2..=32).contains(&new_columns.len()) {
                return Err(Error::input(
                    "Split requires a nonempty delimiter and 2 to 32 output columns",
                ));
            }
            let index = crate::column(&table, &column)?;
            let mut headers = table.headers.clone();
            headers.splice(index..=index, new_columns.clone());
            let mut seen = HashSet::new();
            if headers
                .iter()
                .any(|name| name.trim().is_empty() || !seen.insert(name.clone()))
            {
                return Err(Error::input(
                    "Split output names must be nonempty and unique",
                ));
            }
            if headers.len() > MAX_COLS {
                return Err(Error::new(
                    localwright_core::ErrorKind::Limit,
                    "Split exceeds the 256-column limit",
                ));
            }
            let mut rows = Vec::with_capacity(table.rows.len());
            for row in table.rows {
                job.check()?;
                let parts = if row[index].is_empty() {
                    vec![String::new(); new_columns.len()]
                } else {
                    let parts = row[index].split(&delimiter).collect::<Vec<_>>();
                    if parts.len() != new_columns.len() {
                        return Err(Error::input("Every nonblank cell must split into exactly the declared number of parts"));
                    }
                    parts.into_iter().map(str::to_owned).collect()
                };
                let mut output = row;
                output.splice(index..=index, parts);
                rows.push(output);
            }
            table = crate::validate(headers, rows)?;
        }
        Step::MergeColumns {
            columns,
            name,
            delimiter,
        } => {
            if columns.len() < 2
                || columns.len() > MAX_COLS
                || columns.iter().collect::<HashSet<_>>().len() != columns.len()
            {
                return Err(Error::input(
                    "Merge requires at least two distinct source columns",
                ));
            }
            let indexes = columns
                .iter()
                .map(|name| crate::column(&table, name))
                .collect::<Result<Vec<_>>>()?;
            unique_name(&table, &name, &HashSet::new())?;
            let mut headers = table.headers.clone();
            headers.push(name);
            if headers.len() > MAX_COLS {
                return Err(Error::new(
                    localwright_core::ErrorKind::Limit,
                    "Merge exceeds the 256-column limit",
                ));
            }
            for row in &mut table.rows {
                job.check()?;
                row.push(
                    indexes
                        .iter()
                        .map(|&i| row[i].as_str())
                        .collect::<Vec<_>>()
                        .join(&delimiter),
                );
            }
            table.headers = headers;
        }
        Step::Pivot {
            id_column,
            pivot_column,
            value_column,
        } => {
            let id = crate::column(&table, &id_column)?;
            let pivot = crate::column(&table, &pivot_column)?;
            let value = crate::column(&table, &value_column)?;
            if id == pivot || id == value || pivot == value {
                return Err(Error::input("Pivot requires three distinct columns"));
            }
            let mut categories = BTreeSet::new();
            let mut values = BTreeMap::<(String, String), String>::new();
            let mut ids = BTreeSet::new();
            for row in &table.rows {
                job.check()?;
                if row[pivot].is_empty() {
                    return Err(Error::input("Pivot category values cannot be blank"));
                }
                categories.insert(row[pivot].clone());
                ids.insert(row[id].clone());
                if values
                    .insert((row[id].clone(), row[pivot].clone()), row[value].clone())
                    .is_some()
                {
                    return Err(Error::input(
                        "Pivot has duplicate identifier/category pairs; aggregate duplicates first",
                    ));
                }
            }
            let headers = std::iter::once(id_column)
                .chain(categories.iter().cloned())
                .collect::<Vec<_>>();
            if headers.len() > MAX_COLS || ids.len().saturating_mul(headers.len()) > MAX_CELLS {
                return Err(Error::new(
                    localwright_core::ErrorKind::Limit,
                    "Pivot exceeds table dimensions",
                ));
            }
            let mut rows = Vec::with_capacity(ids.len());
            for id in ids {
                job.check()?;
                let mut row = vec![id.clone()];
                row.extend(categories.iter().map(|category| {
                    values
                        .get(&(id.clone(), category.clone()))
                        .cloned()
                        .unwrap_or_default()
                }));
                rows.push(row);
            }
            table = crate::validate(headers, rows)?;
        }
        Step::Unpivot {
            id_columns,
            value_columns,
            variable_name,
            value_name,
        } => {
            if id_columns.is_empty() || value_columns.is_empty() || id_columns.len() + 2 > MAX_COLS
            {
                return Err(Error::input(
                    "Unpivot requires at least one id column and one value column",
                ));
            }
            let ids = id_columns
                .iter()
                .map(|name| crate::column(&table, name))
                .collect::<Result<Vec<_>>>()?;
            let values = value_columns
                .iter()
                .map(|name| crate::column(&table, name))
                .collect::<Result<Vec<_>>>()?;
            if ids.iter().chain(&values).collect::<HashSet<_>>().len() != ids.len() + values.len() {
                return Err(Error::input("Unpivot columns must be unique and disjoint"));
            }
            if value_columns.len() * table.rows.len() > MAX_CELLS / (ids.len() + 2) {
                return Err(Error::new(
                    localwright_core::ErrorKind::Limit,
                    "Unpivot exceeds the 500,000-cell limit",
                ));
            }
            let mut used = HashSet::new();
            let mut headers = id_columns.clone();
            headers.push(variable_name.clone());
            headers.push(value_name.clone());
            if headers
                .iter()
                .any(|name| name.trim().is_empty() || !used.insert(name.clone()))
            {
                return Err(Error::input(
                    "Unpivot output names must be nonempty and unique",
                ));
            }
            let mut rows = Vec::with_capacity(value_columns.len() * table.rows.len());
            for row in &table.rows {
                for (&index, name) in values.iter().zip(&value_columns) {
                    job.check()?;
                    let mut output = ids.iter().map(|&i| row[i].clone()).collect::<Vec<_>>();
                    output.push(name.clone());
                    output.push(row[index].clone());
                    rows.push(output);
                }
            }
            table = crate::validate(headers, rows)?;
        }
        Step::GroupSum {
            group_column,
            value_column,
        } => {
            table = group_aggregate(&table, &group_column, &value_column, false, job)?;
        }
        Step::GroupMean {
            group_column,
            value_column,
        } => {
            table = group_aggregate(&table, &group_column, &value_column, true, job)?;
        }
        Step::FilterContains { column, value } => {
            let index = crate::column(&table, &column)?;
            if value.is_empty() {
                return Err(Error::input("Search text must not be empty"));
            }
            let mut rows = Vec::new();
            for row in table.rows {
                job.check()?;
                if row[index].contains(&value) {
                    rows.push(row);
                }
            }
            table.rows = rows;
        }
        Step::FilterNotContains { column, value } => {
            let index = crate::column(&table, &column)?;
            if value.is_empty() {
                return Err(Error::input("Search text must not be empty"));
            }
            let mut rows = Vec::new();
            for row in table.rows {
                job.check()?;
                if !row[index].contains(&value) {
                    rows.push(row);
                }
            }
            table.rows = rows;
        }
        Step::HeadRows { rows } => {
            if rows > MAX_CELLS {
                return Err(Error::input("Row limit cannot exceed 500,000"));
            }
            job.check()?;
            table.rows.truncate(rows);
        }
        Step::TailRows { rows } => {
            if rows > MAX_CELLS {
                return Err(Error::input("Row limit cannot exceed 500,000"));
            }
            job.check()?;
            let start = table.rows.len().saturating_sub(rows);
            table.rows = table.rows.split_off(start);
        }
        Step::DropDuplicatesByColumns { columns } => {
            if columns.is_empty() || columns.len() > MAX_COLS {
                return Err(Error::input("Choose 1 to 256 duplicate-key columns"));
            }
            let indexes = columns
                .iter()
                .map(|name| crate::column(&table, name))
                .collect::<Result<Vec<_>>>()?;
            if indexes.iter().collect::<HashSet<_>>().len() != indexes.len() {
                return Err(Error::input("Duplicate-key columns must be unique"));
            }
            let mut seen = HashSet::<Vec<String>>::new();
            let mut rows = Vec::new();
            for row in table.rows {
                job.check()?;
                let key = indexes.iter().map(|&index| row[index].clone()).collect();
                if seen.insert(key) {
                    rows.push(row);
                }
            }
            table.rows = rows;
        }
        Step::AddIndexColumn { name, start } => {
            unique_name(&table, &name, &HashSet::new())?;
            if table.headers.len() >= MAX_COLS
                || table.rows.len().saturating_mul(table.headers.len() + 1) > MAX_CELLS
            {
                return Err(Error::new(
                    localwright_core::ErrorKind::Limit,
                    "Index column exceeds table dimensions",
                ));
            }
            let mut indexes = Vec::with_capacity(table.rows.len());
            let mut added_bytes = name.len();
            for offset in 0..table.rows.len() {
                job.check()?;
                let offset = i64::try_from(offset)
                    .map_err(|_| Error::input("Row index exceeds signed 64-bit range"))?;
                let index = start
                    .checked_add(offset)
                    .ok_or_else(|| Error::input("Row index overflow"))?;
                let index = index.to_string();
                added_bytes = added_bytes
                    .checked_add(index.len())
                    .filter(|bytes| *bytes <= MAX_TEXT_BYTES)
                    .ok_or_else(|| {
                        Error::new(
                            localwright_core::ErrorKind::Limit,
                            "Index column exceeds 32 MiB of cell text",
                        )
                    })?;
                indexes.push(index);
            }
            text_bytes(&table)?
                .checked_add(added_bytes)
                .filter(|bytes| *bytes <= MAX_TEXT_BYTES)
                .ok_or_else(|| {
                    Error::new(
                        localwright_core::ErrorKind::Limit,
                        "Index column exceeds 32 MiB of cell text",
                    )
                })?;
            table.headers.push(name);
            for (row, index) in table.rows.iter_mut().zip(indexes) {
                job.check()?;
                row.push(index);
            }
        }
        Step::AddConstantColumn { name, value } => {
            unique_name(&table, &name, &HashSet::new())?;
            if table.headers.len() >= MAX_COLS
                || table.rows.len().saturating_mul(table.headers.len() + 1) > MAX_CELLS
            {
                return Err(Error::new(
                    localwright_core::ErrorKind::Limit,
                    "Constant column exceeds table dimensions",
                ));
            }
            let added_bytes = value.len().checked_mul(table.rows.len()).ok_or_else(|| {
                Error::new(
                    localwright_core::ErrorKind::Limit,
                    "Constant column exceeds 32 MiB of cell text",
                )
            })?;
            text_bytes(&table)?
                .checked_add(added_bytes)
                .and_then(|bytes| bytes.checked_add(name.len()))
                .filter(|n| *n <= MAX_TEXT_BYTES)
                .ok_or_else(|| {
                    Error::new(
                        localwright_core::ErrorKind::Limit,
                        "Constant column exceeds 32 MiB of cell text",
                    )
                })?;
            table.headers.push(name);
            for row in &mut table.rows {
                job.check()?;
                row.push(value.clone());
            }
        }
        Step::NormalizeText { column, mode } => {
            let index = crate::column(&table, &column)?;
            if !matches!(
                mode.as_str(),
                "trim" | "lower" | "upper" | "collapse-whitespace"
            ) {
                return Err(Error::input(
                    "Choose trim, lower, upper or collapse-whitespace",
                ));
            }
            let mut text_size = text_bytes(&table)?;
            for row in &mut table.rows {
                job.check()?;
                let normalized = match mode.as_str() {
                    "trim" => row[index].trim().to_owned(),
                    "lower" => row[index].to_lowercase(),
                    "upper" => row[index].to_uppercase(),
                    "collapse-whitespace" => {
                        row[index].split_whitespace().collect::<Vec<_>>().join(" ")
                    }
                    _ => unreachable!(),
                };
                text_size = text_size
                    .checked_sub(row[index].len())
                    .and_then(|bytes| bytes.checked_add(normalized.len()))
                    .filter(|bytes| *bytes <= MAX_TEXT_BYTES)
                    .ok_or_else(|| {
                        Error::new(
                            localwright_core::ErrorKind::Limit,
                            "Normalized table exceeds 32 MiB of cell text",
                        )
                    })?;
                row[index] = normalized;
            }
        }
        Step::RenameColumns { mapping } => {
            if mapping.is_empty() || mapping.len() > MAX_COLS {
                return Err(Error::input("Provide 1 to 256 column renames"));
            }
            let mut headers = table.headers.clone();
            for (old, new) in mapping {
                let index = crate::column(&table, &old)?;
                if new.trim().is_empty() {
                    return Err(Error::input(
                        "Column names must contain non-whitespace text",
                    ));
                }
                headers[index] = new;
            }
            let mut seen = HashSet::new();
            if headers.iter().any(|name| !seen.insert(name.clone())) {
                return Err(Error::input("Renamed column names must remain unique"));
            }
            table.headers = headers;
        }
        Step::DropColumns { columns } => {
            if columns.is_empty() || columns.len() >= table.headers.len() {
                return Err(Error::input(
                    "Drop columns must remove at least one column and leave at least one",
                ));
            }
            let indexes = columns
                .iter()
                .map(|name| crate::column(&table, name))
                .collect::<Result<Vec<_>>>()?;
            if indexes.iter().collect::<HashSet<_>>().len() != indexes.len() {
                return Err(Error::input("Drop columns must be unique"));
            }
            let remove = indexes.into_iter().collect::<HashSet<_>>();
            table.headers = table
                .headers
                .into_iter()
                .enumerate()
                .filter_map(|(i, name)| (!remove.contains(&i)).then_some(name))
                .collect();
            for row in &mut table.rows {
                job.check()?;
                *row = std::mem::take(row)
                    .into_iter()
                    .enumerate()
                    .filter_map(|(i, value)| (!remove.contains(&i)).then_some(value))
                    .collect();
            }
        }
        _ => {
            return Err(Error::input(
                "This project step is not a reshape or recode operation",
            ))
        }
    }
    text_bytes(&table)?;
    Ok(table)
}

fn group_aggregate(
    table: &Table,
    group_name: &str,
    value_name: &str,
    mean: bool,
    job: &Job,
) -> Result<Table> {
    let group = crate::column(table, group_name)?;
    let value = crate::column(table, value_name)?;
    if group == value {
        return Err(Error::input("Group and numeric value columns must differ"));
    }
    let mut groups = BTreeMap::<String, (usize, f64)>::new();
    for row in &table.rows {
        job.check()?;
        let entry = groups.entry(row[group].clone()).or_default();
        if let Some(number) = numeric_cell(&row[value])? {
            if mean {
                let next_count = entry.0 + 1;
                let denominator = next_count as f64;
                entry.1 = entry.1 * (entry.0 as f64 / denominator) + number / denominator;
            } else {
                entry.1 += number;
            }
            entry.0 += 1;
            if !entry.1.is_finite() {
                return Err(Error::input("Group aggregate overflow"));
            }
            if !entry.1.is_finite() {
                return Err(Error::input("Group aggregate overflow"));
            }
        }
    }
    let label = if mean { "mean" } else { "sum" };
    let rows = groups
        .into_iter()
        .map(|(name, (count, aggregate))| {
            vec![
                name,
                count.to_string(),
                if count == 0 {
                    String::new()
                } else {
                    aggregate.to_string()
                },
            ]
        })
        .collect::<Vec<_>>();
    crate::validate(vec![group_name.to_owned(), "n".into(), label.into()], rows)
}
