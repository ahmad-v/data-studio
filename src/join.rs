use super::{column, load, validate, Table, MAX_CELLS, MAX_COLS};
use localwright_core::{Error, ErrorKind, Job, Result};
use serde::Deserialize;
use std::{collections::HashMap, path::PathBuf};

const MAX_JOIN_BYTES: usize = 32 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JoinRequest {
    right_path: PathBuf,
    left_key: String,
    right_key: String,
}

pub fn inner(left: Table, parameter: &str, job: &Job) -> Result<Table> {
    if parameter.len() > 32 * 1024 {
        return Err(Error::new(
            ErrorKind::Limit,
            "Join settings exceed the byte limit",
        ));
    }
    let options: JoinRequest = serde_json::from_str(parameter)?;
    let left_key = column(&left, &options.left_key)?;
    let right = load(&options.right_path, None, job)?;
    let right_key = column(&right, &options.right_key)?;
    let mut headers = left.headers.clone();
    headers.extend(
        right
            .headers
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != right_key)
            .map(|(_, name)| format!("right.{name}")),
    );
    if headers.len() > MAX_COLS {
        return Err(Error::new(ErrorKind::Limit, "Joined column limit exceeded"));
    }
    // Check column names before building any result rows.
    let mut output = validate(headers, Vec::new())?;
    let maximum_rows = MAX_CELLS / output.headers.len();
    let mut indexed = HashMap::<&str, Vec<&Vec<String>>>::new();
    for row in &right.rows {
        job.check()?;
        let key = row[right_key].as_str();
        if !key.is_empty() {
            indexed.entry(key).or_default().push(row);
        }
    }

    let mut byte_budget = MAX_JOIN_BYTES;
    for row in left.rows {
        job.check()?;
        if let Some(matches) = indexed.get(row[left_key].as_str()) {
            if matches.len() > maximum_rows - output.rows.len() {
                return Err(Error::new(ErrorKind::Limit, "Joined cell limit exceeded"));
            }
            for right_row in matches {
                job.check()?;
                let appended = right_row
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| *index != right_key)
                    .map(|(_, cell)| cell);
                let bytes = row.iter().chain(appended.clone()).map(String::len).sum();
                byte_budget = byte_budget
                    .checked_sub(bytes)
                    .ok_or_else(|| Error::new(ErrorKind::Limit, "Joined text exceeds 32 MiB"))?;
                output
                    .rows
                    .push(row.iter().chain(appended).cloned().collect());
            }
        }
    }
    Ok(output)
}
