use super::{column, load, validate, Table, MAX_CELLS, MAX_COLS};
use localwright_core::{Error, ErrorKind, Job, Result};
use serde::Deserialize;
use std::{collections::HashMap, path::PathBuf};

const MAX_JOIN_BYTES: usize = 32 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JoinRequest {
    pub(crate) right_path: PathBuf,
    pub(crate) left_key: String,
    pub(crate) right_key: String,
}

#[derive(Clone, Copy)]
pub enum Kind {
    Inner,
    Left,
    Right,
    Full,
}

pub fn run(left: Table, parameter: &str, kind: Kind, job: &Job) -> Result<Table> {
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
    let mut indexed = HashMap::<&str, Vec<usize>>::new();
    if matches!(kind, Kind::Right) {
        for (index, row) in left.rows.iter().enumerate() {
            job.check()?;
            let key = row[left_key].as_str();
            if !key.is_empty() {
                indexed.entry(key).or_default().push(index);
            }
        }
    } else {
        for (index, row) in right.rows.iter().enumerate() {
            job.check()?;
            let key = row[right_key].as_str();
            if !key.is_empty() {
                indexed.entry(key).or_default().push(index);
            }
        }
    }

    let mut byte_budget = MAX_JOIN_BYTES;
    let mut matched_right = vec![false; right.rows.len()];
    let mut append =
        |left_row: Option<&Vec<String>>, right_row: Option<&Vec<String>>| -> Result<()> {
            if output.rows.len() >= maximum_rows {
                return Err(Error::new(ErrorKind::Limit, "Joined cell limit exceeded"));
            }
            let mut row = left_row
                .cloned()
                .unwrap_or_else(|| vec![String::new(); left.headers.len()]);
            if let Some(right_row) = right_row {
                row.extend(
                    right_row
                        .iter()
                        .enumerate()
                        .filter(|(index, _)| *index != right_key)
                        .map(|(_, cell)| cell.clone()),
                );
            } else {
                row.extend((0..right.headers.len().saturating_sub(1)).map(|_| String::new()));
            }
            let bytes = row.iter().map(String::len).sum::<usize>();
            byte_budget = byte_budget
                .checked_sub(bytes)
                .ok_or_else(|| Error::new(ErrorKind::Limit, "Joined text exceeds 32 MiB"))?;
            output.rows.push(row);
            Ok(())
        };
    if matches!(kind, Kind::Right) {
        for right_row in &right.rows {
            job.check()?;
            let matches = if right_row[right_key].is_empty() {
                None
            } else {
                indexed.get(right_row[right_key].as_str())
            };
            if let Some(matches) = matches {
                for &left_index in matches {
                    job.check()?;
                    append(Some(&left.rows[left_index]), Some(right_row))?;
                }
            } else {
                append(None, Some(right_row))?;
            }
        }
    } else {
        for left_row in &left.rows {
            job.check()?;
            let matches = if left_row[left_key].is_empty() {
                None
            } else {
                indexed.get(left_row[left_key].as_str())
            };
            if let Some(matches) = matches {
                for &right_index in matches {
                    job.check()?;
                    matched_right[right_index] = true;
                    append(Some(left_row), Some(&right.rows[right_index]))?;
                }
            } else if matches!(kind, Kind::Left | Kind::Full) {
                append(Some(left_row), None)?;
            }
        }
    }
    if matches!(kind, Kind::Full) {
        for (index, right_row) in right.rows.iter().enumerate() {
            job.check()?;
            if !matched_right[index] {
                append(None, Some(right_row))?;
            }
        }
    }
    Ok(output)
}
