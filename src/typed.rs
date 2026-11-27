use crate::{Table, MAX_CELLS, MAX_COLS};
use localwright_core::{Error, ErrorKind, Job, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

const MAX_SCHEMA_PARAMETER_BYTES: usize = 16 * 1024;
const MAX_METADATA_CHARS: usize = 256;
const MAX_EXAMPLE_CHARS: usize = 128;

/// Physical type selected for a column in a typed table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ColumnType {
    Null,
    Boolean,
    Int64,
    #[serde(rename = "uint64")]
    UInt64,
    Float64,
    Date,
    Time,
    #[serde(rename = "datetime")]
    DateTime,
    Categorical,
    String,
}

/// Optional descriptive metadata attached to a column.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ColumnMetadata {
    pub(crate) label: Option<String>,
    pub(crate) unit: Option<String>,
}

/// Explicit schema choices for inspection and validation.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SchemaRequest {
    #[serde(default)]
    pub(crate) overrides: BTreeMap<String, ColumnType>,
    #[serde(default)]
    pub(crate) metadata: BTreeMap<String, ColumnMetadata>,
}

impl SchemaRequest {
    pub(crate) fn from_json(json: &str) -> Result<Self> {
        if json.len() > MAX_SCHEMA_PARAMETER_BYTES {
            return Err(Error::new(
                ErrorKind::Limit,
                "Schema options are limited to 16 KiB",
            ));
        }
        if json.trim().is_empty() {
            return Ok(Self::default());
        }
        serde_json::from_str(json).map_err(Into::into)
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Scalar {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Date(Date),
    Time(Time),
    DateTime(DateTime),
    Categorical(u32),
    String(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Date {
    year: i32,
    month: u8,
    day: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Time {
    seconds: u32,
    nanoseconds: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DateTime {
    date: Date,
    time: Time,
    offset_minutes: Option<i16>,
}

#[derive(Debug)]
struct Column {
    name: String,
    kind: ColumnType,
    values: Vec<Scalar>,
    categories: Vec<String>,
    metadata: ColumnMetadata,
}

/// A bounded columnar typed view built from a text import without modifying source cells.
#[derive(Debug)]
pub(crate) struct TypedTable {
    columns: Vec<Column>,
    rows: usize,
}

impl TypedTable {
    pub(crate) fn infer(source: &Table, request: SchemaRequest, job: &Job) -> Result<Self> {
        if source.headers.is_empty()
            || source.headers.len() > MAX_COLS
            || source.rows.len().saturating_mul(source.headers.len()) > MAX_CELLS
        {
            return Err(Error::input("Typed table exceeds the schema limits"));
        }

        let indexes = source
            .headers
            .iter()
            .enumerate()
            .map(|(index, name)| (name.as_str(), index))
            .collect::<HashMap<_, _>>();
        if request
            .overrides
            .keys()
            .chain(request.metadata.keys())
            .any(|name| !indexes.contains_key(name.as_str()))
        {
            return Err(Error::input("Schema options refer to an unknown column"));
        }

        for metadata in request.metadata.values() {
            for value in [&metadata.label, &metadata.unit].into_iter().flatten() {
                if value.chars().count() > MAX_METADATA_CHARS || value.chars().any(char::is_control)
                {
                    return Err(Error::input(
                        "Column labels and units must be printable and at most 256 characters",
                    ));
                }
            }
        }

        let mut columns = Vec::with_capacity(source.headers.len());
        for (index, name) in source.headers.iter().enumerate() {
            job.check()?;
            let cells = source
                .rows
                .iter()
                .map(|row| row[index].as_str())
                .collect::<Vec<_>>();
            let kind = request
                .overrides
                .get(name)
                .copied()
                .unwrap_or_else(|| infer_type(&cells));
            let metadata = request.metadata.get(name).cloned().unwrap_or_default();
            let (values, categories) = convert_column(name, kind, &cells, job)?;
            columns.push(Column {
                name: name.clone(),
                kind,
                values,
                categories,
                metadata,
            });
        }
        Ok(Self {
            columns,
            rows: source.rows.len(),
        })
    }

    pub(crate) fn schema_report(&self) -> serde_json::Value {
        let columns = self
            .columns
            .iter()
            .map(|column| {
                let missing = column
                    .values
                    .iter()
                    .filter(|value| matches!(value, Scalar::Null))
                    .count();
                serde_json::json!({
                    "name": &column.name,
                    "type": column.kind,
                    "nullable": missing > 0,
                    "missing": missing,
                    "categories": (column.kind == ColumnType::Categorical)
                        .then_some(column.categories.len()),
                    "example": column.values.iter().find_map(|value| value_json(value, &column.categories)),
                    "label": column.metadata.label.as_deref(),
                    "unit": column.metadata.unit.as_deref(),
                })
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "schema_version": 1,
            "rows": self.rows,
            "columns": columns,
            "missing_semantics": "empty or whitespace-only cells are null; other text is preserved",
        })
    }
}

fn infer_type(cells: &[&str]) -> ColumnType {
    let values = cells
        .iter()
        .copied()
        .filter(|value| !is_missing(value))
        .collect::<Vec<_>>();
    if values.is_empty() {
        return ColumnType::Null;
    }
    if values.iter().all(|value| is_boolean_word(value)) {
        return ColumnType::Boolean;
    }
    if values.iter().all(|value| value.parse::<i64>().is_ok()) {
        return ColumnType::Int64;
    }
    if values.iter().all(|value| value.parse::<u64>().is_ok()) {
        return ColumnType::UInt64;
    }
    if values.iter().any(|value| is_integer_token(value)) {
        return ColumnType::String;
    }
    if values
        .iter()
        .all(|value| value.parse::<f64>().is_ok_and(|number| number.is_finite()))
    {
        return ColumnType::Float64;
    }
    if values.iter().all(|value| parse_datetime(value).is_some()) {
        return ColumnType::DateTime;
    }
    if values.iter().all(|value| parse_date(value).is_some()) {
        return ColumnType::Date;
    }
    if values.iter().all(|value| parse_time(value).is_some()) {
        return ColumnType::Time;
    }
    ColumnType::String
}

fn is_boolean_word(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "true" | "false" | "yes" | "no"
    )
}

fn is_integer_token(value: &str) -> bool {
    let bytes = value.trim().as_bytes();
    let digits = if bytes
        .first()
        .is_some_and(|byte| matches!(*byte, b'+' | b'-'))
    {
        &bytes[1..]
    } else {
        bytes
    };
    !digits.is_empty() && digits.iter().all(u8::is_ascii_digit)
}

fn is_missing(value: &str) -> bool {
    value.trim().is_empty()
}

fn parse_boolean(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "yes" | "1" => Some(true),
        "false" | "no" | "0" => Some(false),
        _ => None,
    }
}

fn parse_date(value: &str) -> Option<Date> {
    let bytes = value.as_bytes();
    if bytes.len() != 10
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !bytes[0..4].iter().all(u8::is_ascii_digit)
        || !bytes[5..7].iter().all(u8::is_ascii_digit)
        || !bytes[8..10].iter().all(u8::is_ascii_digit)
    {
        return None;
    }
    let number = |digits: &[u8]| {
        digits
            .iter()
            .fold(0u32, |value, digit| value * 10 + u32::from(digit - b'0'))
    };
    let year = i32::try_from(number(&bytes[0..4])).ok()?;
    let month = u8::try_from(number(&bytes[5..7])).ok()?;
    let day = u8::try_from(number(&bytes[8..10])).ok()?;
    if !(1..=12).contains(&month) {
        return None;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    (1..=days)
        .contains(&day)
        .then_some(Date { year, month, day })
}

fn parse_time(value: &str) -> Option<Time> {
    let mut parts = value.split(':');
    let hour = parts.next()?.parse::<u32>().ok()?;
    let minute = parts.next()?.parse::<u32>().ok()?;
    let second_part = parts.next()?;
    if parts.next().is_some() || hour > 23 || minute > 59 {
        return None;
    }
    let (whole_second, fraction) = second_part
        .split_once('.')
        .map_or((second_part, None), |(whole, fraction)| {
            (whole, Some(fraction))
        });
    let second = whole_second.parse::<u32>().ok()?;
    if second > 59 {
        return None;
    }
    let nanoseconds = match fraction {
        None => 0,
        Some(fraction)
            if (1..=9).contains(&fraction.len())
                && fraction.bytes().all(|b| b.is_ascii_digit()) =>
        {
            fraction.parse::<u32>().ok()? * 10u32.pow((9 - fraction.len()) as u32)
        }
        _ => return None,
    };
    Some(Time {
        seconds: hour * 3_600 + minute * 60 + second,
        nanoseconds,
    })
}

fn parse_datetime(value: &str) -> Option<DateTime> {
    let separator = value.as_bytes().get(10).copied()?;
    if separator != b'T' && separator != b' ' {
        return None;
    }
    let date = parse_date(value.get(..10)?)?;
    let tail = value.get(11..)?;
    let (clock, offset_minutes) = if let Some(clock) = tail.strip_suffix('Z') {
        (clock, Some(0))
    } else if let Some(sign_index) = tail
        .char_indices()
        .filter(|(index, character)| *index > 0 && matches!(*character, '+' | '-'))
        .map(|(index, _)| index)
        .next_back()
    {
        let (clock, offset) = tail.split_at(sign_index);
        let sign = if offset.starts_with('-') { -1 } else { 1 };
        let mut parts = offset[1..].split(':');
        let hours = parts.next()?.parse::<i16>().ok()?;
        let minutes = parts.next()?.parse::<i16>().ok()?;
        if parts.next().is_some() || hours > 14 || minutes > 59 || (hours == 14 && minutes != 0) {
            return None;
        }
        (clock, Some(sign * (hours * 60 + minutes)))
    } else {
        (tail, None)
    };
    Some(DateTime {
        date,
        time: parse_time(clock)?,
        offset_minutes,
    })
}

fn value_json(value: &Scalar, categories: &[String]) -> Option<serde_json::Value> {
    use serde_json::json;
    Some(match value {
        Scalar::Null => return None,
        Scalar::Boolean(value) => json!(value),
        Scalar::Int64(value) => json!(value),
        Scalar::UInt64(value) => json!(value),
        Scalar::Float64(value) => json!(value),
        Scalar::Date(date) => json!(format!(
            "{:04}-{:02}-{:02}",
            date.year, date.month, date.day
        )),
        Scalar::Time(time) => json!(format_time(*time)),
        Scalar::DateTime(datetime) => json!(format_datetime(*datetime)),
        Scalar::Categorical(index) => json!(bounded_example(categories.get(*index as usize)?)),
        Scalar::String(value) => json!(bounded_example(value)),
    })
}

fn bounded_example(value: &str) -> String {
    let mut characters = value.chars();
    let sample = characters
        .by_ref()
        .take(MAX_EXAMPLE_CHARS)
        .collect::<String>();
    if characters.next().is_some() {
        format!("{sample}…")
    } else {
        sample
    }
}

fn format_time(time: Time) -> String {
    let hour = time.seconds / 3_600;
    let minute = (time.seconds / 60) % 60;
    let second = time.seconds % 60;
    if time.nanoseconds == 0 {
        return format!("{hour:02}:{minute:02}:{second:02}");
    }
    let fraction = format!("{:09}", time.nanoseconds);
    format!(
        "{hour:02}:{minute:02}:{second:02}.{}",
        fraction.trim_end_matches('0')
    )
}

fn format_datetime(datetime: DateTime) -> String {
    let date = format!(
        "{:04}-{:02}-{:02}",
        datetime.date.year, datetime.date.month, datetime.date.day
    );
    let offset = match datetime.offset_minutes {
        None => String::new(),
        Some(0) => "Z".into(),
        Some(minutes) => {
            let sign = if minutes < 0 { '-' } else { '+' };
            let absolute = minutes.unsigned_abs();
            format!("{sign}{:02}:{:02}", absolute / 60, absolute % 60)
        }
    };
    format!("{date}T{}{offset}", format_time(datetime.time))
}

fn convert_column(
    name: &str,
    kind: ColumnType,
    cells: &[&str],
    job: &Job,
) -> Result<(Vec<Scalar>, Vec<String>)> {
    let mut values = Vec::with_capacity(cells.len());
    let mut categories = Vec::new();
    let mut category_ids = HashMap::new();
    for cell in cells {
        job.check()?;
        if is_missing(cell) {
            values.push(Scalar::Null);
            continue;
        }
        let invalid = || {
            Error::input(format!(
                "Column {name:?} contains a value incompatible with {kind:?}"
            ))
        };
        let value = match kind {
            ColumnType::Null => return Err(invalid()),
            ColumnType::Boolean => Scalar::Boolean(parse_boolean(cell).ok_or_else(invalid)?),
            ColumnType::Int64 => Scalar::Int64(cell.trim().parse().map_err(|_| invalid())?),
            ColumnType::UInt64 => Scalar::UInt64(cell.trim().parse().map_err(|_| invalid())?),
            ColumnType::Float64 => {
                let number = cell.trim().parse::<f64>().map_err(|_| invalid())?;
                if !number.is_finite() {
                    return Err(invalid());
                }
                Scalar::Float64(number)
            }
            ColumnType::Date => Scalar::Date(parse_date(cell.trim()).ok_or_else(invalid)?),
            ColumnType::Time => Scalar::Time(parse_time(cell.trim()).ok_or_else(invalid)?),
            ColumnType::DateTime => {
                Scalar::DateTime(parse_datetime(cell.trim()).ok_or_else(invalid)?)
            }
            ColumnType::Categorical => {
                let id = if let Some(id) = category_ids.get(*cell) {
                    *id
                } else {
                    let id = u32::try_from(categories.len())
                        .map_err(|_| Error::new(ErrorKind::Limit, "Too many categories"))?;
                    categories.push((*cell).to_owned());
                    category_ids.insert(*cell, id);
                    id
                };
                Scalar::Categorical(id)
            }
            ColumnType::String => Scalar::String((*cell).to_owned()),
        };
        values.push(value);
    }
    Ok((values, categories))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn table(headers: Vec<&str>, rows: Vec<Vec<&str>>) -> Table {
        crate::validate(
            headers.into_iter().map(str::to_owned).collect(),
            rows.into_iter()
                .map(|row| row.into_iter().map(str::to_owned).collect())
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn infers_column_types_and_uses_blank_only_null_semantics() {
        let source = table(
            vec![
                "bool", "signed", "unsigned", "real", "date", "time", "when", "text", "empty",
            ],
            vec![
                vec![
                    "yes",
                    "-2",
                    "18446744073709551615",
                    "1.25",
                    "2024-02-29",
                    "12:30:01.25",
                    "2024-02-29T12:30:01Z",
                    "  A ",
                    " ",
                ],
                vec![
                    "no",
                    "4",
                    "8",
                    "2.5",
                    "2025-01-01",
                    "00:00:00",
                    "2025-01-01 00:00:00+02:30",
                    "B",
                    "\t",
                ],
            ],
        );
        let typed = TypedTable::infer(&source, SchemaRequest::default(), &Job::new()).unwrap();
        let report = typed.schema_report();
        let kinds = report["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|column| column["type"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            [
                "boolean", "int64", "uint64", "float64", "date", "time", "datetime", "string",
                "null"
            ]
        );
        assert_eq!(report["columns"][8]["missing"], 2);
        assert_eq!(
            report["missing_semantics"],
            "empty or whitespace-only cells are null; other text is preserved"
        );
        assert_eq!(report["columns"][6]["example"], "2024-02-29T12:30:01Z");
        let numeric_tokens = table(vec!["value"], vec![vec!["0"], vec!["1"]]);
        let numeric_schema =
            TypedTable::infer(&numeric_tokens, SchemaRequest::default(), &Job::new()).unwrap();
        assert_eq!(
            numeric_schema.schema_report()["columns"][0]["type"],
            "int64"
        );
        let overflow_integer = table(
            vec!["value"],
            vec![vec!["18446744073709551616"], vec!["2.5"]],
        );
        let overflow_schema =
            TypedTable::infer(&overflow_integer, SchemaRequest::default(), &Job::new()).unwrap();
        assert_eq!(
            overflow_schema.schema_report()["columns"][0]["type"],
            "string"
        );
    }

    #[test]
    fn explicit_categorical_override_builds_dictionary_and_metadata() {
        let source = table(
            vec!["site"],
            vec![vec!["north"], vec!["south"], vec!["north"]],
        );
        let request = SchemaRequest::from_json(
            r#"{"overrides":{"site":"categorical"},"metadata":{"site":{"label":"Study site","unit":null}}}"#,
        )
        .unwrap();
        let typed = TypedTable::infer(&source, request, &Job::new()).unwrap();
        assert_eq!(typed.columns[0].categories, ["north", "south"]);
        assert_eq!(typed.columns[0].values[0], Scalar::Categorical(0));
        assert_eq!(typed.columns[0].values[1], Scalar::Categorical(1));
        assert_eq!(
            typed.columns[0].metadata.label.as_deref(),
            Some("Study site")
        );
        assert_eq!(typed.schema_report()["columns"][0]["categories"], 2);
    }

    #[test]
    fn type_overrides_fail_atomically_on_invalid_or_unknown_data() {
        let source = table(vec!["score"], vec![vec!["1.5"], vec!["bad"]]);
        let request = SchemaRequest::from_json(r#"{"overrides":{"score":"float64"}}"#).unwrap();
        assert!(TypedTable::infer(&source, request, &Job::new()).is_err());
        assert!(SchemaRequest::from_json(&" ".repeat(MAX_SCHEMA_PARAMETER_BYTES + 1)).is_err());
        let unknown = SchemaRequest::from_json(r#"{"overrides":{"other":"int64"}}"#).unwrap();
        assert!(TypedTable::infer(&source, unknown, &Job::new()).is_err());
        let no_nulls = SchemaRequest::from_json(r#"{"overrides":{"score":"null"}}"#).unwrap();
        assert!(TypedTable::infer(&source, no_nulls, &Job::new()).is_err());
    }

    #[test]
    fn dates_reject_invalid_calendar_values_and_category_override_preserves_exact_text() {
        assert!(parse_date("2023-02-29").is_none());
        assert!(parse_time("24:00:00").is_none());
        assert!(parse_datetime("2025-01-01T12:00:00+99:00").is_none());
        assert_eq!(parse_date("2000-02-29").unwrap().day, 29);
        assert!(parse_datetime("éééé-01-01T12:00:00").is_none());

        let source = table(vec!["tag"], vec![vec![" a "], vec!["a"]]);
        let request = SchemaRequest {
            overrides: BTreeMap::from([("tag".into(), ColumnType::Categorical)]),
            metadata: BTreeMap::new(),
        };
        let typed = TypedTable::infer(&source, request, &Job::new()).unwrap();
        assert_eq!(typed.columns[0].categories, [" a ", "a"]);
        assert_eq!(
            typed.schema_report()["columns"][0]["type"],
            json!("categorical")
        );
    }
}
