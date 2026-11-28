use crate::{validate_headers, Table, MAX_BYTES, MAX_CELLS, MAX_COLS};
use arrow_array::{
    types::Int32Type, Array, ArrayRef, BooleanArray, Date32Array, DictionaryArray, Float64Array,
    Int32Array, Int64Array, StringArray, Time64NanosecondArray, TimestampNanosecondArray,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use localwright_core::{read_bounded, write_new, Error, ErrorKind, Job, Result};
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    hash::{BuildHasher, Hash, Hasher},
    io::Cursor,
    path::Path,
    sync::Arc,
};

const MAX_SCHEMA_PARAMETER_BYTES: usize = 16 * 1024;
const MAX_METADATA_CHARS: usize = 256;
const MAX_EXAMPLE_CHARS: usize = 128;
const MAX_WORKSPACE_TEXT_CHARS: usize = 255;
const MAX_WORKSPACE_QUERY_BYTES: usize = 16 * 1024;
const MAX_WORKSPACE_QUERY_FILTERS: usize = 8;
const MAX_WORKSPACE_PAGE_ROWS: usize = 100;
const MAX_TYPED_TRANSFORM_TEXT_BYTES: usize = 32 * 1024 * 1024;

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

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TypedComparison {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum TextMatch {
    Contains,
    NotContains,
    ContainsInsensitive,
    StartsWith,
    EndsWith,
}

/// A bounded query over the complete accepted dataset for a native table page.
/// Filters and sorting are applied before the requested page is sliced.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct WorkspaceQuery {
    pub(crate) worksheet: Option<String>,
    pub(crate) offset: usize,
    pub(crate) limit: Option<usize>,
    pub(crate) filters: Vec<WorkspaceFilter>,
    pub(crate) sort: Option<WorkspaceSort>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkspaceFilter {
    pub(crate) column: String,
    pub(crate) comparison: TypedComparison,
    pub(crate) value: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkspaceSort {
    pub(crate) column: String,
    #[serde(default)]
    pub(crate) descending: bool,
}

impl WorkspaceQuery {
    pub(crate) fn from_json(json: &str) -> Result<Self> {
        if json.len() > MAX_WORKSPACE_QUERY_BYTES {
            return Err(Error::new(
                ErrorKind::Limit,
                "Table query is limited to 16 KiB",
            ));
        }
        let query: Self = if json.trim().is_empty() {
            Self::default()
        } else {
            serde_json::from_str(json)?
        };
        if query.filters.len() > MAX_WORKSPACE_QUERY_FILTERS {
            return Err(Error::new(
                ErrorKind::Limit,
                "A table query may contain at most eight filters",
            ));
        }
        if query
            .filters
            .iter()
            .any(|filter| filter.column.is_empty() || filter.value.len() > 1024)
            || query
                .sort
                .as_ref()
                .is_some_and(|sort| sort.column.is_empty())
        {
            return Err(Error::input(
                "Table query columns must be named and filter values are limited to 1 KiB",
            ));
        }
        if query.worksheet.as_ref().is_some_and(|worksheet| {
            worksheet.is_empty()
                || worksheet.chars().count() > MAX_METADATA_CHARS
                || worksheet.chars().any(char::is_control)
        }) {
            return Err(Error::input(
                "Worksheet names must be nonempty and at most 256 printable characters",
            ));
        }
        if query.limit == Some(0)
            || query
                .limit
                .is_some_and(|limit| limit > MAX_WORKSPACE_PAGE_ROWS)
        {
            return Err(Error::input(format!(
                "Table page size must be between 1 and {MAX_WORKSPACE_PAGE_ROWS} rows"
            )));
        }
        Ok(query)
    }
}

/// Bounded, declarative arithmetic expression used by typed project pipelines.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum NumericExpr {
    Literal {
        value: f64,
    },
    Column {
        name: String,
    },
    Add {
        left: Box<Self>,
        right: Box<Self>,
    },
    Subtract {
        left: Box<Self>,
        right: Box<Self>,
    },
    Multiply {
        left: Box<Self>,
        right: Box<Self>,
    },
    Divide {
        left: Box<Self>,
        right: Box<Self>,
    },
    Negate {
        value: Box<Self>,
    },
    Absolute {
        value: Box<Self>,
    },
    SquareRoot {
        value: Box<Self>,
    },
    Log10 {
        value: Box<Self>,
    },
    Round {
        value: Box<Self>,
        decimal_places: i32,
    },
    Clip {
        value: Box<Self>,
        minimum: f64,
        maximum: f64,
    },
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

impl Date {
    fn days_since_epoch(self) -> i32 {
        let mut year = self.year;
        year -= i32::from(self.month <= 2);
        let era = year.div_euclid(400);
        let year_of_era = year - era * 400;
        let month = i32::from(self.month);
        let day_of_year =
            (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + i32::from(self.day) - 1;
        let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
        era * 146_097 + day_of_era - 719_468
    }
}

impl Time {
    fn nanoseconds_since_midnight(self) -> i64 {
        i64::from(self.seconds) * 1_000_000_000 + i64::from(self.nanoseconds)
    }
}

impl DateTime {
    fn nanoseconds_since_epoch(self) -> Result<i64> {
        let seconds = i64::from(self.date.days_since_epoch())
            .checked_mul(86_400)
            .and_then(|seconds| seconds.checked_add(i64::from(self.time.seconds)))
            .and_then(|seconds| {
                seconds.checked_sub(i64::from(self.offset_minutes.unwrap_or(0)) * 60)
            })
            .ok_or_else(|| Error::input("Datetime is outside Arrow's timestamp range"))?;
        seconds
            .checked_mul(1_000_000_000)
            .and_then(|nanos| nanos.checked_add(i64::from(self.time.nanoseconds)))
            .ok_or_else(|| Error::input("Datetime is outside Arrow's nanosecond timestamp range"))
    }
}

#[derive(Debug, Clone)]
struct Column {
    name: String,
    kind: ColumnType,
    values: Vec<Scalar>,
    /// Preserve source spellings that differ from canonical typed serialization.
    raw_text: Vec<Option<String>>,
    categories: Vec<String>,
    metadata: ColumnMetadata,
}

fn validate_column_name(name: &str) -> Result<()> {
    if name.trim().is_empty()
        || name.chars().count() > MAX_METADATA_CHARS
        || name.chars().any(char::is_control)
    {
        return Err(Error::input(
            "Column name must be printable and at most 256 characters",
        ));
    }
    Ok(())
}

fn typed_join_key(column: &Column, value: &Scalar) -> Result<Option<String>> {
    let key = match value {
        Scalar::Null => return Ok(None),
        Scalar::Boolean(value) => format!("b:{value}"),
        Scalar::Int64(value) => format!("i:{value}"),
        Scalar::UInt64(value) => format!("u:{value}"),
        Scalar::Float64(value) => {
            format!("f:{:016x}", if *value == 0.0 { 0 } else { value.to_bits() })
        }
        Scalar::Date(value) => format!("d:{}", value.days_since_epoch()),
        Scalar::Time(value) => format!("t:{}", value.nanoseconds_since_midnight()),
        Scalar::DateTime(value) => format!(
            "z:{}:{}",
            value.offset_minutes.is_some(),
            datetime_order_value(*value)
        ),
        Scalar::Categorical(index) => {
            let label = column
                .categories
                .get(*index as usize)
                .ok_or_else(|| Error::input("Typed categorical index is outside its dictionary"))?;
            format!("c:{}", serde_json::to_string(label)?)
        }
        Scalar::String(value) => format!("s:{}", serde_json::to_string(value)?),
    };
    Ok(Some(key))
}

fn validate_schema_request(headers: &[String], request: &SchemaRequest) -> Result<()> {
    validate_headers(headers)?;
    let names = headers
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    if request
        .overrides
        .keys()
        .chain(request.metadata.keys())
        .any(|name| !names.contains(name.as_str()))
    {
        return Err(Error::input("Schema options refer to an unknown column"));
    }
    for metadata in request.metadata.values() {
        validate_metadata(metadata)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct TypeInference {
    nonmissing: bool,
    all_boolean: bool,
    all_i64: bool,
    all_u64: bool,
    any_integer: bool,
    all_float: bool,
    all_datetime: bool,
    all_date: bool,
    all_time: bool,
}

impl Default for TypeInference {
    fn default() -> Self {
        Self {
            nonmissing: false,
            all_boolean: true,
            all_i64: true,
            all_u64: true,
            any_integer: false,
            all_float: true,
            all_datetime: true,
            all_date: true,
            all_time: true,
        }
    }
}

impl TypeInference {
    fn observe(&mut self, value: &str) {
        if is_missing(value) {
            return;
        }
        self.nonmissing = true;
        self.all_boolean &= is_boolean_word(value);
        self.all_i64 &= value.parse::<i64>().is_ok();
        self.all_u64 &= value.parse::<u64>().is_ok();
        self.any_integer |= is_integer_token(value);
        self.all_float &= value.parse::<f64>().is_ok_and(f64::is_finite);
        self.all_datetime &= parse_datetime(value).is_some();
        self.all_date &= parse_date(value).is_some();
        self.all_time &= parse_time(value).is_some();
    }

    fn finish(self) -> ColumnType {
        if !self.nonmissing {
            ColumnType::Null
        } else if self.all_boolean {
            ColumnType::Boolean
        } else if self.all_i64 {
            ColumnType::Int64
        } else if self.all_u64 {
            ColumnType::UInt64
        } else if self.any_integer {
            ColumnType::String
        } else if self.all_float {
            ColumnType::Float64
        } else if self.all_datetime {
            ColumnType::DateTime
        } else if self.all_date {
            ColumnType::Date
        } else if self.all_time {
            ColumnType::Time
        } else {
            ColumnType::String
        }
    }
}

fn parse_filter_value(column: &Column, literal: &str) -> Result<Scalar> {
    if is_missing(literal) {
        return Ok(Scalar::Null);
    }
    let invalid = || {
        Error::input(format!(
            "Filter literal is incompatible with column {:?} ({:?})",
            column.name, column.kind
        ))
    };
    let parsed = match column.kind {
        ColumnType::Null => return Err(invalid()),
        ColumnType::Boolean => Scalar::Boolean(parse_boolean(literal).ok_or_else(invalid)?),
        ColumnType::Int64 => Scalar::Int64(literal.trim().parse().map_err(|_| invalid())?),
        ColumnType::UInt64 => Scalar::UInt64(literal.trim().parse().map_err(|_| invalid())?),
        ColumnType::Float64 => {
            let number = literal.trim().parse::<f64>().map_err(|_| invalid())?;
            if !number.is_finite() {
                return Err(invalid());
            }
            Scalar::Float64(number)
        }
        ColumnType::Date => Scalar::Date(parse_date(literal.trim()).ok_or_else(invalid)?),
        ColumnType::Time => Scalar::Time(parse_time(literal.trim()).ok_or_else(invalid)?),
        ColumnType::DateTime => {
            Scalar::DateTime(parse_datetime(literal.trim()).ok_or_else(invalid)?)
        }
        ColumnType::Categorical => {
            let Some(index) = column
                .categories
                .iter()
                .position(|category| category == literal)
            else {
                return Ok(Scalar::String(literal.to_owned()));
            };
            Scalar::Categorical(
                u32::try_from(index)
                    .map_err(|_| Error::new(ErrorKind::Limit, "Too many categorical values"))?,
            )
        }
        ColumnType::String => Scalar::String(literal.to_owned()),
    };
    Ok(parsed)
}

fn validate_datetime_ordering(column: &Column) -> Result<()> {
    if column.kind != ColumnType::DateTime {
        return Ok(());
    }
    let mut aware = None;
    for value in &column.values {
        if let Scalar::DateTime(datetime) = value {
            let has_offset = datetime.offset_minutes.is_some();
            if aware.is_some_and(|previous| previous != has_offset) {
                return Err(Error::input(format!(
                    "Datetime column {:?} mixes timezone-aware and timezone-naive values",
                    column.name
                )));
            }
            aware = Some(has_offset);
        }
    }
    Ok(())
}

fn datetime_order_value(datetime: DateTime) -> i128 {
    let seconds = i128::from(datetime.date.days_since_epoch()) * 86_400
        + i128::from(datetime.time.seconds)
        - i128::from(datetime.offset_minutes.unwrap_or(0)) * 60;
    seconds * 1_000_000_000 + i128::from(datetime.time.nanoseconds)
}

fn compare_scalar(column: &Column, left: &Scalar, right: &Scalar) -> Result<std::cmp::Ordering> {
    use std::cmp::Ordering;
    if matches!(left, Scalar::Null) || matches!(right, Scalar::Null) {
        return Ok(match (left, right) {
            (Scalar::Null, Scalar::Null) => Ordering::Equal,
            (Scalar::Null, _) => Ordering::Greater,
            (_, Scalar::Null) => Ordering::Less,
            _ => unreachable!(),
        });
    }
    Ok(match (left, right) {
        (Scalar::Boolean(a), Scalar::Boolean(b)) => a.cmp(b),
        (Scalar::Int64(a), Scalar::Int64(b)) => a.cmp(b),
        (Scalar::UInt64(a), Scalar::UInt64(b)) => a.cmp(b),
        (Scalar::Float64(a), Scalar::Float64(b)) => {
            if a == b {
                Ordering::Equal
            } else {
                a.total_cmp(b)
            }
        }
        (Scalar::Date(a), Scalar::Date(b)) => a.days_since_epoch().cmp(&b.days_since_epoch()),
        (Scalar::Time(a), Scalar::Time(b)) => a
            .nanoseconds_since_midnight()
            .cmp(&b.nanoseconds_since_midnight()),
        (Scalar::DateTime(a), Scalar::DateTime(b)) => {
            if a.offset_minutes.is_some() != b.offset_minutes.is_some() {
                return Err(Error::input(format!(
                    "Filter datetime literal has different timezone semantics from column {:?}",
                    column.name
                )));
            }
            datetime_order_value(*a).cmp(&datetime_order_value(*b))
        }
        (Scalar::Categorical(a), Scalar::Categorical(b)) => {
            let a = column
                .categories
                .get(*a as usize)
                .ok_or_else(|| Error::input("Typed categorical index is outside its dictionary"))?;
            let b = column
                .categories
                .get(*b as usize)
                .ok_or_else(|| Error::input("Typed categorical index is outside its dictionary"))?;
            a.cmp(b)
        }
        (Scalar::Categorical(a), Scalar::String(b)) => column
            .categories
            .get(*a as usize)
            .ok_or_else(|| Error::input("Typed categorical index is outside its dictionary"))?
            .cmp(b),
        (Scalar::String(a), Scalar::Categorical(b)) => a.cmp(
            column
                .categories
                .get(*b as usize)
                .ok_or_else(|| Error::input("Typed categorical index is outside its dictionary"))?,
        ),
        (Scalar::String(a), Scalar::String(b)) => a.cmp(b),
        _ => {
            return Err(Error::input(format!(
                "Typed values do not match column {:?} schema",
                column.name
            )))
        }
    })
}

const MAX_NUMERIC_EXPRESSION_DEPTH: usize = 16;
const MAX_NUMERIC_EXPRESSION_NODES: usize = 64;
const MAX_EXACT_FLOAT64_INTEGER: u64 = 9_007_199_254_740_992;

fn validate_metadata(metadata: &ColumnMetadata) -> Result<()> {
    for value in [&metadata.label, &metadata.unit].into_iter().flatten() {
        if value.chars().count() > MAX_METADATA_CHARS || value.chars().any(char::is_control) {
            return Err(Error::input(
                "Column labels and units must be printable and at most 256 characters",
            ));
        }
    }
    Ok(())
}

fn validate_numeric_expression(
    expression: &NumericExpr,
    table: &TypedTable,
    depth: usize,
    nodes: &mut usize,
) -> Result<()> {
    *nodes += 1;
    if depth > MAX_NUMERIC_EXPRESSION_DEPTH || *nodes > MAX_NUMERIC_EXPRESSION_NODES {
        return Err(Error::new(
            ErrorKind::Limit,
            "Numeric expressions are limited to depth 16 and 64 nodes",
        ));
    }
    match expression {
        NumericExpr::Literal { value } if !value.is_finite() => {
            return Err(Error::input("Numeric expression literals must be finite"));
        }
        NumericExpr::Column { name } => {
            let index = table.column_index(name)?;
            if !matches!(
                table.columns[index].kind,
                ColumnType::Int64 | ColumnType::UInt64 | ColumnType::Float64
            ) {
                return Err(Error::input(format!(
                    "Numeric expression column {name:?} must have a numeric type"
                )));
            }
        }
        NumericExpr::Add { left, right }
        | NumericExpr::Subtract { left, right }
        | NumericExpr::Multiply { left, right }
        | NumericExpr::Divide { left, right } => {
            validate_numeric_expression(left, table, depth + 1, nodes)?;
            validate_numeric_expression(right, table, depth + 1, nodes)?;
        }
        NumericExpr::Negate { value }
        | NumericExpr::Absolute { value }
        | NumericExpr::SquareRoot { value }
        | NumericExpr::Log10 { value } => {
            validate_numeric_expression(value, table, depth + 1, nodes)?;
        }
        NumericExpr::Round {
            value,
            decimal_places,
        } => {
            if !(-12..=12).contains(decimal_places) {
                return Err(Error::input(
                    "Numeric expression round precision must be from -12 through 12",
                ));
            }
            validate_numeric_expression(value, table, depth + 1, nodes)?;
        }
        NumericExpr::Clip {
            value,
            minimum,
            maximum,
        } => {
            if !minimum.is_finite() || !maximum.is_finite() || minimum > maximum {
                return Err(Error::input(
                    "Numeric expression clip bounds must be finite and ordered",
                ));
            }
            validate_numeric_expression(value, table, depth + 1, nodes)?;
        }
        NumericExpr::Literal { .. } => {}
    }
    Ok(())
}

fn scalar_to_f64(column: &Column, value: &Scalar) -> Result<Option<f64>> {
    match value {
        Scalar::Null => Ok(None),
        Scalar::Int64(value) if value.unsigned_abs() <= MAX_EXACT_FLOAT64_INTEGER => {
            Ok(Some(*value as f64))
        }
        Scalar::UInt64(value) if *value <= MAX_EXACT_FLOAT64_INTEGER => Ok(Some(*value as f64)),
        Scalar::Float64(value) if value.is_finite() => Ok(Some(*value)),
        Scalar::Int64(_) | Scalar::UInt64(_) => Err(Error::input(format!(
            "Integer in column {:?} exceeds the exact float64 expression range",
            column.name
        ))),
        _ => Err(Error::input(format!(
            "Value in column {:?} is not a finite numeric value",
            column.name
        ))),
    }
}

fn finite_numeric_result(value: f64) -> Result<f64> {
    value
        .is_finite()
        .then_some(value)
        .ok_or_else(|| Error::input("Numeric expression produced a non-finite result"))
}

fn evaluate_numeric_unary(
    expression: &NumericExpr,
    table: &TypedTable,
    row: usize,
    operation: impl Fn(f64) -> Result<f64>,
) -> Result<Option<f64>> {
    evaluate_numeric_expression(expression, table, row)?
        .map(|value| operation(value).and_then(finite_numeric_result))
        .transpose()
}

fn evaluate_numeric_binary(
    left: &NumericExpr,
    right: &NumericExpr,
    table: &TypedTable,
    row: usize,
    operation: impl Fn(f64, f64) -> f64,
) -> Result<Option<f64>> {
    let left = evaluate_numeric_expression(left, table, row)?;
    let Some(left) = left else {
        return Ok(None);
    };
    let right = evaluate_numeric_expression(right, table, row)?;
    right
        .map(|right| finite_numeric_result(operation(left, right)))
        .transpose()
}

fn evaluate_numeric_expression(
    expression: &NumericExpr,
    table: &TypedTable,
    row: usize,
) -> Result<Option<f64>> {
    match expression {
        NumericExpr::Literal { value } => finite_numeric_result(*value).map(Some),
        NumericExpr::Column { name } => {
            let index = table.column_index(name)?;
            scalar_to_f64(&table.columns[index], &table.columns[index].values[row])
        }
        NumericExpr::Add { left, right } => {
            evaluate_numeric_binary(left, right, table, row, |a, b| a + b)
        }
        NumericExpr::Subtract { left, right } => {
            evaluate_numeric_binary(left, right, table, row, |a, b| a - b)
        }
        NumericExpr::Multiply { left, right } => {
            evaluate_numeric_binary(left, right, table, row, |a, b| a * b)
        }
        NumericExpr::Divide { left, right } => {
            let left = evaluate_numeric_expression(left, table, row)?;
            let Some(left) = left else {
                return Ok(None);
            };
            let right = evaluate_numeric_expression(right, table, row)?;
            match right {
                Some(right) => {
                    if right == 0.0 {
                        Err(Error::input("Numeric expression division by zero"))
                    } else {
                        finite_numeric_result(left / right).map(Some)
                    }
                }
                None => Ok(None),
            }
        }
        NumericExpr::Negate { value } => {
            evaluate_numeric_unary(value, table, row, |value| Ok(-value))
        }
        NumericExpr::Absolute { value } => {
            evaluate_numeric_unary(value, table, row, |value| Ok(value.abs()))
        }
        NumericExpr::SquareRoot { value } => evaluate_numeric_unary(value, table, row, |value| {
            if value < 0.0 {
                Err(Error::input(
                    "Numeric expression square root requires a nonnegative value",
                ))
            } else {
                Ok(value.sqrt())
            }
        }),
        NumericExpr::Log10 { value } => evaluate_numeric_unary(value, table, row, |value| {
            if value <= 0.0 {
                Err(Error::input(
                    "Numeric expression log10 requires a positive value",
                ))
            } else {
                Ok(value.log10())
            }
        }),
        NumericExpr::Round {
            value,
            decimal_places,
        } => evaluate_numeric_unary(value, table, row, |value| {
            let factor = 10.0_f64.powi(*decimal_places);
            let scaled = value * factor;
            if !scaled.is_finite() {
                return Err(Error::input("Numeric expression rounding overflowed"));
            }
            Ok(scaled.round() / factor)
        }),
        NumericExpr::Clip {
            value,
            minimum,
            maximum,
        } => evaluate_numeric_unary(value, table, row, |value| {
            Ok(value.clamp(*minimum, *maximum))
        }),
    }
}

/// A bounded columnar typed view built from a text import without modifying source cells.
#[derive(Debug)]
pub(crate) struct TypedTable {
    columns: Vec<Column>,
    rows: usize,
}

impl TypedTable {
    /// Load supported inputs directly into typed columns without creating a
    /// row-oriented text `Table` where the format exposes native cell values.
    pub(crate) fn from_path(
        path: &Path,
        sheet: Option<&str>,
        request: SchemaRequest,
        job: &Job,
    ) -> Result<Self> {
        match path
            .extension()
            .and_then(|extension| extension.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some(kind @ ("csv" | "tsv")) => {
                let bytes = read_bounded(path, MAX_BYTES)?;
                job.check()?;
                Self::from_delimited_bytes(
                    &bytes,
                    if kind == "tsv" { b'\t' } else { b',' },
                    request,
                    job,
                )
            }
            Some("xlsx") => Self::from_xlsx(path, sheet, request, job),
            _ => {
                let source = super::load(path, sheet, job)?;
                Self::infer(&source, request, job)
            }
        }
    }

    fn from_xlsx(
        path: &Path,
        sheet: Option<&str>,
        request: SchemaRequest,
        job: &Job,
    ) -> Result<Self> {
        use calamine::{Data, Reader, Xlsx};

        let bytes = read_bounded(path, MAX_BYTES)?;
        job.check()?;
        super::xlsx_safety::validate_workbook(&bytes, job)?;

        let mut workbook: Xlsx<_> =
            calamine::open_workbook_from_rs::<Xlsx<_>, _>(Cursor::new(bytes))
                .map_err(|error| Error::input(error.to_string()))?;
        let names = workbook.sheet_names();
        let name = match sheet {
            Some(name) if names.iter().any(|candidate| candidate == name) => name,
            Some(name) => return Err(Error::input(format!("Worksheet not found: {name}"))),
            None => names
                .first()
                .ok_or_else(|| Error::input("Workbook has no worksheets"))?,
        };
        let range = workbook
            .worksheet_range(name)
            .map_err(|error| Error::input(error.to_string()))?;
        let (height, width) = range.get_size();
        if height == 0 || width == 0 || width > MAX_COLS || height.saturating_mul(width) > MAX_CELLS
        {
            return Err(Error::input("Worksheet exceeds typed table limits"));
        }
        let rows = range.rows().collect::<Vec<_>>();
        let headers = rows[0].iter().map(ToString::to_string).collect::<Vec<_>>();
        validate_schema_request(&headers, &request)?;

        let mut inference = vec![TypeInference::default(); width];
        let mut explicit_empty_text = vec![false; width];
        for (row_index, row) in rows.iter().enumerate().skip(1) {
            job.check()?;
            for (column_index, cell) in row.iter().enumerate() {
                if let Data::Error(error) = cell {
                    return Err(Error::input(format!(
                        "Worksheet cell error at row {}, column {}: {error}",
                        row_index + 1,
                        column_index + 1
                    )));
                }
                match cell {
                    Data::Empty => {}
                    Data::String(value) if value.is_empty() => {
                        explicit_empty_text[column_index] = true;
                    }
                    Data::Float(value) if !value.is_finite() => {
                        return Err(Error::input("Worksheet contains a non-finite number"));
                    }
                    Data::Float(value) => {
                        let token = if value.fract() == 0.0 {
                            format!("{value:.1}")
                        } else {
                            value.to_string()
                        };
                        inference[column_index].observe(&token);
                    }
                    Data::DateTime(value) if value.is_duration() => {
                        inference[column_index].observe(&value.to_string());
                    }
                    Data::DateTime(value) => {
                        let (year, month, day, hour, minute, second, millis) =
                            value.to_ymd_hms_milli();
                        let token = format!(
                            "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}"
                        );
                        inference[column_index].observe(&token);
                    }
                    Data::DateTimeIso(value) => inference[column_index].observe(value),
                    Data::DurationIso(value) | Data::String(value) => {
                        inference[column_index].observe(value)
                    }
                    _ => inference[column_index].observe(&cell.to_string()),
                }
            }
        }
        let kinds = headers
            .iter()
            .enumerate()
            .map(|(index, name)| {
                request.overrides.get(name).copied().unwrap_or_else(|| {
                    if explicit_empty_text[index] {
                        ColumnType::String
                    } else {
                        inference[index].finish()
                    }
                })
            })
            .collect::<Vec<_>>();
        let mut values = (0..width)
            .map(|_| Vec::with_capacity(height - 1))
            .collect::<Vec<_>>();
        let mut raw_text = (0..width)
            .map(|_| Vec::with_capacity(height - 1))
            .collect::<Vec<Vec<Option<String>>>>();
        let mut categories = (0..width).map(|_| Vec::new()).collect::<Vec<_>>();
        let mut category_ids = (0..width)
            .map(|_| HashMap::<String, u32>::new())
            .collect::<Vec<_>>();

        for (row_index, row) in rows.iter().enumerate().skip(1) {
            job.check()?;
            for column_index in 0..width {
                let cell = &row[column_index];
                let value = xlsx_scalar(
                    &headers[column_index],
                    kinds[column_index],
                    cell,
                    &mut categories[column_index],
                    &mut category_ids[column_index],
                )
                .map_err(|error| {
                    Error::input(format!(
                        "Worksheet row {}, column {:?}: {}",
                        row_index + 2,
                        headers[column_index],
                        error
                    ))
                })?;
                let lexical = match cell {
                    Data::Empty => String::new(),
                    Data::String(value) => value.clone(),
                    Data::DateTime(value) if !value.is_duration() => {
                        let (y, m, d, h, min, s, ms) = value.to_ymd_hms_milli();
                        format!("{y:04}-{m:02}-{d:02}T{h:02}:{min:02}:{s:02}.{ms:03}")
                    }
                    _ => cell.to_string(),
                };
                raw_text[column_index].push(
                    if matches!(cell, Data::String(text) if text.is_empty())
                        && matches!(value, Scalar::String(_))
                    {
                        None
                    } else {
                        preserved_raw_text(&lexical, &value)
                    },
                );
                values[column_index].push(value);
            }
        }
        let columns = headers
            .into_iter()
            .enumerate()
            .map(|(index, name)| Column {
                metadata: request.metadata.get(&name).cloned().unwrap_or_default(),
                name,
                kind: kinds[index],
                values: std::mem::take(&mut values[index]),
                raw_text: std::mem::take(&mut raw_text[index]),
                categories: std::mem::take(&mut categories[index]),
            })
            .collect();
        Ok(Self {
            columns,
            rows: height - 1,
        })
    }

    fn from_delimited_bytes(
        bytes: &[u8],
        delimiter: u8,
        request: SchemaRequest,
        job: &Job,
    ) -> Result<Self> {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .from_reader(bytes);
        let headers = reader
            .headers()
            .map_err(|error| Error::input(error.to_string()))?
            .iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        validate_schema_request(&headers, &request)?;

        let mut inference = vec![TypeInference::default(); headers.len()];
        let mut row_count = 0usize;
        for record in reader.records() {
            job.check()?;
            let record = record.map_err(|error| Error::input(error.to_string()))?;
            if record.len() != headers.len() {
                return Err(Error::input("Cell limit or inconsistent row width"));
            }
            row_count = row_count
                .checked_add(1)
                .filter(|rows| rows.saturating_mul(headers.len()) <= MAX_CELLS)
                .ok_or_else(|| Error::input("Cell limit exceeded"))?;
            for (state, value) in inference.iter_mut().zip(record.iter()) {
                state.observe(value);
            }
        }

        let kinds = headers
            .iter()
            .zip(inference)
            .map(|(name, state)| {
                request
                    .overrides
                    .get(name)
                    .copied()
                    .unwrap_or_else(|| state.finish())
            })
            .collect::<Vec<_>>();
        let mut values = (0..headers.len())
            .map(|_| Vec::with_capacity(row_count))
            .collect::<Vec<Vec<Scalar>>>();
        let mut raw_text = (0..headers.len())
            .map(|_| Vec::with_capacity(row_count))
            .collect::<Vec<Vec<Option<String>>>>();
        let mut categories = (0..headers.len())
            .map(|_| Vec::new())
            .collect::<Vec<Vec<String>>>();
        let mut category_ids = (0..headers.len())
            .map(|_| HashMap::<String, u32>::new())
            .collect::<Vec<_>>();
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .from_reader(bytes);
        for record in reader.records() {
            job.check()?;
            let record = record.map_err(|error| Error::input(error.to_string()))?;
            if record.len() != headers.len() {
                return Err(Error::input("Cell limit or inconsistent row width"));
            }
            for index in 0..headers.len() {
                let cell = &record[index];
                let value = convert_cell(
                    &headers[index],
                    kinds[index],
                    cell,
                    &mut categories[index],
                    &mut category_ids[index],
                )?;
                raw_text[index].push(preserved_raw_text(cell, &value));
                values[index].push(value);
            }
        }

        let columns = headers
            .into_iter()
            .enumerate()
            .map(|(index, name)| Column {
                metadata: request.metadata.get(&name).cloned().unwrap_or_default(),
                name,
                kind: kinds[index],
                values: std::mem::take(&mut values[index]),
                raw_text: std::mem::take(&mut raw_text[index]),
                categories: std::mem::take(&mut categories[index]),
            })
            .collect();
        Ok(Self {
            columns,
            rows: row_count,
        })
    }

    pub(crate) fn infer(source: &Table, request: SchemaRequest, job: &Job) -> Result<Self> {
        if source.headers.is_empty()
            || source.headers.len() > MAX_COLS
            || source.rows.len().saturating_mul(source.headers.len()) > MAX_CELLS
        {
            return Err(Error::input("Typed table exceeds the schema limits"));
        }

        validate_schema_request(&source.headers, &request)?;

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
            let raw_text = cells
                .iter()
                .zip(&values)
                .map(|(cell, value)| preserved_raw_text(cell, value))
                .collect();
            columns.push(Column {
                name: name.clone(),
                kind,
                values,
                raw_text,
                categories,
                metadata,
            });
        }
        Ok(Self {
            columns,
            rows: source.rows.len(),
        })
    }

    /// Keep rows whose selected cell equals a literal parsed using that column's type.
    pub(crate) fn filter_equal(&mut self, name: &str, literal: &str, job: &Job) -> Result<()> {
        self.filter_compare(name, TypedComparison::Eq, literal, job)
    }

    /// Filter a typed column using a case-sensitive or insensitive text predicate.
    /// Values are formatted only for matching; the retained scalar values and schema
    /// remain typed and unchanged.
    pub(crate) fn filter_text(
        &mut self,
        name: &str,
        needle: &str,
        mode: TextMatch,
        job: &Job,
    ) -> Result<()> {
        if needle.is_empty() {
            return Err(Error::input("Text filter value must not be empty"));
        }
        if needle.len() > MAX_EXAMPLE_CHARS * 8 {
            return Err(Error::new(
                ErrorKind::Limit,
                "Text filter exceeds the 1 KiB limit",
            ));
        }
        let index = self.column_index(name)?;
        let column = &self.columns[index];
        let needle_folded = needle.to_lowercase();
        let mut keep = Vec::with_capacity(self.rows);
        for value in &column.values {
            job.check()?;
            let text = match value {
                Scalar::Null => {
                    keep.push(false);
                    continue;
                }
                Scalar::Categorical(id) => column
                    .categories
                    .get(*id as usize)
                    .ok_or_else(|| Error::input("Invalid categorical value"))?
                    .clone(),
                value => scalar_text(value, &column.categories, None),
            };
            let matched = match mode {
                TextMatch::Contains => text.contains(needle),
                TextMatch::NotContains => !text.contains(needle),
                TextMatch::ContainsInsensitive => text.to_lowercase().contains(&needle_folded),
                TextMatch::StartsWith => text.starts_with(needle),
                TextMatch::EndsWith => text.ends_with(needle),
            };
            keep.push(matched);
        }
        self.retain_rows(&keep, job)
    }

    /// Whether the selected column has text semantics suitable for an
    /// in-place, type-preserving text transform.
    pub(crate) fn supports_text_transform(&self, name: &str) -> Result<bool> {
        let column = &self.columns[self.column_index(name)?];
        Ok(matches!(
            column.kind,
            ColumnType::String | ColumnType::Categorical | ColumnType::Null
        ))
    }

    /// Normalize a string or categorical column while preserving nulls,
    /// categorical dictionaries, labels, units, and all other typed columns.
    pub(crate) fn normalize_text_column(
        &mut self,
        name: &str,
        mode: &str,
        job: &Job,
    ) -> Result<()> {
        if !matches!(mode, "trim" | "lower" | "upper" | "collapse-whitespace") {
            return Err(Error::input(
                "Choose trim, lower, upper or collapse-whitespace",
            ));
        }
        self.transform_text_column(name, job, |text, max_bytes| {
            let transformed = match mode {
                "trim" => text.trim().to_owned(),
                "lower" => text.to_lowercase(),
                "upper" => text.to_uppercase(),
                "collapse-whitespace" => text.split_whitespace().collect::<Vec<_>>().join(" "),
                _ => unreachable!("normalization mode was validated"),
            };
            if safe_text_len(&transformed) > max_bytes {
                return Err(Error::new(
                    ErrorKind::Limit,
                    "Normalized table exceeds 32 MiB of cell text",
                ));
            }
            Ok(transformed)
        })
    }

    /// Replace literal, non-overlapping text matches without changing the
    /// selected column's string or categorical type.
    pub(crate) fn replace_literal_text(
        &mut self,
        name: &str,
        find: &str,
        replace: &str,
        job: &Job,
    ) -> Result<()> {
        if find.is_empty() {
            return Err(Error::input("Find text must not be empty"));
        }
        self.transform_text_column(name, job, |text, max_bytes| {
            let matches = text.matches(find).count();
            let removed = find
                .len()
                .checked_mul(matches)
                .ok_or_else(|| Error::new(ErrorKind::Limit, "Replacement size overflow"))?;
            let added = replace
                .len()
                .checked_mul(matches)
                .ok_or_else(|| Error::new(ErrorKind::Limit, "Replacement size overflow"))?;
            let new_len = text
                .len()
                .checked_sub(removed)
                .and_then(|size| size.checked_add(added))
                .ok_or_else(|| Error::new(ErrorKind::Limit, "Replacement size overflow"))?;
            if new_len > max_bytes {
                return Err(Error::new(
                    ErrorKind::Limit,
                    "Replaced table exceeds 32 MiB of cell text",
                ));
            }
            let transformed = text.replace(find, replace);
            if safe_text_len(&transformed) > max_bytes {
                return Err(Error::new(
                    ErrorKind::Limit,
                    "Replaced table exceeds 32 MiB of cell text",
                ));
            }
            Ok(transformed)
        })
    }

    fn transform_text_column(
        &mut self,
        name: &str,
        job: &Job,
        mut transform: impl FnMut(&str, usize) -> Result<String>,
    ) -> Result<()> {
        let column_index = self.column_index(name)?;
        let kind = self.columns[column_index].kind;
        if kind == ColumnType::Null {
            return Ok(());
        }
        if !matches!(kind, ColumnType::String | ColumnType::Categorical) {
            return Err(Error::input(format!(
                "Text transforms require a string or categorical column; {name:?} has type {kind:?}"
            )));
        }
        let mut text_bytes = self.serialized_text_bytes(job)?;
        if text_bytes > MAX_TYPED_TRANSFORM_TEXT_BYTES {
            return Err(Error::new(
                ErrorKind::Limit,
                "Typed table exceeds 32 MiB of cell text",
            ));
        }

        let (values, raw_text, categories) = {
            let column = &self.columns[column_index];
            match kind {
                ColumnType::String => {
                    let mut values = Vec::with_capacity(self.rows);
                    let mut raw_text = Vec::with_capacity(self.rows);
                    for (value, raw) in column.values.iter().zip(&column.raw_text) {
                        job.check()?;
                        match value {
                            Scalar::Null => {
                                values.push(Scalar::Null);
                                raw_text.push(raw.clone());
                            }
                            Scalar::String(text) => {
                                let old_bytes =
                                    scalar_text(value, &column.categories, raw.as_deref()).len();
                                let max_bytes = MAX_TYPED_TRANSFORM_TEXT_BYTES
                                    .saturating_sub(text_bytes.saturating_sub(old_bytes));
                                let transformed = transform(text, max_bytes)?;
                                let new_bytes = safe_text_len(&transformed);
                                text_bytes = text_bytes
                                    .checked_sub(old_bytes)
                                    .and_then(|size| size.checked_add(new_bytes))
                                    .filter(|size| *size <= MAX_TYPED_TRANSFORM_TEXT_BYTES)
                                    .ok_or_else(|| {
                                        Error::new(
                                            ErrorKind::Limit,
                                            "Transformed table exceeds 32 MiB of cell text",
                                        )
                                    })?;
                                values.push(Scalar::String(transformed));
                                raw_text.push(None);
                            }
                            _ => {
                                return Err(Error::input(
                                    "Typed string column contains an incompatible scalar",
                                ))
                            }
                        }
                    }
                    (values, raw_text, column.categories.clone())
                }
                ColumnType::Categorical => {
                    let mut counts = vec![0usize; column.categories.len()];
                    let mut old_bytes = vec![0usize; column.categories.len()];
                    for (value, raw) in column.values.iter().zip(&column.raw_text) {
                        job.check()?;
                        if let Scalar::Categorical(index) = value {
                            let index = *index as usize;
                            let _label = column
                                .categories
                                .get(index)
                                .ok_or_else(|| Error::input("Invalid categorical value"))?;
                            counts[index] = counts[index].checked_add(1).ok_or_else(|| {
                                Error::new(ErrorKind::Limit, "Category count overflow")
                            })?;
                            old_bytes[index] = old_bytes[index]
                                .checked_add(
                                    scalar_text(value, &column.categories, raw.as_deref()).len(),
                                )
                                .ok_or_else(|| {
                                    Error::new(ErrorKind::Limit, "Category size overflow")
                                })?;
                        }
                    }
                    let mut category_map = vec![None; column.categories.len()];
                    let mut new_categories = Vec::new();
                    let mut new_category_ids = HashMap::<String, u32>::new();
                    for (index, label) in column.categories.iter().enumerate() {
                        job.check()?;
                        let count = counts[index];
                        if count == 0 {
                            continue;
                        }
                        let available = MAX_TYPED_TRANSFORM_TEXT_BYTES
                            .saturating_sub(text_bytes.saturating_sub(old_bytes[index]));
                        let max_bytes_per_cell = available / count;
                        let transformed = transform(label, max_bytes_per_cell)?;
                        let per_cell_bytes = safe_text_len(&transformed);
                        let transformed_bytes =
                            per_cell_bytes.checked_mul(count).ok_or_else(|| {
                                Error::new(ErrorKind::Limit, "Category size overflow")
                            })?;
                        text_bytes = text_bytes
                            .checked_sub(old_bytes[index])
                            .and_then(|size| size.checked_add(transformed_bytes))
                            .filter(|size| *size <= MAX_TYPED_TRANSFORM_TEXT_BYTES)
                            .ok_or_else(|| {
                                Error::new(
                                    ErrorKind::Limit,
                                    "Transformed table exceeds 32 MiB of cell text",
                                )
                            })?;
                        let id = if let Some(id) = new_category_ids.get(&transformed) {
                            *id
                        } else {
                            let id = u32::try_from(new_categories.len()).map_err(|_| {
                                Error::new(ErrorKind::Limit, "Too many categorical values")
                            })?;
                            new_categories.push(transformed.clone());
                            new_category_ids.insert(transformed, id);
                            id
                        };
                        category_map[index] = Some(id);
                    }
                    let mut values = Vec::with_capacity(self.rows);
                    let mut raw_text = Vec::with_capacity(self.rows);
                    for (value, raw) in column.values.iter().zip(&column.raw_text) {
                        job.check()?;
                        match value {
                            Scalar::Null => {
                                values.push(Scalar::Null);
                                raw_text.push(raw.clone());
                            }
                            Scalar::Categorical(index) => {
                                let mapped = category_map
                                    .get(*index as usize)
                                    .and_then(|value| *value)
                                    .ok_or_else(|| Error::input("Invalid categorical value"))?;
                                values.push(Scalar::Categorical(mapped));
                                raw_text.push(None);
                            }
                            _ => {
                                return Err(Error::input(
                                    "Typed categorical column contains an incompatible scalar",
                                ))
                            }
                        }
                    }
                    (values, raw_text, new_categories)
                }
                _ => unreachable!("text column type was validated"),
            }
        };
        let column = &mut self.columns[column_index];
        column.values = values;
        column.raw_text = raw_text;
        column.categories = categories;
        Ok(())
    }

    fn serialized_text_bytes(&self, job: &Job) -> Result<usize> {
        let mut bytes = self
            .columns
            .iter()
            .try_fold(0usize, |total, column| total.checked_add(column.name.len()))
            .ok_or_else(|| Error::new(ErrorKind::Limit, "Typed text size overflow"))?;
        for column in &self.columns {
            for (value, raw_text) in column.values.iter().zip(&column.raw_text) {
                job.check()?;
                bytes = bytes
                    .checked_add(scalar_text(value, &column.categories, raw_text.as_deref()).len())
                    .filter(|size| *size <= MAX_TYPED_TRANSFORM_TEXT_BYTES)
                    .ok_or_else(|| {
                        Error::new(ErrorKind::Limit, "Typed table exceeds 32 MiB of cell text")
                    })?;
            }
        }
        Ok(bytes)
    }

    /// Filter a numeric column by a finite real threshold. Integer columns are
    /// rejected outside the exact f64 integer range rather than silently
    /// rounding values during comparison.
    pub(crate) fn filter_number(
        &mut self,
        name: &str,
        comparison: TypedComparison,
        threshold: f64,
        job: &Job,
    ) -> Result<()> {
        if !threshold.is_finite() {
            return Err(Error::input("Numeric filter threshold must be finite"));
        }
        let index = self.column_index(name)?;
        let column = &self.columns[index];
        if !matches!(
            column.kind,
            ColumnType::Int64 | ColumnType::UInt64 | ColumnType::Float64
        ) {
            return Err(Error::input(format!(
                "Numeric filter column {name:?} must have an integer or floating-point type"
            )));
        }
        let mut keep = Vec::with_capacity(self.rows);
        for value in &column.values {
            job.check()?;
            let ordering = match value {
                Scalar::Null => None,
                Scalar::Float64(value) => value.partial_cmp(&threshold),
                Scalar::Int64(value) if value.unsigned_abs() <= MAX_EXACT_FLOAT64_INTEGER => {
                    (*value as f64).partial_cmp(&threshold)
                }
                Scalar::UInt64(value) if *value <= MAX_EXACT_FLOAT64_INTEGER => {
                    (*value as f64).partial_cmp(&threshold)
                }
                Scalar::Int64(_) | Scalar::UInt64(_) => {
                    return Err(Error::input(format!(
                        "Numeric comparison for {name:?} exceeds the exact floating-point range"
                    )))
                }
                _ => {
                    return Err(Error::input(
                        "Typed numeric column contains an invalid value",
                    ))
                }
            };
            keep.push(ordering.is_some_and(|ordering| match comparison {
                TypedComparison::Eq => ordering == std::cmp::Ordering::Equal,
                TypedComparison::Ne => ordering != std::cmp::Ordering::Equal,
                TypedComparison::Lt => ordering == std::cmp::Ordering::Less,
                TypedComparison::Lte => ordering != std::cmp::Ordering::Greater,
                TypedComparison::Gt => ordering == std::cmp::Ordering::Greater,
                TypedComparison::Gte => ordering != std::cmp::Ordering::Less,
            }));
        }
        self.retain_rows(&keep, job)
    }

    /// Keep rows by a type-aware literal comparison. Nulls match only `eq` with a null literal.
    pub(crate) fn filter_compare(
        &mut self,
        name: &str,
        comparison: TypedComparison,
        literal: &str,
        job: &Job,
    ) -> Result<()> {
        let index = self.column_index(name)?;
        let column = &self.columns[index];
        let expected = parse_filter_value(column, literal)?;
        let mut keep = Vec::with_capacity(self.rows);
        for value in &column.values {
            job.check()?;
            let value_is_null = matches!(value, Scalar::Null);
            let literal_is_null = matches!(&expected, &Scalar::Null);
            let retain = if value_is_null || literal_is_null {
                matches!(comparison, TypedComparison::Eq) && value_is_null && literal_is_null
            } else {
                let ordering = compare_scalar(column, value, &expected)?;
                match comparison {
                    TypedComparison::Eq => ordering == std::cmp::Ordering::Equal,
                    TypedComparison::Ne => ordering != std::cmp::Ordering::Equal,
                    TypedComparison::Lt => ordering == std::cmp::Ordering::Less,
                    TypedComparison::Lte => ordering != std::cmp::Ordering::Greater,
                    TypedComparison::Gt => ordering == std::cmp::Ordering::Greater,
                    TypedComparison::Gte => ordering != std::cmp::Ordering::Less,
                }
            };
            keep.push(retain);
        }
        self.retain_rows(&keep, job)
    }

    pub(crate) fn derive_numeric_column(
        &mut self,
        name: String,
        expression: &NumericExpr,
        metadata: ColumnMetadata,
        job: &Job,
    ) -> Result<()> {
        if name.trim().is_empty()
            || name.chars().count() > MAX_METADATA_CHARS
            || name.chars().any(char::is_control)
            || self.columns.iter().any(|column| column.name == name)
        {
            return Err(Error::input(
                "Derived column name must be printable, unique and at most 256 characters",
            ));
        }
        if self.columns.len() >= MAX_COLS
            || self
                .rows
                .saturating_mul(self.columns.len().saturating_add(1))
                > MAX_CELLS
        {
            return Err(Error::new(
                ErrorKind::Limit,
                "Derived column would exceed the typed table limits",
            ));
        }
        validate_metadata(&metadata)?;
        let mut nodes = 0;
        validate_numeric_expression(expression, self, 1, &mut nodes)?;
        let mut values = Vec::with_capacity(self.rows);
        for row in 0..self.rows {
            job.check()?;
            let value = evaluate_numeric_expression(expression, self, row).map_err(|error| {
                Error::new(
                    error.kind,
                    format!(
                        "Numeric expression failed on row {}: {}",
                        row + 1,
                        error.message
                    ),
                )
            })?;
            values.push(value.map_or(Scalar::Null, Scalar::Float64));
        }
        self.columns.push(Column {
            name,
            kind: ColumnType::Float64,
            values,
            raw_text: vec![None; self.rows],
            categories: Vec::new(),
            metadata,
        });
        Ok(())
    }

    /// Rename a column without changing its physical type, values or metadata.
    pub(crate) fn rename_column(&mut self, old: &str, new: String) -> Result<()> {
        let index = self.column_index(old)?;
        validate_column_name(&new)?;
        if self
            .columns
            .iter()
            .enumerate()
            .any(|(i, column)| i != index && column.name == new)
        {
            return Err(Error::input("Renamed column must have a unique name"));
        }
        self.columns[index].name = new;
        Ok(())
    }

    /// Rename multiple columns atomically while preserving values and metadata.
    pub(crate) fn rename_columns(
        &mut self,
        mapping: &std::collections::BTreeMap<String, String>,
    ) -> Result<()> {
        for old in mapping.keys() {
            self.column_index(old)?;
        }
        let mut names = std::collections::HashSet::with_capacity(self.columns.len());
        let proposed = self
            .columns
            .iter()
            .map(|column| {
                let name = mapping
                    .get(&column.name)
                    .cloned()
                    .unwrap_or_else(|| column.name.clone());
                validate_column_name(&name)?;
                if !names.insert(name.clone()) {
                    return Err(Error::input(
                        "Renamed columns must have unique names; no partial rename was applied",
                    ));
                }
                Ok(name)
            })
            .collect::<Result<Vec<_>>>()?;
        for (column, name) in self.columns.iter_mut().zip(proposed) {
            column.name = name;
        }
        Ok(())
    }

    /// Drop named columns while retaining source order and at least one column.
    pub(crate) fn drop_columns(&mut self, names: &[String], job: &Job) -> Result<()> {
        if names.is_empty() {
            return Err(Error::input("Select at least one column to drop"));
        }
        let mut dropped = std::collections::HashSet::with_capacity(names.len());
        for name in names {
            job.check()?;
            self.column_index(name)?;
            if !dropped.insert(name.as_str()) {
                return Err(Error::input("A column was selected more than once"));
            }
        }
        if dropped.len() >= self.columns.len() {
            return Err(Error::input("At least one typed column must remain"));
        }
        self.columns
            .retain(|column| !dropped.contains(column.name.as_str()));
        Ok(())
    }

    /// Add a checked signed integer row index without changing existing columns.
    pub(crate) fn add_index_column(&mut self, name: String, start: i64, job: &Job) -> Result<()> {
        self.validate_new_column(&name)?;
        let mut values = Vec::with_capacity(self.rows);
        for row in 0..self.rows {
            job.check()?;
            let offset = i64::try_from(row)
                .map_err(|_| Error::new(ErrorKind::Limit, "Index exceeds signed integer range"))?;
            let value = start.checked_add(offset).ok_or_else(|| {
                Error::input(format!(
                    "Index column overflows signed integer at row {}",
                    row + 1
                ))
            })?;
            values.push(Scalar::Int64(value));
        }
        self.columns.push(Column {
            name,
            kind: ColumnType::Int64,
            values,
            raw_text: vec![None; self.rows],
            categories: Vec::new(),
            metadata: ColumnMetadata::default(),
        });
        Ok(())
    }

    /// Add a constant column using inferred or explicitly requested scalar type.
    pub(crate) fn add_constant_column(
        &mut self,
        name: String,
        literal: &str,
        requested_type: Option<ColumnType>,
        job: &Job,
    ) -> Result<()> {
        self.validate_new_column(&name)?;
        let kind = requested_type.unwrap_or_else(|| infer_type(&[literal]));
        let (one_value, categories) = convert_column(&name, kind, &[literal], job)?;
        let value = one_value
            .first()
            .cloned()
            .ok_or_else(|| Error::input("Constant column has no value"))?;
        let raw = preserved_raw_text(literal, &value);
        let values = (0..self.rows)
            .map(|_| {
                job.check()?;
                Ok(value.clone())
            })
            .collect::<Result<Vec<_>>>()?;
        self.columns.push(Column {
            name,
            kind,
            values,
            raw_text: vec![raw; self.rows],
            categories,
            metadata: ColumnMetadata::default(),
        });
        Ok(())
    }

    fn validate_new_column(&self, name: &str) -> Result<()> {
        validate_column_name(name)?;
        if self.columns.len() >= MAX_COLS
            || self
                .rows
                .saturating_mul(self.columns.len().saturating_add(1))
                > MAX_CELLS
        {
            return Err(Error::new(
                ErrorKind::Limit,
                "Added column would exceed the typed table limits",
            ));
        }
        if self.columns.iter().any(|column| column.name == name) {
            return Err(Error::input("Added column name must be unique"));
        }
        Ok(())
    }

    /// Join typed tables with type-aware keys; null keys never match. Outer
    /// joins preserve unmatched rows while missing-side columns remain
    /// explicitly nullable. The right key remains omitted for compatibility
    /// with the existing join output contract.
    pub(crate) fn join(
        mut self,
        right: TypedTable,
        left_key: &str,
        right_key: &str,
        kind: crate::join::Kind,
        job: &Job,
    ) -> Result<Self> {
        let left_index = self.column_index(left_key)?;
        let right_index = right.column_index(right_key)?;
        let left_column = &self.columns[left_index];
        let right_column = &right.columns[right_index];
        if left_column.kind != right_column.kind {
            return Err(Error::input(
                "Join keys must have the same typed column type",
            ));
        }
        if left_column.kind == ColumnType::DateTime {
            validate_datetime_ordering(left_column)?;
            validate_datetime_ordering(right_column)?;
            let left_aware = left_column.values.iter().find_map(|value| match value {
                Scalar::DateTime(value) => Some(value.offset_minutes.is_some()),
                _ => None,
            });
            let right_aware = right_column.values.iter().find_map(|value| match value {
                Scalar::DateTime(value) => Some(value.offset_minutes.is_some()),
                _ => None,
            });
            if left_aware.is_some() && right_aware.is_some() && left_aware != right_aware {
                return Err(Error::input(
                    "Join datetime keys have different timezone semantics",
                ));
            }
        }

        let mut names = self
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let right_columns = right
            .columns
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != right_index)
            .map(|(_, column)| column)
            .collect::<Vec<_>>();
        for column in &right_columns {
            let name = format!("right.{}", column.name);
            if names.contains(&name) {
                return Err(Error::input(format!(
                    "Join result column {name:?} conflicts with an existing column"
                )));
            }
            validate_column_name(&name)?;
            names.push(name);
        }
        if names.is_empty() || names.len() > MAX_COLS {
            return Err(Error::new(ErrorKind::Limit, "Joined column limit exceeded"));
        }

        let mut right_index = HashMap::<String, Vec<usize>>::new();
        for (row, value) in right_column.values.iter().enumerate() {
            job.check()?;
            if let Some(key) = typed_join_key(right_column, value)? {
                right_index.entry(key).or_default().push(row);
            }
        }
        let mut left_index = HashMap::<String, Vec<usize>>::new();
        if matches!(kind, crate::join::Kind::Right) {
            for (row, value) in left_column.values.iter().enumerate() {
                job.check()?;
                if let Some(key) = typed_join_key(left_column, value)? {
                    left_index.entry(key).or_default().push(row);
                }
            }
        }

        let mut pairs = Vec::<(Option<usize>, Option<usize>)>::new();
        let max_rows = MAX_CELLS / names.len();
        let mut matched_right = vec![false; right.rows];
        let mut append_pair = |left_row, right_row| -> Result<()> {
            if pairs.len() >= max_rows {
                return Err(Error::new(
                    ErrorKind::Limit,
                    "Typed join exceeds the cell limit",
                ));
            }
            pairs.push((left_row, right_row));
            Ok(())
        };

        if matches!(kind, crate::join::Kind::Right) {
            for (right_row, value) in right_column.values.iter().enumerate() {
                job.check()?;
                let matches =
                    typed_join_key(right_column, value)?.and_then(|key| left_index.get(&key));
                if let Some(matches) = matches {
                    for left_row in matches {
                        append_pair(Some(*left_row), Some(right_row))?;
                        matched_right[right_row] = true;
                    }
                } else {
                    append_pair(None, Some(right_row))?;
                }
            }
        } else {
            for (left_row, value) in left_column.values.iter().enumerate() {
                job.check()?;
                let matches =
                    typed_join_key(left_column, value)?.and_then(|key| right_index.get(&key));
                if let Some(matches) = matches {
                    for right_row in matches {
                        append_pair(Some(left_row), Some(*right_row))?;
                        matched_right[*right_row] = true;
                    }
                } else if matches!(kind, crate::join::Kind::Left | crate::join::Kind::Full) {
                    append_pair(Some(left_row), None)?;
                }
            }
            if matches!(kind, crate::join::Kind::Full) {
                for (right_row, matched) in matched_right.iter().enumerate() {
                    job.check()?;
                    if !matched {
                        append_pair(None, Some(right_row))?;
                    }
                }
            }
        }

        let left_column_count = self.columns.len();
        let mut output_columns = Vec::with_capacity(names.len());
        for (column, name) in self
            .columns
            .drain(..)
            .zip(names.iter().take(left_column_count))
        {
            let mut values = Vec::with_capacity(pairs.len());
            let mut raw_text = Vec::with_capacity(pairs.len());
            for (left_row, _) in &pairs {
                job.check()?;
                if let Some(left_row) = left_row {
                    values.push(column.values[*left_row].clone());
                    raw_text.push(column.raw_text[*left_row].clone());
                } else {
                    values.push(Scalar::Null);
                    raw_text.push(None);
                }
            }
            output_columns.push(Column {
                name: name.clone(),
                values,
                raw_text,
                ..column
            });
        }
        for (column, name) in right_columns
            .into_iter()
            .zip(names.iter().skip(output_columns.len()))
        {
            let mut values = Vec::with_capacity(pairs.len());
            let mut raw_text = Vec::with_capacity(pairs.len());
            for (_, right_row) in &pairs {
                job.check()?;
                if let Some(right_row) = right_row {
                    values.push(column.values[*right_row].clone());
                    raw_text.push(column.raw_text[*right_row].clone());
                } else {
                    values.push(Scalar::Null);
                    raw_text.push(None);
                }
            }
            output_columns.push(Column {
                name: name.clone(),
                values,
                raw_text,
                ..(*column).clone()
            });
        }
        Ok(Self {
            columns: output_columns,
            rows: pairs.len(),
        })
    }

    pub(crate) fn inner_join(
        self,
        right: TypedTable,
        left_key: &str,
        right_key: &str,
        job: &Job,
    ) -> Result<Self> {
        self.join(right, left_key, right_key, crate::join::Kind::Inner, job)
    }

    /// Aggregate one numeric column by typed key, retaining first-seen group order.
    /// Null keys form one group; null values are omitted, and all-null groups yield null.
    pub(crate) fn group_numeric(
        &mut self,
        group_name: &str,
        value_name: &str,
        mean: bool,
        job: &Job,
    ) -> Result<()> {
        struct Accumulator {
            key: Scalar,
            raw_text: Option<String>,
            sum: f64,
            mean: f64,
            count: usize,
        }

        let group_index = self.column_index(group_name)?;
        let value_index = self.column_index(value_name)?;
        let group_column = &self.columns[group_index];
        let value_column = &self.columns[value_index];
        if !matches!(
            value_column.kind,
            ColumnType::Int64 | ColumnType::UInt64 | ColumnType::Float64
        ) {
            return Err(Error::input(format!(
                "Aggregation column {value_name:?} must have a numeric type"
            )));
        }
        let aggregate_name = format!("{}_{}", if mean { "mean" } else { "sum" }, value_name);
        validate_column_name(&aggregate_name)?;
        if aggregate_name == group_name {
            return Err(Error::input(
                "Generated aggregation column conflicts with the group column",
            ));
        }

        let mut lookup = HashMap::<String, usize>::new();
        let mut groups = Vec::<Accumulator>::new();
        for row in 0..self.rows {
            job.check()?;
            let key_value = &group_column.values[row];
            let key = typed_join_key(group_column, key_value)?.unwrap_or_else(|| "n:".into());
            let group_index = if let Some(index) = lookup.get(&key) {
                *index
            } else {
                if groups.len() >= MAX_CELLS / 2 {
                    return Err(Error::new(
                        ErrorKind::Limit,
                        "Grouped output exceeds the typed table cell limit",
                    ));
                }
                let index = groups.len();
                lookup.insert(key, index);
                groups.push(Accumulator {
                    key: key_value.clone(),
                    raw_text: group_column.raw_text[row].clone(),
                    sum: 0.0,
                    mean: 0.0,
                    count: 0,
                });
                index
            };
            let Some(value) =
                scalar_to_f64(value_column, &value_column.values[row]).map_err(|error| {
                    Error::new(
                        error.kind,
                        format!("Aggregation failed on row {}: {}", row + 1, error.message),
                    )
                })?
            else {
                continue;
            };
            let group = &mut groups[group_index];
            let next_count = group.count.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorKind::Limit,
                    "Grouped value count exceeds the supported range",
                )
            })?;
            if mean {
                let result = if group.count == 0 {
                    value
                } else {
                    let divisor = next_count as f64;
                    group.mean * (group.count as f64 / divisor) + value / divisor
                };
                group.mean = finite_numeric_result(result).map_err(|error| {
                    Error::new(
                        error.kind,
                        format!("Mean overflow in group on row {}", row + 1),
                    )
                })?;
            } else {
                group.sum = finite_numeric_result(group.sum + value).map_err(|error| {
                    Error::new(
                        error.kind,
                        format!("Sum overflow in group on row {}", row + 1),
                    )
                })?;
            }
            group.count = next_count;
        }

        let mut key_output = group_column.clone();
        key_output.values = groups.iter().map(|group| group.key.clone()).collect();
        key_output.raw_text = groups.iter().map(|group| group.raw_text.clone()).collect();
        let aggregate_values = groups
            .iter()
            .map(|group| {
                if group.count == 0 {
                    Scalar::Null
                } else {
                    Scalar::Float64(if mean { group.mean } else { group.sum })
                }
            })
            .collect::<Vec<_>>();
        let metadata = ColumnMetadata {
            label: Some(aggregate_name.clone()),
            unit: value_column.metadata.unit.clone(),
        };
        validate_metadata(&metadata)?;
        let aggregate_column = Column {
            name: aggregate_name,
            kind: ColumnType::Float64,
            values: aggregate_values,
            raw_text: vec![None; groups.len()],
            categories: Vec::new(),
            metadata,
        };
        *self = Self {
            columns: vec![key_output, aggregate_column],
            rows: groups.len(),
        };
        Ok(())
    }

    /// Standalone command form of grouped sum/mean. Keep the established
    /// `group, n, sum|mean` output shape while using typed keys and values.
    pub(crate) fn group_numeric_operation(
        &mut self,
        group_name: &str,
        value_name: &str,
        mean: bool,
        job: &Job,
    ) -> Result<()> {
        let group_index = self.column_index(group_name)?;
        let value_index = self.column_index(value_name)?;
        let group_column = &self.columns[group_index];
        let value_column = &self.columns[value_index];
        if !matches!(
            value_column.kind,
            ColumnType::Int64 | ColumnType::UInt64 | ColumnType::Float64
        ) {
            return Err(Error::input(format!(
                "Aggregation column {value_name:?} must have a numeric type"
            )));
        }
        let aggregate_name = if mean { "mean" } else { "sum" };
        if [group_name, "n", aggregate_name]
            .iter()
            .enumerate()
            .any(|(index, name)| {
                [group_name, "n", aggregate_name]
                    .iter()
                    .skip(index + 1)
                    .any(|other| name == other)
            })
        {
            return Err(Error::input(
                "Group aggregation output names conflict with the group column",
            ));
        }

        let mut counts = HashMap::<String, i64>::new();
        for row in 0..self.rows {
            job.check()?;
            if scalar_to_f64(value_column, &value_column.values[row])?.is_some() {
                let key = typed_join_key(group_column, &group_column.values[row])?
                    .unwrap_or_else(|| "n:".into());
                let count = counts.entry(key).or_default();
                *count = count.checked_add(1).ok_or_else(|| {
                    Error::new(
                        ErrorKind::Limit,
                        "Grouped value count exceeds the supported range",
                    )
                })?;
            }
        }

        self.group_numeric(group_name, value_name, mean, job)?;
        let group = self.columns[0].clone();
        let mut order = (0..self.rows).collect::<Vec<_>>();
        order.sort_by(|left, right| {
            let label = |row: usize| {
                scalar_text(
                    &group.values[row],
                    &group.categories,
                    group.raw_text[row].as_deref(),
                )
            };
            label(*left).cmp(&label(*right))
        });
        self.reorder_rows(&order, job)?;

        let group = self.columns[0].clone();
        let mut aggregate = self.columns[1].clone();
        let counts = group
            .values
            .iter()
            .map(|value| {
                let key = typed_join_key(&group, value)?.unwrap_or_else(|| "n:".into());
                Ok(Scalar::Int64(*counts.get(&key).unwrap_or(&0)))
            })
            .collect::<Result<Vec<_>>>()?;
        let count_column = Column {
            name: "n".into(),
            kind: ColumnType::Int64,
            values: counts,
            raw_text: vec![None; self.rows],
            categories: Vec::new(),
            metadata: ColumnMetadata {
                label: Some("n".into()),
                unit: None,
            },
        };
        aggregate.name = aggregate_name.into();
        aggregate.metadata.label = Some(aggregate_name.into());
        validate_metadata(&aggregate.metadata)?;
        self.columns = vec![group, count_column, aggregate];
        Ok(())
    }

    /// Build the descriptive grouped-statistics result using typed key and
    /// numeric columns while retaining the established output header contract.
    pub(crate) fn group_summary(
        &self,
        group_name: &str,
        value_name: &str,
        job: &Job,
    ) -> Result<Self> {
        struct Summary {
            key: Scalar,
            raw_text: Option<String>,
            n: i64,
            missing: i64,
            mean: f64,
            m2: f64,
            min: f64,
            max: f64,
        }

        let group_column = &self.columns[self.column_index(group_name)?];
        let value_column = &self.columns[self.column_index(value_name)?];
        if !matches!(
            value_column.kind,
            ColumnType::Null | ColumnType::Int64 | ColumnType::UInt64 | ColumnType::Float64
        ) {
            return Err(Error::input(format!(
                "Summary column {value_name:?} must have a numeric type"
            )));
        }

        let mut lookup = HashMap::<String, usize>::new();
        let mut groups = Vec::<Summary>::new();
        for row in 0..self.rows {
            job.check()?;
            let key_value = &group_column.values[row];
            let key = typed_join_key(group_column, key_value)?.unwrap_or_else(|| "n:".into());
            let group_index = if let Some(index) = lookup.get(&key) {
                *index
            } else {
                if groups.len() >= MAX_CELLS / 7 {
                    return Err(Error::new(
                        ErrorKind::Limit,
                        "Grouped summary exceeds the typed cell limit",
                    ));
                }
                let index = groups.len();
                lookup.insert(key, index);
                groups.push(Summary {
                    key: key_value.clone(),
                    raw_text: group_column.raw_text[row].clone(),
                    n: 0,
                    missing: 0,
                    mean: 0.0,
                    m2: 0.0,
                    min: f64::INFINITY,
                    max: f64::NEG_INFINITY,
                });
                index
            };
            let group = &mut groups[group_index];
            let Some(value) = scalar_to_f64(value_column, &value_column.values[row])? else {
                group.missing = group.missing.checked_add(1).ok_or_else(|| {
                    Error::new(
                        ErrorKind::Limit,
                        "Missing-value count exceeds the supported range",
                    )
                })?;
                continue;
            };
            let next_n = group.n.checked_add(1).ok_or_else(|| {
                Error::new(ErrorKind::Limit, "Group count exceeds the supported range")
            })?;
            let delta = value - group.mean;
            group.mean += delta / next_n as f64;
            group.m2 += delta * (value - group.mean);
            group.min = group.min.min(value);
            group.max = group.max.max(value);
            if !group.mean.is_finite() || !group.m2.is_finite() {
                return Err(Error::input("Numerical overflow in grouped summary"));
            }
            group.n = next_n;
        }

        let mut order = (0..groups.len()).collect::<Vec<_>>();
        order.sort_by(|left, right| {
            let label = |index: usize| {
                let group = &groups[index];
                scalar_text(
                    &group.key,
                    &group_column.categories,
                    group.raw_text.as_deref(),
                )
            };
            label(*left).cmp(&label(*right))
        });
        let groups = order
            .iter()
            .map(|index| &groups[*index])
            .collect::<Vec<_>>();

        let mut group_output = group_column.clone();
        group_output.name = "group".into();
        group_output.values = groups.iter().map(|group| group.key.clone()).collect();
        group_output.raw_text = groups.iter().map(|group| group.raw_text.clone()).collect();
        let count_column = |name: &str, select: fn(&Summary) -> i64| Column {
            name: name.into(),
            kind: ColumnType::Int64,
            values: groups
                .iter()
                .map(|group| Scalar::Int64(select(group)))
                .collect(),
            raw_text: vec![None; groups.len()],
            categories: Vec::new(),
            metadata: ColumnMetadata {
                label: Some(name.into()),
                unit: None,
            },
        };
        let numeric_column =
            |name: &str, unit: Option<String>, select: fn(&Summary) -> Option<f64>| Column {
                name: name.into(),
                kind: ColumnType::Float64,
                values: groups
                    .iter()
                    .map(|group| select(group).map_or(Scalar::Null, Scalar::Float64))
                    .collect(),
                raw_text: vec![None; groups.len()],
                categories: Vec::new(),
                metadata: ColumnMetadata {
                    label: Some(name.into()),
                    unit,
                },
            };
        let columns = vec![
            group_output,
            count_column("n", |group| group.n),
            count_column("missing", |group| group.missing),
            numeric_column("mean", value_column.metadata.unit.clone(), |group| {
                (group.n > 0).then_some(group.mean)
            }),
            numeric_column("sample_sd", value_column.metadata.unit.clone(), |group| {
                (group.n > 1).then(|| (group.m2 / (group.n - 1) as f64).max(0.0).sqrt())
            }),
            numeric_column("min", value_column.metadata.unit.clone(), |group| {
                (group.n > 0).then_some(group.min)
            }),
            numeric_column("max", value_column.metadata.unit.clone(), |group| {
                (group.n > 0).then_some(group.max)
            }),
        ];
        Ok(Self {
            columns,
            rows: groups.len(),
        })
    }

    /// Stable, ascending sort using the typed value; nulls always sort after values.
    pub(crate) fn sort_by(&mut self, name: &str, job: &Job) -> Result<()> {
        self.sort_by_direction(name, false, job)
    }

    /// Stable typed sort that keeps nulls last in both directions.
    pub(crate) fn sort_by_direction(
        &mut self,
        name: &str,
        descending: bool,
        job: &Job,
    ) -> Result<()> {
        let index = self.column_index(name)?;
        let column = &self.columns[index];
        validate_datetime_ordering(column)?;
        let mut order = (0..self.rows).collect::<Vec<_>>();
        let mut cancelled = false;
        let mut comparison_error = None;
        order.sort_by(|left, right| {
            if job.check().is_err() {
                cancelled = true;
                return std::cmp::Ordering::Equal;
            }
            let left_value = &column.values[*left];
            let right_value = &column.values[*right];
            match (left_value, right_value) {
                (Scalar::Null, Scalar::Null) => std::cmp::Ordering::Equal,
                (Scalar::Null, _) => std::cmp::Ordering::Greater,
                (_, Scalar::Null) => std::cmp::Ordering::Less,
                _ => match compare_scalar(column, left_value, right_value) {
                    Ok(ordering) if descending => ordering.reverse(),
                    Ok(ordering) => ordering,
                    Err(error) => {
                        if comparison_error.is_none() {
                            comparison_error = Some(error);
                        }
                        std::cmp::Ordering::Equal
                    }
                },
            }
        });
        if cancelled {
            job.check()?;
        }
        if let Some(error) = comparison_error {
            return Err(error);
        }
        self.reorder_rows(&order, job)
    }

    pub(crate) fn select_columns(&mut self, names: &[String], job: &Job) -> Result<()> {
        if names.is_empty() {
            return Err(Error::input("Select at least one column"));
        }
        let mut selected = Vec::with_capacity(names.len());
        for name in names {
            job.check()?;
            let index = self.column_index(name)?;
            if selected.contains(&index) {
                return Err(Error::input("A column was selected more than once"));
            }
            selected.push(index);
        }
        let old = std::mem::take(&mut self.columns);
        let mut slots = old.into_iter().map(Some).collect::<Vec<_>>();
        self.columns = selected
            .into_iter()
            .map(|index| slots[index].take().expect("selected columns are unique"))
            .collect();
        Ok(())
    }

    pub(crate) fn drop_missing(&mut self, name: &str, job: &Job) -> Result<()> {
        let index = self.column_index(name)?;
        let mut keep = Vec::with_capacity(self.rows);
        for value in &self.columns[index].values {
            job.check()?;
            keep.push(!matches!(value, Scalar::Null));
        }
        self.retain_rows(&keep, job)
    }

    /// Fill null cells with one literal parsed using the column's existing type.
    /// The source spelling is retained for delimited output, while Parquet keeps
    /// the original Arrow type and nullability semantics.
    pub(crate) fn fill_missing(&mut self, name: &str, literal: &str, job: &Job) -> Result<()> {
        if is_missing(literal) {
            return Err(Error::input("Replacement value must not be blank"));
        }
        let index = self.column_index(name)?;
        let column = &mut self.columns[index];
        let mut category_ids = HashMap::with_capacity(column.categories.len());
        for (index, category) in column.categories.iter().enumerate() {
            let id = u32::try_from(index)
                .map_err(|_| Error::new(ErrorKind::Limit, "Too many categories"))?;
            category_ids.insert(category.clone(), id);
        }
        let mut categories = column.categories.clone();
        let replacement = convert_cell(
            &column.name,
            column.kind,
            literal,
            &mut categories,
            &mut category_ids,
        )?;
        if matches!(&replacement, Scalar::Null) {
            return Err(Error::input("Replacement value must not be blank"));
        }
        for row in 0..self.rows {
            job.check()?;
            if matches!(&column.values[row], Scalar::Null) {
                column.values[row] = replacement.clone();
                column.raw_text[row] = Some(literal.to_owned());
            }
        }
        column.categories = categories;
        Ok(())
    }

    /// Propagate the nearest non-null scalar forward or backward without
    /// changing the column type or the original order of any non-null values.
    pub(crate) fn fill_direction(&mut self, name: &str, forward: bool, job: &Job) -> Result<()> {
        let index = self.column_index(name)?;
        let column = &mut self.columns[index];
        let mut previous: Option<(Scalar, Option<String>)> = None;
        if forward {
            for row in 0..self.rows {
                job.check()?;
                if matches!(&column.values[row], Scalar::Null) {
                    if let Some((value, raw)) = &previous {
                        column.values[row] = value.clone();
                        column.raw_text[row] = raw.clone();
                    }
                } else {
                    previous = Some((column.values[row].clone(), column.raw_text[row].clone()));
                }
            }
        } else {
            for row in (0..self.rows).rev() {
                job.check()?;
                if matches!(&column.values[row], Scalar::Null) {
                    if let Some((value, raw)) = &previous {
                        column.values[row] = value.clone();
                        column.raw_text[row] = raw.clone();
                    }
                } else {
                    previous = Some((column.values[row].clone(), column.raw_text[row].clone()));
                }
            }
        }
        Ok(())
    }

    /// Extract finite numeric observations for statistics while preserving null
    /// semantics. Integer-to-float conversion is limited to the exact f64 range
    /// so statistics never silently merge distinct large integer values.
    pub(crate) fn numeric_values(&self, name: &str, job: &Job) -> Result<Vec<f64>> {
        Ok(self
            .numeric_values_with_nulls(name, job)?
            .into_iter()
            .flatten()
            .collect())
    }

    pub(crate) fn drop_rows_any_missing(&mut self, job: &Job) -> Result<()> {
        let mut keep = vec![true; self.rows];
        for column in &self.columns {
            for (row, value) in column.values.iter().enumerate() {
                job.check()?;
                if matches!(value, Scalar::Null) {
                    keep[row] = false;
                }
            }
        }
        self.retain_rows(&keep, job)
    }

    pub(crate) fn head_rows(&mut self, count: usize, job: &Job) -> Result<()> {
        if count > MAX_CELLS {
            return Err(Error::new(
                ErrorKind::Limit,
                "Row window exceeds the 500,000-row limit",
            ));
        }
        let count = count.min(self.rows);
        self.reorder_rows(&(0..count).collect::<Vec<_>>(), job)
    }

    pub(crate) fn tail_rows(&mut self, count: usize, job: &Job) -> Result<()> {
        if count > MAX_CELLS {
            return Err(Error::new(
                ErrorKind::Limit,
                "Row window exceeds the 500,000-row limit",
            ));
        }
        let start = self.rows.saturating_sub(count);
        self.reorder_rows(&(start..self.rows).collect::<Vec<_>>(), job)
    }

    pub(crate) fn reverse_rows(&mut self, job: &Job) -> Result<()> {
        self.reorder_rows(&(0..self.rows).rev().collect::<Vec<_>>(), job)
    }

    /// Keep the first occurrence of each typed key. Nulls compare equal for
    /// deduplication; categorical values compare by label, not dictionary id.
    pub(crate) fn deduplicate_rows(&mut self, names: Option<&[String]>, job: &Job) -> Result<()> {
        let selected = match names {
            Some([]) => {
                return Err(Error::input("Select at least one duplicate key column"));
            }
            Some(names) => {
                let mut indexes = Vec::with_capacity(names.len());
                for name in names {
                    let index = self.column_index(name)?;
                    if indexes.contains(&index) {
                        return Err(Error::input("A duplicate key column was selected twice"));
                    }
                    indexes.push(index);
                }
                indexes
            }
            None => (0..self.columns.len()).collect(),
        };
        let mut buckets = HashMap::<u64, Vec<usize>>::new();
        let hash_builder = std::collections::hash_map::RandomState::new();
        let mut keep = vec![false; self.rows];
        for (row, keep_row) in keep.iter_mut().enumerate() {
            job.check()?;
            let mut hasher = hash_builder.build_hasher();
            for &column_index in &selected {
                let column = &self.columns[column_index];
                typed_join_key(column, &column.values[row])?.hash(&mut hasher);
            }
            let hash = hasher.finish();
            let candidates = buckets.entry(hash).or_default();
            let duplicate = candidates.iter().copied().any(|candidate| {
                selected.iter().all(|&column_index| {
                    self.columns[column_index].values[row]
                        == self.columns[column_index].values[candidate]
                })
            });
            if !duplicate {
                *keep_row = true;
                candidates.push(row);
            }
        }
        self.retain_rows(&keep, job)
    }

    fn column_index(&self, name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|column| column.name == name)
            .ok_or_else(|| Error::input(format!("Column {name:?} not found")))
    }

    fn retain_rows(&mut self, keep: &[bool], job: &Job) -> Result<()> {
        if keep.len() != self.rows {
            return Err(Error::input("Typed row selection has an invalid length"));
        }
        for column in &mut self.columns {
            let mut values = Vec::with_capacity(keep.iter().filter(|value| **value).count());
            let mut raw_text = Vec::with_capacity(values.capacity());
            for (index, (value, raw)) in column
                .values
                .drain(..)
                .zip(column.raw_text.drain(..))
                .enumerate()
            {
                job.check()?;
                if keep[index] {
                    values.push(value);
                    raw_text.push(raw);
                }
            }
            column.values = values;
            column.raw_text = raw_text;
        }
        self.rows = keep.iter().filter(|retain| **retain).count();
        Ok(())
    }

    fn reorder_rows(&mut self, order: &[usize], job: &Job) -> Result<()> {
        if order.iter().any(|index| *index >= self.rows) {
            return Err(Error::input("Typed row order contains an invalid index"));
        }
        for column in &mut self.columns {
            let old = std::mem::take(&mut column.values);
            let mut slots = old.into_iter().map(Some).collect::<Vec<_>>();
            let old_missing = std::mem::take(&mut column.raw_text);
            let mut missing_slots = old_missing.into_iter().map(Some).collect::<Vec<_>>();
            let mut reordered = Vec::with_capacity(order.len());
            let mut reordered_missing = Vec::with_capacity(order.len());
            for index in order {
                job.check()?;
                reordered.push(
                    slots[*index].take().ok_or_else(|| {
                        Error::input("Typed row order contains a duplicate index")
                    })?,
                );
                reordered_missing.push(
                    missing_slots[*index].take().ok_or_else(|| {
                        Error::input("Typed row order contains a duplicate index")
                    })?,
                );
            }
            column.values = reordered;
            column.raw_text = reordered_missing;
        }
        self.rows = order.len();
        Ok(())
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

    /// Apply a typed, dataset-wide table query and return only its requested page.
    pub(crate) fn workspace_query_report(
        &mut self,
        query: WorkspaceQuery,
        job: &Job,
    ) -> Result<serde_json::Value> {
        let source_rows = self.rows;
        for filter in &query.filters {
            job.check()?;
            self.filter_compare(&filter.column, filter.comparison, &filter.value, job)?;
        }
        if let Some(sort) = &query.sort {
            job.check()?;
            self.sort_by_direction(&sort.column, sort.descending, job)?;
        }
        let limit = query.limit.unwrap_or(MAX_WORKSPACE_PAGE_ROWS);
        let mut report = self.workspace_page_report(query.offset, limit, job)?;
        let object = report
            .as_object_mut()
            .ok_or_else(|| Error::input("Unable to construct table query report"))?;
        object.insert("source_rows".into(), serde_json::json!(source_rows));
        object.insert(
            "dataset_filtered".into(),
            serde_json::json!(!query.filters.is_empty()),
        );
        object.insert(
            "dataset_sort_column".into(),
            query.sort.as_ref().map_or(serde_json::Value::Null, |sort| {
                serde_json::json!(sort.column)
            }),
        );
        object.insert(
            "dataset_sort_descending".into(),
            serde_json::json!(query.sort.as_ref().is_some_and(|sort| sort.descending)),
        );
        Ok(report)
    }

    fn workspace_page_report(
        &self,
        offset: usize,
        limit: usize,
        job: &Job,
    ) -> Result<serde_json::Value> {
        let row_offset = offset.min(self.rows);
        let displayed_rows = limit.min(self.rows.saturating_sub(row_offset));
        let row_end = row_offset + displayed_rows;
        let mut rows = Vec::with_capacity(displayed_rows);
        for row in row_offset..row_end {
            job.check()?;
            rows.push(
                self.columns
                    .iter()
                    .map(|column| match &column.values[row] {
                        Scalar::Null => serde_json::Value::Null,
                        Scalar::Boolean(value) => serde_json::json!(value),
                        Scalar::Int64(value) => serde_json::json!(value),
                        Scalar::UInt64(value) => serde_json::json!(value),
                        Scalar::Float64(value) => serde_json::json!(value),
                        Scalar::Date(date) => serde_json::json!(format!(
                            "{:04}-{:02}-{:02}",
                            date.year, date.month, date.day
                        )),
                        Scalar::Time(time) => serde_json::json!(format_time(*time)),
                        Scalar::DateTime(datetime) => {
                            serde_json::json!(format_datetime(*datetime))
                        }
                        Scalar::Categorical(index) => serde_json::json!(bounded_workspace_text(
                            column
                                .categories
                                .get(*index as usize)
                                .map(String::as_str)
                                .unwrap_or("")
                        )),
                        Scalar::String(value) => serde_json::json!(bounded_workspace_text(value)),
                    })
                    .collect::<Vec<_>>(),
            );
        }
        let mut schema = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            let mut missing = 0usize;
            for value in &column.values {
                job.check()?;
                missing += usize::from(matches!(value, Scalar::Null));
            }
            schema.push(serde_json::json!({
                "name": column.name,
                "type": column.kind,
                "nullable": missing > 0,
                "missing": missing,
                "label": column.metadata.label,
                "unit": column.metadata.unit,
            }));
        }
        let report = serde_json::json!({
            "workspace_version": 1,
            "rows": rows,
            "total_rows": self.rows,
            "row_offset": row_offset,
            "displayed_rows": displayed_rows,
            "truncated": row_end < self.rows,
            "headers": self.columns.iter().map(|column| &column.name).collect::<Vec<_>>(),
            "schema": schema,
        });
        if serde_json::to_vec(&report)
            .map_err(|error| Error::input(error.to_string()))?
            .len()
            > 4 * 1024 * 1024
        {
            return Err(Error::new(
                ErrorKind::Limit,
                "Native table preview exceeds the 4 MiB payload limit",
            ));
        }
        Ok(report)
    }

    pub(crate) fn preview_report(&self) -> serde_json::Value {
        let preview = (0..self.rows.min(30))
            .map(|row| {
                self.columns
                    .iter()
                    .map(|column| match &column.values[row] {
                        Scalar::String(value) => {
                            column.raw_text[row].as_ref().unwrap_or(value).clone()
                        }
                        Scalar::Categorical(index) => column.raw_text[row]
                            .as_ref()
                            .or_else(|| column.categories.get(*index as usize))
                            .cloned()
                            .unwrap_or_default(),
                        value => {
                            scalar_text(value, &column.categories, column.raw_text[row].as_deref())
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "rows": self.rows,
            "headers": self.columns.iter().map(|column| &column.name).collect::<Vec<_>>(),
            "preview": preview,
            "schema": self.schema_report(),
        })
    }

    /// Return a numeric column as one optional value per row. The declared
    /// physical type is checked before reading values, nulls remain absent,
    /// and integers outside float64's exact range are rejected.
    pub(crate) fn numeric_values_with_nulls(
        &self,
        name: &str,
        job: &Job,
    ) -> Result<Vec<Option<f64>>> {
        let index = self.column_index(name)?;
        let column = &self.columns[index];
        if !matches!(
            column.kind,
            ColumnType::Int64 | ColumnType::UInt64 | ColumnType::Float64
        ) {
            return Err(Error::input(format!(
                "Statistical input column {name:?} must have a numeric type"
            )));
        }
        column
            .values
            .iter()
            .map(|value| {
                job.check()?;
                scalar_to_f64(column, value)
            })
            .collect()
    }

    /// Return row-wise complete numeric pairs while preserving source row
    /// alignment until null exclusion has been applied.
    pub(crate) fn numeric_pairs(
        &self,
        left_name: &str,
        right_name: &str,
        job: &Job,
    ) -> Result<(Vec<f64>, Vec<f64>)> {
        let left = self.numeric_values_with_nulls(left_name, job)?;
        let right = self.numeric_values_with_nulls(right_name, job)?;
        let mut left_values = Vec::with_capacity(left.len().min(right.len()));
        let mut right_values = Vec::with_capacity(left.len().min(right.len()));
        for (left, right) in left.into_iter().zip(right) {
            job.check()?;
            if let (Some(left), Some(right)) = (left, right) {
                left_values.push(left);
                right_values.push(right);
            }
        }
        Ok((left_values, right_values))
    }

    /// Group finite numeric values by typed keys. Empty text group labels and
    /// null labels are excluded to match the existing statistical contract.
    pub(crate) fn grouped_numeric_values(
        &self,
        group_name: &str,
        value_name: &str,
        job: &Job,
    ) -> Result<Vec<crate::statistics::GroupSample>> {
        let group_index = self.column_index(group_name)?;
        let group_column = &self.columns[group_index];
        let values = self.numeric_values_with_nulls(value_name, job)?;
        let mut groups = BTreeMap::<String, Vec<f64>>::new();
        for (row, value) in values.into_iter().enumerate() {
            job.check()?;
            let Some(value) = value else { continue };
            let key = &group_column.values[row];
            if matches!(key, Scalar::Null) {
                continue;
            }
            let label = scalar_text(
                key,
                &group_column.categories,
                group_column.raw_text[row].as_deref(),
            );
            if label.trim().is_empty() {
                continue;
            }
            groups.entry(label).or_default().push(value);
        }
        Ok(groups
            .into_iter()
            .map(|(label, values)| crate::statistics::GroupSample { label, values })
            .collect())
    }

    pub(crate) fn write_parquet(&self, path: &std::path::Path, job: &Job) -> Result<()> {
        let mut fields = Vec::with_capacity(self.columns.len());
        let mut arrays = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            job.check()?;
            let (data_type, array, mut metadata) = arrow_column(column)?;
            let nullable = column
                .values
                .iter()
                .any(|value| matches!(value, Scalar::Null));
            metadata.insert(
                "localwright.logical_type".into(),
                format!("{:?}", column.kind).to_ascii_lowercase(),
            );
            if let Some(label) = &column.metadata.label {
                metadata.insert("localwright.label".into(), label.clone());
            }
            if let Some(unit) = &column.metadata.unit {
                metadata.insert("localwright.unit".into(), unit.clone());
            }
            fields.push(Field::new(&column.name, data_type, nullable).with_metadata(metadata));
            arrays.push(array);
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = arrow_array::RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|error| Error::input(error.to_string()))?;
        job.check()?;
        write_new(path, |file| {
            let mut writer = ArrowWriter::try_new(file, schema, None)
                .map_err(|error| Error::input(error.to_string()))?;
            writer
                .write(&batch)
                .map_err(|error| Error::input(error.to_string()))?;
            job.check()?;
            writer
                .close()
                .map_err(|error| Error::input(error.to_string()))?;
            Ok(())
        })
    }

    /// Write typed processing results as delimited text. Original spellings are retained where
    /// they differ from canonical typed serialization. True nulls serialize as empty fields;
    /// empty strings and nulls still cannot be distinguished after reimport.
    pub(crate) fn write_delimited(
        &self,
        path: &std::path::Path,
        delimiter: u8,
        job: &Job,
    ) -> Result<()> {
        write_new(path, |file| {
            let mut writer = csv::WriterBuilder::new()
                .delimiter(delimiter)
                .from_writer(file);
            writer
                .write_record(self.columns.iter().map(|column| safe_text(&column.name)))
                .map_err(|error| Error::input(error.to_string()))?;
            for row in 0..self.rows {
                job.check()?;
                let values = self
                    .columns
                    .iter()
                    .map(|column| {
                        scalar_text(
                            &column.values[row],
                            &column.categories,
                            column.raw_text[row].as_deref(),
                        )
                    })
                    .collect::<Vec<_>>();
                writer
                    .write_record(values)
                    .map_err(|error| Error::input(error.to_string()))?;
            }
            writer.flush()?;
            Ok(())
        })
    }
}

fn scalar_text(value: &Scalar, categories: &[String], raw_text: Option<&str>) -> String {
    match value {
        Scalar::Null => raw_text.unwrap_or_default().to_owned(),
        Scalar::Boolean(value) => raw_text.map_or_else(|| value.to_string(), str::to_owned),
        Scalar::Int64(value) => raw_text.map_or_else(|| value.to_string(), str::to_owned),
        Scalar::UInt64(value) => raw_text.map_or_else(|| value.to_string(), str::to_owned),
        Scalar::Float64(value) => raw_text.map_or_else(|| value.to_string(), str::to_owned),
        Scalar::Date(date) => raw_text.map_or_else(
            || format!("{:04}-{:02}-{:02}", date.year, date.month, date.day),
            str::to_owned,
        ),
        Scalar::Time(time) => raw_text.map_or_else(|| format_time(*time), str::to_owned),
        Scalar::DateTime(datetime) => {
            raw_text.map_or_else(|| format_datetime(*datetime), str::to_owned)
        }
        Scalar::Categorical(index) => categories
            .get(*index as usize)
            .map(|value| safe_text(raw_text.unwrap_or(value)))
            .unwrap_or_default(),
        Scalar::String(value) => safe_text(raw_text.unwrap_or(value)),
    }
}

fn preserved_raw_text(value: &str, scalar: &Scalar) -> Option<String> {
    let canonical = match scalar {
        Scalar::Null => return (!value.is_empty()).then(|| value.to_owned()),
        Scalar::Boolean(value) => value.to_string(),
        Scalar::Int64(value) => value.to_string(),
        Scalar::UInt64(value) => value.to_string(),
        Scalar::Float64(value) => value.to_string(),
        Scalar::Date(date) => format!("{:04}-{:02}-{:02}", date.year, date.month, date.day),
        Scalar::Time(time) => format_time(*time),
        Scalar::DateTime(datetime) => format_datetime(*datetime),
        Scalar::Categorical(_) | Scalar::String(_) => return None,
    };
    (canonical != value).then(|| value.to_owned())
}

fn safe_text(value: &str) -> String {
    if value.trim_start().starts_with(['=', '+', '-', '@']) || value.starts_with(['\t', '\r', '\n'])
    {
        format!("'{value}")
    } else {
        value.to_owned()
    }
}

fn safe_text_len(value: &str) -> usize {
    value.len()
        + if value.trim_start().starts_with(['=', '+', '-', '@'])
            || value.starts_with(['\t', '\r', '\n'])
        {
            1
        } else {
            0
        }
}

fn arrow_column(column: &Column) -> Result<(DataType, ArrayRef, HashMap<String, String>)> {
    let mut metadata = HashMap::new();
    let (data_type, array): (DataType, ArrayRef) = match column.kind {
        ColumnType::Null => {
            metadata.insert("localwright.logical_type".into(), "null".into());
            (
                DataType::Utf8,
                Arc::new(StringArray::from(vec![None::<&str>; column.values.len()])),
            )
        }
        ColumnType::Boolean => (
            DataType::Boolean,
            Arc::new(BooleanArray::from(
                column
                    .values
                    .iter()
                    .map(|value| match value {
                        Scalar::Null => None,
                        Scalar::Boolean(value) => Some(*value),
                        _ => unreachable!("typed scalar matches its column kind"),
                    })
                    .collect::<Vec<_>>(),
            )),
        ),
        ColumnType::Int64 => (
            DataType::Int64,
            Arc::new(Int64Array::from(
                column
                    .values
                    .iter()
                    .map(|value| match value {
                        Scalar::Null => None,
                        Scalar::Int64(value) => Some(*value),
                        _ => unreachable!("typed scalar matches its column kind"),
                    })
                    .collect::<Vec<_>>(),
            )),
        ),
        ColumnType::UInt64 => (
            DataType::UInt64,
            Arc::new(UInt64Array::from(
                column
                    .values
                    .iter()
                    .map(|value| match value {
                        Scalar::Null => None,
                        Scalar::UInt64(value) => Some(*value),
                        _ => unreachable!("typed scalar matches its column kind"),
                    })
                    .collect::<Vec<_>>(),
            )),
        ),
        ColumnType::Float64 => (
            DataType::Float64,
            Arc::new(Float64Array::from(
                column
                    .values
                    .iter()
                    .map(|value| match value {
                        Scalar::Null => None,
                        Scalar::Float64(value) => Some(*value),
                        _ => unreachable!("typed scalar matches its column kind"),
                    })
                    .collect::<Vec<_>>(),
            )),
        ),
        ColumnType::Date => (
            DataType::Date32,
            Arc::new(Date32Array::from(
                column
                    .values
                    .iter()
                    .map(|value| match value {
                        Scalar::Null => None,
                        Scalar::Date(date) => Some(date.days_since_epoch()),
                        _ => unreachable!("typed scalar matches its column kind"),
                    })
                    .collect::<Vec<_>>(),
            )),
        ),
        ColumnType::Time => (
            DataType::Time64(TimeUnit::Nanosecond),
            Arc::new(Time64NanosecondArray::from(
                column
                    .values
                    .iter()
                    .map(|value| match value {
                        Scalar::Null => None,
                        Scalar::Time(time) => Some(time.nanoseconds_since_midnight()),
                        _ => unreachable!("typed scalar matches its column kind"),
                    })
                    .collect::<Vec<_>>(),
            )),
        ),
        ColumnType::DateTime => {
            let offsets = column
                .values
                .iter()
                .filter_map(|value| match value {
                    Scalar::DateTime(value) => Some(value.offset_minutes.is_some()),
                    _ => None,
                })
                .collect::<std::collections::HashSet<_>>();
            if offsets.len() > 1 {
                return Err(Error::input(format!(
                    "Datetime column {:?} mixes timezone-aware and timezone-naive values",
                    column.name
                )));
            }
            let aware = offsets.contains(&true);
            let values = column
                .values
                .iter()
                .map(|value| match value {
                    Scalar::Null => Ok(None),
                    Scalar::DateTime(value) => value.nanoseconds_since_epoch().map(Some),
                    _ => unreachable!("typed scalar matches its column kind"),
                })
                .collect::<Result<Vec<_>>>()?;
            metadata.insert(
                "localwright.datetime_semantics".into(),
                if aware {
                    "offset-aware values normalized to UTC"
                } else {
                    "timezone-naive wall-clock values"
                }
                .into(),
            );
            let array = TimestampNanosecondArray::from(values);
            if aware {
                (
                    DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+00:00"))),
                    Arc::new(array.with_timezone("+00:00")),
                )
            } else {
                (
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    Arc::new(array),
                )
            }
        }
        ColumnType::Categorical => {
            let keys = column
                .values
                .iter()
                .map(|value| match value {
                    Scalar::Null => Ok(None),
                    Scalar::Categorical(key) => i32::try_from(*key).map(Some).map_err(|_| {
                        Error::new(
                            ErrorKind::Limit,
                            "Categorical dictionary exceeds Int32 keys",
                        )
                    }),
                    _ => unreachable!("typed scalar matches its column kind"),
                })
                .collect::<Result<Vec<_>>>()?;
            let dictionary = DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(keys),
                Arc::new(StringArray::from(column.categories.clone())),
            )
            .map_err(|error| Error::input(error.to_string()))?;
            (dictionary.data_type().clone(), Arc::new(dictionary))
        }
        ColumnType::String => (
            DataType::Utf8,
            Arc::new(StringArray::from(
                column
                    .values
                    .iter()
                    .map(|value| match value {
                        Scalar::Null => None,
                        Scalar::String(value) => Some(value.as_str()),
                        _ => unreachable!("typed scalar matches its column kind"),
                    })
                    .collect::<Vec<_>>(),
            )),
        ),
    };
    Ok((data_type, array, metadata))
}

fn infer_type(cells: &[&str]) -> ColumnType {
    let mut inference = TypeInference::default();
    for value in cells {
        inference.observe(value);
    }
    inference.finish()
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

fn bounded_workspace_text(value: &str) -> String {
    let mut characters = value.chars();
    let text = characters
        .by_ref()
        .take(MAX_WORKSPACE_TEXT_CHARS)
        .collect::<String>();
    if characters.next().is_some() {
        format!("{text}…")
    } else {
        text
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
        values.push(convert_cell(
            name,
            kind,
            cell,
            &mut categories,
            &mut category_ids,
        )?);
    }
    Ok((values, categories))
}

fn convert_cell(
    name: &str,
    kind: ColumnType,
    cell: &str,
    categories: &mut Vec<String>,
    category_ids: &mut HashMap<String, u32>,
) -> Result<Scalar> {
    if is_missing(cell) {
        return Ok(Scalar::Null);
    }
    let invalid = || {
        Error::input(format!(
            "Column {name:?} contains a value incompatible with {kind:?}"
        ))
    };
    Ok(match kind {
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
        ColumnType::DateTime => Scalar::DateTime(parse_datetime(cell.trim()).ok_or_else(invalid)?),
        ColumnType::Categorical => {
            let id = if let Some(id) = category_ids.get(cell) {
                *id
            } else {
                let id = u32::try_from(categories.len())
                    .map_err(|_| Error::new(ErrorKind::Limit, "Too many categories"))?;
                categories.push(cell.to_owned());
                category_ids.insert(cell.to_owned(), id);
                id
            };
            Scalar::Categorical(id)
        }
        ColumnType::String => Scalar::String(cell.to_owned()),
    })
}

fn xlsx_scalar(
    name: &str,
    kind: ColumnType,
    cell: &calamine::Data,
    categories: &mut Vec<String>,
    category_ids: &mut HashMap<String, u32>,
) -> Result<Scalar> {
    use calamine::Data;

    if matches!(cell, Data::Empty) {
        return Ok(Scalar::Null);
    }
    if let Data::Error(error) = cell {
        return Err(Error::input(format!("Excel cell error: {error}")));
    }
    if let Data::String(value) = cell {
        if value.is_empty() && matches!(kind, ColumnType::String | ColumnType::Categorical) {
            if kind == ColumnType::String {
                return Ok(Scalar::String(String::new()));
            }
            let id = if let Some(id) = category_ids.get(value) {
                *id
            } else {
                let id = u32::try_from(categories.len())
                    .map_err(|_| Error::new(ErrorKind::Limit, "Too many categories"))?;
                categories.push(value.clone());
                category_ids.insert(value.clone(), id);
                id
            };
            return Ok(Scalar::Categorical(id));
        }
    }
    let token = match cell {
        Data::DateTime(value) if value.is_duration() => value.to_string(),
        Data::DateTime(value) => {
            let (year, month, day, hour, minute, second, millis) = value.to_ymd_hms_milli();
            format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}")
        }
        Data::DateTimeIso(value) | Data::DurationIso(value) | Data::String(value) => value.clone(),
        Data::Float(value) if value.fract() == 0.0 => format!("{value:.1}"),
        _ => cell.to_string(),
    };

    if let Data::Int(value) = cell {
        match kind {
            ColumnType::UInt64 if *value >= 0 => return Ok(Scalar::UInt64(*value as u64)),
            ColumnType::UInt64 => {
                return Err(Error::input(format!(
                    "Column {name:?} contains a negative value"
                )))
            }
            ColumnType::Int64 => return Ok(Scalar::Int64(*value)),
            ColumnType::Float64 if value.unsigned_abs() <= (1u64 << 53) => {
                return Ok(Scalar::Float64(*value as f64))
            }
            ColumnType::Float64 => {
                return Err(Error::input(format!(
                    "Column {name:?} integer cannot be represented exactly as float64"
                )))
            }
            _ => {}
        }
    }
    if let Data::Float(value) = cell {
        if !value.is_finite() {
            return Err(Error::input(format!("Column {name:?} is not finite")));
        }
        match kind {
            ColumnType::Float64 => return Ok(Scalar::Float64(*value)),
            ColumnType::Int64
                if value.fract() == 0.0
                    && *value >= i64::MIN as f64
                    && *value < -(i64::MIN as f64) =>
            {
                let integer = *value as i64;
                if integer as f64 == *value {
                    return Ok(Scalar::Int64(integer));
                }
            }
            ColumnType::UInt64
                if value.fract() == 0.0 && *value >= 0.0 && *value < u64::MAX as f64 =>
            {
                let integer = *value as u64;
                if integer as f64 == *value {
                    return Ok(Scalar::UInt64(integer));
                }
            }
            _ => {}
        }
    }
    convert_cell(name, kind, &token, categories, category_ids)
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
    fn direct_delimited_loader_matches_legacy_typed_inference() {
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("typed.tsv");
        std::fs::write(
            &input,
            "id\twhen\tgroup\tnote\n2\t2024-02-29T12:30:00Z\ta\t\"quoted\ttext\"\n1\t\tb\t \n\t2024-03-01T00:00:00Z\ta\tlast\n",
        )
        .unwrap();
        let request = SchemaRequest::from_json(
            r#"{"overrides":{"group":"categorical"},"metadata":{"group":{"label":"Study group","unit":null}}}"#,
        )
        .unwrap();
        let job = Job::new();
        let legacy = crate::load(&input, None, &job).unwrap();
        let inferred = TypedTable::infer(&legacy, request.clone(), &job).unwrap();
        let direct = TypedTable::from_path(&input, None, request, &job).unwrap();
        assert_eq!(direct.schema_report(), inferred.schema_report());
        assert_eq!(direct.preview_report(), inferred.preview_report());
    }

    #[test]
    fn direct_xlsx_loader_preserves_native_numeric_types_and_sheet_selection() {
        let input = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/two-sheets.xlsx");
        let job = Job::new();
        let first = TypedTable::from_path(&input, None, SchemaRequest::default(), &job).unwrap();
        assert_eq!(first.schema_report()["columns"][0]["type"], "string");
        assert_eq!(first.schema_report()["columns"][1]["type"], "float64");
        assert_eq!(first.schema_report()["columns"][2]["type"], "float64");
        assert_eq!(first.preview_report()["preview"][0][1], "1");
        assert_eq!(first.preview_report()["preview"][1][2], "4");

        let selected =
            TypedTable::from_path(&input, Some("Second sheet"), SchemaRequest::default(), &job)
                .unwrap();
        assert_eq!(selected.schema_report()["columns"][1]["type"], "float64");
        assert_eq!(selected.preview_report()["preview"][0][1], "42");
    }

    #[test]
    fn direct_xlsx_loader_applies_typed_overrides() {
        let input = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/table.xlsx");
        let request =
            SchemaRequest::from_json(r#"{"overrides":{"x":"float64","group":"categorical"}}"#)
                .unwrap();
        let typed = TypedTable::from_path(&input, None, request, &Job::new()).unwrap();
        assert_eq!(typed.schema_report()["columns"][0]["type"], "categorical");
        assert_eq!(typed.schema_report()["columns"][1]["type"], "float64");
        assert_eq!(typed.preview_report()["preview"][0][1], "1");
    }

    #[test]
    fn workspace_page_report_bounds_rows_and_text_and_observes_cancellation() {
        let long_text = "x".repeat(1_024);
        let source = crate::validate(
            vec!["note".into()],
            (0..MAX_WORKSPACE_PAGE_ROWS + 1)
                .map(|_| vec![long_text.clone()])
                .collect(),
        )
        .unwrap();
        let typed = TypedTable::infer(&source, SchemaRequest::default(), &Job::new()).unwrap();
        let report = typed
            .workspace_page_report(0, MAX_WORKSPACE_PAGE_ROWS, &Job::new())
            .unwrap();
        assert_eq!(report["total_rows"], MAX_WORKSPACE_PAGE_ROWS + 1);
        assert_eq!(report["displayed_rows"], MAX_WORKSPACE_PAGE_ROWS);
        assert_eq!(report["truncated"], true);
        assert_eq!(
            report["rows"][0][0].as_str().unwrap().chars().count(),
            MAX_WORKSPACE_TEXT_CHARS + 1
        );

        let cancelled = Job::new();
        cancelled.cancel();
        assert!(typed
            .workspace_page_report(0, MAX_WORKSPACE_PAGE_ROWS, &cancelled)
            .is_err());
    }

    #[test]
    fn workspace_query_filters_and_sorts_entire_typed_dataset_before_paging() {
        let source = table(
            vec!["id", "score"],
            vec![
                vec!["1", "1"],
                vec!["2", "5"],
                vec!["3", "3"],
                vec!["4", " "],
                vec!["5", "4"],
                vec!["6", "2"],
            ],
        );
        let mut typed = TypedTable::infer(&source, SchemaRequest::default(), &Job::new()).unwrap();
        let query = WorkspaceQuery::from_json(
            r#"{"filters":[{"column":"score","comparison":"gte","value":"2"}],"sort":{"column":"score","descending":true},"offset":1,"limit":2}"#,
        )
        .unwrap();
        let report = typed.workspace_query_report(query, &Job::new()).unwrap();
        assert_eq!(report["source_rows"], 6);
        assert_eq!(report["total_rows"], 4);
        assert_eq!(report["row_offset"], 1);
        assert_eq!(report["displayed_rows"], 2);
        assert_eq!(report["dataset_filtered"], true);
        assert_eq!(report["dataset_sort_column"], "score");
        assert_eq!(report["rows"][0][0], 5);
        assert_eq!(report["rows"][1][0], 3);
    }

    #[test]
    fn descending_workspace_sort_keeps_nulls_after_values_and_offsets_clamp() {
        let source = table(
            vec!["id", "score"],
            vec![vec!["1", "1"], vec!["2", " "], vec!["3", "2"]],
        );
        let mut typed = TypedTable::infer(&source, SchemaRequest::default(), &Job::new()).unwrap();
        let query = WorkspaceQuery::from_json(
            r#"{"sort":{"column":"score","descending":true},"offset":1,"limit":2}"#,
        )
        .unwrap();
        let report = typed.workspace_query_report(query, &Job::new()).unwrap();
        assert_eq!(report["rows"][0][0], 1);
        assert_eq!(report["rows"][1][0], 2);

        let empty = WorkspaceQuery::from_json(r#"{"offset":99}"#).unwrap();
        let report = typed.workspace_query_report(empty, &Job::new()).unwrap();
        assert_eq!(report["row_offset"], 3);
        assert_eq!(report["displayed_rows"], 0);
        assert_eq!(report["truncated"], false);
    }

    #[test]
    fn workspace_query_rejects_unbounded_or_ambiguous_pages() {
        assert!(WorkspaceQuery::from_json(r#"{"limit":0}"#).is_err());
        assert!(WorkspaceQuery::from_json(r#"{"limit":101}"#).is_err());
        assert!(WorkspaceQuery::from_json(r#"{"unknown":1}"#).is_err());
        let too_many_filters = serde_json::json!({
            "filters": (0..9)
                .map(|_| serde_json::json!({
                    "column": "id",
                    "comparison": "eq",
                    "value": "1"
                }))
                .collect::<Vec<_>>()
        });
        assert!(WorkspaceQuery::from_json(&too_many_filters.to_string()).is_err());
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
    fn typed_inner_join_matches_categories_by_label_and_never_matches_nulls() {
        let left_source = table(vec!["key"], vec![vec!["a"], vec!["b"], vec![""]]);
        let right_source = table(
            vec!["lookup", "value"],
            vec![vec!["b", "B"], vec!["a", "A"], vec!["", "missing"]],
        );
        let categorical = |name: &str| SchemaRequest {
            overrides: BTreeMap::from([(name.to_owned(), ColumnType::Categorical)]),
            metadata: BTreeMap::new(),
        };
        let left = TypedTable::infer(&left_source, categorical("key"), &Job::new()).unwrap();
        let right = TypedTable::infer(&right_source, categorical("lookup"), &Job::new()).unwrap();
        let joined = left
            .inner_join(right, "key", "lookup", &Job::new())
            .unwrap();
        assert_eq!(joined.rows, 2);
        assert_eq!(joined.columns[0].categories, ["a", "b"]);
        assert_eq!(
            joined.columns[0].values,
            [Scalar::Categorical(0), Scalar::Categorical(1)]
        );
        assert_eq!(
            joined.columns[1].values,
            [Scalar::String("A".into()), Scalar::String("B".into())]
        );
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

    #[test]
    fn parquet_export_rejects_mixed_datetime_semantics_and_out_of_range_values() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("invalid.parquet");
        let mixed = table(
            vec!["when"],
            vec![vec!["2025-01-01T12:00:00Z"], vec!["2025-01-01T12:00:00"]],
        );
        let typed = TypedTable::infer(&mixed, SchemaRequest::default(), &Job::new()).unwrap();
        assert!(typed.write_parquet(&output, &Job::new()).is_err());
        assert!(!output.exists());

        let old = table(vec!["when"], vec![vec!["0001-01-01T00:00:00Z"]]);
        let typed = TypedTable::infer(&old, SchemaRequest::default(), &Job::new()).unwrap();
        assert!(typed.write_parquet(&output, &Job::new()).is_err());
        assert!(!output.exists());
    }

    #[test]
    fn typed_sort_is_numeric_stable_and_places_nulls_last() {
        let source = table(
            vec!["value", "label"],
            vec![
                vec!["10", "first"],
                vec!["2", "two"],
                vec!["", "missing"],
                vec!["2", "second"],
            ],
        );
        let mut typed = TypedTable::infer(&source, SchemaRequest::default(), &Job::new()).unwrap();
        typed.sort_by("value", &Job::new()).unwrap();
        assert_eq!(
            typed.columns[0].values,
            vec![
                Scalar::Int64(2),
                Scalar::Int64(2),
                Scalar::Int64(10),
                Scalar::Null
            ]
        );
        assert_eq!(
            typed.columns[1]
                .values
                .iter()
                .map(|value| match value {
                    Scalar::String(value) => value.as_str(),
                    _ => panic!("expected string cell"),
                })
                .collect::<Vec<_>>(),
            vec!["two", "second", "first", "missing"]
        );
    }

    #[test]
    fn typed_text_filters_preserve_schema_values_and_null_semantics() {
        let source = table(
            vec!["group", "score"],
            vec![vec!["North", "1"], vec!["south", "2"], vec!["", "3"]],
        );
        let request = SchemaRequest {
            overrides: BTreeMap::from([("group".into(), ColumnType::Categorical)]),
            metadata: BTreeMap::new(),
        };
        let mut typed = TypedTable::infer(&source, request, &Job::new()).unwrap();
        typed
            .filter_text(
                "group",
                "NORTH",
                TextMatch::ContainsInsensitive,
                &Job::new(),
            )
            .unwrap();
        assert_eq!(typed.rows, 1);
        assert_eq!(typed.columns[0].kind, ColumnType::Categorical);
        assert_eq!(typed.columns[0].categories[0], "North");
        assert_eq!(typed.columns[0].values, [Scalar::Categorical(0)]);
        assert_eq!(typed.columns[1].values, [Scalar::Int64(1)]);
    }

    #[test]
    fn typed_text_transforms_preserve_categories_nulls_metadata_and_other_types() {
        let source = table(
            vec!["group", "note", "count"],
            vec![
                vec![" alpha ", "  Ada  ", "1"],
                vec!["alpha", "ada", "2"],
                vec!["beta", "Straße", "3"],
                vec!["", "  ", "4"],
            ],
        );
        let request = SchemaRequest {
            overrides: BTreeMap::from([
                ("group".into(), ColumnType::Categorical),
                ("note".into(), ColumnType::String),
            ]),
            metadata: BTreeMap::from([(
                "note".into(),
                ColumnMetadata {
                    label: Some("Notes".into()),
                    unit: None,
                },
            )]),
        };
        let mut typed = TypedTable::infer(&source, request, &Job::new()).unwrap();
        typed
            .normalize_text_column("group", "upper", &Job::new())
            .unwrap();
        typed
            .normalize_text_column("group", "trim", &Job::new())
            .unwrap();
        typed
            .normalize_text_column("note", "upper", &Job::new())
            .unwrap();
        typed
            .replace_literal_text("note", "A", "x", &Job::new())
            .unwrap();

        let group = &typed.columns[0];
        assert_eq!(group.kind, ColumnType::Categorical);
        assert_eq!(group.categories, ["ALPHA", "BETA"]);
        assert_eq!(
            group.values,
            [
                Scalar::Categorical(0),
                Scalar::Categorical(0),
                Scalar::Categorical(1),
                Scalar::Null
            ]
        );
        let note = &typed.columns[1];
        assert_eq!(note.kind, ColumnType::String);
        assert_eq!(note.metadata.label.as_deref(), Some("Notes"));
        assert_eq!(
            note.values,
            [
                Scalar::String("  xDx  ".into()),
                Scalar::String("xDx".into()),
                Scalar::String("STRxSSE".into()),
                Scalar::Null
            ]
        );
        assert_eq!(typed.columns[2].kind, ColumnType::Int64);
        assert_eq!(
            typed.columns[2].values,
            [
                Scalar::Int64(1),
                Scalar::Int64(2),
                Scalar::Int64(3),
                Scalar::Int64(4)
            ]
        );
        assert!(!typed.supports_text_transform("count").unwrap());
        assert!(typed
            .normalize_text_column("count", "upper", &Job::new())
            .is_err());
        assert!(typed
            .replace_literal_text("note", "", "x", &Job::new())
            .is_err());
    }

    #[test]
    fn typed_missing_fills_keep_scalar_types_and_distinct_null_semantics() {
        let source = table(
            vec!["score"],
            vec![vec!["01"], vec![""], vec!["3"], vec![" "], vec!["5"]],
        );
        let mut typed = TypedTable::infer(&source, SchemaRequest::default(), &Job::new()).unwrap();
        typed.fill_missing("score", "02", &Job::new()).unwrap();
        assert_eq!(typed.columns[0].kind, ColumnType::Int64);
        assert_eq!(
            typed.columns[0].values,
            [
                Scalar::Int64(1),
                Scalar::Int64(2),
                Scalar::Int64(3),
                Scalar::Int64(2),
                Scalar::Int64(5),
            ]
        );
        assert!(typed
            .fill_missing("score", "not a number", &Job::new())
            .is_err());
        assert!(typed.fill_missing("score", "  ", &Job::new()).is_err());
        assert_eq!(typed.columns[0].values[1], Scalar::Int64(2));

        let mut forward = TypedTable::infer(
            &table(
                vec!["score"],
                vec![vec![""], vec!["2"], vec![""], vec!["4"], vec![""]],
            ),
            SchemaRequest::default(),
            &Job::new(),
        )
        .unwrap();
        forward.fill_direction("score", true, &Job::new()).unwrap();
        assert_eq!(
            forward.columns[0].values,
            [
                Scalar::Null,
                Scalar::Int64(2),
                Scalar::Int64(2),
                Scalar::Int64(4),
                Scalar::Int64(4),
            ]
        );
        forward.fill_direction("score", false, &Job::new()).unwrap();
        assert_eq!(
            forward.columns[0].values,
            [
                Scalar::Int64(2),
                Scalar::Int64(2),
                Scalar::Int64(2),
                Scalar::Int64(4),
                Scalar::Int64(4),
            ]
        );
    }

    #[test]
    fn typed_deduplication_uses_types_nulls_and_first_row_order() {
        let source = table(
            vec!["key", "label"],
            vec![
                vec!["010", "first"],
                vec!["10", "second"],
                vec!["", "missing"],
                vec![" ", "another missing"],
            ],
        );
        let mut typed = TypedTable::infer(&source, SchemaRequest::default(), &Job::new()).unwrap();
        typed
            .deduplicate_rows(Some(&["key".into()]), &Job::new())
            .unwrap();
        assert_eq!(typed.rows, 2);
        assert_eq!(typed.columns[0].values, [Scalar::Int64(10), Scalar::Null]);
        assert_eq!(typed.columns[1].values[0], Scalar::String("first".into()));
        assert_eq!(typed.columns[1].values[1], Scalar::String("missing".into()));
    }

    #[test]
    fn typed_numeric_expression_propagates_nulls_and_comparison_uses_numeric_order() {
        let source = table(
            vec!["base", "delta"],
            vec![vec!["2", "1"], vec!["", "3"], vec!["10", "2"]],
        );
        let mut typed = TypedTable::infer(&source, SchemaRequest::default(), &Job::new()).unwrap();
        let expression: NumericExpr = serde_json::from_str(
            r#"{"op":"add","left":{"op":"column","name":"base"},"right":{"op":"column","name":"delta"}}"#,
        )
        .unwrap();
        typed
            .derive_numeric_column(
                "total".into(),
                &expression,
                ColumnMetadata::default(),
                &Job::new(),
            )
            .unwrap();
        assert_eq!(
            typed.columns[2].values,
            vec![Scalar::Float64(3.0), Scalar::Null, Scalar::Float64(12.0)]
        );
        typed
            .filter_compare("total", TypedComparison::Gte, "10", &Job::new())
            .unwrap();
        assert_eq!(typed.rows, 1);
        assert_eq!(typed.columns[2].values, vec![Scalar::Float64(12.0)]);
    }

    #[test]
    fn typed_delimited_export_preserves_lexemes_and_safes_text_cells() {
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("source.csv");
        let output = directory.path().join("result.csv");
        std::fs::write(
            &input,
            "value,count,note\n-2.5,002,\"=SUM(1,2)\"\n, ,plain\n ,003,space-only\n",
        )
        .unwrap();
        let typed =
            TypedTable::from_path(&input, None, SchemaRequest::default(), &Job::new()).unwrap();
        typed.write_delimited(&output, b',', &Job::new()).unwrap();
        let output = std::fs::read_to_string(&output).unwrap();
        assert!(output.contains("-2.5,002,\"'=SUM(1,2)\""));
        assert!(output.contains("\n, ,plain\n"));
        assert!(output.contains("\n ,003,space-only\n"));
    }

    #[test]
    fn numeric_expressions_reject_inexact_integer_conversion() {
        let source = table(vec!["large"], vec![vec!["9007199254740993"]]);
        let mut typed = TypedTable::infer(&source, SchemaRequest::default(), &Job::new()).unwrap();
        let expression: NumericExpr =
            serde_json::from_str(r#"{"op":"column","name":"large"}"#).unwrap();
        let error = typed
            .derive_numeric_column(
                "converted".into(),
                &expression,
                ColumnMetadata::default(),
                &Job::new(),
            )
            .unwrap_err();
        assert!(error.message.contains("exact float64 expression range"));
        assert_eq!(typed.columns.len(), 1);
    }
}
