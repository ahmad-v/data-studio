use crate::{Table, MAX_CELLS, MAX_COLS};
use arrow_array::{
    types::Int32Type, Array, ArrayRef, BooleanArray, Date32Array, DictionaryArray, Float64Array,
    Int32Array, Int64Array, StringArray, Time64NanosecondArray, TimestampNanosecondArray,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use localwright_core::{write_new, Error, ErrorKind, Job, Result};
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

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

    /// Keep rows whose selected cell equals a literal parsed using that column's type.
    pub(crate) fn filter_equal(&mut self, name: &str, literal: &str, job: &Job) -> Result<()> {
        self.filter_compare(name, TypedComparison::Eq, literal, job)
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

    /// Inner join two typed tables using exact type-aware keys; null keys never match.
    pub(crate) fn inner_join(
        mut self,
        right: TypedTable,
        left_key: &str,
        right_key: &str,
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

        let mut index = HashMap::<String, Vec<usize>>::new();
        for (row, value) in right_column.values.iter().enumerate() {
            job.check()?;
            if let Some(key) = typed_join_key(right_column, value)? {
                index.entry(key).or_default().push(row);
            }
        }
        let mut pairs = Vec::new();
        let max_rows = MAX_CELLS / names.len();
        for (left_row, value) in left_column.values.iter().enumerate() {
            job.check()?;
            if let Some(key) = typed_join_key(left_column, value)? {
                if let Some(matches) = index.get(&key) {
                    if pairs.len().saturating_add(matches.len()) > max_rows {
                        return Err(Error::new(
                            ErrorKind::Limit,
                            "Typed join exceeds the cell limit",
                        ));
                    }
                    pairs.extend(matches.iter().map(|right_row| (left_row, *right_row)));
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
            for (left_row, _) in &pairs {
                job.check()?;
                values.push(column.values[*left_row].clone());
            }
            output_columns.push(Column {
                name: name.clone(),
                values,
                ..column
            });
        }
        for (column, name) in right_columns
            .into_iter()
            .zip(names.iter().skip(output_columns.len()))
        {
            let mut values = Vec::with_capacity(pairs.len());
            for (_, right_row) in &pairs {
                job.check()?;
                values.push(column.values[*right_row].clone());
            }
            output_columns.push(Column {
                name: name.clone(),
                values,
                ..(*column).clone()
            });
        }
        Ok(Self {
            columns: output_columns,
            rows: pairs.len(),
        })
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
                    sum: 0.0,
                    mean: 0.0,
                    count: 0,
                });
                index
            };
            let Some(value) = scalar_to_f64(value_column, &value_column.values[row]).map_err(
                |error| {
                    Error::new(
                        error.kind,
                        format!("Aggregation failed on row {}: {}", row + 1, error.message),
                    )
                },
            )? else {
                continue;
            };
            let group = &mut groups[group_index];
            let next_count = group.count.checked_add(1).ok_or_else(|| {
                Error::new(ErrorKind::Limit, "Grouped value count exceeds the supported range")
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
            categories: Vec::new(),
            metadata,
        };
        *self = Self {
            columns: vec![key_output, aggregate_column],
            rows: groups.len(),
        };
        Ok(())
    }

    /// Stable, ascending sort using the typed value; nulls always sort after values.
    pub(crate) fn sort_by(&mut self, name: &str, job: &Job) -> Result<()> {
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
            match compare_scalar(column, &column.values[*left], &column.values[*right]) {
                Ok(ordering) => ordering,
                Err(error) => {
                    if comparison_error.is_none() {
                        comparison_error = Some(error);
                    }
                    std::cmp::Ordering::Equal
                }
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
        let count = count.min(self.rows);
        self.reorder_rows(&(0..count).collect::<Vec<_>>(), job)
    }

    pub(crate) fn tail_rows(&mut self, count: usize, job: &Job) -> Result<()> {
        let start = self.rows.saturating_sub(count);
        self.reorder_rows(&(start..self.rows).collect::<Vec<_>>(), job)
    }

    pub(crate) fn reverse_rows(&mut self, job: &Job) -> Result<()> {
        self.reorder_rows(&(0..self.rows).rev().collect::<Vec<_>>(), job)
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
            let mut row = 0;
            let mut cancelled = false;
            column.values.retain(|_| {
                if job.check().is_err() {
                    cancelled = true;
                    return false;
                }
                let retain = keep[row];
                row += 1;
                retain
            });
            if cancelled {
                job.check()?;
            }
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
            let mut reordered = Vec::with_capacity(order.len());
            for index in order {
                job.check()?;
                reordered.push(
                    slots[*index].take().ok_or_else(|| {
                        Error::input("Typed row order contains a duplicate index")
                    })?,
                );
            }
            column.values = reordered;
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

    pub(crate) fn preview_report(&self) -> serde_json::Value {
        let preview = (0..self.rows.min(30))
            .map(|row| {
                self.columns
                    .iter()
                    .map(|column| {
                        (
                            column.name.clone(),
                            value_json(&column.values[row], &column.categories)
                                .unwrap_or(serde_json::Value::Null),
                        )
                    })
                    .collect::<serde_json::Map<_, _>>()
            })
            .map(serde_json::Value::Object)
            .collect::<Vec<_>>();
        serde_json::json!({
            "schema": self.schema_report(),
            "rows": self.rows,
            "preview": preview,
        })
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
