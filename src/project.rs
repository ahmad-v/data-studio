use super::{
    apply_step,
    typed::{SchemaRequest, TypedTable},
    Table,
};
use localwright_core::{read_bounded, Error, Job, Result};
use serde::Deserialize;
use std::path::Path;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Project {
    schema_version: u32,
    #[serde(default)]
    typed_schema: Option<SchemaRequest>,
    pub worksheet: Option<String>,
    steps: Vec<Step>,
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Step {
    Filter {
        column: String,
        value: String,
    },
    SelectColumns {
        columns: Vec<String>,
    },
    Sort {
        column: String,
    },
    Deduplicate {},
    MaskColumn {
        column: String,
    },
    DropMissing {
        column: String,
    },
    FillMissing {
        column: String,
        value: String,
    },
    RenameColumn {
        column: String,
        name: String,
    },
    RecodeValues {
        column: String,
        mapping: std::collections::BTreeMap<String, String>,
    },
    ConvertNumber {
        column: String,
    },
    ConvertInteger {
        column: String,
    },
    ConvertBoolean {
        column: String,
    },
    SplitColumn {
        column: String,
        delimiter: String,
        new_columns: Vec<String>,
    },
    MergeColumns {
        columns: Vec<String>,
        name: String,
        delimiter: String,
    },
    Pivot {
        id_column: String,
        pivot_column: String,
        value_column: String,
    },
    Unpivot {
        id_columns: Vec<String>,
        value_columns: Vec<String>,
        variable_name: String,
        value_name: String,
    },
    GroupSum {
        group_column: String,
        value_column: String,
    },
    GroupMean {
        group_column: String,
        value_column: String,
    },
    FilterContains {
        column: String,
        value: String,
    },
    FilterNotContains {
        column: String,
        value: String,
    },
    HeadRows {
        rows: usize,
    },
    TailRows {
        rows: usize,
    },
    DropDuplicatesByColumns {
        columns: Vec<String>,
    },
    AddIndexColumn {
        name: String,
        start: i64,
    },
    AddConstantColumn {
        name: String,
        value: String,
    },
    NormalizeText {
        column: String,
        mode: String,
    },
    RenameColumns {
        mapping: std::collections::BTreeMap<String, String>,
    },
    DropColumns {
        columns: Vec<String>,
    },
    FilterNotEqual {
        column: String,
        value: String,
    },
    FilterContainsInsensitive {
        column: String,
        value: String,
    },
    FilterStartsWith {
        column: String,
        value: String,
    },
    FilterEndsWith {
        column: String,
        value: String,
    },
    FilterNumberGreater {
        column: String,
        value: f64,
    },
    FilterNumberGreaterEqual {
        column: String,
        value: f64,
    },
    FilterNumberLess {
        column: String,
        value: f64,
    },
    FilterNumberLessEqual {
        column: String,
        value: f64,
    },
    FilterNumberBetween {
        column: String,
        lower: f64,
        upper: f64,
    },
    AddNumericColumns {
        left: String,
        right: String,
        name: String,
    },
    SubtractNumericColumns {
        left: String,
        right: String,
        name: String,
    },
    MultiplyNumericColumns {
        left: String,
        right: String,
        name: String,
    },
    DivideNumericColumns {
        left: String,
        right: String,
        name: String,
    },
    PercentChangeColumn {
        current: String,
        baseline: String,
        name: String,
    },
    AbsoluteValueColumn {
        column: String,
        name: String,
    },
    SquareRootColumn {
        column: String,
        name: String,
    },
    Log10Column {
        column: String,
        name: String,
    },
    RoundNumericColumn {
        column: String,
        name: String,
        decimal_places: i32,
    },
    ClipNumericColumn {
        column: String,
        name: String,
        minimum: f64,
        maximum: f64,
    },
    ReplaceLiteral {
        column: String,
        find: String,
        replace: String,
    },
    DropRowsAnyMissing {},
    FillForward {
        column: String,
    },
    FillBackward {
        column: String,
    },
    DropEmptyColumns {},
    ReverseRows {},
}

impl Project {
    pub fn open(path: &Path) -> Result<Self> {
        let project: Self = serde_json::from_slice(&read_bounded(path, 128 * 1024)?)?;
        if project.schema_version != 1 {
            return Err(Error::input("Unsupported data project version"));
        }
        if !(1..=32).contains(&project.steps.len()) {
            return Err(Error::input("Data projects require 1 to 32 steps"));
        }
        Ok(project)
    }

    pub(crate) fn typed_schema(&self) -> Option<&SchemaRequest> {
        self.typed_schema.as_ref()
    }

    pub(super) fn run(self, mut table: Table, job: &Job) -> Result<(Table, usize, bool)> {
        let count = self.steps.len();
        let mut masked = false;
        for (index, step) in self.steps.into_iter().enumerate() {
            job.check()?;
            masked |= matches!(step, Step::MaskColumn { .. });
            table = apply_step(table, step, job).map_err(|error| {
                Error::new(
                    error.kind,
                    format!("Project step {}: {}", index + 1, error.message),
                )
            })?;
        }
        Ok((table, count, masked))
    }

    pub(super) fn run_typed(self, mut table: TypedTable, job: &Job) -> Result<(TypedTable, usize)> {
        let count = self.steps.len();
        for (index, step) in self.steps.into_iter().enumerate() {
            job.check()?;
            let result = match step {
                Step::Filter { column, value } => table.filter_equal(&column, &value, job),
                Step::SelectColumns { columns } => table.select_columns(&columns, job),
                Step::Sort { column } => table.sort_by(&column, job),
                Step::DropMissing { column } => table.drop_missing(&column, job),
                Step::DropRowsAnyMissing {} => table.drop_rows_any_missing(job),
                Step::HeadRows { rows } => table.head_rows(rows, job),
                Step::TailRows { rows } => table.tail_rows(rows, job),
                Step::ReverseRows {} => table.reverse_rows(job),
                _ => Err(Error::input(
                    "This operation is not supported in a typed pipeline yet; use one of filter, sort, select-columns, drop-missing, drop-rows-any-missing, head-rows, tail-rows or reverse-rows",
                )),
            };
            result.map_err(|error| {
                Error::new(
                    error.kind,
                    format!("Typed project step {}: {}", index + 1, error.message),
                )
            })?;
        }
        Ok((table, count))
    }
}
