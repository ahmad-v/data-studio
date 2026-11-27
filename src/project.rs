use super::{
    apply_step,
    typed::{ColumnMetadata, NumericExpr, SchemaRequest, TextMatch, TypedComparison, TypedTable},
    Table,
};
use localwright_core::{read_bounded, Error, Job, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Project {
    schema_version: u32,
    #[serde(default)]
    typed_schema: Option<SchemaRequest>,
    pub worksheet: Option<String>,
    steps: Vec<Step>,
    #[serde(skip)]
    project_directory: PathBuf,
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Step {
    Filter {
        column: String,
        value: String,
    },
    FilterCompare {
        column: String,
        comparison: TypedComparison,
        value: String,
    },
    DeriveNumericColumn {
        name: String,
        expression: NumericExpr,
        #[serde(default)]
        label: Option<String>,
        #[serde(default)]
        unit: Option<String>,
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
    TypedInnerJoin {
        right_path: PathBuf,
        left_key: String,
        right_key: String,
        right_schema: SchemaRequest,
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
        let mut project: Self = serde_json::from_slice(&read_bounded(path, 128 * 1024)?)?;
        if project.schema_version != 1 {
            return Err(Error::input("Unsupported data project version"));
        }
        if !(1..=32).contains(&project.steps.len()) {
            return Err(Error::input("Data projects require 1 to 32 steps"));
        }
        project.project_directory = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
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
                Step::TypedInnerJoin {
                    right_path,
                    left_key,
                    right_key,
                    right_schema,
                } => {
                    let path = if right_path.is_absolute() {
                        right_path
                    } else {
                        self.project_directory.join(right_path)
                    };
                    TypedTable::from_path(&path, None, right_schema, job)
                        .and_then(|right| table.inner_join(right, &left_key, &right_key, job))
                }
                step => {
                    let result = match step {
                        Step::Filter { column, value } => {
                            table.filter_equal(&column, &value, job)
                        }
                        Step::FilterCompare {
                            column,
                            comparison,
                            value,
                        } => table.filter_compare(&column, comparison, &value, job),
                        Step::FilterContains { column, value } => {
                            table.filter_text(&column, &value, TextMatch::Contains, job)
                        }
                        Step::FilterNotContains { column, value } => {
                            table.filter_text(&column, &value, TextMatch::NotContains, job)
                        }
                        Step::FilterContainsInsensitive { column, value } => table
                            .filter_text(&column, &value, TextMatch::ContainsInsensitive, job),
                        Step::FilterStartsWith { column, value } => {
                            table.filter_text(&column, &value, TextMatch::StartsWith, job)
                        }
                        Step::FilterEndsWith { column, value } => {
                            table.filter_text(&column, &value, TextMatch::EndsWith, job)
                        }
                        Step::DeriveNumericColumn {
                            name,
                            expression,
                            label,
                            unit,
                        } => table.derive_numeric_column(
                            name,
                            &expression,
                            ColumnMetadata { label, unit },
                            job,
                        ),
                        Step::RenameColumn { column, name } => {
                            table.rename_column(&column, name)
                        }
                        Step::GroupSum {
                            group_column,
                            value_column,
                        } => table.group_numeric(&group_column, &value_column, false, job),
                        Step::GroupMean {
                            group_column,
                            value_column,
                        } => table.group_numeric(&group_column, &value_column, true, job),
                        Step::SelectColumns { columns } => table.select_columns(&columns, job),
                        Step::Sort { column } => table.sort_by(&column, job),
                        Step::DropMissing { column } => table.drop_missing(&column, job),
                        Step::DropRowsAnyMissing {} => table.drop_rows_any_missing(job),
                        Step::HeadRows { rows } => table.head_rows(rows, job),
                        Step::TailRows { rows } => table.tail_rows(rows, job),
                        Step::ReverseRows {} => table.reverse_rows(job),
                        _ => Err(Error::input(
                            "This operation is not supported in a typed pipeline yet; use filter, filter-compare, derive-numeric-column, rename-column, typed-inner-join, group-sum, group-mean, sort, select-columns, drop-missing, drop-rows-any-missing, head-rows, tail-rows or reverse-rows",
                        )),
                    };
                    result.map(|()| table)
                }
            };
            table = result.map_err(|error| {
                Error::new(
                    error.kind,
                    format!("Typed project step {}: {}", index + 1, error.message),
                )
            })?;
        }
        Ok((table, count))
    }
}
