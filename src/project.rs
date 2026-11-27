use super::{apply_step, Table};
use localwright_core::{read_bounded, Error, Job, Result};
use serde::Deserialize;
use std::path::Path;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Project {
    schema_version: u32,
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
}
