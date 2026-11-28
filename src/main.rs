#![forbid(unsafe_code)]

#[cfg(feature = "gui")]
fn table_workspace_from_result(
    payload: &str,
) -> std::result::Result<Option<localwright_ui::TableWorkspace>, String> {
    use localwright_ui::{CellValue, TableColumnInfo, TableModel, TableWorkspace};
    use serde_json::Value;

    if payload.len() > 4 * 1024 * 1024 {
        return if payload.contains("\"workspace_version\"") {
            Err("table payload exceeds the 4 MiB size limit".into())
        } else {
            Ok(None)
        };
    }
    let Ok(report) = serde_json::from_str::<Value>(payload) else {
        return Ok(None);
    };
    if report["workspace_version"].as_u64() != Some(1) {
        return typed_project_preview_workspace(&report);
    }
    let headers = report["headers"]
        .as_array()
        .ok_or_else(|| "table payload has no headers".to_owned())?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| "table header is not text".to_owned())
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let schema = report["schema"]
        .as_array()
        .ok_or_else(|| "table payload has no schema".to_owned())?
        .iter()
        .map(|column| {
            Ok(TableColumnInfo {
                name: column["name"]
                    .as_str()
                    .ok_or_else(|| "schema column has no name".to_owned())?
                    .to_owned(),
                data_type: column["type"]
                    .as_str()
                    .ok_or_else(|| "schema column has no type".to_owned())?
                    .to_owned(),
                nullable: column["nullable"]
                    .as_bool()
                    .ok_or_else(|| "schema column has no nullable flag".to_owned())?,
                missing: usize::try_from(
                    column["missing"]
                        .as_u64()
                        .ok_or_else(|| "schema column has no missing count".to_owned())?,
                )
                .map_err(|_| "missing count exceeds the platform size".to_owned())?,
                label: column["label"].as_str().map(str::to_owned),
                unit: column["unit"].as_str().map(str::to_owned),
            })
        })
        .collect::<std::result::Result<Vec<_>, String>>()?;
    if schema.len() != headers.len() {
        return Err("table headers and schema have different lengths".into());
    }
    let rows = report["rows"]
        .as_array()
        .ok_or_else(|| "table payload has no rows".to_owned())?
        .iter()
        .map(|row| {
            let values = row
                .as_array()
                .ok_or_else(|| "table row is not an array".to_owned())?;
            if values.len() != schema.len() {
                return Err("table row width does not match its schema".to_owned());
            }
            values
                .iter()
                .zip(&schema)
                .map(|(value, column)| {
                    if value.is_null() {
                        return Ok(CellValue::Null);
                    }
                    match column.data_type.as_str() {
                        "boolean" => value
                            .as_bool()
                            .map(CellValue::Boolean)
                            .ok_or_else(|| "invalid Boolean table cell".to_owned()),
                        "int64" => value
                            .as_i64()
                            .map(CellValue::Integer)
                            .ok_or_else(|| "invalid signed integer table cell".to_owned()),
                        "uint64" => value
                            .as_u64()
                            .map(CellValue::UnsignedInteger)
                            .ok_or_else(|| "invalid unsigned integer table cell".to_owned()),
                        "float64" => value
                            .as_f64()
                            .filter(|number| number.is_finite())
                            .map(CellValue::Float)
                            .ok_or_else(|| "invalid floating-point table cell".to_owned()),
                        "date" | "time" | "datetime" => value
                            .as_str()
                            .map(|text| CellValue::DateTime(text.to_owned()))
                            .ok_or_else(|| "invalid temporal table cell".to_owned()),
                        "categorical" => value
                            .as_str()
                            .map(|text| CellValue::Categorical(text.to_owned()))
                            .ok_or_else(|| "invalid categorical table cell".to_owned()),
                        "string" => value
                            .as_str()
                            .map(|text| CellValue::Text(text.to_owned()))
                            .ok_or_else(|| "invalid string table cell".to_owned()),
                        "null" => Err("non-null value in null-typed column".into()),
                        _ => Err(format!("unsupported typed column {:?}", column.data_type)),
                    }
                })
                .collect::<std::result::Result<Vec<_>, _>>()
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let total_rows = usize::try_from(
        report["total_rows"]
            .as_u64()
            .ok_or_else(|| "table payload has no total row count".to_owned())?,
    )
    .map_err(|_| "total row count exceeds platform size".to_owned())?;
    let displayed_rows = usize::try_from(
        report["displayed_rows"]
            .as_u64()
            .ok_or_else(|| "table payload has no displayed row count".to_owned())?,
    )
    .map_err(|_| "displayed row count exceeds platform size".to_owned())?;
    let truncated = report["truncated"]
        .as_bool()
        .ok_or_else(|| "table payload has no truncation flag".to_owned())?;
    let row_offset = usize::try_from(report["row_offset"].as_u64().unwrap_or(0))
        .map_err(|_| "table row offset exceeds the platform size".to_owned())?;
    if displayed_rows != rows.len()
        || displayed_rows > total_rows
        || row_offset > total_rows
        || row_offset.saturating_add(displayed_rows) > total_rows
        || truncated != (row_offset.saturating_add(displayed_rows) < total_rows)
    {
        return Err("table payload row counts do not match its data".into());
    }
    let column_count = headers.len();
    let table = TableModel::new(headers, rows).map_err(|error| error.to_string())?;
    let source_rows = report["source_rows"].as_u64().unwrap_or(total_rows as u64);
    let filtering = report["dataset_filtered"].as_bool().unwrap_or(false);
    let sort_column = report["dataset_sort_column"].as_str();
    let descending = report["dataset_sort_descending"].as_bool().unwrap_or(false);
    let dataset_scope = if filtering || sort_column.is_some() {
        let filter_note = if filtering {
            format!("; filters applied to all {source_rows} input rows")
        } else {
            String::new()
        };
        let sort_note = sort_column.map_or(String::new(), |column| {
            let direction = if descending {
                "descending"
            } else {
                "ascending"
            };
            format!("; dataset sorted {direction} by {column:?} before paging")
        });
        format!("{filter_note}{sort_note}; header sorting/search applies to this page")
    } else {
        "; header sorting/search applies to this page".to_owned()
    };
    let range = if displayed_rows == 0 {
        "no rows".to_owned()
    } else {
        format!(
            "rows {}–{} of {}",
            row_offset + 1,
            row_offset + displayed_rows,
            total_rows
        )
    };
    let summary = format!(
        "Showing {range} across {column_count} columns{dataset_scope}. Table selection indices are relative to this page. Cell text is capped at 256 characters; source data is unchanged."
    );
    Ok(Some(TableWorkspace {
        title: "Typed data table".into(),
        summary,
        schema,
        table,
    }))
}

/// Typed projects expose only a 30-row preview in their result payload. Present
/// those rows in the native table view while making the sample scope explicit.
#[cfg(feature = "gui")]
fn typed_project_preview_workspace(
    report: &serde_json::Value,
) -> std::result::Result<Option<localwright_ui::TableWorkspace>, String> {
    use localwright_ui::{
        CellValue, TableColumnInfo, TableModel, TableWorkspace, MAX_TABLE_COLUMNS,
    };

    let schema_report = &report["schema"];
    if schema_report["schema_version"].as_u64() != Some(1) || !report["preview"].is_array() {
        return Ok(None);
    }

    let headers = report["headers"]
        .as_array()
        .ok_or_else(|| "typed project preview has no headers".to_owned())?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| "typed project preview header is not text".to_owned())
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let schema_columns = schema_report["columns"]
        .as_array()
        .ok_or_else(|| "typed project preview has no column schema".to_owned())?;
    if schema_columns.len() != headers.len() {
        return Err("typed project preview headers and schema have different lengths".into());
    }
    if schema_columns.len() > MAX_TABLE_COLUMNS {
        return Ok(None);
    }
    let schema = schema_columns
        .iter()
        .map(|column| {
            let data_type = column["type"]
                .as_str()
                .ok_or_else(|| "typed project schema column has no type".to_owned())?
                .to_owned();
            if !matches!(
                data_type.as_str(),
                "boolean"
                    | "int64"
                    | "uint64"
                    | "float64"
                    | "date"
                    | "time"
                    | "datetime"
                    | "categorical"
                    | "string"
                    | "null"
            ) {
                return Err(format!("unsupported typed project column {data_type:?}"));
            }
            Ok(TableColumnInfo {
                name: column["name"]
                    .as_str()
                    .ok_or_else(|| "typed project schema column has no name".to_owned())?
                    .to_owned(),
                data_type,
                nullable: column["nullable"]
                    .as_bool()
                    .ok_or_else(|| "typed project schema column has no nullable flag".to_owned())?,
                missing: usize::try_from(column["missing"].as_u64().ok_or_else(|| {
                    "typed project schema column has no missing count".to_owned()
                })?)
                .map_err(|_| "typed project missing count exceeds the platform size".to_owned())?,
                label: column["label"].as_str().map(str::to_owned),
                unit: column["unit"].as_str().map(str::to_owned),
            })
        })
        .collect::<std::result::Result<Vec<_>, String>>()?;
    if schema
        .iter()
        .zip(&headers)
        .any(|(column, header)| column.name.as_str() != header.as_str())
    {
        return Err("typed project preview headers do not match the schema".into());
    }
    let total_rows = usize::try_from(
        report["rows"]
            .as_u64()
            .ok_or_else(|| "typed project preview has no total row count".to_owned())?,
    )
    .map_err(|_| "typed project row count exceeds the platform size".to_owned())?;
    if schema_report["rows"].as_u64() != Some(total_rows as u64) {
        return Err("typed project preview and schema row counts differ".into());
    }
    let preview_rows = report["preview"]
        .as_array()
        .ok_or_else(|| "typed project preview rows are missing".to_owned())?;
    if preview_rows.len() > 30 || preview_rows.len() != total_rows.min(30) {
        return Err("typed project preview row count is outside its bound".into());
    }
    let rows = preview_rows
        .iter()
        .map(|row| {
            let values = row
                .as_array()
                .ok_or_else(|| "typed project preview row is not an array".to_owned())?;
            if values.len() != schema.len() {
                return Err("typed project preview row width does not match its schema".to_owned());
            }
            values
                .iter()
                .zip(&schema)
                .map(|(value, column)| {
                    if value.is_null() {
                        return if column.nullable {
                            Ok(CellValue::Null)
                        } else {
                            Err(format!(
                                "null value in non-nullable typed column {:?}",
                                column.name
                            ))
                        };
                    }
                    let text = value
                        .as_str()
                        .ok_or_else(|| "typed project preview cell is not text".to_owned())?;
                    if text.trim().is_empty() {
                        if column.data_type == "string" {
                            // Project previews encode string nulls and empty strings alike.
                            return Ok(if column.nullable {
                                CellValue::Null
                            } else {
                                CellValue::Text(text.to_owned())
                            });
                        }
                        return if column.nullable || column.data_type == "null" {
                            Ok(CellValue::Null)
                        } else {
                            Err(format!(
                                "blank value in non-nullable typed column {:?}",
                                column.name
                            ))
                        };
                    }
                    match column.data_type.as_str() {
                        "boolean" => match text.trim().to_ascii_lowercase().as_str() {
                            "true" | "yes" | "1" => Ok(CellValue::Boolean(true)),
                            "false" | "no" | "0" => Ok(CellValue::Boolean(false)),
                            _ => Err("invalid Boolean project preview cell".to_owned()),
                        },
                        "int64" => text
                            .trim()
                            .parse::<i64>()
                            .map(CellValue::Integer)
                            .map_err(|_| "invalid signed integer project preview cell".to_owned()),
                        "uint64" => text
                            .trim()
                            .parse::<u64>()
                            .map(CellValue::UnsignedInteger)
                            .map_err(|_| {
                                "invalid unsigned integer project preview cell".to_owned()
                            }),
                        "float64" => text
                            .parse::<f64>()
                            .ok()
                            .filter(|number| number.is_finite())
                            .map(CellValue::Float)
                            .ok_or_else(|| "invalid float project preview cell".to_owned()),
                        "date" | "time" | "datetime" => Ok(CellValue::DateTime(text.to_owned())),
                        "categorical" => Ok(CellValue::Categorical(text.to_owned())),
                        "null" => Err("non-null value in null-typed project column".into()),
                        _ => Err(format!(
                            "unsupported typed project column {:?}",
                            column.data_type
                        )),
                    }
                })
                .collect::<std::result::Result<Vec<_>, String>>()
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let displayed = rows.len();
    let column_count = schema.len();
    let table = TableModel::new(headers, rows).map_err(|error| error.to_string())?;
    let range = if displayed == 0 {
        "no rows".to_owned()
    } else {
        format!("rows 1–{displayed} of {total_rows}")
    };
    let coverage = if displayed == total_rows {
        "The complete project result fits in this preview."
    } else {
        "This is only the first 30-row project preview; table sorting and search affect these displayed rows only. Add sort/filter steps to the project to transform the full result."
    };
    let string_ambiguity = if schema
        .iter()
        .any(|column| column.data_type == "string" && column.nullable)
    {
        " Blank cells in nullable string columns may represent either null or empty text in the project preview."
    } else {
        ""
    };
    Ok(Some(TableWorkspace {
        title: "Typed project result".into(),
        summary: format!(
            "Showing {range} across {column_count} columns. {coverage}{string_ambiguity} Cell text is capped at 256 characters; the source remains unchanged."
        ),
        schema,
        table,
    }))
}

#[cfg(all(test, feature = "gui"))]
mod tests {
    use super::*;
    use localwright_ui::CellValue;

    #[test]
    fn table_workspace_provider_preserves_typed_cells_and_null_empty_distinction() {
        let report = serde_json::json!({
            "workspace_version": 1,
            "total_rows": 2,
            "displayed_rows": 2,
            "truncated": false,
            "headers": ["score", "note"],
            "schema": [
                {"name":"score", "type":"float64", "nullable":true, "missing":1, "label":"Score", "unit":"points"},
                {"name":"note", "type":"string", "nullable":true, "missing":1, "label":null, "unit":null}
            ],
            "rows": [[1.25, ""], [null, null]]
        })
        .to_string();
        let table = table_workspace_from_result(&report)
            .unwrap()
            .expect("typed workspace payload");
        assert_eq!(table.table.row_count(), 2);
        assert_eq!(table.schema[0].label.as_deref(), Some("Score"));
        assert_eq!(table.schema[0].unit.as_deref(), Some("points"));
        assert_eq!(table.table.value_at(0, 0), Some(&CellValue::Float(1.25)));
        assert!(matches!(
            table.table.value_at(0, 1),
            Some(CellValue::Text(value)) if value.is_empty()
        ));
        assert_eq!(table.table.value_at(1, 1), Some(&CellValue::Null));
    }

    #[test]
    fn table_workspace_summary_reports_dataset_query_scope_and_absolute_page_range() {
        let report = serde_json::json!({
            "workspace_version": 1,
            "source_rows": 60,
            "total_rows": 50,
            "row_offset": 20,
            "displayed_rows": 2,
            "truncated": true,
            "dataset_filtered": true,
            "dataset_sort_column": "score",
            "dataset_sort_descending": true,
            "headers": ["score"],
            "schema": [
                {"name":"score", "type":"int64", "nullable":false, "missing":0, "label":null, "unit":null}
            ],
            "rows": [[5], [4]]
        })
        .to_string();
        let table = table_workspace_from_result(&report)
            .unwrap()
            .expect("typed workspace payload");
        assert!(table.summary.contains("rows 21–22 of 50"));
        assert!(table
            .summary
            .contains("filters applied to all 60 input rows"));
        assert!(table
            .summary
            .contains("dataset sorted descending by \"score\" before paging"));
        assert!(table
            .summary
            .contains("header sorting/search applies to this page"));
    }

    #[test]
    fn typed_project_results_open_in_a_bounded_workspace_with_explicit_scope() {
        let preview = (0..30)
            .map(|row| {
                if row == 1 {
                    serde_json::json!([" ", ""])
                } else {
                    serde_json::json!([row.to_string(), format!("sample-{row}")])
                }
            })
            .collect::<Vec<_>>();
        let report = serde_json::json!({
            "rows": 40,
            "headers": ["count", "note"],
            "preview": preview,
            "schema": {
                "schema_version": 1,
                "rows": 40,
                "columns": [
                    {"name":"count", "type":"int64", "nullable":true, "missing":1, "label":"Count", "unit":null},
                    {"name":"note", "type":"string", "nullable":true, "missing":1, "label":null, "unit":null}
                ]
            }
        })
        .to_string();
        let workspace = table_workspace_from_result(&report)
            .unwrap()
            .expect("typed project table preview");
        assert_eq!(workspace.table.row_count(), 30);
        assert_eq!(workspace.schema[0].label.as_deref(), Some("Count"));
        assert_eq!(workspace.table.value_at(0, 0), Some(&CellValue::Integer(0)));
        assert_eq!(workspace.table.value_at(1, 0), Some(&CellValue::Null));
        assert_eq!(workspace.table.value_at(1, 1), Some(&CellValue::Null));
        assert!(workspace.summary.contains("rows 1–30 of 40"));
        assert!(workspace
            .summary
            .contains("sorting and search affect these displayed rows only"));
        assert!(workspace.summary.contains(
            "blank cells in nullable string columns may represent either null or empty text"
        ));
    }

    #[test]
    fn typed_project_preview_rejects_inconsistent_row_bounds() {
        let report = serde_json::json!({
            "rows": 31,
            "headers": ["value"],
            "preview": (0..31).map(|value| vec![value.to_string()]).collect::<Vec<_>>(),
            "schema": {
                "schema_version": 1,
                "rows": 31,
                "columns": [
                    {"name":"value", "type":"int64", "nullable":false, "missing":0, "label":null, "unit":null}
                ]
            }
        })
        .to_string();
        assert!(table_workspace_from_result(&report).is_err());
    }

    #[test]
    fn table_workspace_provider_leaves_ordinary_previews_unchanged() {
        assert!(table_workspace_from_result("finished successfully")
            .unwrap()
            .is_none());
        assert!(
            table_workspace_from_result(r#"{"rows":1,"preview":[["x"]]}"#)
                .unwrap()
                .is_none()
        );
        assert!(table_workspace_from_result(r#"{"workspace_version":1}"#).is_err());
    }
}

fn main() {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.first().map(String::as_str) == Some("--cli") {
        args.remove(0);
        if let Err(e) = localwright_core::cli(args, data_studio::OPERATIONS, data_studio::handle) {
            eprintln!("{e}");
            std::process::exit(1);
        }
        return;
    }
    #[cfg(feature = "gui")]
    if let Err(e) = localwright_ui::run_with_saved_appearance_and_table_workspace(
        "Data Studio",
        "data-studio",
        data_studio::OPERATIONS,
        data_studio::handle,
        table_workspace_from_result,
    ) {
        eprintln!("{e}");
        std::process::exit(1);
    }
    #[cfg(not(feature = "gui"))]
    {
        eprintln!("Built without GUI; use --cli --help");
        std::process::exit(2);
    }
}
