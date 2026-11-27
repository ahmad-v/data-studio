use super::{validate, Table, MAX_CELLS, MAX_COLS};
use localwright_core::{Error, ErrorKind, Job, Result};
use rusqlite::{
    hooks::{AuthAction, AuthContext, Authorization},
    limits::Limit,
    params_from_iter,
    types::ValueRef,
    Connection, ErrorCode,
};

const MAX_QUERY_BYTES: usize = 16 * 1024;
const MAX_RESULT_BYTES: usize = 32 * 1024 * 1024;

pub fn query(table: Table, statement: &str, job: &Job) -> Result<Table> {
    if statement.trim().is_empty() || statement.len() > MAX_QUERY_BYTES {
        return Err(Error::input("Enter a SELECT statement up to 16 KiB"));
    }
    if statement.bytes().filter(|&byte| byte == b';').count() > 64 {
        return Err(Error::input("SQL punctuation exceeds the parsing limit"));
    }
    let mut db = Connection::open_in_memory().map_err(super::err)?;
    db.execute_batch(
        "PRAGMA temp_store=MEMORY; PRAGMA trusted_schema=OFF; PRAGMA max_page_count=16384;",
    )
    .map_err(super::err)?;
    db.set_limit(Limit::SQLITE_LIMIT_LENGTH, 16 * 1024 * 1024)
        .map_err(super::err)?;
    load(&mut db, table, job)?;
    for (limit, maximum) in [
        (Limit::SQLITE_LIMIT_COLUMN, MAX_COLS as i32),
        (Limit::SQLITE_LIMIT_SQL_LENGTH, MAX_QUERY_BYTES as i32),
        (Limit::SQLITE_LIMIT_EXPR_DEPTH, 64),
        (Limit::SQLITE_LIMIT_COMPOUND_SELECT, 16),
        (Limit::SQLITE_LIMIT_VDBE_OP, 50_000),
        (Limit::SQLITE_LIMIT_ATTACHED, 0),
    ] {
        db.set_limit(limit, maximum).map_err(super::err)?;
    }
    db.authorizer(Some(authorize)).map_err(super::err)?;
    let combined_job = job.clone();
    let mut checks = 0;
    db.progress_handler(
        1_000,
        Some(move || {
            checks += 1;
            checks > 5_000 || combined_job.check().is_err()
        }),
    )
    .map_err(super::err)?;
    let result = read_result(&db, statement, job);
    job.check()?;
    result
}

fn load(db: &mut Connection, table: Table, job: &Job) -> Result<()> {
    let transaction = db.transaction().map_err(super::err)?;
    let columns = table
        .headers
        .iter()
        .map(|name| format!("\"{}\" TEXT", name.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(",");
    transaction
        .execute(&format!("CREATE TABLE input ({columns})"), [])
        .map_err(super::err)?;
    let parameters = vec!["?"; table.headers.len()].join(",");
    {
        let mut insert = transaction
            .prepare(&format!("INSERT INTO input VALUES ({parameters})"))
            .map_err(super::err)?;
        for row in table.rows {
            job.check()?;
            insert.execute(params_from_iter(row)).map_err(super::err)?;
        }
    }
    transaction.commit().map_err(super::err)
}

fn authorize(context: AuthContext<'_>) -> Authorization {
    match context.action {
        AuthAction::Select => Authorization::Allow,
        AuthAction::Read {
            table_name: "input",
            ..
        } if context.database_name == Some("main") => Authorization::Allow,
        AuthAction::Function {
            function_name:
                "abs" | "avg" | "coalesce" | "count" | "glob" | "ifnull" | "instr" | "length" | "like"
                | "lower" | "ltrim" | "max" | "min" | "nullif" | "replace" | "round" | "rtrim"
                | "substr" | "substring" | "sum" | "total" | "trim" | "typeof" | "upper",
        } => Authorization::Allow,
        _ => Authorization::Deny,
    }
}

fn query_error(error: rusqlite::Error) -> Error {
    if matches!(&error, rusqlite::Error::SqliteFailure(raw, _) if raw.code == ErrorCode::OperationInterrupted)
    {
        Error::new(ErrorKind::Limit, "SQL execution exceeded the work limit")
    } else {
        super::err(error)
    }
}

fn read_result(db: &Connection, sql: &str, job: &Job) -> Result<Table> {
    let mut statement = db.prepare(sql).map_err(query_error)?;
    if !statement.readonly() || statement.column_count() == 0 || statement.parameter_count() != 0 {
        return Err(Error::input(
            "Use one read-only SELECT without bound parameters",
        ));
    }
    let headers = statement.column_names().map(str::to_owned).collect();
    let mut output = validate(headers, Vec::new())?;
    let width = output.headers.len();
    let max_rows = MAX_CELLS / width;
    let mut budget = MAX_RESULT_BYTES;
    let mut rows = statement.query([]).map_err(query_error)?;
    while let Some(row) = rows.next().map_err(query_error)? {
        job.check()?;
        if output.rows.len() == max_rows {
            return Err(Error::new(
                ErrorKind::Limit,
                "SQL result exceeds the cell limit",
            ));
        }
        let mut cells = Vec::with_capacity(width);
        for index in 0..width {
            let value = row.get_ref(index).map_err(super::err)?;
            let text = match value {
                ValueRef::Null => String::new(),
                ValueRef::Integer(n) => n.to_string(),
                ValueRef::Real(n) if n.is_finite() => n.to_string(),
                ValueRef::Text(bytes) => std::str::from_utf8(bytes).map_err(super::err)?.into(),
                _ => {
                    return Err(Error::input(
                        "SQL output contains a BLOB or nonfinite value",
                    ))
                }
            };
            budget = budget
                .checked_sub(text.len())
                .ok_or_else(|| Error::new(ErrorKind::Limit, "SQL result text exceeds 32 MiB"))?;
            cells.push(text);
        }
        output.rows.push(cells);
    }
    Ok(output)
}
