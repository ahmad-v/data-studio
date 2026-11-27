use arrow_array::Array;
use localwright_core::{Job, Request};
use std::fs;

fn run(input: &std::path::Path, output: &std::path::Path, operation: &str, parameter: &str) {
    data_studio::handle(
        Request {
            operation: operation.into(),
            input: input.to_path_buf(),
            output: output.to_path_buf(),
            parameter: parameter.into(),
            ..Request::default()
        },
        Job::new(),
    )
    .unwrap();
}

#[test]
fn version_two_projects_use_typed_numeric_filters_and_shared_expressions() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.csv");
    let project = dir.path().join("project.json");
    let output = dir.path().join("result.parquet");
    let original = "baseline,current\n10,15\n20,30\n0,2\n";
    fs::write(&input, original).unwrap();
    fs::write(
        &project,
        r#"{
            "schema_version": 2,
            "steps": [
                {"operation":"filter-number-between","column":"current","lower":15.0,"upper":30.0},
                {"operation":"add-numeric-columns","left":"baseline","right":"current","name":"total"},
                {"operation":"percent-change-column","current":"current","baseline":"baseline","name":"change_pct"}
            ]
        }"#,
    )
    .unwrap();

    data_studio::handle(
        Request {
            operation: "run-project".into(),
            input: input.clone(),
            output: output.clone(),
            parameter: project.to_string_lossy().into_owned(),
            ..Request::default()
        },
        Job::new(),
    )
    .unwrap();

    let bytes = localwright_core::read_bounded(&output, 16 * 1024 * 1024).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        bytes::Bytes::from(bytes),
    )
    .unwrap();
    let schema = reader.schema();
    assert_eq!(schema.field(0).data_type(), &arrow_schema::DataType::Int64);
    assert_eq!(
        schema.field(2).data_type(),
        &arrow_schema::DataType::Float64
    );
    assert_eq!(
        schema.field(3).data_type(),
        &arrow_schema::DataType::Float64
    );
    let batches = reader
        .build()
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
    let totals = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .unwrap();
    assert_eq!(totals.values(), &[25.0, 50.0]);
    let changes = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .unwrap();
    assert_eq!(changes.values(), &[50.0, 50.0]);
    assert_eq!(fs::read_to_string(input).unwrap(), original);
}

#[test]
fn ordinary_csv_numeric_filters_use_typed_values_and_keep_source() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.csv");
    let original = "id,score\na,1.0\nb,2.5\nc,3.5\nd,\n";
    fs::write(&input, original).unwrap();

    let output = dir.path().join("filtered.csv");
    run(
        &input,
        &output,
        "filter-number-between",
        r#"{"column":"score","lower":2.5,"upper":3.5}"#,
    );
    assert_eq!(
        fs::read_to_string(output).unwrap(),
        "id,score\nb,2.5\nc,3.5\n"
    );
    assert_eq!(fs::read_to_string(input).unwrap(), original);
}

#[test]
fn ordinary_csv_missing_and_row_window_operations_keep_typed_route() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.csv");
    let original = "score,label\n10,ten\n2,two\n,missing\n";
    fs::write(&input, original).unwrap();

    let no_missing = dir.path().join("no-missing.csv");
    run(&input, &no_missing, "drop-missing", "score");
    assert_eq!(
        fs::read_to_string(no_missing).unwrap(),
        "score,label\n10,ten\n2,two\n"
    );

    let sorted = dir.path().join("sorted.csv");
    run(&input, &sorted, "sort", "score");
    assert_eq!(
        fs::read_to_string(sorted).unwrap(),
        "score,label\n2,two\n10,ten\n,missing\n"
    );

    let head = dir.path().join("head.csv");
    run(&input, &head, "head-rows", r#"{"rows":1}"#);
    assert_eq!(fs::read_to_string(head).unwrap(), "score,label\n10,ten\n");
    assert_eq!(fs::read_to_string(input).unwrap(), original);
}

#[test]
fn typed_inequality_and_null_filters_are_type_aware_and_guard_precision() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.csv");
    fs::write(&input, "value,label\n02,two\n3,three\n,missing\n").unwrap();

    let output = dir.path().join("not-equal.csv");
    run(
        &input,
        &output,
        "filter-not-equal",
        r#"{"column":"value","value":"2"}"#,
    );
    assert_eq!(
        fs::read_to_string(output).unwrap(),
        "value,label\n3,three\n"
    );

    let output = dir.path().join("complete.csv");
    run(&input, &output, "drop-rows-any-missing", "{}");
    assert_eq!(
        fs::read_to_string(output).unwrap(),
        "value,label\n02,two\n3,three\n"
    );

    let large = dir.path().join("large.csv");
    fs::write(&large, "value\n9007199254740993\n").unwrap();
    let rejected_output = dir.path().join("unsafe-comparison.csv");
    assert!(data_studio::handle(
        Request {
            operation: "filter-number-greater".into(),
            input: large,
            output: rejected_output.clone(),
            parameter: r#"{"column":"value","value":9007199254740992.0}"#.into(),
            ..Request::default()
        },
        Job::new(),
    )
    .is_err());
    assert!(!rejected_output.exists());
}

#[test]
fn ordinary_missing_value_fills_use_typed_scalars_for_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.csv");
    let original = "score\n01\n\"\"\n3\n";
    fs::write(&input, original).unwrap();

    let filled = dir.path().join("filled.parquet");
    run(&input, &filled, "fill-missing", "score=02");
    let bytes = localwright_core::read_bounded(&filled, 16 * 1024 * 1024).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        bytes::Bytes::from(bytes),
    )
    .unwrap();
    assert_eq!(
        reader.schema().field(0).data_type(),
        &arrow_schema::DataType::Int64
    );
    let batch = reader.build().unwrap().next().unwrap().unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(values.values(), &[1, 2, 3]);
    assert_eq!(values.null_count(), 0);

    let backwards_input = dir.path().join("backwards.csv");
    fs::write(&backwards_input, "score\n\"\"\n2\n\"\"\n4\n\"\"\n").unwrap();
    let forward = dir.path().join("forward.parquet");
    run(
        &backwards_input,
        &forward,
        "fill-forward",
        r#"{"column":"score"}"#,
    );
    let bytes = localwright_core::read_bounded(&forward, 16 * 1024 * 1024).unwrap();
    let batch = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        bytes::Bytes::from(bytes),
    )
    .unwrap()
    .build()
    .unwrap()
    .next()
    .unwrap()
    .unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        [None, Some(2), Some(2), Some(4), Some(4)]
    );

    let backward = dir.path().join("backward.parquet");
    run(
        &backwards_input,
        &backward,
        "fill-backward",
        r#"{"column":"score"}"#,
    );
    let bytes = localwright_core::read_bounded(&backward, 16 * 1024 * 1024).unwrap();
    let batch = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        bytes::Bytes::from(bytes),
    )
    .unwrap()
    .build()
    .unwrap()
    .next()
    .unwrap()
    .unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        [Some(2), Some(2), Some(4), Some(4), None]
    );
}
