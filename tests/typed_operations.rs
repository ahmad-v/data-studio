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
