use data_studio::handle;
use localwright_core::{Job, Request};
use serde_json::{json, Value};
use std::fs;

fn operation(csv: &str, kind: &str, parameter: Value) -> Value {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.csv");
    fs::write(&input, csv).unwrap();
    let outcome = handle(
        Request {
            operation: kind.into(),
            input,
            parameter: parameter.to_string(),
            ..Request::default()
        },
        Job::new(),
    )
    .unwrap_or_else(|error| panic!("{kind}: {error}"));
    serde_json::from_str(&outcome.preview).unwrap()
}

fn fails(csv: &str, kind: &str, parameter: Value) -> bool {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.csv");
    fs::write(&input, csv).unwrap();
    handle(
        Request {
            operation: kind.into(),
            input,
            parameter: parameter.to_string(),
            ..Request::default()
        },
        Job::new(),
    )
    .is_err()
}

#[test]
fn recodes_only_exact_mapped_values() {
    let result = operation(
        "status,value\nold,1\nOLD,2\n,3\n",
        "recode-values",
        json!({"column":"status","mapping":{"old":"current","":"missing"}}),
    );
    assert_eq!(
        result["preview"],
        json!([["current", "1"], ["OLD", "2"], ["missing", "3"]])
    );
}

#[test]
fn numeric_integer_and_boolean_conversion_are_strict_and_preserve_blanks() {
    let source = "number,whole,flag\n01.500,12,YES\n,,0\n";
    let numeric = operation(source, "convert-number", json!({"column":"number"}));
    assert_eq!(numeric["preview"][0][0], "1.5");
    assert_eq!(numeric["preview"][1][0], "");
    let integer = operation(source, "convert-integer", json!({"column":"whole"}));
    assert_eq!(integer["preview"][0][1], "12");
    assert!(fails(source, "convert-integer", json!({"column":"number"})));
    let boolean = operation(source, "convert-boolean", json!({"column":"flag"}));
    assert_eq!(boolean["preview"][0][2], "true");
    assert!(fails(
        "flag\nmaybe\n",
        "convert-boolean",
        json!({"column":"flag"})
    ));
}

#[test]
fn split_and_merge_columns_enforce_shapes_and_keep_source_data() {
    let source = "id,name\n1,Ada Lovelace\n2,Grace Hopper\n";
    let split = operation(
        source,
        "split-column",
        json!({"column":"name","delimiter":" ","new_columns":["first","last"]}),
    );
    assert_eq!(split["headers"], json!(["id", "first", "last"]));
    assert_eq!(split["preview"][1], json!(["2", "Grace", "Hopper"]));
    let merged = operation(
        source,
        "merge-columns",
        json!({"columns":["id","name"],"name":"label","delimiter":": "}),
    );
    assert_eq!(merged["headers"], json!(["id", "name", "label"]));
    assert_eq!(merged["preview"][0][2], "1: Ada Lovelace");
    assert!(fails(
        source,
        "split-column",
        json!({"column":"name","delimiter":" ","new_columns":["a","b","c"]})
    ));
}

#[test]
fn pivot_and_unpivot_round_trip_long_form_values() {
    let source = "id,year,value\na,2024,2\na,2025,3\nb,2024,4\nb,2025,5\n";
    let wide = operation(
        source,
        "pivot",
        json!({"id_column":"id","pivot_column":"year","value_column":"value"}),
    );
    assert_eq!(wide["headers"], json!(["id", "2024", "2025"]));
    assert_eq!(wide["preview"], json!([["a", "2", "3"], ["b", "4", "5"]]));
    let long = operation(
        "id,2024,2025\na,2,3\nb,4,5\n",
        "unpivot",
        json!({"id_columns":["id"],"value_columns":["2024","2025"],"variable_name":"year","value_name":"value"}),
    );
    assert_eq!(long["headers"], json!(["id", "year", "value"]));
    assert_eq!(
        long["preview"],
        json!([
            ["a", "2024", "2"],
            ["a", "2025", "3"],
            ["b", "2024", "4"],
            ["b", "2025", "5"]
        ])
    );
    assert!(fails(
        "id,key,value\na,x,1\na,x,2\n",
        "pivot",
        json!({"id_column":"id","pivot_column":"key","value_column":"value"})
    ));
}

#[test]
fn grouped_sum_and_mean_ignore_blank_numeric_cells_but_keep_groups() {
    let source = "site,score\nA,2\nA,4\nB,\n\n,8\n";
    let sum = operation(
        source,
        "group-sum",
        json!({"group_column":"site","value_column":"score"}),
    );
    assert_eq!(sum["headers"], json!(["site", "n", "sum"]));
    assert_eq!(
        sum["preview"],
        json!([["", "1", "8"], ["A", "2", "6"], ["B", "0", ""]])
    );
    let mean = operation(
        source,
        "group-mean",
        json!({"group_column":"site","value_column":"score"}),
    );
    assert_eq!(mean["headers"], json!(["site", "n", "mean"]));
    assert_eq!(mean["preview"][1], json!(["A", "2", "3"]));
    let extreme = operation(
        "site,value\nA,1e308\nA,-1e308\n",
        "group-mean",
        json!({"group_column":"site","value_column":"value"}),
    );
    assert_eq!(extreme["preview"][0][2], "0");
    assert!(fails(
        "site,value\nA,1e308\nA,1e308\n",
        "group-sum",
        json!({"group_column":"site","value_column":"value"})
    ));
}

#[test]
fn all_ten_transformations_replay_in_a_version_one_project() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.csv");
    let project = dir.path().join("project.json");
    fs::write(&input, "id,status,flag,parts,first,last,category,value\na,old,yes,x|y,A,Ada,q,2\nb,new,no,z|w,G,Grace,r,4\n").unwrap();
    fs::write(
        &project,
        json!({"schema_version":1,"steps":[
            {"operation":"recode-values","column":"status","mapping":{"old":"current"}},
            {"operation":"convert-number","column":"value"},
            {"operation":"convert-integer","column":"value"},
            {"operation":"convert-boolean","column":"flag"},
            {"operation":"split-column","column":"parts","delimiter":"|","new_columns":["left","right"]},
            {"operation":"merge-columns","columns":["first","last"],"name":"full","delimiter":" "},
            {"operation":"pivot","id_column":"id","pivot_column":"category","value_column":"value"},
            {"operation":"unpivot","id_columns":["id"],"value_columns":["q","r"],"variable_name":"category","value_name":"value"},
            {"operation":"group-sum","group_column":"category","value_column":"value"},
            {"operation":"group-mean","group_column":"category","value_column":"sum"}
        ]}).to_string(),
    ).unwrap();
    let output = handle(
        Request {
            operation: "run-project".into(),
            input,
            parameter: project.to_string_lossy().into_owned(),
            ..Request::default()
        },
        Job::new(),
    )
    .unwrap();
    let result: Value = serde_json::from_str(&output.preview).unwrap();
    assert_eq!(result["headers"], json!(["category", "n", "mean"]));
    assert_eq!(result["preview"], json!([["q", "1", "2"], ["r", "1", "4"]]));
}

#[test]
fn transformed_export_is_new_and_the_input_is_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("source.csv");
    let output = dir.path().join("split.csv");
    let source = "id,full\n1,Ada Lovelace\n";
    fs::write(&input, source).unwrap();
    let request = || Request {
        operation: "split-column".into(),
        input: input.clone(),
        output: output.clone(),
        parameter: json!({"column":"full","delimiter":" ","new_columns":["first","last"]})
            .to_string(),
        ..Request::default()
    };
    handle(request(), Job::new()).unwrap();
    let expected = "id,first,last\n1,Ada,Lovelace\n";
    assert_eq!(fs::read_to_string(&output).unwrap(), expected);
    assert_eq!(fs::read_to_string(&input).unwrap(), source);
    assert!(handle(request(), Job::new()).is_err());
    assert_eq!(fs::read_to_string(&output).unwrap(), expected);
}
