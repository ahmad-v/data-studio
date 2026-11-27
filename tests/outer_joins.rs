use localwright_core::{Job, Request};
use serde_json::{json, Value};
use std::fs;

fn join(kind: &str, left: &str, right: &str) -> Value {
    let dir = tempfile::tempdir().unwrap();
    let left_path = dir.path().join("left.csv");
    let right_path = dir.path().join("right.csv");
    fs::write(&left_path, left).unwrap();
    fs::write(&right_path, right).unwrap();
    let parameter = json!({"right_path":right_path,"left_key":"id","right_key":"id"}).to_string();
    let outcome = data_studio::handle(
        Request {
            operation: kind.into(),
            input: left_path,
            parameter,
            ..Request::default()
        },
        Job::new(),
    )
    .unwrap();
    serde_json::from_str(&outcome.preview).unwrap()
}

#[test]
fn left_join_preserves_unmatched_left_and_expands_duplicate_matches() {
    let result = join(
        "left-join",
        "id,name\n1,A\n2,B\n3,C\n",
        "id,score\n1,low\n1,high\n4,other\n",
    );
    assert_eq!(result["rows"], 4);
    assert_eq!(result["headers"], json!(["id", "name", "right.score"]));
    assert_eq!(
        result["preview"],
        json!([
            ["1", "A", "low"],
            ["1", "A", "high"],
            ["2", "B", ""],
            ["3", "C", ""]
        ])
    );
}

#[test]
fn right_and_full_join_preserve_unmatched_rows_including_blank_keys() {
    let left = "id,name\n1,A\n,blank-left\n";
    let right = "id,score\n1,ok\n2,extra\n,blank-right\n";
    let right_result = join("right-join", left, right);
    assert_eq!(
        right_result["preview"],
        json!([["1", "A", "ok"], ["", "", "extra"], ["", "", "blank-right"]])
    );
    let full_result = join("full-join", left, right);
    assert_eq!(
        full_result["preview"],
        json!([
            ["1", "A", "ok"],
            ["", "blank-left", ""],
            ["", "", "extra"],
            ["", "", "blank-right"]
        ])
    );
}
