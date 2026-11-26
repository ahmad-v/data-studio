use localwright_core::{Job, Request};
#[test]
fn xlsx_fixture() {
    let r = data_studio::handle(
        Request {
            operation: "preview".into(),
            input: "tests/fixtures/table.xlsx".into(),
            ..Request::default()
        },
        Job::new(),
    )
    .unwrap();
    let v: serde_json::Value = serde_json::from_str(&r.preview).unwrap();
    assert_eq!(v["rows"], 2);
    assert_eq!(v["preview"][0][0], "a");
}

#[test]
fn preview_named_worksheet() {
    let request = |name: &str| Request {
        operation: "preview".into(),
        input: "tests/fixtures/two-sheets.xlsx".into(),
        parameter: name.into(),
        ..Request::default()
    };
    let outcome = data_studio::handle(request("Second sheet"), Job::new()).unwrap();
    let preview: serde_json::Value = serde_json::from_str(&outcome.preview).unwrap();
    assert_eq!(preview["preview"], serde_json::json!([["selected", "42"]]));
    assert!(data_studio::handle(request("Missing sheet"), Job::new()).is_err());
}
