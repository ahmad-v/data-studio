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
