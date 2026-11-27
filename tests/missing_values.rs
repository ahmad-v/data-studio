use localwright_core::{Job, Request};
use std::fs;

#[test]
fn missing_value_transforms_and_rename_support_projects() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("sample.csv");
    let original = "name,group\nAda,A\n, B\n ,C\nRen,D\n";
    fs::write(&input, original).unwrap();
    let project = dir.path().join("project.json");
    fs::write(&project, r#"{"schema_version":1,"steps":[{"operation":"fill-missing","column":"name","value":"unknown"},{"operation":"rename-column","column":"name","name":"person"},{"operation":"drop-missing","column":"person"}]}"#).unwrap();
    for (operation, parameter, expected) in [
        ("drop-missing", "name", "name,group\nAda,A\nRen,D\n"),
        (
            "fill-missing",
            "name=unknown",
            "name,group\nAda,A\nunknown, B\nunknown,C\nRen,D\n",
        ),
        (
            "rename-column",
            "name=person",
            "person,group\nAda,A\n, B\n ,C\nRen,D\n",
        ),
        (
            "run-project",
            project.to_str().unwrap(),
            "person,group\nAda,A\nunknown, B\nunknown,C\nRen,D\n",
        ),
    ] {
        let output = dir.path().join(format!("{operation}.csv"));
        data_studio::handle(
            Request {
                operation: operation.into(),
                input: input.clone(),
                output: output.clone(),
                parameter: parameter.into(),
                ..Request::default()
            },
            Job::new(),
        )
        .unwrap();
        assert_eq!(fs::read_to_string(output).unwrap(), expected);
    }
    assert!(data_studio::handle(
        Request {
            operation: "rename-column".into(),
            input: input.clone(),
            parameter: "name=group".into(),
            ..Request::default()
        },
        Job::new()
    )
    .is_err());
    assert_eq!(fs::read_to_string(input).unwrap(), original);
}
