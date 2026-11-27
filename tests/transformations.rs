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
fn literal_filters_are_case_sensitive_and_reject_empty_needle() {
    let source = "name,team\nAda,red\nGrace,blue\nAdele,green\n";
    let contains = operation(
        source,
        "filter-contains",
        json!({"column":"name","value":"a"}),
    );
    assert_eq!(
        contains["preview"],
        json!([["Ada", "red"], ["Grace", "blue"]])
    );
    let excludes = operation(
        source,
        "filter-not-contains",
        json!({"column":"name","value":"Grace"}),
    );
    assert_eq!(
        excludes["preview"],
        json!([["Ada", "red"], ["Adele", "green"]])
    );
    assert!(fails(
        source,
        "filter-contains",
        json!({"column":"name","value":""})
    ));
}

#[test]
fn head_and_tail_keep_requested_rows_in_source_order() {
    let source = "id\n1\n2\n3\n4\n";
    assert_eq!(
        operation(source, "head-rows", json!({"rows":2}))["preview"],
        json!([["1"], ["2"]])
    );
    assert_eq!(
        operation(source, "tail-rows", json!({"rows":2}))["preview"],
        json!([["3"], ["4"]])
    );
    assert_eq!(
        operation(source, "tail-rows", json!({"rows":10}))["preview"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    assert!(fails(source, "head-rows", json!({"rows":500001})));
}

#[test]
fn subset_duplicate_removal_keeps_the_first_exact_key() {
    let result = operation(
        "id,email,name\n1,a,A\n2,a,B\n3,,C\n4,,D\n",
        "drop-duplicates-by-columns",
        json!({"columns":["email"]}),
    );
    assert_eq!(result["preview"], json!([["1", "a", "A"], ["3", "", "C"]]));
    assert!(fails(
        "id\n1\n",
        "drop-duplicates-by-columns",
        json!({"columns":[]})
    ));
}

#[test]
fn index_and_constant_columns_are_bounded_and_ordered() {
    let source = "name\nAda\nGrace\n";
    let indexed = operation(source, "add-index-column", json!({"name":"row","start":7}));
    assert_eq!(indexed["headers"], json!(["name", "row"]));
    assert_eq!(indexed["preview"], json!([["Ada", "7"], ["Grace", "8"]]));
    let tagged = operation(
        source,
        "add-constant-column",
        json!({"name":"cohort","value":"pilot"}),
    );
    assert_eq!(
        tagged["preview"],
        json!([["Ada", "pilot"], ["Grace", "pilot"]])
    );
    assert!(fails(
        source,
        "add-index-column",
        json!({"name":"row","start":9223372036854775807_i64})
    ));
}

#[test]
fn text_normalization_supports_trim_case_and_unicode_whitespace() {
    let source = "name\n  ÉCOLE  \nStraße\nA\t B\n";
    assert_eq!(
        operation(
            source,
            "normalize-text",
            json!({"column":"name","mode":"trim"})
        )["preview"][0][0],
        "ÉCOLE"
    );
    assert_eq!(
        operation(
            source,
            "normalize-text",
            json!({"column":"name","mode":"lower"})
        )["preview"][0][0],
        "  école  "
    );
    assert_eq!(
        operation(
            source,
            "normalize-text",
            json!({"column":"name","mode":"upper"})
        )["preview"][1][0],
        "STRASSE"
    );
    assert_eq!(
        operation(
            source,
            "normalize-text",
            json!({"column":"name","mode":"collapse-whitespace"})
        )["preview"][2][0],
        "A B"
    );
    assert!(fails(
        source,
        "normalize-text",
        json!({"column":"name","mode":"locale"})
    ));
}

#[test]
fn multi_rename_is_atomic_and_drop_columns_preserves_order() {
    let source = "first,last,temp\nAda,Lovelace,x\n";
    let swapped = operation(
        source,
        "rename-columns",
        json!({"mapping":{"first":"last","last":"first"}}),
    );
    assert_eq!(swapped["headers"], json!(["last", "first", "temp"]));
    assert!(fails(
        source,
        "rename-columns",
        json!({"mapping":{"first":"temp"}})
    ));
    let dropped = operation(source, "drop-columns", json!({"columns":["temp"]}));
    assert_eq!(dropped["headers"], json!(["first", "last"]));
    assert_eq!(dropped["preview"], json!([["Ada", "Lovelace"]]));
    assert!(fails(
        source,
        "drop-columns",
        json!({"columns":["first","last","temp"]})
    ));
}

#[test]
fn all_ten_new_transformations_replay_in_a_project() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.csv");
    let project = dir.path().join("project.json");
    fs::write(
        &input,
        "name,team\nAda Lovelace,alpha\nAda Byron,alpha\nGrace Hopper,beta\n",
    )
    .unwrap();
    fs::write(
        &project,
        json!({"schema_version":1,"steps":[
            {"operation":"filter-contains","column":"name","value":"Ada"},
            {"operation":"filter-not-contains","column":"name","value":"Byron"},
            {"operation":"head-rows","rows":5},
            {"operation":"tail-rows","rows":1},
            {"operation":"drop-duplicates-by-columns","columns":["team"]},
            {"operation":"add-index-column","name":"row","start":1},
            {"operation":"add-constant-column","name":"group","value":"pilot"},
            {"operation":"normalize-text","column":"name","mode":"lower"},
            {"operation":"rename-columns","mapping":{"name":"participant"}},
            {"operation":"drop-columns","columns":["team"]}
        ]})
        .to_string(),
    )
    .unwrap();
    let outcome = handle(
        Request {
            operation: "run-project".into(),
            input,
            parameter: project.to_string_lossy().into_owned(),
            ..Request::default()
        },
        Job::new(),
    )
    .unwrap();
    let result: Value = serde_json::from_str(&outcome.preview).unwrap();
    assert_eq!(result["headers"], json!(["participant", "row", "group"]));
    assert_eq!(result["preview"], json!([["ada lovelace", "1", "pilot"]]));
}

#[test]
fn additional_filters_use_explicit_case_and_literal_rules() {
    let source = "name,code\nÉcole,AB-1\nAda,AB-2\nGrace,xy-1\n";
    assert_eq!(
        operation(
            source,
            "filter-not-equal",
            json!({"column":"code","value":"AB-1"})
        )["preview"],
        json!([["Ada", "AB-2"], ["Grace", "xy-1"]])
    );
    assert_eq!(
        operation(
            source,
            "filter-contains-insensitive",
            json!({"column":"name","value":"ÉCOLE"})
        )["preview"],
        json!([["École", "AB-1"]])
    );
    assert_eq!(
        operation(
            source,
            "filter-starts-with",
            json!({"column":"code","value":"AB"})
        )["preview"],
        json!([["École", "AB-1"], ["Ada", "AB-2"]])
    );
    assert_eq!(
        operation(
            source,
            "filter-ends-with",
            json!({"column":"code","value":"-1"})
        )["preview"],
        json!([["École", "AB-1"], ["Grace", "xy-1"]])
    );
    assert!(fails(
        source,
        "filter-contains-insensitive",
        json!({"column":"name","value":""})
    ));
}

#[test]
fn literal_replacement_is_nonoverlapping_and_preserves_unmatched_text() {
    let source = "text\na--a\nnone\n";
    let changed = operation(
        source,
        "replace-literal",
        json!({"column":"text","find":"a","replace":"xy"}),
    );
    assert_eq!(changed["preview"], json!([["xy--xy"], ["none"]]));
    assert!(fails(
        source,
        "replace-literal",
        json!({"column":"text","find":"","replace":"x"})
    ));
}

#[test]
fn drop_rows_any_missing_drops_blank_and_whitespace_cells() {
    let result = operation("a,b\n1,ok\n2, \n,ok\n", "drop-rows-any-missing", json!({}));
    assert_eq!(result["preview"], json!([["1", "ok"]]));
}

#[test]
fn directional_fill_uses_nearest_value_and_leaves_unbounded_edges_blank() {
    let forward = operation(
        "group,id\n,A\nwest,B\n,C\n",
        "fill-forward",
        json!({"column":"group"}),
    );
    assert_eq!(
        forward["preview"],
        json!([["", "A"], ["west", "B"], ["west", "C"]])
    );
    let backward = operation(
        "group,id\n,A\nwest,B\n,C\n, D\n",
        "fill-backward",
        json!({"column":"group"}),
    );
    assert_eq!(
        backward["preview"],
        json!([["west", "A"], ["west", "B"], ["", "C"], ["", " D"]])
    );
}

#[test]
fn empty_column_removal_and_row_reversal_preserve_remaining_values() {
    let source = "id,blank\n1, \n2,\n";
    assert_eq!(
        operation(source, "drop-empty-columns", json!({}))["headers"],
        json!(["id"])
    );
    assert_eq!(
        operation("id\n1\n2\n3\n", "reverse-rows", json!({}))["preview"],
        json!([["3"], ["2"], ["1"]])
    );
    assert!(fails("a,b\n, \n", "drop-empty-columns", json!({})));
}

#[test]
fn numeric_greater_filter_keeps_only_finite_values_above_threshold() {
    let result = operation(
        "score,label\n1,a\n2,b\n3,c\n,missing\n",
        "filter-number-greater",
        json!({"column":"score","value":2}),
    );
    assert_eq!(result["preview"], json!([["3", "c"]]));
}

#[test]
fn numeric_greater_equal_filter_includes_the_threshold() {
    let result = operation(
        "score\n1\n2\n3\n",
        "filter-number-greater-equal",
        json!({"column":"score","value":2}),
    );
    assert_eq!(result["preview"], json!([["2"], ["3"]]));
}

#[test]
fn numeric_less_filter_excludes_values_equal_to_threshold() {
    let result = operation(
        "score\n1\n2\n3\n",
        "filter-number-less",
        json!({"column":"score","value":2}),
    );
    assert_eq!(result["preview"], json!([["1"]]));
}

#[test]
fn numeric_less_equal_filter_includes_the_threshold() {
    let result = operation(
        "score\n1\n2\n3\n",
        "filter-number-less-equal",
        json!({"column":"score","value":2}),
    );
    assert_eq!(result["preview"], json!([["1"], ["2"]]));
}

#[test]
fn numeric_between_filter_has_inclusive_bounds_and_rejects_bad_data_or_bounds() {
    let result = operation(
        "score\n1\n2\n3\n4\n",
        "filter-number-between",
        json!({"column":"score","lower":2,"upper":3}),
    );
    assert_eq!(result["preview"], json!([["2"], ["3"]]));
    assert!(fails(
        "score\n1\ninvalid\n",
        "filter-number-between",
        json!({"column":"score","lower":0,"upper":2})
    ));
    assert!(fails(
        "score\n1\n",
        "filter-number-between",
        json!({"column":"score","lower":2,"upper":1})
    ));
}

#[test]
fn all_five_numeric_filters_replay_in_a_version_one_project() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.csv");
    let project = dir.path().join("project.json");
    fs::write(&input, "id,score\na,1\nb,2\nc,3\nd,4\ne,5\n").unwrap();
    fs::write(
        &project,
        json!({"schema_version":1,"steps":[
            {"operation":"filter-number-greater","column":"score","value":0},
            {"operation":"filter-number-greater-equal","column":"score","value":1},
            {"operation":"filter-number-less","column":"score","value":6},
            {"operation":"filter-number-less-equal","column":"score","value":5},
            {"operation":"filter-number-between","column":"score","lower":2,"upper":4}
        ]})
        .to_string(),
    )
    .unwrap();
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
    assert_eq!(
        result["preview"],
        json!([["b", "2"], ["c", "3"], ["d", "4"]])
    );
}

#[test]
fn all_ten_added_operations_replay_in_order_in_a_project() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.csv");
    let project = dir.path().join("project.json");
    fs::write(
        &input,
        "id,name,tag,route,empty\n1,  Ada  ,A,, \n2, Grace ,,west,\n",
    )
    .unwrap();
    fs::write(
        &project,
        json!({"schema_version":1,"steps":[
            {"operation":"filter-not-equal","column":"id","value":"9"},
            {"operation":"filter-contains-insensitive","column":"name","value":"a"},
            {"operation":"filter-starts-with","column":"name","value":" "},
            {"operation":"filter-ends-with","column":"name","value":" "},
            {"operation":"replace-literal","column":"name","find":"  ","replace":" "},
            {"operation":"fill-backward","column":"route"},
            {"operation":"fill-forward","column":"tag"},
            {"operation":"drop-empty-columns"},
            {"operation":"drop-rows-any-missing"},
            {"operation":"reverse-rows"}
        ]})
        .to_string(),
    )
    .unwrap();
    let result = handle(
        Request {
            operation: "run-project".into(),
            input,
            parameter: project.to_string_lossy().into_owned(),
            ..Request::default()
        },
        Job::new(),
    )
    .unwrap();
    let result: Value = serde_json::from_str(&result.preview).unwrap();
    assert_eq!(result["headers"], json!(["id", "name", "tag", "route"]));
    assert_eq!(
        result["preview"],
        json!([["2", " Grace ", "A", "west"], ["1", " Ada ", "A", "west"]])
    );
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
