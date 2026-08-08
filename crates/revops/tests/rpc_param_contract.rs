use revops::rpc_params::{
    decode_and_call, decode_params, load_rpc_contract, method_spec, ParamBinding, ParamCoercion,
    ParamDecodeError, ParamSpec, RpcMethodSpec,
};
use serde_json::{json, Map, Value};
use std::sync::atomic::{AtomicUsize, Ordering};

fn method(params: Vec<ParamSpec>) -> RpcMethodSpec {
    RpcMethodSpec {
        name: "test-method".to_string(),
        python_binding: ParamBinding::PositionalOrNamed,
        allow_extra_named: false,
        params,
    }
}

fn optional(name: &str, default: Value, coercion: ParamCoercion) -> ParamSpec {
    ParamSpec {
        name: name.to_string(),
        required: false,
        has_default: true,
        default,
        coercion,
        python_kind: None,
    }
}

#[test]
fn checked_in_contract_has_exactly_39_unique_methods() {
    let contract = load_rpc_contract();
    let mut names = contract
        .methods
        .iter()
        .map(|method| method.name.as_str())
        .collect::<Vec<_>>();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), 39);
    assert_eq!(
        contract.python_source_commit,
        "a5c2e2f65019df5cefe4e1261b7de2823a03e448"
    );
}

#[test]
fn method_lookup_returns_the_exact_generated_contract() {
    let contract = load_rpc_contract();
    let spec = method_spec(&contract, "revenue-wake-all");
    assert_eq!(spec.name, "revenue-wake-all");
    assert_eq!(spec.python_binding, ParamBinding::PositionalOrNamed);
}

#[test]
#[should_panic(expected = "missing embedded RPC contract for revenue-no-such-method")]
fn method_lookup_refuses_an_unregistered_name() {
    let contract = load_rpc_contract();
    let _ = method_spec(&contract, "revenue-no-such-method");
}

#[test]
fn object_and_positional_array_bind_to_the_same_parameter_map() {
    let spec = method(vec![
        optional("window", json!(24), ParamCoercion::PythonInt),
        optional("include", json!(false), ParamCoercion::PythonTruthy),
    ]);
    let named = decode_params(
        &spec,
        &json!({"window": "48", "include": "false"}),
        ParamBinding::PositionalOrNamed,
    )
    .unwrap();
    let positional = decode_params(
        &spec,
        &json!(["48", "false"]),
        ParamBinding::PositionalOrNamed,
    )
    .unwrap();
    assert_eq!(named, positional);
    assert_eq!(named["window"], json!(48));
    assert_eq!(named["include"], json!(true));
}

#[test]
fn empty_array_and_null_are_both_no_params_and_apply_defaults() {
    let spec = method(vec![optional("limit", json!(20), ParamCoercion::PythonInt)]);
    let expected = Map::from_iter([("limit".to_string(), json!(20))]);
    assert_eq!(
        decode_params(&spec, &json!([]), ParamBinding::PositionalOrNamed).unwrap(),
        expected
    );
    assert_eq!(
        decode_params(&spec, &Value::Null, ParamBinding::PositionalOrNamed).unwrap(),
        expected
    );
}

#[test]
fn scalar_params_and_excess_arguments_are_typed_errors() {
    let spec = method(vec![optional("limit", json!(20), ParamCoercion::PythonInt)]);
    assert!(matches!(
        decode_params(&spec, &json!(42), ParamBinding::PositionalOrNamed),
        Err(ParamDecodeError::InvalidShape { .. })
    ));
    assert!(matches!(
        decode_params(&spec, &json!([1, 2]), ParamBinding::PositionalOrNamed),
        Err(ParamDecodeError::ExcessPositional {
            expected: 1,
            actual: 2
        })
    ));
    assert!(matches!(
        decode_params(&spec, &json!({"limit": 1, "extra": 2}), ParamBinding::PositionalOrNamed),
        Err(ParamDecodeError::UnknownNamed { name }) if name == "extra"
    ));
}

#[test]
fn missing_required_and_coercion_failures_are_loud() {
    let required = ParamSpec {
        name: "peer_id".to_string(),
        required: true,
        has_default: false,
        default: Value::Null,
        coercion: ParamCoercion::String,
        python_kind: None,
    };
    let spec = method(vec![
        required,
        optional("limit", json!(20), ParamCoercion::PythonInt),
    ]);
    assert!(matches!(
        decode_params(&spec, &json!({}), ParamBinding::PositionalOrNamed),
        Err(ParamDecodeError::MissingRequired { name }) if name == "peer_id"
    ));
    assert!(matches!(
        decode_params(&spec, &json!({"peer_id": "02aa", "limit": "garbage"}), ParamBinding::PositionalOrNamed),
        Err(ParamDecodeError::Coercion { name, .. }) if name == "limit"
    ));
}

#[test]
fn deliberate_named_only_refusal_happens_before_handler_execution() {
    let calls = AtomicUsize::new(0);
    let spec = method(vec![optional("limit", json!(20), ParamCoercion::PythonInt)]);
    let result = decode_and_call(&spec, &json!([7]), ParamBinding::NamedOnly, |_| {
        calls.fetch_add(1, Ordering::SeqCst);
        json!({"unexpected": true})
    });
    assert!(matches!(result, Err(ParamDecodeError::NamedOnly)));
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let empty = decode_and_call(&spec, &json!([]), ParamBinding::NamedOnly, |params| {
        calls.fetch_add(1, Ordering::SeqCst);
        json!(params)
    })
    .unwrap();
    assert_eq!(empty["limit"], json!(20));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
