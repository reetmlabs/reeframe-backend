//! JSONPath (RFC 9535) lookups into an event payload or a manual trigger's
//! parameters, exposed to filter and condition expressions as functions.

use evalexpr::{
    ContextWithMutableFunctions, EvalexprError, EvalexprResult, Function, HashMapContext,
    Value as EvalValue,
};
use serde_json::Value as JsonValue;
use serde_json_path::JsonPath;

/// Adds `json_path(path)` (first match) and `json_paths(path)` (every match,
/// as a tuple) over `payload`.
pub(crate) fn register_json_path(ctx: &mut HashMapContext, payload: Option<&JsonValue>) {
    register(ctx, "json_path", "json_paths", payload);
}

/// Adds `manual_path(path)` and `manual_paths(path)` over a manual trigger's
/// parameters, with the same semantics as [`register_json_path`].
pub(crate) fn register_manual_path(ctx: &mut HashMapContext, params: Option<&JsonValue>) {
    register(ctx, "manual_path", "manual_paths", params);
}

fn register(ctx: &mut HashMapContext, first: &str, all: &str, root: Option<&JsonValue>) {
    let root = root.cloned().unwrap_or(JsonValue::Null);
    let first_root = root.clone();
    ctx.set_function(
        first.into(),
        Function::new(move |arg| {
            Ok(query(&first_root, arg)?
                .first()
                .map_or(EvalValue::Empty, |v| to_eval(v)))
        }),
    )
    .ok();
    ctx.set_function(
        all.into(),
        Function::new(move |arg| {
            Ok(EvalValue::Tuple(
                query(&root, arg)?.into_iter().map(to_eval).collect(),
            ))
        }),
    )
    .ok();
}

fn query<'a>(root: &'a JsonValue, arg: &EvalValue) -> EvalexprResult<Vec<&'a JsonValue>> {
    let path = JsonPath::parse(&arg.as_string()?)
        .map_err(|e| EvalexprError::CustomMessage(format!("invalid JSONPath: {e}")))?;
    Ok(path.query(root).all())
}

/// Scalars map to their evalexpr value, whole numbers as `Int`. Null, objects
/// and arrays map to `Empty`, since an expression can only compare scalars.
pub(crate) fn to_eval(v: &JsonValue) -> EvalValue {
    match v {
        JsonValue::String(s) => EvalValue::String(s.clone()),
        JsonValue::Bool(b) => EvalValue::Boolean(*b),
        JsonValue::Number(n) => match n.as_i64() {
            Some(i) => EvalValue::Int(i),
            None => n.as_f64().map_or(EvalValue::Empty, EvalValue::Float),
        },
        JsonValue::Null | JsonValue::Array(_) | JsonValue::Object(_) => EvalValue::Empty,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use evalexpr::{eval_boolean_with_context, eval_with_context};
    use serde_json::json;

    fn ctx_for(payload: JsonValue) -> HashMapContext {
        let mut ctx = HashMapContext::new();
        register_json_path(&mut ctx, Some(&payload));
        ctx
    }

    #[test]
    fn json_path_reads_nested_objects_and_array_elements() {
        let ctx = ctx_for(json!({"data": {"items": [{"label": "car"}, {"label": "bus"}]}}));
        assert_eq!(
            eval_boolean_with_context(r#"json_path("$.data.items[1].label") == "bus""#, &ctx),
            Ok(true)
        );
    }

    #[test]
    fn json_path_returns_the_first_of_several_matches() {
        let ctx = ctx_for(json!({"items": [{"label": "car"}, {"label": "bus"}]}));
        assert_eq!(
            eval_with_context(r#"json_path("$.items[*].label")"#, &ctx),
            Ok(EvalValue::String("car".into()))
        );
    }

    #[test]
    fn json_paths_returns_every_match_for_contains() {
        let ctx = ctx_for(json!({"items": [{"label": "car"}, {"label": "bus"}]}));
        assert_eq!(
            eval_boolean_with_context(r#"contains(json_paths("$.items[*].label"), "bus")"#, &ctx),
            Ok(true)
        );
    }

    #[test]
    fn missing_path_is_empty_and_never_equal() {
        let ctx = ctx_for(json!({"a": 1}));
        assert_eq!(
            eval_with_context(r#"json_path("$.b")"#, &ctx),
            Ok(EvalValue::Empty)
        );
        assert_eq!(
            eval_boolean_with_context(r#"json_path("$.b") == "car""#, &ctx),
            Ok(false)
        );
    }

    #[test]
    fn numbers_map_to_int_or_float_and_containers_to_empty() {
        let ctx = ctx_for(json!({"n": 3, "f": 0.5, "o": {"k": 1}, "a": [1], "z": null}));
        let eval = |path: &str| eval_with_context(&format!(r#"json_path("{path}")"#), &ctx);
        assert_eq!(eval("$.n"), Ok(EvalValue::Int(3)));
        assert_eq!(eval("$.f"), Ok(EvalValue::Float(0.5)));
        assert_eq!(eval("$.o"), Ok(EvalValue::Empty));
        assert_eq!(eval("$.a"), Ok(EvalValue::Empty));
        assert_eq!(eval("$.z"), Ok(EvalValue::Empty));
    }

    #[test]
    fn invalid_path_is_an_evaluation_error() {
        let ctx = ctx_for(json!({}));
        let err = eval_with_context(r#"json_path("$.[")"#, &ctx).unwrap_err();
        assert!(err.to_string().contains("invalid JSONPath"), "{err}");
    }

    #[test]
    fn missing_payload_matches_nothing() {
        let mut ctx = HashMapContext::new();
        register_json_path(&mut ctx, None);
        assert_eq!(
            eval_with_context(r#"json_paths("$.a")"#, &ctx),
            Ok(EvalValue::Tuple(vec![]))
        );
    }

    #[test]
    fn manual_path_reads_manual_params() {
        let mut ctx = HashMapContext::new();
        register_manual_path(&mut ctx, Some(&json!({"zone": {"id": 4}})));
        assert_eq!(
            eval_boolean_with_context(r#"manual_path("$.zone.id") == 4"#, &ctx),
            Ok(true)
        );
    }
}
