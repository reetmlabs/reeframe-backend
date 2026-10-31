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
fn to_eval(v: &JsonValue) -> EvalValue {
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
