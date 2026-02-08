use edi_ir::Value;

pub(crate) fn value_to_f64(value: &Value, arg_label: &str) -> crate::Result<f64> {
    match value {
        Value::Integer(i) => i.to_string().parse::<f64>().map_err(|error| {
            crate::Error::Transform(format!(
                "Cannot convert {arg_label} integer argument to number: {error}"
            ))
        }),
        Value::Decimal(d) => Ok(*d),
        Value::String(s) => s.parse::<f64>().map_err(|_| {
            crate::Error::Transform(format!("Cannot parse {arg_label} argument as number"))
        }),
        _ => Err(crate::Error::Transform(format!(
            "Invalid {arg_label} argument type"
        ))),
    }
}
