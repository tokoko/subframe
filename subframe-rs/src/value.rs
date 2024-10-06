use substrait::proto::{Expression, Type};

pub struct Value {
    pub expression: Expression,
    pub dtype: Type,
}
