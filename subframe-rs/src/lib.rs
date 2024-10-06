mod table;
mod value;

pub use table::Table;
pub use value::Value;

use substrait::proto::r#type::{Kind, Struct, I16, I32, I64, I8};
use substrait::proto::{
    read_rel::{NamedTable, ReadType},
    rel, NamedStruct, ReadRel, Rel, RelRoot, Type,
};

fn substrait_type_from_string(type_: &String) -> Type {
    let kind = match type_.as_str() {
        "i8" => Some(Kind::I8(I8 {
            ..Default::default()
        })),
        "i16" => Some(Kind::I16(I16 {
            ..Default::default()
        })),
        "i32" => Some(Kind::I32(I32 {
            ..Default::default()
        })),
        "i64" => Some(Kind::I64(I64 {
            ..Default::default()
        })),
        _ => None,
    };

    Type { kind }
}

pub fn table(_schema: Vec<(String, String)>, name: String) -> Table {
    let names = _schema.iter().map(|x| x.0.clone()).collect::<Vec<_>>();

    let types = _schema
        .iter()
        .map(|x| substrait_type_from_string(&x.1))
        .collect::<Vec<_>>();

    let _struct = Struct {
        types: types,
        ..Default::default()
    };

    let named_struct = NamedStruct {
        names: names.clone(),
        r#struct: Some(_struct.clone()),
    };

    let named_table = NamedTable {
        names: vec![name],
        ..Default::default()
    };

    let read_rel = ReadRel {
        base_schema: Some(named_struct),
        read_type: Some(ReadType::NamedTable(named_table)),
        ..Default::default()
    };

    let rel = Rel {
        rel_type: Some(rel::RelType::Read(Box::new(read_rel))),
    };

    let rel_root = RelRoot {
        input: Some(rel),
        names: names,
    };

    Table {
        rel_root,
        schema: _struct,
    }
}
