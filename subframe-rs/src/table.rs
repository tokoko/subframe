use std::vec;

use super::Value;
use substrait::proto::expression::field_reference::{ReferenceType, RootReference};
use substrait::proto::expression::reference_segment::{self, StructField};
use substrait::proto::expression::{field_reference, FieldReference, ReferenceSegment, RexType};
use substrait::proto::plan_rel::RelType;
use substrait::proto::r#type::Struct;
use substrait::proto::rel_common::Emit;
use substrait::proto::{rel, Plan, PlanRel, Rel, RelCommon, RelRoot};
use substrait::proto::{Expression, ProjectRel};

#[derive(Debug)]
pub struct Table {
    pub rel_root: RelRoot,
    pub schema: Struct,
}

impl Table {
    pub fn to_substrait(&self) -> Plan {
        let plan_rel = PlanRel {
            rel_type: Some(RelType::Root(self.rel_root.clone())),
        };

        Plan {
            relations: vec![plan_rel],
            ..Default::default()
        }
    }

    fn col(&self, field: String) -> Value {
        let position = self
            .rel_root
            .names
            .iter()
            .position(|x| *x == field)
            .unwrap();

        let struct_field = StructField {
            field: position as i32,
            ..Default::default()
        };

        let reference_segment = ReferenceSegment {
            reference_type: Some(reference_segment::ReferenceType::StructField(Box::new(
                struct_field,
            ))),
        };

        let field_reference = FieldReference {
            reference_type: Some(ReferenceType::DirectReference(reference_segment)),
            root_type: Some(field_reference::RootType::RootReference(RootReference {})),
        };

        let expression = Expression {
            rex_type: Some(RexType::Selection(Box::new(field_reference))),
        };

        Value {
            expression,
            dtype: self.schema.types[position].clone(),
        }
    }

    pub fn select(&self, columns: Vec<String>) -> Table {
        let expressions: Vec<Value> = columns.iter().map(|f| self.col(f.clone())).collect();

        let start_index: i32 = self.schema.types.len().try_into().unwrap();
        let num_expressions: i32 = expressions.len().try_into().unwrap();

        let project_rel = ProjectRel {
            input: Some(Box::new(self.rel_root.input.clone().unwrap())),
            expressions: expressions.iter().map(|e| e.expression.clone()).collect(),
            common: Some(RelCommon {
                emit_kind: Some(substrait::proto::rel_common::EmitKind::Emit(Emit {
                    output_mapping: (start_index..start_index + num_expressions).collect(),
                })),
                ..Default::default()
            }),
            ..Default::default()
        };

        let rel = Rel {
            rel_type: Some(rel::RelType::Project(Box::new(project_rel))),
        };

        let rel_root = RelRoot {
            input: Some(rel),
            names: columns.clone(),
        };

        let _struct = Struct {
            types: expressions.iter().map(|e| e.dtype.clone()).collect(),
            ..Default::default()
        };

        Table {
            rel_root,
            schema: _struct,
        }
    }
}
