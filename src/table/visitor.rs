use std::collections::HashMap;

use crate::types::AttributeType;

use super::queries::Node;

pub fn walk_binop<V: Visitor + ?Sized>(v: &V, n: &mut Node) {
    match n {
        Node::Binop { lhs, rhs, .. } => {
            match lhs.as_mut() {
                n @ Node::Binop { .. } => v.visit_binop(n),
                n @ Node::FunctionCall { .. } => v.visit_function_call(n),
                n @ Node::Attribute(_) => v.visit_attribute(n),
                n @ Node::Placeholder(_) => v.visit_placeholder(n),
            }
            match rhs.as_mut() {
                n @ Node::Binop { .. } => v.visit_binop(n),
                n @ Node::FunctionCall { .. } => v.visit_function_call(n),
                n @ Node::Attribute(_) => v.visit_attribute(n),
                n @ Node::Placeholder(_) => v.visit_placeholder(n),
            }
        }
        _ => unreachable!(),
    }
}
pub fn walk_function_call<V: Visitor + ?Sized>(_: &V, _: &mut Node) {}
pub fn walk_attribute<V: Visitor + ?Sized>(_: &V, _: &mut Node) {}
pub fn walk_placeholder<V: Visitor + ?Sized>(_: &V, _: &mut Node) {}

pub trait Visitor {
    fn visit_binop(&self, n: &mut Node) {
        walk_binop(self, n);
    }

    fn visit_function_call(&self, n: &mut Node) {
        walk_function_call(self, n);
    }

    fn visit_attribute(&self, n: &mut Node) {
        walk_attribute(self, n);
    }

    fn visit_placeholder(&self, n: &mut Node) {
        walk_placeholder(self, n);
    }
}

/// Responsible for visiting all nodes in an AST and potentially performing transforms
pub struct NodeVisitor<'a> {
    expression_attribute_names: &'a Option<HashMap<String, String>>,
    expression_attribute_values: &'a Option<HashMap<String, HashMap<AttributeType, String>>>,
}

impl<'a> NodeVisitor<'a> {
    pub fn new(
        expression_attribute_names: &'a Option<HashMap<String, String>>,
        expression_attribute_values: &'a Option<HashMap<String, HashMap<AttributeType, String>>>,
    ) -> Self {
        Self {
            expression_attribute_names,
            expression_attribute_values,
        }
    }

    #[allow(clippy::needless_borrow)]
    pub fn visit(&self, mut ast: Node) -> Node {
        match &mut ast {
            mut n @ Node::Binop { .. } => self.visit_binop(&mut n),
            mut n @ Node::FunctionCall { .. } => self.visit_function_call(&mut n),
            mut n @ Node::Attribute(_) => self.visit_attribute(&mut n),
            mut n @ Node::Placeholder(_) => self.visit_placeholder(&mut n),
        }
        ast
    }
}

impl<'a> Visitor for NodeVisitor<'a> {
    fn visit_placeholder(&self, n: &mut Node) {
        // convert the placeholder to attribute
        let key = n.as_str().unwrap();

        let name_key = format!("#{key}");
        let value_key = format!(":{key}");

        if let Some(value) = self
            .expression_attribute_names
            .as_ref()
            .and_then(|names| names.get(name_key.as_str()))
        {
            *n = Node::Attribute(value.to_string());
            return;
        }

        if let Some(possible_values) = self
            .expression_attribute_values
            .as_ref()
            .and_then(|values| values.get(&value_key))
        {
            let value = possible_values
                .values()
                .next()
                .expect("attribute values map empty");
            *n = Node::Attribute(value.to_string());
            return;
        }

        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use crate::table::queries::Operator;

    use super::*;

    #[test]
    fn visit_all_nodes() {
        let ast = Node::Binop {
            lhs: Box::new(Node::Binop {
                lhs: Box::new(Node::Placeholder("a".to_string())),
                rhs: Box::new(Node::Placeholder("b".to_string())),
                op: Operator::Eq,
            }),
            rhs: Box::new(Node::Binop {
                lhs: Box::new(Node::Placeholder("c".to_string())),
                rhs: Box::new(Node::Placeholder("d".to_string())),
                op: Operator::Eq,
            }),
            op: Operator::And,
        };

        let expression_attribute_names = {
            let mut h = HashMap::new();
            h.insert("#a".to_string(), "e".to_string());
            h.insert("#c".to_string(), "g".to_string());
            Some(h)
        };

        macro_rules! attr {
            ($name:expr) => {{
                let mut h = HashMap::new();
                h.insert(AttributeType::S, $name.to_string());
                h
            }};
        }

        let expression_attribute_values = {
            let mut h = HashMap::new();
            h.insert(":b".to_string(), attr!("f"));
            h.insert(":d".to_string(), attr!("h"));
            Some(h)
        };

        let visitor = NodeVisitor::new(&expression_attribute_names, &expression_attribute_values);
        let new_ast = visitor.visit(ast);
        assert_eq!(
            new_ast,
            Node::Binop {
                lhs: Box::new(Node::Binop {
                    lhs: Box::new(Node::Attribute("e".to_string())),
                    rhs: Box::new(Node::Attribute("f".to_string())),
                    op: Operator::Eq,
                }),
                rhs: Box::new(Node::Binop {
                    lhs: Box::new(Node::Attribute("g".to_string())),
                    rhs: Box::new(Node::Attribute("h".to_string())),
                    op: Operator::Eq,
                }),
                op: Operator::And,
            }
        );
    }
}
