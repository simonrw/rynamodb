use std::collections::HashMap;

use crate::types::AttributeType;

use super::queries::Node;

/// Responsible for visiting all nodes in an AST and potentially performing transforms
pub struct NodeVisitor {}

impl NodeVisitor {
    pub fn substitute_placeholders(
        &self,
        ast: Node,
        expression_attribute_names: &HashMap<&str, &str>,
        expression_attribute_values: &HashMap<String, HashMap<AttributeType, String>>,
    ) -> Node {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::table::queries::Operator;

    use super::*;

    #[test]
    #[ignore]
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

        let visitor = NodeVisitor {};
        let expression_attribute_names = {
            let mut h = HashMap::new();
            h.insert("#a", "e");
            h.insert("#c", "g");
            h
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
            h
        };

        let new_ast = visitor.substitute_placeholders(
            ast,
            &expression_attribute_names,
            &expression_attribute_values,
        );
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
