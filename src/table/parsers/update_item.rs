use super::ParserError;
use pest::{iterators::Pair, Parser};

#[derive(pest_derive::Parser)]
#[grammar = "update_item.pest"]
struct UpdateItemParser;

// the AST
#[derive(PartialEq, Debug, Clone)]
pub enum Node {
    Set(Vec<Assignment>),
}

#[derive(PartialEq, Debug, Clone)]
pub struct Assignment {
    lhs: Name,
    rhs: Name,
}

#[derive(PartialEq, Debug, Clone)]
pub enum Name {
    Placeholder(String),
    Path(Vec<String>),
}

pub fn parse(query: &str) -> Result<Node, ParserError> {
    let pairs = UpdateItemParser::parse(Rule::update_expression, query)
        .map_err(|e| ParserError::ParseError(format!("{e}")))?
        .next()
        .unwrap();
    // sanity check that we have matched the correct rule
    assert_eq!(pairs.as_rule(), Rule::update_expression);
    let mut expression = pairs.into_inner().peekable();
    match expression.peek().map(|e| e.as_rule()) {
        Some(Rule::set_action) => parse_set_action(expression.next().unwrap()),
        Some(Rule::remove_action) => todo!(),
        Some(Rule::add_action) => todo!(),
        Some(Rule::delete_action) => todo!(),
        _ => unreachable!(),
    }
}

fn parse_set_action(pair: Pair<Rule>) -> Result<Node, ParserError> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ast parsing tests
    #[test]
    #[ignore]
    fn parser_simple() {
        let s = "SET a = b";
        assert_eq!(
            parse(s).unwrap(),
            Node::Set(vec![Assignment {
                lhs: Name::Path(vec!["a".to_string()]),
                rhs: Name::Path(vec!["b".to_string()]),
            },])
        );
    }

    // pest tests

    #[test]
    fn simple() {
        for s in &["SET a = b", "SET a = :b", "SET #a = b", "SET #k = :b"] {
            assert!(UpdateItemParser::parse(Rule::update_expression, s).is_ok());
        }
    }

    #[test]

    fn multiple() {
        for s in &["SET a = b, c = d"] {
            assert!(UpdateItemParser::parse(Rule::update_expression, s).is_ok());
        }
    }
}
