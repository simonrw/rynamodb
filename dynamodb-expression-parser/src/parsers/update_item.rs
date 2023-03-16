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
    lhs: Path,
    rhs: Name,
}

#[derive(PartialEq, Debug, Clone)]
pub struct Path(Vec<Name>);

#[derive(PartialEq, Debug, Clone)]
pub enum Name {
    Placeholder(String),
    Value(String),
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
        Some(Rule::set_action) => parse_set_actions(expression),
        Some(Rule::remove_action) => todo!(),
        Some(Rule::add_action) => todo!(),
        Some(Rule::delete_action) => todo!(),
        _ => unreachable!(),
    }
}

fn parse_set_actions<'a, I>(items: I) -> Result<Node, ParserError>
where
    I: Iterator<Item = Pair<'a, Rule>>,
{
    let assignments: Vec<Assignment> = items.map(parse_set_action).collect();

    Ok(Node::Set(assignments))
}

fn parse_set_action(item: Pair<'_, Rule>) -> Assignment {
    assert_eq!(item.as_rule(), Rule::set_action);
    let mut inners = item.into_inner();
    let lhs = parse_path(inners.next().unwrap());
    let rhs = parse_value(inners.next().unwrap());

    Assignment { lhs, rhs }
}

fn parse_path(item: Pair<'_, Rule>) -> Path {
    assert_eq!(item.as_rule(), Rule::path);
    // TODO
    Path(vec![Name::Value("a".to_string())])
}

fn parse_value(item: Pair<'_, Rule>) -> Name {
    assert_eq!(item.as_rule(), Rule::value);
    let pairs: Vec<_> = item.into_inner().collect();
    if pairs.len() == 1 {
        // parse_operand(pairs[0])
        todo!()
    } else if pairs.len() == 2 {
        // binop
        Name::Value("lkjansdg".to_string())
    } else {
        unreachable!()
    }
}

fn parse_operand(item: Pair<'_, Rule>) -> Name {
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
                lhs: Path(vec![Name::Value("a".to_string())]),
                rhs: Name::Value("b".to_string()),
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
