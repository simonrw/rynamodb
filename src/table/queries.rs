use pest::{iterators::Pair, Parser};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParserError {
    #[error("parse error: {0}")]
    ParseError(String),
    #[error("end of items reached unexpectedly")]
    Eoi,
}

#[derive(pest_derive::Parser)]
#[grammar = "grammar.pest"]
struct DynamoDBParser;

#[derive(PartialEq, Debug)]
pub enum Node {
    Binop {
        lhs: Box<Node>,
        rhs: Box<Node>,
        op: Operator,
    },
    FunctionCall {
        name: String,
        args: Vec<Node>,
    },
    Attribute(String),
    Placeholder(String),
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum Operator {
    Eq,
    And,
}

fn parse_and_condition(root: Pair<Rule>) -> Result<Node, ParserError> {
    assert_eq!(root.as_rule(), Rule::and_condition);

    let mut pairs = root.into_inner();
    let lhs = parse_condition(pairs.next().ok_or(ParserError::Eoi)?)?;
    let rhs = parse_condition(pairs.next().ok_or(ParserError::Eoi)?)?;

    Ok(Node::Binop {
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
        op: Operator::And,
    })
}

fn parse_key(root: Pair<Rule>) -> Result<Node, ParserError> {
    assert_eq!(root.as_rule(), Rule::key);

    let inner = root.into_inner().next().ok_or(ParserError::Eoi)?;
    let node = match inner.as_rule() {
        Rule::column_name => Node::Attribute(inner.as_str().to_string()),
        Rule::key_placeholder => {
            let s = inner.as_str().strip_prefix('#').unwrap();
            Node::Placeholder(s.to_string())
        }
        r => unreachable!("{r:?}"),
    };
    Ok(node)
}

fn parse_value(root: Pair<Rule>) -> Result<Node, ParserError> {
    assert_eq!(root.as_rule(), Rule::value);
    let inner = root.into_inner().next().ok_or(ParserError::Eoi)?;
    let node = match inner.as_rule() {
        Rule::column_name => Node::Attribute(inner.as_str().to_string()),
        Rule::value_placeholder => {
            let s = inner.as_str().strip_prefix(':').unwrap();
            Node::Placeholder(s.to_string())
        }
        r => unreachable!("{r:?}"),
    };
    Ok(node)
}

fn parse_begins_with(root: Pair<Rule>) -> Result<Node, ParserError> {
    assert_eq!(root.as_rule(), Rule::begins_with);

    let mut pairs = root.into_inner();
    let key = parse_key(pairs.next().ok_or(ParserError::Eoi)?)?;
    let value = parse_value(pairs.next().ok_or(ParserError::Eoi)?)?;

    let node = Node::FunctionCall {
        name: "begins_with".to_string(),
        args: vec![key, value],
    };
    Ok(node)
}

fn parse_function(root: Pair<Rule>) -> Result<Node, ParserError> {
    assert_eq!(root.as_rule(), Rule::function);

    let inner = root.into_inner().next().ok_or(ParserError::Eoi)?;
    let node = match inner.as_rule() {
        Rule::begins_with => parse_begins_with(inner)?,
        r => unreachable!("{r:?}"),
    };

    Ok(node)
}

fn parse_condition(root: Pair<Rule>) -> Result<Node, ParserError> {
    assert_eq!(root.as_rule(), Rule::condition);

    let mut pairs = root.into_inner();

    // determine what kind of condition we have
    if let Some(next) = pairs.peek() {
        if next.as_rule() == Rule::function {
            // short circuit the function parse tree
            return parse_function(next.clone());
        }
    }

    let lhs = {
        let node = pairs.next().ok_or(ParserError::Eoi)?;
        match node.as_rule() {
            Rule::key => parse_key(node)?,
            Rule::value => parse_value(node)?,
            Rule::function => parse_function(node)?,
            r => unreachable!("{r:?}"),
        }
    };

    // TODO: op
    let _ = pairs.next().ok_or(ParserError::Eoi)?;

    let rhs = {
        let node = pairs.next().ok_or(ParserError::Eoi)?;
        match node.as_rule() {
            Rule::key => parse_key(node)?,
            Rule::value => parse_value(node)?,
            r => unreachable!("{r:?}"),
        }
    };

    Ok(Node::Binop {
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
        op: Operator::Eq,
    })
}

pub fn parse(input: &str) -> Result<Node, ParserError> {
    let mut pairs = DynamoDBParser::parse(Rule::condition_expression, input).unwrap();
    let root = pairs
        .next()
        .ok_or(ParserError::Eoi)?
        .into_inner()
        .next()
        .ok_or(ParserError::Eoi)?;
    match root.as_rule() {
        Rule::and_condition => parse_and_condition(root),
        Rule::condition => parse_condition(root),
        r => unreachable!("{r:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_1() {
        let s = "Id = :id AND begins_with(ReplyDateTime, :dt)";
        let ast = parse(s).unwrap();
        assert_eq!(
            ast,
            Node::Binop {
                lhs: Box::new(Node::Binop {
                    lhs: Box::new(Node::Attribute("Id".to_string())),
                    rhs: Box::new(Node::Placeholder("id".to_string())),
                    op: Operator::Eq,
                }),
                rhs: Box::new(Node::FunctionCall {
                    name: "begins_with".to_string(),
                    args: vec![
                        Node::Attribute("ReplyDateTime".to_string()),
                        Node::Placeholder("dt".to_string()),
                    ],
                }),
                op: Operator::And,
            }
        );
    }

    #[test]
    fn example_2() {
        let s = "ForumName = :name";
        let ast = parse(s).unwrap();
        assert_eq!(
            ast,
            Node::Binop {
                lhs: Box::new(Node::Attribute("ForumName".to_string())),
                rhs: Box::new(Node::Placeholder("name".to_string())),
                op: Operator::Eq,
            }
        );
    }
}
