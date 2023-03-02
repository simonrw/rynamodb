use std::str::FromStr;

use thiserror::Error;

#[derive(Error, Debug)]
pub(super) enum QueryError {
    #[error("parsing input")]
    ParseError,
}

type Result<T> = std::result::Result<T, QueryError>;

mod parser {
    use pest::Parser;

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

    #[derive(PartialEq, Debug)]
    pub enum Operator {
        Eq,
        And,
    }

    fn parse(input: &str) -> Result<Node, Box<dyn std::error::Error>> {
        let mut pairs = DynamoDBParser::parse(Rule::condition_expression, input).unwrap();
        let pairs: Vec<_> = pairs.next().unwrap().into_inner().collect();
        let tree = if pairs.len() == 2 {
            // partition key and range key
            let exp1: Vec<_> = pairs[0].clone().into_inner().collect();
            let first_node = if exp1.len() == 1 {
                // function
                todo!()
            } else if exp1.len() == 3 {
                assert_eq!(exp1[0].as_rule(), Rule::key);
                assert_eq!(exp1[1].as_rule(), Rule::comparator);
                assert_eq!(exp1[2].as_rule(), Rule::value);

                let lhs = {
                    let pair = exp1[0].clone().into_inner().next().unwrap();
                    match pair.as_rule() {
                        Rule::column_name => Node::Attribute(pair.as_str().to_string()),
                        Rule::key_placeholder => Node::Placeholder(pair.as_str().to_string()),
                        _ => unreachable!(),
                    }
                };

                let rhs = {
                    let pair = exp1[2].clone().into_inner().next().unwrap();
                    match pair.as_rule() {
                        Rule::column_name => Node::Attribute(pair.as_str().to_string()),
                        Rule::value_placeholder => {
                            let s = pair.as_str().strip_prefix(':').unwrap();
                            Node::Placeholder(s.to_string())
                        }
                        _ => unreachable!(),
                    }
                };

                Node::Binop {
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                    op: Operator::Eq,
                }
            } else {
                todo!()
            };

            let exp2 = pairs[1].clone().into_inner().next().unwrap();
            let second_node = match exp2.as_rule() {
                Rule::function => {
                    let inner = exp2.clone().into_inner().next().unwrap();
                    match inner.as_rule() {
                        Rule::begins_with => {
                            let mut args = inner.clone().into_inner();
                            let arg1 = args.next().unwrap().into_inner().next().unwrap();
                            let arg1 = match arg1.as_rule() {
                                Rule::column_name => Node::Attribute(arg1.as_str().to_string()),
                                Rule::key_placeholder => {
                                    Node::Placeholder(arg1.as_str().to_string())
                                }
                                _ => todo!(),
                            };
                            let arg2 = args.next().unwrap().into_inner().next().unwrap();
                            let arg2 = match arg2.as_rule() {
                                Rule::column_name => Node::Attribute(arg2.as_str().to_string()),
                                Rule::value_placeholder => {
                                    let s = arg2.as_str().strip_prefix(':').unwrap();
                                    Node::Placeholder(s.to_string())
                                }
                                r => todo!("rule: {r:?}"),
                            };

                            Node::FunctionCall {
                                name: "begins_with".to_string(),
                                args: vec![arg1, arg2],
                            }
                        }
                        _ => todo!(),
                    }
                }
                _ => todo!(),
            };

            Node::Binop {
                lhs: Box::new(first_node),
                rhs: Box::new(second_node),
                op: Operator::And,
            }
        } else {
            // partition key only
            let mut inner = pairs[0].clone().into_inner();
            // key
            let lhs = {
                let lhs = inner.next().unwrap().into_inner().next().unwrap();
                match lhs.as_rule() {
                    Rule::column_name => Node::Attribute(lhs.as_str().to_string()),
                    Rule::key_placeholder => Node::Placeholder(lhs.as_str().to_string()),
                    _ => unreachable!(),
                }
            };
            // op
            let _ = inner.next().unwrap();
            let rhs = {
                let rhs = inner.next().unwrap().into_inner().next().unwrap();
                match rhs.as_rule() {
                    Rule::column_name => Node::Attribute(rhs.as_str().to_string()),
                    Rule::value_placeholder => {
                        let s = rhs.as_str().strip_prefix(':').unwrap();
                        Node::Placeholder(s.to_string())
                    }
                    _ => unreachable!(),
                }
            };

            Node::Binop {
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
                op: Operator::Eq,
            }
        };

        Ok(tree)
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
}

mod components {
    use nom::branch::alt;
    use nom::bytes::complete::{tag, take_while};
    use nom::character::complete::multispace0;
    use nom::character::is_alphabetic;
    use nom::combinator::map;
    use nom::error::ParseError;
    use nom::sequence::delimited;
    use nom::IResult;

    use super::{Name, Operation};

    #[allow(unused_macros)]
    macro_rules! debug_input {
        ($input:tt) => {{
            let s = std::str::from_utf8($input).unwrap();
            dbg!(s);
        }};
    }

    fn ws<'a, F: 'a, O, E: ParseError<&'a [u8]>>(
        inner: F,
    ) -> impl FnMut(&'a [u8]) -> IResult<&'a [u8], O, E>
    where
        F: Fn(&'a [u8]) -> IResult<&'a [u8], O, E>,
    {
        delimited(multispace0, inner, multispace0)
    }

    fn key_placeholder(input: &[u8]) -> IResult<&[u8], Name> {
        let (input, _) = tag("#")(input)?;
        let (input, name) = take_while(is_alphabetic)(input)?;
        Ok((
            input,
            Name::Placeholder(std::str::from_utf8(name).unwrap().to_string()),
        ))
    }

    fn value_placeholder(input: &[u8]) -> IResult<&[u8], Name> {
        let (input, _) = tag(":")(input)?;
        let (input, name) = take_while(is_alphabetic)(input)?;
        Ok((
            input,
            Name::Placeholder(std::str::from_utf8(name).unwrap().to_string()),
        ))
    }

    fn operation_parser(input: &[u8]) -> IResult<&[u8], Operation> {
        let op = alt((tag("="),));
        map(op, |s: &[u8]| {
            let ss = std::str::from_utf8(s).unwrap();
            ss.parse::<Operation>().unwrap()
        })(input)
    }

    pub(super) fn binop(input: &[u8]) -> IResult<&[u8], BinOp> {
        let mut lhs_parser = alt((ws(key_placeholder),));
        let mut rhs_parser = alt((ws(value_placeholder),));

        let (input, lhs) = lhs_parser(input)?;
        let (input, op) = ws(operation_parser)(input)?;
        let (input, rhs) = rhs_parser(input)?;

        Ok((
            input,
            BinOp {
                lhs,
                rhs,
                operation: op,
            },
        ))
    }

    #[derive(Debug, PartialEq)]
    pub(super) struct BinOp {
        pub(super) lhs: Name,
        pub(super) rhs: Name,
        pub(super) operation: Operation,
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_key_placeholder() {
            let s = "#abc";
            let (_, name) = key_placeholder(s.as_bytes()).unwrap();
            assert_eq!(name, Name::Placeholder("abc".to_string()));
        }

        #[test]
        fn test_value_placeholder() {
            let s = ":abc";
            let (_, name) = value_placeholder(s.as_bytes()).unwrap();
            assert_eq!(name, Name::Placeholder("abc".to_string()));
        }

        #[test]
        fn test_binop() {
            let s = "#K = :v";
            match binop(s.as_bytes()) {
                Ok((_, binop)) => {
                    assert_eq!(
                        binop,
                        BinOp {
                            lhs: Name::Placeholder("K".to_string()),
                            rhs: Name::Placeholder("v".to_string()),
                            operation: Operation::Eq,
                        }
                    );
                }
                Err(e) => {
                    let _ = e.map(|e| {
                        let input = std::str::from_utf8(e.input).unwrap();
                        panic!("error parsing: {e:?}, remaining input: `{input}`");
                    });
                }
            }
        }
    }
}

pub(super) fn parse_query(query: impl Into<String>) -> Result<Query> {
    let query = query.into();
    let query_bytes = query.as_bytes();
    let (_, binop) = components::binop(query_bytes).map_err(|_| QueryError::ParseError)?;
    Ok(Query {
        conditions: vec![Condition {
            name: binop.lhs,
            value: binop.rhs,
            operation: binop.operation,
        }],
    })
}

#[derive(Debug, PartialEq)]
pub(super) struct Query {
    conditions: Vec<Condition>,
}

#[derive(Debug, PartialEq)]
pub(super) struct Condition {
    name: Name,
    value: Name,
    operation: Operation,
}

#[derive(Debug, PartialEq)]
pub(super) enum Name {
    Placeholder(String),
}

#[derive(Debug, PartialEq)]
pub(super) enum Operation {
    Eq,
}

impl FromStr for Operation {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "=" => Ok(Self::Eq),
            _ => Err("invalid operation"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let query = "#K = :val";
        let parsed = parse_query(query).unwrap();
        assert_eq!(
            parsed,
            Query {
                conditions: vec![Condition {
                    name: Name::Placeholder("K".to_string()),
                    value: Name::Placeholder("val".to_string()),
                    operation: Operation::Eq,
                },],
            }
        );
    }
}
