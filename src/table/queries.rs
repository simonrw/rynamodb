use std::str::FromStr;

use thiserror::Error;

#[derive(Error, Debug)]
pub(super) enum QueryError {
    #[error("parsing input")]
    ParseError,
}

type Result<T> = std::result::Result<T, QueryError>;

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
