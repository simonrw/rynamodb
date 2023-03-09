use thiserror::Error;

pub mod query;

#[derive(Debug, Error)]
pub enum ParserError {
    #[error("parse error: {0}")]
    ParseError(String),
    #[error("end of items reached unexpectedly")]
    Eoi,
    #[error("can not convert node to string")]
    NotStringlike,
}
