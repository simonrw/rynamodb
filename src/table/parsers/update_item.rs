use super::ParserError;
use pest::Parser;

#[derive(pest_derive::Parser)]
#[grammar = "update_item.pest"]
struct UpdateItemParser;
