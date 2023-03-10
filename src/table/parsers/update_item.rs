use super::ParserError;
use pest::Parser;

#[derive(pest_derive::Parser)]
#[grammar = "update_item.pest"]
struct UpdateItemParser;

#[cfg(test)]
mod tests {
    use super::*;

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
