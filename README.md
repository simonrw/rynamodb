<!-- [![Crates.io](https://img.shields.io/crates/v/cargo-index-transit.svg)](https://crates.io/crates/cargo-index-transit) -->
<!-- [![Documentation](https://docs.rs/cargo-index-transit/badge.svg)](https://docs.rs/cargo-index-transit/) -->
[![Codecov](https://codecov.io/github/simonrw/rynamodb/coverage.svg?branch=main)](https://codecov.io/gh/simonrw/rynamodb)
[![Dependency status](https://deps.rs/repo/github/simonrw/rynamodb/status.svg)](https://deps.rs/repo/github/simonrw/rynamodb)

# DynamoDB emulation layer in Rust

> [!WARNING]
> This repository has been archived as I have moved on to other projects. This code is a proof of concept only.

## Installation

Install the binary with `cargo` (the only supported solution for now, sorry).

```
cargo install --git https://github.com/simonrw/rynamodb
RUST_LOG=rynamodb=debug rynamodb
```

Then interact with the AWS CLI:

*Create table*
```
$ aws --endpoint-url http://localhost:3050 dynamodb create-table --table-name foo --attribute-definitions AttributeName=pk,AttributeType=S --key-schema AttributeName=pk,KeyType=HASH
{
    "TableDescription": {
        "AttributeDefinitions": [
            {
                "AttributeName": "pk",
                "AttributeType": "S"
            }
        ],
        "TableName": "foo",
        "KeySchema": [
            {
                "AttributeName": "pk",
                "KeyType": "HASH"
            }
        ],
        "TableStatus": "ACTIVE",
        "CreationDateTime": "1970-01-01T00:00:00+00:00",
        "ProvisionedThroughput": {
            "NumberOfDecreasesToday": 0,
            "ReadCapacityUnits": 10,
            "WriteCapacityUnits": 10
        },
        "TableSizeBytes": 0,
        "ItemCount": 0,
        "TableArn": "arn:aws:dynamodb:us-east-1:000000000000:table/foo",
        "TableId": "35e8ca72-b025-476f-822c-0030928860e6"
    }
}
```

*Put item*
```
$ aws --endpoint-url http://localhost:3050 dynamodb put-item --table-name foo --item '{"pk": {"S": "abc"}, "something": {"S": "def"}}'
```

*Basic query*
```
$ aws --endpoint-url http://localhost:3050 dynamodb query --table-name foo --key-condition-expression 'pk = :V' --expression-attribute-values '{":V": {"S": "abc"}}'
{
    "Items": [
        {
            "pk": {
                "S": "abc"
            },
            "something": {
                "S": "def"
            }
        }
    ],
    "Count": 1,
    "ScannedCount": 1,
    "ConsumedCapacity": null
}
```

*Delete table*

```
$ aws --endpoint-url http://localhost:3050 dynamodb delete-table --table-name foo
```

## Integration tests

The test suite from [ScyllaDB alternator](https://github.com/scylladb/scylladb) has been copied across and the tests can be run via:

```bash
# in one terminal
cargo run -- --port 8000

# in another terminal
pytest compliance-tests
```

### Installing dependencies

```bash
# set up a python virtual environment
python3 -m venv venv
source ./venv/bin/activate

# install dependencies
pip install pytest boto3 requests

# optionally install these pytest plugins
pip install pytest-randomly pytest-instafail pytest-xdist
```
