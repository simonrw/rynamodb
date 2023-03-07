# DynamoDB emulation layer in Rust

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
        "TableArn": "arn:aws:dynamodb:eu-west-2:678133472802:table/table-d787c77d-76d4-473e-8165-b006241c6a5d",
        "TableId": "90d9a4e1-7970-4565-b004-61f9d441afaa"
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
