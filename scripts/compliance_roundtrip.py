#!/usr/bin/env python

from datetime import datetime, timezone
from decimal import Decimal
import json

import boto3
from boto3.dynamodb.conditions import Key

ENDPOINT_URL = "http://127.0.0.1:3050"
dynamodb = boto3.resource("dynamodb", endpoint_url=ENDPOINT_URL)

# create the table
# TODO: what happens when the same table is created twice?
table = dynamodb.create_table(
    TableName="rynamdob",
    AttributeDefinitions=[
        dict(
            AttributeName="branch",
            AttributeType="S",
        ),
        dict(
            AttributeName="uploaded",
            AttributeType="S",
        ),
    ],
    KeySchema=[
        dict(
            AttributeName="branch",
            KeyType="HASH",
        ),
        dict(
            AttributeName="uploaded",
            KeyType="RANGE",
        ),
    ],
)
table.wait_until_exists()

# insert some data
with table.batch_writer() as batch:
    batch.put_item(
        Item={
            "branch": "main",
            "uploaded": datetime.now(tz=timezone.utc).isoformat(),
            "errors": 10,
            "failed": 3,
            "skipped": 0,
            "passed": 200,
            "duration": Decimal("2.1"),
            "commit-sha": "randomsha",
            "committer": "jondoe",
        }
    )
    batch.put_item(
        Item={
            "branch": "main",
            "uploaded": datetime.now(tz=timezone.utc).isoformat(),
            "errors": 9,
            "failed": 2,
            "skipped": 1,
            "passed": 199,
            "duration": Decimal("12.1"),
            "commit-sha": "randomsha2",
            "committer": "jondoe",
        }
    )
    batch.put_item(
        Item={
            "branch": "main",
            "uploaded": datetime.now(tz=timezone.utc).isoformat(),
            "errors": 13,
            "failed": 6,
            "skipped": 3,
            "passed": 203,
            "duration": Decimal("42.1"),
            "commit-sha": "randomsha3",
            "committer": "jondoe",
        }
    )

class Serializer(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)

# get the coverage for the main branch
res = table.query(
    KeyConditionExpression=Key("branch").eq("main")
)
items = res["Items"]
print(json.dumps(items, indent=2, cls=Serializer))
