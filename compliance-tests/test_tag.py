# -*- coding: utf-8 -*-
# Copyright 2019-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for Tagging:
# 1. TagResource - tagging a table with a (key, value) pair
# 2. UntagResource
# 3. ListTagsOfResource

import pytest
from botocore.exceptions import ClientError
from util import multiset, create_test_table, unique_table_name
from packaging.version import Version


def delete_tags(table, arn):
    got = table.meta.client.list_tags_of_resource(ResourceArn=arn)
    print(got["Tags"])
    if len(got["Tags"]):
        table.meta.client.untag_resource(
            ResourceArn=arn, TagKeys=[tag["Key"] for tag in got["Tags"]]
        )


# Test checking that tagging and untagging is correctly handled
def test_tag_resource_basic(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)["Table"]
    arn = got["TableArn"]
    tags = [
        {"Key": "string", "Value": "string"},
        {"Key": "string2", "Value": "string4"},
        {"Key": "7", "Value": " "},
        {"Key": " ", "Value": "9"},
    ]

    delete_tags(test_table, arn)
    got = test_table.meta.client.list_tags_of_resource(ResourceArn=arn)
    assert len(got["Tags"]) == 0
    test_table.meta.client.tag_resource(ResourceArn=arn, Tags=tags)
    got = test_table.meta.client.list_tags_of_resource(ResourceArn=arn)
    assert "Tags" in got
    assert multiset(got["Tags"]) == multiset(tags)

    # Removing non-existent tags is legal
    test_table.meta.client.untag_resource(
        ResourceArn=arn, TagKeys=["string2", "non-nexistent", "zzz2"]
    )
    tags.remove({"Key": "string2", "Value": "string4"})
    got = test_table.meta.client.list_tags_of_resource(ResourceArn=arn)
    assert "Tags" in got
    assert multiset(got["Tags"]) == multiset(tags)

    delete_tags(test_table, arn)
    got = test_table.meta.client.list_tags_of_resource(ResourceArn=arn)
    assert len(got["Tags"]) == 0


def test_tag_resource_overwrite(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)["Table"]
    arn = got["TableArn"]
    tags = [
        {"Key": "string", "Value": "string"},
    ]
    delete_tags(test_table, arn)
    test_table.meta.client.tag_resource(ResourceArn=arn, Tags=tags)
    got = test_table.meta.client.list_tags_of_resource(ResourceArn=arn)
    assert "Tags" in got
    assert multiset(got["Tags"]) == multiset(tags)
    tags = [
        {"Key": "string", "Value": "different_string_value"},
    ]
    test_table.meta.client.tag_resource(ResourceArn=arn, Tags=tags)
    got = test_table.meta.client.list_tags_of_resource(ResourceArn=arn)
    assert "Tags" in got
    assert multiset(got["Tags"]) == multiset(tags)


PREDEFINED_TAGS = [
    {"Key": "str1", "Value": "str2"},
    {"Key": "kkk", "Value": "vv"},
    {"Key": "keykey", "Value": "valvalvalval"},
]


@pytest.fixture(scope="module")
def test_table_tags(dynamodb):
    # The feature of creating a table already with tags was only added to
    # DynamoDB in April 2019, and to the botocore library in version 1.12.136
    # https://aws.amazon.com/about-aws/whats-new/2019/04/now-you-can-tag-amazon-dynamodb-tables-when-you-create-them/
    # so older versions of the library cannot run this test.
    import botocore

    if Version(botocore.__version__) < Version("1.12.136"):
        pytest.skip("Botocore version 1.12.136 or above required to run this test")

    table = create_test_table(
        dynamodb,
        KeySchema=[
            {"AttributeName": "p", "KeyType": "HASH"},
            {"AttributeName": "c", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "p", "AttributeType": "S"},
            {"AttributeName": "c", "AttributeType": "N"},
        ],
        Tags=PREDEFINED_TAGS,
    )
    yield table
    table.delete()


# Test checking that tagging works during table creation
def test_list_tags_from_creation(test_table_tags):
    got = test_table_tags.meta.client.describe_table(TableName=test_table_tags.name)[
        "Table"
    ]
    arn = got["TableArn"]
    got = test_table_tags.meta.client.list_tags_of_resource(ResourceArn=arn)
    assert multiset(got["Tags"]) == multiset(PREDEFINED_TAGS)


# Test checking that incorrect parameters return proper error codes
def test_tag_resource_incorrect(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)["Table"]
    arn = got["TableArn"]
    # Note: Tags must have two entries in the map: Key and Value, and their values
    # must be at least 1 character long, but these are validated on boto3 level
    with pytest.raises(ClientError, match="AccessDeniedException"):
        test_table.meta.client.tag_resource(
            ResourceArn="I_do_not_exist", Tags=[{"Key": "7", "Value": "8"}]
        )
    with pytest.raises(ClientError, match="ValidationException"):
        test_table.meta.client.tag_resource(ResourceArn=arn, Tags=[])
    test_table.meta.client.tag_resource(
        ResourceArn=arn, Tags=[{"Key": str(i), "Value": str(i)} for i in range(30)]
    )
    test_table.meta.client.tag_resource(
        ResourceArn=arn, Tags=[{"Key": str(i), "Value": str(i)} for i in range(20, 40)]
    )
    with pytest.raises(ClientError, match="ValidationException"):
        test_table.meta.client.tag_resource(
            ResourceArn=arn,
            Tags=[{"Key": str(i), "Value": str(i)} for i in range(40, 60)],
        )
    for incorrect_arn in [
        "arn:not/a/good/format",
        "x" * 125,
        "arn:" + "scylla/" * 15,
        ":/" * 30,
        " ",
        "незаконные буквы",
    ]:
        with pytest.raises(ClientError, match=".*Exception"):
            test_table.meta.client.tag_resource(
                ResourceArn=incorrect_arn, Tags=[{"Key": "x", "Value": "y"}]
            )
    for incorrect_tag in [("ok", "#!%%^$$&"), ("->>;-)])", "ok"), ("!!!\\|", "<><")]:
        with pytest.raises(ClientError, match="ValidationException"):
            test_table.meta.client.tag_resource(
                ResourceArn=arn,
                Tags=[{"Key": incorrect_tag[0], "Value": incorrect_tag[1]}],
            )


# Test that if trying to create a table with forbidden tags (in this test,
# a list of tags longer than the maximum allowed of 50 tags), the table
# is not created at all.
def test_too_long_tags_from_creation(dynamodb):
    # The feature of creating a table already with tags was only added to
    # DynamoDB in April 2019, and to the botocore library in version 1.12.136
    # so older versions of the library cannot run this test.
    import botocore

    if Version(botocore.__version__) < Version("1.12.136"):
        pytest.skip("Botocore version 1.12.136 or above required to run this test")
    name = unique_table_name()
    # Setting 100 tags is not allowed, the following table creation should fail:
    with pytest.raises(ClientError, match="ValidationException"):
        dynamodb.create_table(
            TableName=name,
            BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "p", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "p", "AttributeType": "S"}],
            Tags=[{"Key": str(i), "Value": str(i)} for i in range(100)],
        )
    # After the table creation failed, the table should not exist.
    with pytest.raises(ClientError, match="ResourceNotFoundException"):
        dynamodb.meta.client.describe_table(TableName=name)

# Test checking that unicode tags are allowed
@pytest.mark.xfail(reason="unicode tags not yet supported")
def test_tag_resource_unicode(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)["Table"]
    arn = got["TableArn"]
    tags = [
        {"Key": "законные буквы", "Value": "string"},
        {"Key": "ѮѮ Ѯ", "Value": "string4"},
        {"Key": "ѮѮ", "Value": "ѮѮѮѮѮѮѮѮѮѮѮѮѮѮ"},
        {"Key": "keyѮѮѮ", "Value": "ѮѮѮvalue"},
    ]

    delete_tags(test_table, arn)
    got = test_table.meta.client.list_tags_of_resource(ResourceArn=arn)
    assert len(got["Tags"]) == 0
    test_table.meta.client.tag_resource(ResourceArn=arn, Tags=tags)
    got = test_table.meta.client.list_tags_of_resource(ResourceArn=arn)
    assert "Tags" in got
    assert multiset(got["Tags"]) == multiset(tags)


# Test that the Tags option of TagResource is required
def test_tag_resource_missing_tags(test_table):
    client = test_table.meta.client
    arn = client.describe_table(TableName=test_table.name)["Table"]["TableArn"]
    with pytest.raises(ClientError, match="ValidationException"):
        client.tag_resource(ResourceArn=arn)


# A simple table with both gsi and lsi (which happen to be the same), which
# we'll use for testing tagging of GSIs and LSIs
# Use a function-scoped fixture to get a fresh untagged table in each test.
@pytest.fixture(scope="function")
def table_lsi_gsi(dynamodb):
    table = create_test_table(
        dynamodb,
        KeySchema=[
            {"AttributeName": "p", "KeyType": "HASH"},
            {"AttributeName": "c", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "p", "AttributeType": "S"},
            {"AttributeName": "c", "AttributeType": "S"},
            {"AttributeName": "x1", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi",
                "KeySchema": [
                    {"AttributeName": "p", "KeyType": "HASH"},
                    {"AttributeName": "x1", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "KEYS_ONLY"},
            }
        ],
        LocalSecondaryIndexes=[
            {
                "IndexName": "lsi",
                "KeySchema": [
                    {"AttributeName": "p", "KeyType": "HASH"},
                    {"AttributeName": "x1", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "KEYS_ONLY"},
            }
        ],
    )
    yield table
    table.delete()


# Although GSIs and LSIs have their own ARN (listed by DescribeTable), it
# turns out that they cannot be used to retrieve or set tags on the GSI or
# LSI. If this is attempted, DynamoDB complains that the given ARN is not
# a *table* ARN:
# "An error occurred (ValidationException) when calling the ListTagsOfResource
# operation: Invalid TableArn: Invalid ResourceArn provided as input
# arn:aws:dynamodb:us-east-1:797456418907:table/alternator_Test_1655117822792/index/gsi"
#
# See issue #10786 discussing maybe we want in Alternator not to follow
# DynamoDB here, and to *allow* tagging GSIs and LSIs separately. But until
# then, this test verifies that we don't allow it - just like DynamoDB.
def test_tag_lsi_gsi(table_lsi_gsi):
    table_desc = table_lsi_gsi.meta.client.describe_table(TableName=table_lsi_gsi.name)[
        "Table"
    ]
    table_arn = table_desc["TableArn"]
    gsi_arn = table_desc["GlobalSecondaryIndexes"][0]["IndexArn"]
    lsi_arn = table_desc["LocalSecondaryIndexes"][0]["IndexArn"]
    assert (
        []
        == table_lsi_gsi.meta.client.list_tags_of_resource(ResourceArn=table_arn)[
            "Tags"
        ]
    )
    with pytest.raises(ClientError, match="ValidationException.*ResourceArn"):
        assert (
            []
            == table_lsi_gsi.meta.client.list_tags_of_resource(ResourceArn=gsi_arn)[
                "Tags"
            ]
        )
    with pytest.raises(ClientError, match="ValidationException.*ResourceArn"):
        assert (
            []
            == table_lsi_gsi.meta.client.list_tags_of_resource(ResourceArn=lsi_arn)[
                "Tags"
            ]
        )
    tags = [{"Key": "hi", "Value": "hello"}]
    table_lsi_gsi.meta.client.tag_resource(ResourceArn=table_arn, Tags=tags)
    with pytest.raises(ClientError, match="ValidationException.*ResourceArn"):
        table_lsi_gsi.meta.client.tag_resource(ResourceArn=gsi_arn, Tags=tags)
    with pytest.raises(ClientError, match="ValidationException.*ResourceArn"):
        table_lsi_gsi.meta.client.tag_resource(ResourceArn=lsi_arn, Tags=tags)
