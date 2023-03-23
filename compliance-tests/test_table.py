# Copyright 2019-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for basic table operations: CreateTable, DeleteTable, ListTables.
# Also some basic tests for UpdateTable - although UpdateTable usually
# enables more elaborate features (such as GSI or Streams) and those are
# tested elsewhere.

import pytest
import time
import threading
from botocore.exceptions import ClientError
from util import (
    list_tables,
    unique_table_name,
    create_test_table,
    random_string,
    new_test_table,
)

# Utility function for create a table with a given name and some valid
# schema.. This function initiates the table's creation, but doesn't
# wait for the table to actually become ready.
def create_table(dynamodb, name, BillingMode="PAY_PER_REQUEST", **kwargs):
    return dynamodb.create_table(
        TableName=name,
        BillingMode=BillingMode,
        KeySchema=[
            {"AttributeName": "p", "KeyType": "HASH"},
            {"AttributeName": "c", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "p", "AttributeType": "S"},
            {"AttributeName": "c", "AttributeType": "S"},
        ],
        **kwargs
    )


# Utility function for creating a table with a given name, and then deleting
# it immediately, waiting for these operations to complete. Since the wait
# uses DescribeTable, this function requires all of CreateTable, DescribeTable
# and DeleteTable to work correctly.
# Note that in DynamoDB, table deletion takes a very long time, so tests
# successfully using this function are very slow.
def create_and_delete_table(dynamodb, name, **kwargs):
    table = create_table(dynamodb, name, **kwargs)
    table.meta.client.get_waiter("table_exists").wait(TableName=name)
    table.delete()
    table.meta.client.get_waiter("table_not_exists").wait(TableName=name)


##############################################################################

# Test creating a table, and then deleting it, waiting for each operation
# to have completed before proceeding. Since the wait uses DescribeTable,
# this tests requires all of CreateTable, DescribeTable and DeleteTable to
# function properly in their basic use cases.
# Unfortunately, this test is extremely slow with DynamoDB because deleting
# a table is extremely slow until it really happens.
def test_create_and_delete_table(dynamodb):
    create_and_delete_table(dynamodb, "alternator_test")


# Test that recreating a table right after deleting it works without issues
def test_recreate_table(dynamodb):
    create_and_delete_table(dynamodb, "alternator_recr_test")
    create_and_delete_table(dynamodb, "alternator_recr_test")


# DynamoDB documentation specifies that table names must be 3-255 characters,
# and match the regex [a-zA-Z0-9._-]+. Names not matching these rules should
# be rejected, and no table be created.
def test_create_table_unsupported_names(dynamodb):
    with pytest.raises(ClientError, match="ValidationException"):
        create_table(dynamodb, "n")
    with pytest.raises(ClientError, match="ValidationException"):
        create_table(dynamodb, "nn")
    with pytest.raises(ClientError, match="ValidationException"):
        create_table(dynamodb, "n" * 256)
    with pytest.raises(ClientError, match="ValidationException"):
        create_table(dynamodb, "nyh@test")


# On the other hand, names following the above rules should be accepted. Even
# names which the Scylla rules forbid, such as a name starting with .
def test_create_and_delete_table_non_scylla_name(dynamodb):
    create_and_delete_table(dynamodb, ".alternator_test")


# names with 255 characters are allowed in Dynamo, but they are not currently
# supported in Scylla because we create a directory whose name is the table's
# name followed by 33 bytes (underscore and UUID). So currently, we only
# correctly support names with length up to 222.
def test_create_and_delete_table_very_long_name(dynamodb):
    # In the future, this should work:
    # create_and_delete_table(dynamodb, 'n' * 255)
    # But for now, only 222 works:
    create_and_delete_table(dynamodb, "n" * 222)
    # We cannot test the following on DynamoDB because it will succeed
    # (DynamoDB allows up to 255 bytes)
    # with pytest.raises(ClientError, match='ValidationException'):
    #   create_table(dynamodb, 'n' * 223)


# Tests creating a table with an invalid schema should return a
# ValidationException error.
def test_create_table_invalid_schema(dynamodb):
    # The name of the table "created" by this test shouldn't matter, the
    # creation should not succeed anyway.
    with pytest.raises(ClientError, match="ValidationException"):
        dynamodb.create_table(
            TableName="name_doesnt_matter",
            BillingMode="PAY_PER_REQUEST",
            KeySchema=[
                {"AttributeName": "p", "KeyType": "HASH"},
                {"AttributeName": "c", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "p", "AttributeType": "S"},
                {"AttributeName": "c", "AttributeType": "S"},
            ],
        )
    with pytest.raises(ClientError, match="ValidationException"):
        dynamodb.create_table(
            TableName="name_doesnt_matter",
            BillingMode="PAY_PER_REQUEST",
            KeySchema=[
                {"AttributeName": "p", "KeyType": "RANGE"},
                {"AttributeName": "c", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "p", "AttributeType": "S"},
                {"AttributeName": "c", "AttributeType": "S"},
            ],
        )
    with pytest.raises(ClientError, match="ValidationException"):
        dynamodb.create_table(
            TableName="name_doesnt_matter",
            BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "c", "KeyType": "RANGE"}],
            AttributeDefinitions=[
                {"AttributeName": "c", "AttributeType": "S"},
            ],
        )
    with pytest.raises(ClientError, match="ValidationException"):
        dynamodb.create_table(
            TableName="name_doesnt_matter",
            BillingMode="PAY_PER_REQUEST",
            KeySchema=[
                {"AttributeName": "c", "KeyType": "HASH"},
                {"AttributeName": "p", "KeyType": "RANGE"},
                {"AttributeName": "z", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "c", "AttributeType": "S"},
                {"AttributeName": "p", "AttributeType": "S"},
                {"AttributeName": "z", "AttributeType": "S"},
            ],
        )
    with pytest.raises(ClientError, match="ValidationException"):
        dynamodb.create_table(
            TableName="name_doesnt_matter",
            BillingMode="PAY_PER_REQUEST",
            KeySchema=[
                {"AttributeName": "c", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[{"AttributeName": "z", "AttributeType": "S"}],
        )
    with pytest.raises(ClientError, match="ValidationException"):
        dynamodb.create_table(
            TableName="name_doesnt_matter",
            BillingMode="PAY_PER_REQUEST",
            KeySchema=[
                {"AttributeName": "k", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[{"AttributeName": "k", "AttributeType": "Q"}],
        )


# Test that trying to create a table that already exists fails in the
# appropriate way (ResourceInUseException)
def test_create_table_already_exists(dynamodb, test_table):
    with pytest.raises(
        ClientError, match="ResourceInUseException.*Table.*already exists"
    ):
        create_table(dynamodb, test_table.name)


# Test that BillingMode error path works as expected - only the values
# PROVISIONED or PAY_PER_REQUEST are allowed. The former requires
# ProvisionedThroughput to be set, the latter forbids it.
# If BillingMode is outright missing, it defaults (as original
# DynamoDB did) to PROVISIONED so ProvisionedThroughput is allowed.
def test_create_table_billing_mode_errors(dynamodb, test_table):
    with pytest.raises(ClientError, match="ValidationException"):
        create_table(dynamodb, unique_table_name(), BillingMode="unknown")
    # billing mode is case-sensitive
    with pytest.raises(ClientError, match="ValidationException"):
        create_table(dynamodb, unique_table_name(), BillingMode="pay_per_request")
    # PAY_PER_REQUEST cannot come with a ProvisionedThroughput:
    with pytest.raises(ClientError, match="ValidationException"):
        create_table(
            dynamodb,
            unique_table_name(),
            BillingMode="PAY_PER_REQUEST",
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )
    # On the other hand, PROVISIONED requires ProvisionedThroughput:
    # By the way, ProvisionedThroughput not only needs to appear, it must
    # have both ReadCapacityUnits and WriteCapacityUnits - but we can't test
    # this with boto3, because boto3 has its own verification that if
    # ProvisionedThroughput is given, it must have the correct form.
    with pytest.raises(ClientError, match="ValidationException"):
        create_table(dynamodb, unique_table_name(), BillingMode="PROVISIONED")
    # If BillingMode is completely missing, it defaults to PROVISIONED, so
    # ProvisionedThroughput is required
    with pytest.raises(ClientError, match="ValidationException"):
        dynamodb.create_table(
            TableName=unique_table_name(),
            KeySchema=[{"AttributeName": "p", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "p", "AttributeType": "S"}],
        )


# Even before Alternator gains full support for the DynamoDB stream API
# and CreateTable's StreamSpecification option, we should support the
# options which mean it is turned *off*.
def test_table_streams_off(dynamodb):
    # If StreamSpecification is given, but has StreamEnabled=false, it's as
    # if StreamSpecification was missing. StreamViewType isn't needed.
    table = create_test_table(
        dynamodb,
        StreamSpecification={"StreamEnabled": False},
        KeySchema=[{"AttributeName": "p", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "p", "AttributeType": "S"}],
    )
    table.delete()
    # DynamoDB doesn't allow StreamSpecification to be empty map - if it
    # exists, it must have a StreamEnabled
    # Unfortunately, new versions of boto3 doesn't let us pass this...
    # with pytest.raises(ClientError, match='ValidationException'):
    #    table = create_test_table(dynamodb, StreamSpecification={},
    #        KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }],
    #        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }]);
    #    table.delete();
    # Unfortunately, boto3 doesn't allow us to pass StreamSpecification=None.
    # This is what we had in issue #5796.


# For tests with 'StreamEnabled': True, and different 'StreamViewType', see
# test_streams.py.

# Our first implementation had a special column name called "attrs" where
# we stored a map for all non-key columns. If the user tried to name one
# of the key columns with this same name, the result was a disaster - Scylla
# goes into a bad state after trying to write data with two updates to same-
# named columns. Starting with commit fc946ddfbac324cc3cc2550ab7fade5002fced01
# the result was no longer a disaster, but is still an error from CreateTable
# saying that the column name ':attrs' is reserved.
# Reproduces #5009
@pytest.mark.xfail(reason="#5009: name ':attrs' not allowed for key column")
def test_create_table_special_column_name(dynamodb):
    for c in ["attrs", ":attrs"]:
        # Try the suspicious attribute name as a partition key:
        with new_test_table(
            dynamodb,
            KeySchema=[{"AttributeName": c, "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": c, "AttributeType": "S"}],
        ) as table:
            s = random_string()
            expected = {c: s, "hello": random_string()}
            table.put_item(Item=expected)
            assert expected == table.get_item(Key={c: s}, ConsistentRead=True)["Item"]
        # Try the suspicious attribute name as a clustering key:
        with new_test_table(
            dynamodb,
            KeySchema=[
                {"AttributeName": "p", "KeyType": "HASH"},
                {"AttributeName": c, "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "p", "AttributeType": "S"},
                {"AttributeName": c, "AttributeType": "S"},
            ],
        ) as table:
            p = random_string()
            s = random_string()
            expected = {"p": p, c: s, "hello": random_string()}
            table.put_item(Item=expected)
            assert (
                expected
                == table.get_item(Key={"p": p, c: s}, ConsistentRead=True)["Item"]
            )


# Whereas test_create_table_special_column_name above tests what happen when
# the name ":attrs" is used for a key column (partition key or sort key), in
# the following tests test_special_attribute_name_* we want to see what happens
# if the user tries to set or get a non-key attribute with that special name
# using various operations like PutItem, GetItem, UpdateItem and various
# expressions involving attribute names. Before issue #5009 was fixed, some of
# these tests used to crash Scylla or otherwise fail.
def test_special_attribute_name_putitem(test_table_s):
    p = random_string()
    expected = {"p": p, ":attrs": random_string()}
    test_table_s.put_item(Item=expected)
    assert expected == test_table_s.get_item(Key={"p": p}, ConsistentRead=True)["Item"]


def test_special_attribute_name_updateitem_put(test_table_s):
    p = random_string()
    s = random_string()
    test_table_s.update_item(
        Key={"p": p}, AttributeUpdates={":attrs": {"Value": s, "Action": "PUT"}}
    )
    assert {"p": p, ":attrs": s} == test_table_s.get_item(
        Key={"p": p}, ConsistentRead=True
    )["Item"]


def test_special_attribute_name_updateitem_delete(test_table_s):
    p = random_string()
    s = random_string()
    test_table_s.put_item(Item={"p": p, ":attrs": s, "animal": "dog"})
    assert {"p": p, ":attrs": s, "animal": "dog"} == test_table_s.get_item(
        Key={"p": p}, ConsistentRead=True
    )["Item"]
    test_table_s.update_item(
        Key={"p": p}, AttributeUpdates={":attrs": {"Action": "DELETE"}}
    )
    assert {"p": p, "animal": "dog"} == test_table_s.get_item(
        Key={"p": p}, ConsistentRead=True
    )["Item"]


def test_special_attribute_name_updateitem_rmw(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={"p": p, ":attrs": 7})
    assert {"p": p, ":attrs": 7} == test_table_s.get_item(
        Key={"p": p}, ConsistentRead=True
    )["Item"]
    test_table_s.update_item(
        Key={"p": p},
        UpdateExpression="SET #a = #a + :one",
        ExpressionAttributeValues={":one": 1},
        ExpressionAttributeNames={"#a": ":attrs"},
    )
    assert {"p": p, ":attrs": 8} == test_table_s.get_item(
        Key={"p": p}, ConsistentRead=True
    )["Item"]


def test_special_attribute_name_updateitem_expected(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={"p": p, ":attrs": 7})
    assert {"p": p, ":attrs": 7} == test_table_s.get_item(
        Key={"p": p}, ConsistentRead=True
    )["Item"]
    test_table_s.update_item(
        Key={"p": p},
        AttributeUpdates={"animal": {"Value": "dog", "Action": "PUT"}},
        Expected={":attrs": {"ComparisonOperator": "EQ", "AttributeValueList": [7]}},
    )
    assert {"p": p, ":attrs": 7, "animal": "dog"} == test_table_s.get_item(
        Key={"p": p}, ConsistentRead=True
    )["Item"]


def test_special_attribute_name_updateitem_condition(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={"p": p, ":attrs": 7})
    assert {"p": p, ":attrs": 7} == test_table_s.get_item(
        Key={"p": p}, ConsistentRead=True
    )["Item"]
    test_table_s.update_item(
        Key={"p": p},
        UpdateExpression="SET #b = :val",
        ExpressionAttributeValues={":val": "dog", ":seven": 7},
        ExpressionAttributeNames={"#a": ":attrs", "#b": "animal"},
        ConditionExpression="#a = :seven",
    )
    assert {"p": p, ":attrs": 7, "animal": "dog"} == test_table_s.get_item(
        Key={"p": p}, ConsistentRead=True
    )["Item"]


def test_special_attribute_name_getitem_projection(test_table_s):
    p = random_string()
    test_table_s.put_item(Item={"p": p, ":attrs": 7, "animal": "dog"})
    assert {"p": p, ":attrs": 7, "animal": "dog"} == test_table_s.get_item(
        Key={"p": p}, ConsistentRead=True
    )["Item"]
    assert {":attrs": 7} == test_table_s.get_item(
        Key={"p": p},
        ProjectionExpression="#a",
        ExpressionAttributeNames={"#a": ":attrs"},
        ConsistentRead=True,
    )["Item"]


# Test that all tables we create are listed, and pagination works properly.
# Note that the DyanamoDB setup we run this against may have hundreds of
# other tables, for all we know. We just need to check that the tables we
# created are indeed listed.
def test_list_tables_paginated(dynamodb, test_table, test_table_s, test_table_b):
    my_tables_set = {table.name for table in [test_table, test_table_s, test_table_b]}
    for limit in [1, 2, 3, 4, 50, 100]:
        print("testing limit={}".format(limit))
        list_tables_set = set(list_tables(dynamodb, limit))
        assert my_tables_set.issubset(list_tables_set)


# Test that pagination limit is validated
def test_list_tables_wrong_limit(dynamodb):
    # lower limit (min. 1) is imposed by boto3 library checks
    with pytest.raises(ClientError, match="ValidationException"):
        dynamodb.meta.client.list_tables(Limit=101)


# Even before Alternator gains support for configuring server-side encryption
# ("encryption at rest") with CreateTable's SSESpecification option, we should
# support the option "Enabled=false" which is the default, and means the server
# takes care of whatever server-side encryption is done, on its own.
# Reproduces issue #7031.
def test_table_sse_off(dynamodb):
    # If StreamSpecification is given, but has StreamEnabled=false, it's as
    # if StreamSpecification was missing, and fine. No other attribues are
    # necessary.
    table = create_test_table(
        dynamodb,
        SSESpecification={"Enabled": False},
        KeySchema=[{"AttributeName": "p", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "p", "AttributeType": "S"}],
    )
    table.delete()


# Test that trying to delete a table that doesn't exist fails in the
# appropriate way (ResourceNotFoundException)
def test_delete_table_non_existent(dynamodb, test_table):
    client = dynamodb.meta.client
    with pytest.raises(ClientError, match="ResourceNotFoundException"):
        client.delete_table(TableName=random_string(20))


# Test that trying to update a table that doesn't exist fails in the
# appropriate way (ResourceNotFoundException)
def test_update_table_non_existent(dynamodb, test_table):
    client = dynamodb.meta.client
    with pytest.raises(ClientError, match="ResourceNotFoundException"):
        client.update_table(TableName=random_string(20), BillingMode="PAY_PER_REQUEST")


# Test for reproducing issues #6391 and #9868 - where CreateTable did not
# *atomically* perform all the schema modifications - creating a keyspace,
# a table, secondary indexes and tags - and instead it created the different
# pieces one after another. In that case it is possible that if we
# concurrently create and delete the same table, the deletion may, for
# example, delete the table just created and then creating the secondary
# index in it would fail.
#
# In each test we'll have two threads - one looping repeatedly creating
# the same table, and the second looping repeatedly deleting this table.
# It is expected for the table creation to fail because the table already
# exists, and for the table deletion to fail because the table doesn't
# exist. But what we don't want to see is a different kind failure (e.g.,
# InternalServerError) in the *middle* of the table creation or deletion.
# Such a failure may even leave behind some half-created table.
#
# NOTE: This test, like all cql-pytest tests, runs on a single node. So it
# doesn't exercise the most general possibility that two concurrent schema
# modifications from two different coordinators collide. So multi-node
# tests will be needed to check for that potential problem as well.
# But even on one coordinator this test can already reproduce the bugs
# described in #6391 and #9868, so it's worth having this single-node test
# as well.
@pytest.mark.parametrize(
    "table_def",
    [
        # A table without a secondary index - requiring the creation of
        # a keyspace and a table in it.
        {
            "KeySchema": [{"AttributeName": "p", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "p", "AttributeType": "S"}],
        },
        # Reproducer for #9868 - CreateTable needs to create a keyspace,
        # a table, and a materialized view.
        {
            "KeySchema": [
                {"AttributeName": "p", "KeyType": "HASH"},
                {"AttributeName": "c", "KeyType": "RANGE"},
            ],
            "AttributeDefinitions": [
                {"AttributeName": "p", "AttributeType": "S"},
                {"AttributeName": "c", "AttributeType": "S"},
            ],
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "hello",
                    "KeySchema": [
                        {"AttributeName": "c", "KeyType": "HASH"},
                        {"AttributeName": "p", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        },
        # Reproducer for #6391, a table with tags - which we used to add
        # non-atomically after having already created the table.
        {
            "KeySchema": [{"AttributeName": "p", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "p", "AttributeType": "S"}],
            "Tags": [{"Key": "k1", "Value": "v1"}],
        },
    ],
)
def test_concurrent_create_and_delete_table(
    dynamodb, table_def, fails_without_consistent_cluster_management
):
    # According to boto3 documentation, "Unlike Resources and Sessions,
    # clients are generally thread-safe.". So because we have two threads
    # in this test, we must not use "dynamodb" (containing the boto3
    # "resource") - we should only the boto3 "client":
    client = dynamodb.meta.client
    # Unfortunately by default Python threads print their exceptions
    # (e.g., assertion failures) but don't propagate them to the join(),
    # so the overall test doesn't fail. The following Thread wrapper
    # causes join() to rethrow the exception, so the test will fail.
    class ThreadWrapper(threading.Thread):
        def run(self):
            try:
                self.ret = self._target(*self._args, **self._kwargs)
            except BaseException as e:
                self.exception = e

        def join(self, timeout=None):
            super().join(timeout)
            if hasattr(self, "exception"):
                raise self.exception
            return self.ret

    table_name = unique_table_name()
    # The more iterations we do, the higher the chance of reproducing
    # this issue. On my laptop, count = 10 reproduces the bug every time.
    # Lower numbers have some chance of not catching the bug. If this
    # issue starts to xpass, we may need to increase the count.
    count = 10

    def deletes():
        for i in range(count):
            try:
                client.delete_table(TableName=table_name)
            except Exception as e:
                # Expect either success or a ResourceNotFoundException.
                # On DynamoDB we can also get a ResourceInUseException
                # if we try to delete a table while it's in the middle
                # of being created.
                # Anything else (e.g., InternalServerError) is a bug.
                assert isinstance(e, ClientError) and (
                    "ResourceNotFoundException" in str(e)
                    or "ResourceInUseException" in str(e)
                )
            else:
                print("delete successful")

    def creates():
        for i in range(count):
            try:
                client.create_table(
                    TableName=table_name, BillingMode="PAY_PER_REQUEST", **table_def
                )
            except Exception as e:
                # Expect either success or a ResourceInUseException.
                # Anything else (e.g., InternalServerError) is a bug.
                assert isinstance(e, ClientError) and "ResourceInUseException" in str(e)
            else:
                print("create successful")

    t1 = ThreadWrapper(target=deletes)
    t2 = ThreadWrapper(target=creates)
    t1.start()
    t2.start()
    try:
        t1.join()
        t2.join()
    finally:
        # Make sure that in any case, the table is deleted before the
        # test finishes. On DynamoDB, we can't just call DeleteTable -
        # if some CreateTable is still in progress we can't call
        # DeleteTable until it finishes...
        timeout = time.time() + 120
        while time.time() < timeout:
            try:
                client.delete_table(TableName=table_name)
                break
            except ClientError as e:
                if "ResourceNotFoundException" in str(e):
                    # The table was already deleted by the deletion thread,
                    # nothing left to do :-)
                    break
                if "ResourceInUseException" in str(e):
                    # A CreateTable opereration is still in progress,
                    # we can't delete the table yet.
                    time.sleep(1)
                    continue
                raise
