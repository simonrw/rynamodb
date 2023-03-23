# Copyright 2021 ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for the Time To Live (TTL) feature for item expiration.

import pytest
import time
import re
import math
from botocore.exceptions import ClientError
from util import (
    new_test_table,
    random_string,
    unique_table_name,
    client_no_transform,
)
from contextlib import contextmanager
from decimal import Decimal

pytestmark = [pytest.mark.xfail(reason="ttls not supported yet")]

# passes_or_raises() is similar to pytest.raises(), except that while raises()
# expects a certain exception must happen, the new passes_or_raises()
# expects the code to either pass (not raise), or if it throws, it must
# throw the specific specified exception.
@contextmanager
def passes_or_raises(expected_exception, match=None):
    # Sadly __tracebackhide__=True only drops some of the unhelpful backtrace
    # lines. See https://github.com/pytest-dev/pytest/issues/2057
    __tracebackhide__ = True
    try:
        yield
        # The user's "with" code is running during the yield. If it didn't
        # throw we return from the function - the raises_or_not() passed as
        # the "or not" case.
        return
    except expected_exception as err:
        if match is None or re.search(match, str(err)):
            # The raises_or_not() passed on as the "raises" case
            return
        pytest.fail(f"exception message '{err}' did not match '{match}'")
    except Exception as err:
        pytest.fail(
            f"Got unexpected exception type {type(err).__name__} instead of {expected_exception.__name__}: {err}"
        )


# Test the DescribeTimeToLive operation on a table where the time-to-live
# feature was *not* enabled.
def test_describe_ttl_without_ttl(test_table):
    response = test_table.meta.client.describe_time_to_live(TableName=test_table.name)
    assert "TimeToLiveDescription" in response
    assert "TimeToLiveStatus" in response["TimeToLiveDescription"]
    assert response["TimeToLiveDescription"]["TimeToLiveStatus"] == "DISABLED"
    assert "AttributeName" not in response["TimeToLiveDescription"]


# Test that UpdateTimeToLive can be used to pick an expiration-time attribute
# and this information becomes available via DescribeTimeToLive
def test_ttl_enable(dynamodb):
    with new_test_table(
        dynamodb,
        KeySchema=[
            {"AttributeName": "p", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[{"AttributeName": "p", "AttributeType": "S"}],
    ) as table:
        client = table.meta.client
        ttl_spec = {"AttributeName": "expiration", "Enabled": True}
        response = client.update_time_to_live(
            TableName=table.name, TimeToLiveSpecification=ttl_spec
        )
        assert "TimeToLiveSpecification" in response
        assert response["TimeToLiveSpecification"] == ttl_spec
        # Verify that DescribeTimeToLive can recall this setting:
        response = client.describe_time_to_live(TableName=table.name)
        assert "TimeToLiveDescription" in response
        assert response["TimeToLiveDescription"] == {
            "TimeToLiveStatus": "ENABLED",
            "AttributeName": "expiration",
        }
        # Verify that UpdateTimeToLive cannot enable TTL is it is already
        # enabled. A user is not allowed to change the expiration attribute
        # without disabling TTL first, and it's an error even to try to
        # enable TTL with exactly the same attribute as already enabled.
        # (the error message uses slightly different wording in those two
        # cases)
        with pytest.raises(ClientError, match="ValidationException.*(active|enabled)"):
            client.update_time_to_live(
                TableName=table.name, TimeToLiveSpecification=ttl_spec
            )
        with pytest.raises(ClientError, match="ValidationException.*(active|enabled)"):
            new_ttl_spec = {"AttributeName": "new", "Enabled": True}
            client.update_time_to_live(
                TableName=table.name, TimeToLiveSpecification=new_ttl_spec
            )


# Test various *wrong* ways of disabling TTL. Although we test here various
# error cases of how to disable TTL incorrectly, we don't actually check in
# this test case the successful disabling case, because DynamoDB refuses to
# disable TTL if it was enabled in the last hour (according to DynamoDB's
# documentation). We have below a much longer test for the successful TTL
# disable case.
def test_ttl_disable_errors(dynamodb):
    with new_test_table(
        dynamodb,
        KeySchema=[
            {"AttributeName": "p", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[{"AttributeName": "p", "AttributeType": "S"}],
    ) as table:
        client = table.meta.client
        # We can't disable TTL if it's not already enabled.
        with pytest.raises(ClientError, match="ValidationException.*disabled"):
            client.update_time_to_live(
                TableName=table.name,
                TimeToLiveSpecification={
                    "AttributeName": "expiration",
                    "Enabled": False,
                },
            )
        # So enable TTL, before disabling it:
        client.update_time_to_live(
            TableName=table.name,
            TimeToLiveSpecification={"AttributeName": "expiration", "Enabled": True},
        )
        response = client.describe_time_to_live(TableName=table.name)
        assert response["TimeToLiveDescription"] == {
            "TimeToLiveStatus": "ENABLED",
            "AttributeName": "expiration",
        }
        # To disable TTL, the user must specify the current expiration
        # attribute in the command - you can't not specify it, or specify
        # the wrong one!
        with pytest.raises(ClientError, match="ValidationException.*different"):
            client.update_time_to_live(
                TableName=table.name,
                TimeToLiveSpecification={"AttributeName": "dog", "Enabled": False},
            )
        # Finally disable TTL the right way :-) On DynamoDB this fails
        # because you are not allowed to modify the TTL setting twice in
        # one hour, but in our implementation it can also pass quickly.
        with passes_or_raises(ClientError, match="ValidationException.*multiple times"):
            client.update_time_to_live(
                TableName=table.name,
                TimeToLiveSpecification={
                    "AttributeName": "expiration",
                    "Enabled": False,
                },
            )


# Test various errors in the UpdateTimeToLive request.
def test_update_ttl_errors(dynamodb):
    client = dynamodb.meta.client
    # Can't set TTL on a non-existent table
    nonexistent_table = unique_table_name()
    with pytest.raises(ClientError, match="ResourceNotFoundException"):
        client.update_time_to_live(
            TableName=nonexistent_table,
            TimeToLiveSpecification={"AttributeName": "expiration", "Enabled": True},
        )
    with new_test_table(
        dynamodb,
        KeySchema=[
            {"AttributeName": "p", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[{"AttributeName": "p", "AttributeType": "S"}],
    ) as table:
        # AttributeName must be between 1 and 255 characters long.
        with pytest.raises(ClientError, match="ValidationException.*length"):
            client.update_time_to_live(
                TableName=table.name,
                TimeToLiveSpecification={"AttributeName": "x" * 256, "Enabled": True},
            )
        with pytest.raises(ClientError, match="ValidationException.*length"):
            client.update_time_to_live(
                TableName=table.name,
                TimeToLiveSpecification={"AttributeName": "", "Enabled": True},
            )
        # Missing mandatory UpdateTimeToLive parameters - AttributeName or Enabled
        with pytest.raises(ClientError, match="ValidationException.*[aA]ttributeName"):
            client.update_time_to_live(
                TableName=table.name, TimeToLiveSpecification={"Enabled": True}
            )
        with pytest.raises(ClientError, match="ValidationException.*[eE]nabled"):
            client.update_time_to_live(
                TableName=table.name, TimeToLiveSpecification={"AttributeName": "hello"}
            )
        # Wrong types for these mandatory parameters (e.g., string for Enabled)
        # The error type is currently a bit different in Alternator
        # (ValidationException) and in DynamoDB (SerializationException).
        with pytest.raises(ClientError):
            client.update_time_to_live(
                TableName=table.name,
                TimeToLiveSpecification={"AttributeName": "hello", "Enabled": "dog"},
            )
        with pytest.raises(ClientError):
            client.update_time_to_live(
                TableName=table.name,
                TimeToLiveSpecification={"AttributeName": 3, "Enabled": True},
            )


# Basic test that expiration indeed expires items that should be expired,
# and doesn't expire items which shouldn't be expired.
# On AWS, this is an extremely slow test - DynamoDB documentation says that
# expiration may even be delayed for 48 hours. But in practice, at the time
# of this writing, for tiny tables we see delays of around 10 minutes.
# The following test is set to always run for "duration" seconds, currently
# 20 minutes on AWS. During this time, we expect to see the items which should
# have expired to be expired - and the items that should not have expired
# should still exist.
# When running against Scylla configured (for testing purposes) to expire
# items with very short delays, "duration" can be set much lower so this
# test will finish in a much more reasonable time.
@pytest.mark.veryslow
def test_ttl_expiration(dynamodb):
    duration = 10
    # delta is a quarter of the test duration, but no less than one second,
    # and we use it to schedule some expirations a bit after the test starts,
    # not immediately.
    delta = math.ceil(duration / 4)
    assert delta >= 1
    with new_test_table(
        dynamodb,
        KeySchema=[
            {"AttributeName": "p", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[{"AttributeName": "p", "AttributeType": "S"}],
    ) as table:
        # Insert one expiring item *before* enabling the TTL, to verify that
        # items that already exist when TTL is configured also get handled.
        p0 = random_string()
        table.put_item(Item={"p": p0, "expiration": int(time.time()) - 60})
        # Enable TTL, using the attribute "expiration":
        client = table.meta.client
        ttl_spec = {"AttributeName": "expiration", "Enabled": True}
        response = client.update_time_to_live(
            TableName=table.name, TimeToLiveSpecification=ttl_spec
        )
        assert response["TimeToLiveSpecification"] == ttl_spec
        # This item should never expire, it is missing the "expiration"
        # attribute:
        p1 = random_string()
        table.put_item(Item={"p": p1})
        # This item should expire ASAP, as its "expiration" has already
        # passed, one minute ago:
        p2 = random_string()
        table.put_item(Item={"p": p2, "expiration": int(time.time()) - 60})
        # This item has an expiration more than 5 years in the past (it is my
        # birth date...), so according to the DynamoDB documentation it should
        # be ignored and p3 should never expire:
        p3 = random_string()
        table.put_item(Item={"p": p3, "expiration": 162777600})
        # This item has as its expiration delta into the future, which is a
        # small part of the test duration, so should expire by the time the
        # test ends:
        p4 = random_string()
        table.put_item(Item={"p": p4, "expiration": int(time.time()) + delta})
        # This item starts with expiration time delta into the future,
        # but before it expires we will move it again, so it will never expire:
        p5 = random_string()
        table.put_item(Item={"p": p5, "expiration": int(time.time()) + delta})
        # This item has an expiration time two durations into the future, so it
        # will not expire by the time the test ends:
        p6 = random_string()
        table.put_item(Item={"p": p6, "expiration": int(time.time() + duration * 2)})
        # Like p4, this item has an expiration time delta into the future,
        # here the expiration time is wrongly encoded as a string, not a
        # number, so the item should never expire:
        p7 = random_string()
        table.put_item(Item={"p": p7, "expiration": str(int(time.time()) + delta)})
        # Like p2, p8 and p9 also have an already-passed expiration time,
        # and should expire ASAP. However, whereas p2 had a straighforward
        # integer like 12345678 as the expiration time, p8 and p9 have
        # slightly more elaborate numbers: p8 has 1234567e1 and p9 has
        # 12345678.1234. Those formats should be fine, and this test verifies
        # the TTL code's number parsing doesn't get confused (in our original
        # implementation, it did).
        p8 = random_string()
        with client_no_transform(table.meta.client) as client:
            client.put_item(
                TableName=table.name,
                Item={
                    "p": {"S": p8},
                    "expiration": {"N": str((int(time.time()) - 60) // 10) + "e1"},
                },
            )
        # Similarly, floating point expiration time like 12345678.1 should
        # also be fine (note that Python's time.time() returns floating point).
        # This item should also be expired ASAP too.
        p9 = random_string()
        print(Decimal(str(time.time() - 60)))
        table.put_item(Item={"p": p9, "expiration": Decimal(str(time.time() - 60))})

        def check_expired():
            # After the delay, p1,p3,p5,p6,p7 should be alive, p0,p2,p4 should not
            return (
                "Item" not in table.get_item(Key={"p": p0})
                and "Item" in table.get_item(Key={"p": p1})
                and "Item" not in table.get_item(Key={"p": p2})
                and "Item" in table.get_item(Key={"p": p3})
                and "Item" not in table.get_item(Key={"p": p4})
                and "Item" in table.get_item(Key={"p": p5})
                and "Item" in table.get_item(Key={"p": p6})
                and "Item" in table.get_item(Key={"p": p7})
                and "Item" not in table.get_item(Key={"p": p8})
                and "Item" not in table.get_item(Key={"p": p9})
            )

        # We could have just done time.sleep(duration) here, but in case a
        # user is watching this long test, let's output the status every
        # minute, and it also allows us to test what happens when an item
        # p5's expiration time is continuously pushed back into the future:
        start_time = time.time()
        while time.time() < start_time + duration:
            print(f"--- {int(time.time()-start_time)} seconds")
            if "Item" in table.get_item(Key={"p": p0}):
                print("p0 alive")
            if "Item" in table.get_item(Key={"p": p1}):
                print("p1 alive")
            if "Item" in table.get_item(Key={"p": p2}):
                print("p2 alive")
            if "Item" in table.get_item(Key={"p": p3}):
                print("p3 alive")
            if "Item" in table.get_item(Key={"p": p4}):
                print("p4 alive")
            if "Item" in table.get_item(Key={"p": p5}):
                print("p5 alive")
            if "Item" in table.get_item(Key={"p": p6}):
                print("p6 alive")
            if "Item" in table.get_item(Key={"p": p7}):
                print("p7 alive")
            if "Item" in table.get_item(Key={"p": p8}):
                print("p8 alive")
            if "Item" in table.get_item(Key={"p": p9}):
                print("p9 alive")
            # Always keep p5's expiration delta into the future
            table.update_item(
                Key={"p": p5},
                AttributeUpdates={
                    "expiration": {"Value": int(time.time()) + delta, "Action": "PUT"}
                },
            )
            if check_expired():
                break
            time.sleep(duration / 15.0)

        assert check_expired()


# In the above key-attribute tests, the key attribute we used for the
# expiration-time attribute had the expected numeric type. If the key
# attribute has a non-numeric type (e.g., string), it can never contain
# an expiration time, so nothing will ever expire - but although DynamoDB
# knows this, it doesn't refuse this setting anyway - although it could.
# This test demonstrates that:
@pytest.mark.veryslow
def test_ttl_expiration_hash_wrong_type(dynamodb):
    duration = 3
    with new_test_table(
        dynamodb,
        KeySchema=[
            {"AttributeName": "p", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[{"AttributeName": "p", "AttributeType": "S"}],
    ) as table:
        ttl_spec = {"AttributeName": "p", "Enabled": True}
        table.meta.client.update_time_to_live(
            TableName=table.name, TimeToLiveSpecification=ttl_spec
        )
        # p1 is in the past, but it's a string, not the required numeric type,
        # so the item should never expire.
        p1 = str(int(time.time()) - 60)
        table.put_item(Item={"p": p1})
        start_time = time.time()
        while time.time() < start_time + duration:
            print(f"--- {int(time.time()-start_time)} seconds")
            if "Item" in table.get_item(Key={"p": p1}):
                print("p1 alive")
            time.sleep(duration / 30)
        # After the delay, p2 should be alive, p1 should not
        assert "Item" in table.get_item(Key={"p": p1})


# Check that in the DynamoDB Streams API, an event appears about an item
# becoming expired. This event should contain be a REMOVE event, contain
# the appropriate information about the expired item (its key and/or its
# content), and a special userIdentity flag saying that this is not a regular
# REMOVE but an expiration.
@pytest.mark.veryslow
@pytest.mark.xfail(reason="TTL expiration event in streams not yet marked")
def test_ttl_expiration_streams(dynamodb, dynamodbstreams):
    # In my experiments, a 30-minute (1800 seconds) is the typical
    # expiration delay in this test. If the test doesn't finish within
    # max_duration, we report a failure.
    max_duration = 10
    with new_test_table(
        dynamodb,
        KeySchema=[
            {"AttributeName": "p", "KeyType": "HASH"},
            {"AttributeName": "c", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "p", "AttributeType": "S"},
            {"AttributeName": "c", "AttributeType": "S"},
        ],
        StreamSpecification={
            "StreamEnabled": True,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
        },
    ) as table:
        ttl_spec = {"AttributeName": "expiration", "Enabled": True}
        table.meta.client.update_time_to_live(
            TableName=table.name, TimeToLiveSpecification=ttl_spec
        )

        # Before writing to the table, wait for the stream to become active
        # so we can be sure that the expiration - even if it miraculously
        # happens in a second (it usually takes 30 minutes) - is guaranteed
        # to reach the stream.
        stream_enabled = False
        start_time = time.time()
        while time.time() < start_time + 120:
            desc = table.meta.client.describe_table(TableName=table.name)["Table"]
            if "LatestStreamArn" in desc:
                arn = desc["LatestStreamArn"]
                desc = dynamodbstreams.describe_stream(StreamArn=arn)
                if desc["StreamDescription"]["StreamStatus"] == "ENABLED":
                    stream_enabled = True
                    break
            time.sleep(10)
        assert stream_enabled

        # Write a single expiring item. Set its expiration one minute in the
        # past, so the item should expire ASAP.
        p = random_string()
        c = random_string()
        expiration = int(time.time()) - 60
        table.put_item(Item={"p": p, "c": c, "animal": "dog", "expiration": expiration})

        # Wait (up to max_duration) for the item to expire, and for the
        # expiration event to reach the stream:
        start_time = time.time()
        event_found = False
        while time.time() < start_time + max_duration:
            print(f"--- {int(time.time()-start_time)} seconds")
            item_expired = "Item" not in table.get_item(Key={"p": p, "c": c})
            for record in read_entire_stream(dynamodbstreams, table):
                # An expired item has a specific userIdentity as follows:
                if record.get("userIdentity") == {
                    "Type": "Service",
                    "PrincipalId": "dynamodb.amazonaws.com",
                }:
                    # The expired item should be a REMOVE, and because we
                    # asked for old images and both the key and the full
                    # content.
                    assert record["eventName"] == "REMOVE"
                    assert record["dynamodb"]["Keys"] == {"p": {"S": p}, "c": {"S": c}}
                    assert record["dynamodb"]["OldImage"] == {
                        "p": {"S": p},
                        "c": {"S": c},
                        "animal": {"S": "dog"},
                        "expiration": {"N": str(expiration)},
                    }
                    event_found = True
            print(f"item expired {item_expired} event {event_found}")
            if item_expired and event_found:
                return
            time.sleep(max_duration / 15)
        pytest.fail("item did not expire or event did not reach stream")


# Utility function for reading the entire contents of a table's DynamoDB
# Streams. This function is only useful when we expect only a handful of
# items, and performance is not important, because nothing is cached between
# calls. So it's only used in "veryslow"-marked tests above.
def read_entire_stream(dynamodbstreams, table):
    # Look for the latest stream. If there is none, return nothing
    desc = table.meta.client.describe_table(TableName=table.name)["Table"]
    if "LatestStreamArn" not in desc:
        return []
    arn = desc["LatestStreamArn"]
    # List all shards of the stream in an array "shards":
    response = dynamodbstreams.describe_stream(StreamArn=arn)["StreamDescription"]
    shards = [x["ShardId"] for x in response["Shards"]]
    while "LastEvaluatedShardId" in response:
        response = dynamodbstreams.describe_stream(
            StreamArn=arn, ExclusiveStartShardId=response["LastEvaluatedShardId"]
        )["StreamDescription"]
        shards.extend([x["ShardId"] for x in response["Shards"]])
    records = []
    for shard_id in shards:
        # Get an interator for everything (TRIM_HORIZON) in the shard
        iter = dynamodbstreams.get_shard_iterator(
            StreamArn=arn, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"
        )["ShardIterator"]
        while iter is not None:
            response = dynamodbstreams.get_records(ShardIterator=iter)
            # DynamoDB will continue returning records until reaching the
            # current end, and only then we will get an empty response.
            if len(response["Records"]) == 0:
                break
            records.extend(response["Records"])
            iter = response.get("NextShardIterator")
    return records
