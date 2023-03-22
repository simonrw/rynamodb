# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

##############################################################################
# Tests for Scylla's metrics (see docs/dev/metrics.md) for Alternator
# queries. Reproduces issue #9406, where although metrics was implemented for
# Alternator requests, they were missing for some operations (BatchGetItem).
# In the tests here we attempt to ensure that the metrics continue to work
# for the relevant operations as the code evolves.
#
# Note that all tests in this file test Scylla-specific features, and are
# "skipped" when not running against Scylla, or when unable to retrieve
# metrics through out-of-band HTTP requests to Scylla's Prometheus port (9180).
#
# IMPORTANT: we do not want these tests to assume that are not running in
# parallel with any other tests or workload - because such an assumption
# would limit our test deployment options in the future. NOT making this
# assumption means that these tests can't check that a certain operation
# increases a certain counter by exactly 1 - because other concurrent
# operations might increase it further! So our test can only check that the
# counter increases.
##############################################################################

import pytest
import requests
import re
import time
from contextlib import contextmanager
from botocore.exceptions import ClientError

from util import random_string, new_test_table, is_aws

# Fixture for checking if we are able to test Scylla metrics. Scylla metrics
# are not available on AWS (of course), but may also not be available for
# Scylla if for some reason we have only access to the Alternator protocol
# port but no access to the metrics port (9180).
# If metrics are *not* available, tests using this fixture will be skipped.
# Tests using this fixture may call get_metrics(metrics).
@pytest.fixture(scope="module")
def metrics(dynamodb):
    if dynamodb.meta.client._endpoint.host.endswith(".amazonaws.com"):
        pytest.skip("Scylla-only feature not supported by AWS")
    url = dynamodb.meta.client._endpoint.host
    # The Prometheus API is on port 9180, and always http, not https.
    url = re.sub(r":[0-9]+(/|$)", ":9180", url)
    url = re.sub(r"^https:", "http:", url)
    url = url + "/metrics"
    resp = requests.get(url)
    if resp.status_code != 200:
        pytest.skip("Metrics port 9180 is not available")
    yield url


# Utility function for fetching all metrics from Scylla, using an HTTP request
# to port 9180. The response format is defined by the Prometheus protocol.
# Only use get_metrics() in a test using the metrics_available fixture.
def get_metrics(metrics):
    response = requests.get(metrics)
    assert response.status_code == 200
    return response.text


# Utility function for fetching a metric with a given name and optionally a
# given sub-metric label (which should be a name-value map). If multiple
# matches are found, they are summed - this is useful for summing up the
# counts from multiple shards.
def get_metric(metrics, name, requested_labels=None, the_metrics=None):
    if not the_metrics:
        the_metrics = get_metrics(metrics)
    total = 0.0
    lines = re.compile("^" + name + "{.*$", re.MULTILINE)
    for match in re.findall(lines, the_metrics):
        a = match.split()
        metric = a[0]
        val = float(a[1])
        # Check if match also matches the requested labels
        if requested_labels:
            # we know metric begins with name{ and ends with } - the labels
            # are what we have between those
            got_labels = metric[len(name) + 1 : -1].split(",")
            # Check that every one of the requested labels is in got_labels:
            for k, v in requested_labels.items():
                if not f'{k}="{v}"' in got_labels:
                    # No match for requested label, skip this metric (python
                    # doesn't have "continue 2" so let's just set val to 0...
                    val = 0
                    break
        total += float(val)
    return total


# context manager for checking that a certain piece of code increases each
# of the specified metrics. Helps reduce the amount of code duplication
# below.
@contextmanager
def check_increases_metric(metrics, metric_names):
    the_metrics = get_metrics(metrics)
    saved_metrics = {x: get_metric(metrics, x, None, the_metrics) for x in metric_names}
    yield
    the_metrics = get_metrics(metrics)
    for n in metric_names:
        assert saved_metrics[n] < get_metric(
            metrics, n, None, the_metrics
        ), f"metric {n} did not increase"


@contextmanager
def check_increases_operation(metrics, operation_names):
    the_metrics = get_metrics(metrics)
    saved_metrics = {
        x: get_metric(metrics, "scylla_alternator_operation", {"op": x}, the_metrics)
        for x in operation_names
    }
    yield
    the_metrics = get_metrics(metrics)
    for op in operation_names:
        assert saved_metrics[op] < get_metric(
            metrics, "scylla_alternator_operation", {"op": op}, the_metrics
        )


###### Test for metrics that count DynamoDB API operations:


def test_batch_write_item(test_table_s, metrics):
    with check_increases_operation(metrics, ["BatchWriteItem"]):
        test_table_s.meta.client.batch_write_item(
            RequestItems={
                test_table_s.name: [
                    {"PutRequest": {"Item": {"p": random_string(), "a": "hi"}}}
                ]
            }
        )


# Reproduces issue #9406:
def test_batch_get_item(test_table_s, metrics):
    with check_increases_operation(metrics, ["BatchGetItem"]):
        test_table_s.meta.client.batch_get_item(
            RequestItems={
                test_table_s.name: {
                    "Keys": [{"p": random_string()}],
                    "ConsistentRead": True,
                }
            }
        )


# Test counters for CreateTable, DescribeTable, UpdateTable and DeleteTable
def test_table_operations(dynamodb, metrics):
    with check_increases_operation(
        metrics, ["CreateTable", "DescribeTable", "UpdateTable", "DeleteTable"]
    ):
        with new_test_table(
            dynamodb,
            KeySchema=[
                {"AttributeName": "p", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[{"AttributeName": "p", "AttributeType": "S"}],
        ) as table:
            # Run DescribeTable and UpdateTable on the new table to have those
            # counted as well. The DescribeTable call isn't strictly necessary
            # here because new_test_table already does it (to make sure the
            # table exists before returning), but let's not assume it.
            dynamodb.meta.client.describe_table(TableName=table.name)
            dynamodb.meta.client.update_table(TableName=table.name)


# Test counters for DeleteItem, GetItem, PutItem and UpdateItem:
def test_item_operations(test_table_s, metrics):
    with check_increases_operation(
        metrics, ["DeleteItem", "GetItem", "PutItem", "UpdateItem"]
    ):
        p = random_string()
        test_table_s.put_item(Item={"p": p})
        test_table_s.get_item(Key={"p": p})
        test_table_s.delete_item(Key={"p": p})
        test_table_s.update_item(Key={"p": p})


# Test counters for Query and Scan:
def test_item_operations(test_table_s, metrics):
    with check_increases_operation(metrics, ["Query", "Scan"]):
        # We don't care whether test_table_s contains any content when we scan
        # it. We just want the scan to be counted
        test_table_s.scan(Limit=1)
        test_table_s.query(
            Limit=1,
            KeyConditionExpression="p=:p",
            ExpressionAttributeValues={":p": "dog"},
        )


def test_describe_endpoints(dynamodb, metrics):
    with check_increases_operation(metrics, ["DescribeEndpoints"]):
        dynamodb.meta.client.describe_endpoints()


def test_list_tables(dynamodb, metrics):
    with check_increases_operation(metrics, ["ListTables"]):
        dynamodb.meta.client.list_tables(Limit=1)


# Test counters for TagResource, UntagResource, ListTagsOfResource:
def test_tag_operations(test_table_s, metrics):
    with check_increases_operation(
        metrics, ["TagResource", "UntagResource", "ListTagsOfResource"]
    ):
        client = test_table_s.meta.client
        arn = client.describe_table(TableName=test_table_s.name)["Table"]["TableArn"]
        client.list_tags_of_resource(ResourceArn=arn)
        p = random_string()
        client.tag_resource(ResourceArn=arn, Tags=[{"Key": p, "Value": "dog"}])
        client.untag_resource(ResourceArn=arn, TagKeys=[p])


# Test counters for DescribeTimeToLive, UpdateTimeToLive:
def test_ttl_operations(test_table_s, metrics):
    with check_increases_operation(metrics, ["DescribeTimeToLive", "UpdateTimeToLive"]):
        client = test_table_s.meta.client
        client.describe_time_to_live(TableName=test_table_s.name)
        # We don't need to enable time-to-live to check that the
        # UpdateTimeToLive operation is counted. It's fine that it fails.
        with pytest.raises(ClientError, match="ValidationException"):
            client.update_time_to_live(TableName=test_table_s.name)


# Test counters for ListStreams, DescribeStream, GetShardIterator, GetRecords:
def test_streams_operations(test_table_s, dynamodbstreams, metrics):
    # Just to verify that these operations are counted, they don't have
    # to pass. So we can use them on a table which does not have
    # streams enabled, and bogus ARNs, ignoring the errors. This makes
    # this test faster than actually testing working streams (which we
    # test thoroughly in test_streams.py).
    with check_increases_operation(
        metrics, ["ListStreams", "DescribeStream", "GetShardIterator", "GetRecords"]
    ):
        dynamodbstreams.list_streams(TableName=test_table_s.name)
        arn = random_string(37)
        with pytest.raises(ClientError, match="ValidationException"):
            dynamodbstreams.describe_stream(StreamArn=arn)
        shard_id = random_string(28)
        with pytest.raises(ClientError, match="ValidationException"):
            dynamodbstreams.get_shard_iterator(
                StreamArn=arn, ShardId=shard_id, ShardIteratorType="LATEST"
            )
        shard_iterator = "garbage"
        with pytest.raises(ClientError, match="ValidationException"):
            dynamodbstreams.get_records(ShardIterator=shard_iterator)


# TODO: The following DynamoDB operations are not yet supported by Alternator,
# so we can't increment their metrics so we don't have tests for them:
# CreateBackup, DeleteBackup, DescribeBackup, DescribeContinuousBackups,
# ListBackups, RestoreTableFromBackup, RestoreTableToPointInTime,
# UpdateContinuousBackups, TransactGetItems, TransactWriteItems,
# CreateGlobalTable, DescribeGlobalTable, DescribeGlobalTableSettings,
# ListGlobalTables, UpdateGlobalTable, UpdateGlobalTableSettings,
# DescribeLimits.

###### Test for other metrics, not counting specific DynamoDB API operations:

# Test that unsupported operations operations increment a counter. Instead
# using an actual unsupported operation - which can change in the future as
# Alternator supports more operations - let's use a bogus operation name
# which will never be implemented.
# An unsupported operation also increments the total_operations counter.
def test_unsupported_operation(dynamodb, metrics):
    with check_increases_metric(
        metrics,
        [
            "scylla_alternator_unsupported_operations",
            "scylla_alternator_total_operations",
        ],
    ):
        from test_manual_requests import get_signed_request

        req = get_signed_request(dynamodb, "BoguousOperationName", "{}")
        requests.post(req.url, headers=req.headers, data=req.body, verify=False)


# Test that also supported operations (such as DescribeEndPoints in this
# example) increment the total_operations metric:
def test_total_operations(dynamodb, metrics):
    with check_increases_metric(metrics, ["scylla_alternator_total_operations"]):
        dynamodb.meta.client.describe_endpoints()


# A fixture to read alternator-ttl-period-in-seconds from Scylla's
# configuration. If we're testing something which isn't Scylla, or
# this configuration does not exist, skip this test. If the configuration
# isn't low enough (it is more than one second), skip this test unless
# the "--runveryslow" option is used.
@pytest.fixture(scope="session")
def alternator_ttl_period_in_seconds(dynamodb, request):
    # If not running on Scylla, skip the test
    if is_aws(dynamodb):
        pytest.skip("Scylla-only test skipped")
    # In Scylla, we can inspect the configuration via a system table
    # (which is also visible in Alternator)
    config_table = dynamodb.Table(".scylla.alternator.system.config")
    resp = config_table.query(
        KeyConditionExpression="#key=:val",
        ExpressionAttributeNames={"#key": "name"},
        ExpressionAttributeValues={":val": "alternator_ttl_period_in_seconds"},
    )
    if not "Items" in resp:
        pytest.skip("missing TTL feature, skipping test")
    period = float(resp["Items"][0]["value"])
    if period > 1 and not request.config.getoption("runveryslow"):
        pytest.skip("need --runveryslow option to run")
    return period


# Test metrics of the background expiration thread run for Alternator's TTL
# feature. The metrics tested in this test are scylla_expiration_scan_passes,
# scylla_expiration_scan_table and scylla_expiration_items_deleted. The
# metric scylla_expiration_secondary_ranges_scanned is not tested in this
# test - testing it requires a multi-node cluster because it counts the
# number of times that this node took over another node's expiration duty.
#
# Unfortunately, to see TTL expiration in action this test may need to wait
# up to the setting of alternator_ttl_period_in_seconds. test/alternator/run
# sets this to 1 second, which becomes the maximum delay of this test, but
# if it is set higher we skip this test unless --runveryslow is enabled.
def test_ttl_stats(dynamodb, metrics, alternator_ttl_period_in_seconds):
    print(alternator_ttl_period_in_seconds)
    with check_increases_metric(
        metrics,
        [
            "scylla_expiration_scan_passes",
            "scylla_expiration_scan_table",
            "scylla_expiration_items_deleted",
        ],
    ):
        with new_test_table(
            dynamodb,
            KeySchema=[
                {"AttributeName": "p", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[{"AttributeName": "p", "AttributeType": "S"}],
        ) as table:
            # Insert one already-expired item, and then enable TTL:
            p0 = random_string()
            table.put_item(Item={"p": p0, "expiration": int(time.time()) - 60})
            assert "Item" in table.get_item(Key={"p": p0})
            client = table.meta.client
            client.update_time_to_live(
                TableName=table.name,
                TimeToLiveSpecification={
                    "AttributeName": "expiration",
                    "Enabled": True,
                },
            )
            # After alternator_ttl_period_in_seconds, we expect the metrics
            # to have increased. Add some time to that, to account for
            # extremely overloaded test machines.
            start_time = time.time()
            while time.time() < start_time + alternator_ttl_period_in_seconds + 120:
                if not "Item" in table.get_item(Key={"p": p0}):
                    break
                time.sleep(0.1)
            assert not "Item" in table.get_item(Key={"p": p0})


# TODO: there are additional metrics which we don't yet test here. At the
# time of this writing they are: latency histograms
# ({put,get,delete,update}_item_latency, get_records_latency),
# reads_before_write, write_using_lwt, shard_bounce_for_lwt,
# requests_blocked_memory, requests_shed
