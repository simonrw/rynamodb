# Copyright 2019-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# This file contains "test fixtures", a pytest concept described in
# https://docs.pytest.org/en/latest/fixture.html.
# A "fixture" is some sort of setup which an invididual test requires to run.
# The fixture has setup code and teardown code, and if multiple tests
# require the same fixture, it can be set up only once - while still allowing
# the user to run individual tests and automatically set up the fixtures they need.

import pytest
import boto3
from util import create_test_table

# When tests are run with HTTPS, the server often won't have its SSL
# certificate signed by a known authority. So we will disable certificate
# verification with the "verify=False" request option. However, once we do
# that, we start getting scary-looking warning messages, saying that this
# makes HTTPS insecure. The following silences those warnings:
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Test that the Boto libraries are new enough. These tests want to test a
# large variety of DynamoDB API features, and to do this we need a new-enough
# version of the the Boto libraries (boto3 and botocore) so that they can
# access all these API features.
# In particular, the BillingMode feature was added in botocore 1.12.54.
import botocore
import sys
from packaging.version import Version

if Version(botocore.__version__) < Version("1.12.54"):
    pytest.exit(
        "Your Boto library is too old. Please upgrade it,\ne.g. using:\n    sudo pip{} install --upgrade boto3".format(
            sys.version_info[0]
        )
    )

# By default, tests run against a local Scylla installation on localhost:8080/.
# The "--aws" option can be used to run against Amazon DynamoDB in the us-east-1
# region.
def pytest_addoption(parser):
    parser.addoption(
        "--aws",
        action="store_true",
        help="run against AWS instead of a local Scylla installation",
    )
    parser.addoption(
        "--https",
        action="store_true",
        help="communicate via HTTPS protocol on port 8043 instead of HTTP when"
        " running against a local Scylla installation",
    )
    parser.addoption(
        "--url", action="store", help="communicate with given URL instead of defaults"
    )
    parser.addoption(
        "--runveryslow",
        action="store_true",
        help="run tests marked veryslow instead of skipping them",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "veryslow: mark test as very slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runveryslow"):
        # --runveryslow given in cli: do not skip veryslow tests
        return
    skip_veryslow = pytest.mark.skip(reason="need --runveryslow option to run")
    for item in items:
        if "veryslow" in item.keywords:
            item.add_marker(skip_veryslow)


# "dynamodb" fixture: set up client object for communicating with the DynamoDB
# API. Currently this chooses either Amazon's DynamoDB in the default region
# or a local Alternator installation on http://localhost:8080 - depending on the
# existence of the "--aws" option. In the future we should provide options
# for choosing other Amazon regions or local installations.
# We use scope="session" so that all tests will reuse the same client object.
@pytest.fixture(scope="session")
def dynamodb(request):
    # Disable boto3's client-side validation of parameters. This validation
    # only makes it impossible for us to test various error conditions,
    # because boto3 checks them before we can get the server to check them.
    boto_config = botocore.client.Config(parameter_validation=False)
    if request.config.getoption("aws"):
        return boto3.resource("dynamodb", config=boto_config)
    else:
        # Even though we connect to the local installation, Boto3 still
        # requires us to specify dummy region and credential parameters,
        # otherwise the user is forced to properly configure ~/.aws even
        # for local runs.
        if request.config.getoption("url") is not None:
            local_url = request.config.getoption("url")
        else:
            local_url = (
                "https://localhost:8043"
                if request.config.getoption("https")
                else "http://localhost:8000"
            )
        # Disable verifying in order to be able to use self-signed TLS certificates
        verify = not request.config.getoption("https")
        return boto3.resource(
            "dynamodb",
            endpoint_url=local_url,
            verify=verify,
            region_name="us-east-1",
            aws_access_key_id="alternator",
            aws_secret_access_key="secret_pass",
            config=boto_config.merge(
                botocore.client.Config(retries={"max_attempts": 0}, read_timeout=300)
            ),
        )


@pytest.fixture(scope="session")
def dynamodbstreams(request):
    # Disable boto3's client-side validation of parameters. This validation
    # only makes it impossible for us to test various error conditions,
    # because boto3 checks them before we can get the server to check them.
    boto_config = botocore.client.Config(parameter_validation=False)
    if request.config.getoption("aws"):
        return boto3.client("dynamodbstreams", config=boto_config)
    else:
        # Even though we connect to the local installation, Boto3 still
        # requires us to specify dummy region and credential parameters,
        # otherwise the user is forced to properly configure ~/.aws even
        # for local runs.
        if request.config.getoption("url") is not None:
            local_url = request.config.getoption("url")
        else:
            local_url = (
                "https://localhost:8043"
                if request.config.getoption("https")
                else "http://localhost:8000"
            )
        # Disable verifying in order to be able to use self-signed TLS certificates
        verify = not request.config.getoption("https")
        return boto3.client(
            "dynamodbstreams",
            endpoint_url=local_url,
            verify=verify,
            region_name="us-east-1",
            aws_access_key_id="alternator",
            aws_secret_access_key="secret_pass",
            config=boto_config.merge(
                botocore.client.Config(retries={"max_attempts": 0}, read_timeout=300)
            ),
        )


# "test_table" fixture: Create and return a temporary table to be used in tests
# that need a table to work on. The table is automatically deleted at the end.
# We use scope="session" so that all tests will reuse the same client object.
# This "test_table" creates a table which has a specific key schema: both a
# partition key and a sort key, and both are strings. Other fixtures (below)
# can be used to create different types of tables.
#
# TODO: Although we are careful about deleting temporary tables when the
# fixture is torn down, in some cases (e.g., interrupted tests) we can be left
# with some tables not deleted, and they will never be deleted. Because all
# our temporary tables have the same test_table_prefix, we can actually find
# and remove these old tables with this prefix. We can have a fixture, which
# test_table will require, which on teardown will delete all remaining tables
# (possibly from an older run). Because the table's name includes the current
# time, we can also remove just tables older than a particular age. Such
# mechanism will allow running tests in parallel, without the risk of deleting
# a parallel run's temporary tables.
@pytest.fixture(scope="session")
def test_table(dynamodb):
    table = create_test_table(
        dynamodb,
        KeySchema=[
            {"AttributeName": "p", "KeyType": "HASH"},
            {"AttributeName": "c", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "p", "AttributeType": "S"},
            {"AttributeName": "c", "AttributeType": "S"},
        ],
    )
    yield table
    # We get back here when this fixture is torn down. We ask Dynamo to delete
    # this table, but not wait for the deletion to complete. The next time
    # we create a test_table fixture, we'll choose a different table name
    # anyway.
    table.delete()


# The following fixtures test_table_* are similar to test_table but create
# tables with different key schemas.
@pytest.fixture(scope="session")
def test_table_s(dynamodb):
    table = create_test_table(
        dynamodb,
        KeySchema=[
            {"AttributeName": "p", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[{"AttributeName": "p", "AttributeType": "S"}],
    )
    yield table
    table.delete()


# test_table_s_2 has exactly the same schema as test_table_s, and is useful
# for tests which need two different tables with the same schema.
@pytest.fixture(scope="session")
def test_table_s_2(dynamodb):
    table = create_test_table(
        dynamodb,
        KeySchema=[
            {"AttributeName": "p", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[{"AttributeName": "p", "AttributeType": "S"}],
    )
    yield table
    table.delete()


@pytest.fixture(scope="session")
def test_table_b(dynamodb):
    table = create_test_table(
        dynamodb,
        KeySchema=[
            {"AttributeName": "p", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[{"AttributeName": "p", "AttributeType": "B"}],
    )
    yield table
    table.delete()


@pytest.fixture(scope="session")
def test_table_sb(dynamodb):
    table = create_test_table(
        dynamodb,
        KeySchema=[
            {"AttributeName": "p", "KeyType": "HASH"},
            {"AttributeName": "c", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "p", "AttributeType": "S"},
            {"AttributeName": "c", "AttributeType": "B"},
        ],
    )
    yield table
    table.delete()


@pytest.fixture(scope="session")
def test_table_sn(dynamodb):
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
    )
    yield table
    table.delete()


@pytest.fixture(scope="session")
def test_table_ss(dynamodb):
    table = create_test_table(
        dynamodb,
        KeySchema=[
            {"AttributeName": "p", "KeyType": "HASH"},
            {"AttributeName": "c", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "p", "AttributeType": "S"},
            {"AttributeName": "c", "AttributeType": "S"},
        ],
    )
    yield table
    table.delete()


# "filled_test_table" fixture:  Create a temporary table to be used in tests
# that involve reading data - GetItem, Scan, etc. The table is filled with
# 328 items - each consisting of a partition key, clustering key and two
# string attributes. 164 of the items are in a single partition (with the
# partition key 'long') and the 164 other items are each in a separate
# partition. Finally, a 329th item is added with different attributes.
# This table is supposed to be read from, not updated nor overwritten.
# This fixture returns both a table object and the description of all items
# inserted into it.
@pytest.fixture(scope="session")
def filled_test_table(dynamodb):
    table = create_test_table(
        dynamodb,
        KeySchema=[
            {"AttributeName": "p", "KeyType": "HASH"},
            {"AttributeName": "c", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "p", "AttributeType": "S"},
            {"AttributeName": "c", "AttributeType": "S"},
        ],
    )
    count = 164
    items = [
        {"p": str(i), "c": str(i), "attribute": "x" * 7, "another": "y" * 16}
        for i in range(count)
    ]
    items = items + [
        {
            "p": "long",
            "c": str(i),
            "attribute": "x" * (1 + i % 7),
            "another": "y" * (1 + i % 16),
        }
        for i in range(count)
    ]
    items.append(
        {
            "p": "hello",
            "c": "world",
            "str": "and now for something completely different",
        }
    )

    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(item)

    yield table, items
    table.delete()
