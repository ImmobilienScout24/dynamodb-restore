import json
from unittest import TestCase

# from dynamodb_restore import wait_for_table
from mock import patch, Mock, call

__author__ = 'khosemann'


class DynamoDbRestoreTests(TestCase):
    """
    @patch("dynamodb_restore.dynamodb")
    @patch("dynamodb_restore.time.sleep")
    def test_active_table(self, sleep, dynamodb):
        table_name = "best_table_ever"
        with open("../resources/active-table.json", "r") as f:
            dynamodb.describe_table.return_value = json.loads(f.read())

        wait_for_table(table_name)

        print sleep.mock_calls
        dynamodb.describe_table.assert_called_once_with(TableName=table_name)

    @patch("dynamodb_restore.dynamodb")
    @patch("dynamodb_restore.time.sleep")
    def test_inactive_table(self, sleep, dynamodb):
        table_name = "best_table_ever"
        with open("../resources/creating-table.json", "r") as f:
            creating_table = json.loads(f.read())
        with open("../resources/active-table.json", "r") as f:
            active_table = json.loads(f.read())
        dynamodb.describe_table.side_effect = [creating_table, active_table]

        wait_for_table(table_name)

        self.assertListEqual(
            [call(TableName='best_table_ever'), call(TableName='best_table_ever')],
            dynamodb.describe_table.mock_calls
        )
        print sleep.mock_calls
        sleep.assert_called_once_with(10)

    """
