import json
from unittest import TestCase
from dynamodb_restore import wait_for_table

from mock import patch

__author__ = 'khosemann'


class DynamoDbRestoreTests(TestCase):

  @patch("dynamodb_restore.dynamodb")
  def test_wait_for_table(self, dynamodb):
    table_name = "best_table_ever"
    with open("../resources/active-table.json", "r") as f:
      dynamodb.describe_table.return_value = json.loads(f.read())

    wait_for_table(table_name)

    dynamodb.describe_table.assert_called_with(TableName=table_name)