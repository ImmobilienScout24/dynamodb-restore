import unittest2
from mock import Mock
from dynamodb_restore import DynamoDbRestoreHandler

class DynamoDbRestoreTests(unittest2.TestCase):

    def test_pipeline_exists_return_true_for_existing_pipeline(self):
        mocked_response = {'pipelineIdList': [
                               {'id': 'df-09204411HHMSRZ11YXDP', 'name': 'my-pipeline-1'},
                               {'id': 'df-08522353PMJ0KLMYFRUL', 'name': 'my-pipeline-2'},
                               {'id': 'df-08320941WW3ZVXZXG4D2', 'name': 'my-pipeline-3 Backup'}
                           ]}

        client_mock = Mock()
        client_mock.list_pipelines.return_value = mocked_response

        self.assertTrue(DynamoDbRestoreHandler.pipeline_exists(client_mock, "my-pipeline-1"))

    def test_pipeline_exists_return_false_for_non_existing_pipeline(self):
        mocked_response = {'pipelineIdList': [
                               {'id': 'df-09204411HHMSRZ11YXDP', 'name': 'my-pipeline-1'},
                               {'id': 'df-08522353PMJ0KLMYFRUL', 'name': 'my-pipeline-2'},
                               {'id': 'df-08320941WW3ZVXZXG4D2', 'name': 'my-pipeline-3 Backup'}
                           ]}

        client_mock = Mock()
        client_mock.list_pipelines.return_value = mocked_response

        self.assertFalse(DynamoDbRestoreHandler.pipeline_exists(client_mock, "my-new-pipeline"))