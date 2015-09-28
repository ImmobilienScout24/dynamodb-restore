import json
import tempfile
import re
import uuid
import datetime
import boto3


class DynamoDbRestoreHandler(object):
    def __init__(self, data_only, table_name, table_definition_uri, pipeline_definition_uri, backup_source, subnet_id,
                 log_dest, mail_address, desired_write_throughput, region):

        self.data_only = data_only
        self.table_definition = self.load_schema(table_definition_uri) if table_definition_uri else None
        self.restore_table_name = self.get_restore_table_name(data_only, table_name, self.table_definition)
        self.pipeline_definition_uri = pipeline_definition_uri
        self.backup_source = backup_source
        self.subnet_id = subnet_id
        self.log_dest = log_dest
        self.mail_address = mail_address
        self.desired_write_throughput = int(desired_write_throughput) if desired_write_throughput else 0
        self.region = region

    @staticmethod
    def get_restore_table_name(data_only, table_name, table_definition):
        if data_only:
            if not table_name:
                raise Exception("Please specify table name if you use data-only!")

            return table_name

        else:
            if not table_definition:
                raise Exception("Please specify table-definition-uri!")

        return table_name if table_name else table_definition["TableName"]

    @staticmethod
    def _get_provisioned_throughput(table_definition):
        return {
            "ReadCapacityUnits": table_definition["ProvisionedThroughput"]["ReadCapacityUnits"],
            "WriteCapacityUnits": table_definition["ProvisionedThroughput"]["WriteCapacityUnits"]
        }

    @staticmethod
    def _get_index(source_index):
        return {
            "IndexName": source_index["IndexName"],
            "KeySchema": source_index["KeySchema"],
            "Projection": source_index["Projection"]
        }

    def restore_schema(self, table_definition, table_name, region):
        dynamodb = boto3.client('dynamodb', region_name=region)

        create_table_args = {
            "TableName": table_name,
            "AttributeDefinitions": table_definition['AttributeDefinitions'],
            "KeySchema": table_definition['KeySchema'],
            "ProvisionedThroughput": self._get_provisioned_throughput(table_definition)
        }

        global_secondary_index_definitions = table_definition.get('GlobalSecondaryIndexes', [])
        if global_secondary_index_definitions:
            global_secondary_indexes = []
            for source_index in global_secondary_index_definitions:
                target_index = self._get_index(source_index)
                target_index["ProvisionedThroughput"] = self._get_provisioned_throughput(source_index)
                global_secondary_indexes.append(target_index)

            create_table_args['GlobalSecondaryIndexes'] = global_secondary_indexes

        local_secondary_index_definitions = table_definition.get("LocalSecondaryIndexes", [])
        if local_secondary_index_definitions:
            local_secondary_indexes = []
            for source_index in local_secondary_index_definitions:
                local_secondary_indexes.append(self._get_index(source_index))

            create_table_args['LocalSecondaryIndexes'] = local_secondary_indexes

        if table_definition.has_key("StreamSpecification"):
            create_table_args['StreamSpecification'] = table_definition["StreamSpecification"]

        dynamodb.create_table(**create_table_args)

        print("Waiting for table {0} reaching state ACTIVE".format(table_name))
        dynamodb.get_waiter("table_exists").wait(TableName=table_name)

        return table_name

    def create_notification_topic(self, mail_address):
        topic_name = "dynamodb_restore_notification_{0}".format(self.restore_table_name)

        print("Creating notification topic for {0}. Please ensure to confirm the subscription mail sent to the address!"
              .format(mail_address))

        client = boto3.client("sns", region_name=self.region)

        response = client.create_topic(Name=topic_name)
        topic_arn = response["TopicArn"]

        client.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=mail_address)

        return topic_arn

    @classmethod
    def load_schema(cls, schema_location):
        return cls.load_json_file(schema_location)['Table']

    @staticmethod
    def load_json_file(location):
        m = re.match("s3://([\w-]+)/(.*)", location)
        if m:
            s3 = boto3.client("s3")
            bucket = m.group(1)
            object_name = m.group(2)
            local_file = tempfile.gettempdir() + "/" + str(uuid.uuid4()) + ".json"
            s3.download_file(bucket, object_name, local_file)
        else:
            local_file = location

        with open(local_file, "r") as f:
            return json.load(f)

    def create_datapipeline(self, definition, subnet_id, log_dest, ddb_table_name, s3_loc, notification_topic_arn,
                            region):
        pipeline_name = "restore_" + self.restore_table_name
        pipeline_template = self.load_json_file(definition)

        parameter_values = [{"id": "mySubnetId", "stringValue": subnet_id},
                            {"id": "myDDBTableName", "stringValue": ddb_table_name},
                            {"id": "myInputS3Loc", "stringValue": s3_loc},
                            {"id": "myLogS3Loc", "stringValue": log_dest},
                            {"id": "myNotificationTopicArn", "stringValue": notification_topic_arn}]

        client = boto3.client("datapipeline", region_name=region)

        if self.pipeline_exists(client, pipeline_name):
            raise Exception("pipeline: {0} already exists".format(pipeline_name))

        pipeline = client.create_pipeline(name=pipeline_name, uniqueId=pipeline_name)
        pipeline_id = pipeline["pipelineId"]

        client.put_pipeline_definition(pipelineId=pipeline_id,
                                       pipelineObjects=pipeline_template["pipelineObjects"],
                                       parameterObjects=pipeline_template["parameterObjects"],
                                       parameterValues=parameter_values)

        client.activate_pipeline(
            pipelineId=pipeline_id,
            startTimestamp=datetime.datetime.utcnow()
        )

        return pipeline_id

    def set_write_throughput(self, restore_table_name, desired_write_throughput):
        client = boto3.client("dynamodb", region_name=self.region)
        provisioned_throughput = client.describe_table(TableName=restore_table_name)['Table']['ProvisionedThroughput']
        if desired_write_throughput > provisioned_throughput['WriteCapacityUnits']:
            print("Provisioned WriteThroughput on {0}: {1}".format(restore_table_name,
                                                                   provisioned_throughput['WriteCapacityUnits']))
            print("overwriting with: {0}".format(desired_write_throughput))
            print("When finished please adjust WriteThroughput to the originally provisioned value!")
            client.update_table(TableName=restore_table_name,
                                ProvisionedThroughput={'ReadCapacityUnits': provisioned_throughput['ReadCapacityUnits'],
                                                       'WriteCapacityUnits': desired_write_throughput})

            client.get_waiter("table_exists").wait(TableName=restore_table_name)

    @staticmethod
    def pipeline_exists(client, pipeline_name):
        for item in client.list_pipelines()["pipelineIdList"]:
            if item["name"] == pipeline_name:
                return True

        return False

    def restore(self):
        if not self.data_only:
            print "Restoring schema for {0}".format(self.restore_table_name)
            self.restore_schema(self.table_definition, self.restore_table_name, self.region)

        if self.desired_write_throughput:
            self.set_write_throughput(self.restore_table_name, self.desired_write_throughput)

        notification_topic_arn = self.create_notification_topic(self.mail_address)

        print "Creating datapipeline"
        pipeline_id = self.create_datapipeline(self.pipeline_definition_uri, self.subnet_id, self.log_dest,
                                               self.restore_table_name, self.backup_source, notification_topic_arn,
                                               self.region)

        print "Restore triggered successfully, see data pipeline with id {0}".format(pipeline_id)

# client = boto3.client("datapipeline", region_name="eu-west-1")
# print client.list_pipelines()
