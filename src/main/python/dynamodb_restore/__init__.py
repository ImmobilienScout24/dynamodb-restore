from __future__ import print_function
import time
import json

import boto3

dynamodb = boto3.client('dynamodb')

def get_provisioned_throughput(parent):
    return {
        "ReadCapacityUnits": parent["ProvisionedThroughput"]["ReadCapacityUnits"],
        "WriteCapacityUnits": parent["ProvisionedThroughput"]["WriteCapacityUnits"]
    }

def get_index(source_index):
    return {
        "IndexName": source_index["IndexName"],
        "KeySchema": source_index["KeySchema"],
        "Projection": source_index["Projection"]
    }


def restore_schema(table_name):
    with open("performance_objectPerformance.json", "r") as f:
        table = json.loads(f.read())['Table']

    global_secondary_indexes = []
    for source_index in table.get('GlobalSecondaryIndexes', []):
        target_index = get_index(source_index)
        target_index["ProvisionedThroughput"] = get_provisioned_throughput(source_index)
        global_secondary_indexes.append(target_index)

    local_secondary_indexes = []
    for source_index in table.get("LocalSecondaryIndexes", []):
        local_secondary_indexes.append(get_index(source_index))

    create_table_args = {
        "TableName": table_name,
        "AttributeDefinitions": table['AttributeDefinitions'],
        "KeySchema": table['KeySchema'],
        "ProvisionedThroughput": get_provisioned_throughput(table)
    }

    if len(global_secondary_indexes) > 0:
        create_table_args['GlobalSecondaryIndexes'] = global_secondary_indexes
    if len(local_secondary_indexes) > 0:
        create_table_args['LocalSecondaryIndexes'] = local_secondary_indexes
    if table.has_key("StreamSpecification"):
        create_table_args['StreamSpecification'] = table["StreamSpecification"]

    dynamodb.create_table(**create_table_args)

    wait_for_table(table_name)


def wait_for_table(table_name):
    for i in xrange(0, 100):
        try:
            table = dynamodb.describe_table(TableName=table_name)
            if table['Table']['TableStatus'] == "ACTIVE":
                return
        finally:
            time.sleep(10 * i)


def create_datapipeline(definition, subnet_id, ddb_table_name, s3_loc):
    _pipeline_name = "restore_" + ddb_table_name
    with open(definition) as definition_file:
        pipeline_objects = json.load(definition_file)

    parameter_values = [{"id": "mySubnetId", "stringValue": subnet_id},
                        {"id": "myDDBTableName", "stringValue": ddb_table_name},
                        {"id": "myInputS3Loc", "stringValue": s3_loc}]

    client = boto3.client("datapipeline", region_name="eu-west-1")
    pipeline = client.create_pipeline(name=_pipeline_name, uniqueId=_pipeline_name)
    client.put_pipeline_definition(pipelineId=pipeline["pipelineId"],
                                   pipelineObjects=pipeline_objects["objects"],
                                   parameterValues=parameter_values)
