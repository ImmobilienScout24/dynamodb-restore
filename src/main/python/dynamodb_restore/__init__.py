import json
import tempfile
import re

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


def restore_schema(target_table_name, table_definition):
    create_table_args = {
        "TableName": target_table_name,
        "AttributeDefinitions": table_definition['AttributeDefinitions'],
        "KeySchema": table_definition['KeySchema'],
        "ProvisionedThroughput": get_provisioned_throughput(table_definition)
    }

    global_secondary_index_definitions = table_definition.get('GlobalSecondaryIndexes', [])
    if global_secondary_index_definitions:
        global_secondary_indexes = []
        for source_index in global_secondary_index_definitions:
            target_index = get_index(source_index)
            target_index["ProvisionedThroughput"] = get_provisioned_throughput(source_index)
            global_secondary_indexes.append(target_index)

        create_table_args['GlobalSecondaryIndexes'] = global_secondary_indexes

    local_secondary_index_definitions = table_definition.get("LocalSecondaryIndexes", [])
    if local_secondary_index_definitions:
        local_secondary_indexes = []
        for source_index in local_secondary_index_definitions:
            local_secondary_indexes.append(get_index(source_index))

        create_table_args['LocalSecondaryIndexes'] = local_secondary_indexes

    if table_definition.has_key("StreamSpecification"):
        create_table_args['StreamSpecification'] = table_definition["StreamSpecification"]

    dynamodb.create_table(**create_table_args)

    dynamodb.get_waiter("table_exists").wait(TableName=target_table_name)


def load_schema(schema_location, table_name):
    m = re.match("s3://([\w-]+)/(.*)", schema_location)
    if m:
        s3 = boto3.client("s3")
        bucket = m.group(1)
        object_name = m.group(2)
        local_file = tempfile.gettempdir() + "/" + table_name + "-schema.json"
        s3.download_file(bucket, object_name, local_file)
    else:
        local_file = schema_location

    with open(local_file, "r") as f:
        return json.loads(f.read())['Table']


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
