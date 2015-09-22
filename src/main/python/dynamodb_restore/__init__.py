import json
import tempfile
import re
import uuid
import datetime
import boto3


def _get_provisioned_throughput(table_definition):
    return {
        "ReadCapacityUnits": table_definition["ProvisionedThroughput"]["ReadCapacityUnits"],
        "WriteCapacityUnits": table_definition["ProvisionedThroughput"]["WriteCapacityUnits"]
    }

def _get_index(source_index):
    return {
        "IndexName": source_index["IndexName"],
        "KeySchema": source_index["KeySchema"],
        "Projection": source_index["Projection"]
    }


def restore_schema(table_definition, region, table_name):

    dynamodb = boto3.client('dynamodb', region_name=region)

    create_table_args = {
        "TableName": table_name,
        "AttributeDefinitions": table_definition['AttributeDefinitions'],
        "KeySchema": table_definition['KeySchema'],
        "ProvisionedThroughput": _get_provisioned_throughput(table_definition)
    }

    global_secondary_index_definitions = table_definition.get('GlobalSecondaryIndexes', [])
    if global_secondary_index_definitions:
        global_secondary_indexes = []
        for source_index in global_secondary_index_definitions:
            target_index = _get_index(source_index)
            target_index["ProvisionedThroughput"] = _get_provisioned_throughput(source_index)
            global_secondary_indexes.append(target_index)

        create_table_args['GlobalSecondaryIndexes'] = global_secondary_indexes

    local_secondary_index_definitions = table_definition.get("LocalSecondaryIndexes", [])
    if local_secondary_index_definitions:
        local_secondary_indexes = []
        for source_index in local_secondary_index_definitions:
            local_secondary_indexes.append(_get_index(source_index))

        create_table_args['LocalSecondaryIndexes'] = local_secondary_indexes

    if table_definition.has_key("StreamSpecification"):
        create_table_args['StreamSpecification'] = table_definition["StreamSpecification"]

    dynamodb.create_table(**create_table_args)

    dynamodb.get_waiter("table_exists").wait(TableName=table_name)

    return table_name


def load_schema(schema_location):
    return load_json_file(schema_location)['Table']


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


def create_datapipeline(definition, subnet_id, ddb_table_name, s3_loc, region):
    pipeline_name = "restore_" + ddb_table_name
    pipeline_template = load_json_file(definition)

    parameter_values = [{"id": "mySubnetId", "stringValue": subnet_id},
                        {"id": "myDDBTableName", "stringValue": ddb_table_name},
                        {"id": "myInputS3Loc", "stringValue": s3_loc}]

    client = boto3.client("datapipeline", region_name=region)
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

def restore(data_only, table_name, table_definition_uri, pipeline_definition_uri, backup_source, subnet_id, region):
    if data_only:
        if not table_name:
            raise Exception("Please specify --tablename if you use --data-only!")

        restore_table_name = table_name

    else:
        if not table_definition_uri:
            raise Exception("Please specify --table-definition-uri!")

        table_definition = load_schema(table_definition_uri)
        restore_table_name = table_name if table_name else table_definition["TableName"]

        print "Restoring schema for {0}".format(restore_table_name)
        restore_schema(table_definition, region, restore_table_name)
    print "Creating datapipeline"
    create_datapipeline(pipeline_definition_uri, subnet_id, restore_table_name, backup_source,
                        region)
    print "Restore triggered successfully!"