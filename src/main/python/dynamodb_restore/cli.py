"""
dynamodb-restore.py

Usage:
  dynamodb-restore.py
    --subnetId=subnetId
    --backup-source=s3_uri
    [--table-definition-uri=local_file_or_s3_uri]
    [--pipeline-definition-uri=local_file_or_s3_uri]
    [--tablename=targetTableName]
    [--region=region]
    [--data-only]

Options:
  -h --help                         Show this screen.
  --pipeline-definition-uri=url     Restore DataPipeline template URL                           [default: s3://is24-cfn-templates/infrastructure/restore-dynamodb-from-s3.datapipeline.json]
  --region=region                   Region to create DynamoDB Table and DataPipeline Job in     [default: eu-west-1]
"""

import sys
from docopt import docopt
from dynamodb_restore import restore
from botocore.exceptions import ClientError

def main():
    args = docopt(__doc__)
    pipeline_definition_uri = args["--pipeline-definition-uri"]
    table_definition_uri = args["--table-definition-uri"]
    table_name = args["--tablename"]
    backup_source = args["--backup-source"]
    subnet_id = args["--subnetId"]
    region = args["--region"]
    data_only = args["--data-only"]

    try:
        restore(data_only, table_name, table_definition_uri, pipeline_definition_uri, backup_source, subnet_id, region)
    except ClientError as e:
        print "Error calling the AWS API. Your credentials may be expired! ({0})".format(e)
        sys.exit(1)
    except Exception as e:
        print e
        sys.exit(1)