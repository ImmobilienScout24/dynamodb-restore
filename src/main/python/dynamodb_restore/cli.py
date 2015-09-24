"""
dynamodb-restore

Restore DynamoDB tables from S3 by DataPipeline Job.
This tool supports two modes. If you specify --data-only, the table and its schema must exist.
Otherwise this tool will create a table. You need to specify a table-definition-url (output from dynamodb describe_table) for the schema.


Usage:
  dynamodb-restore --data-only --backup-source=s3_uri --tablename=tablename [--pipeline-definition-uri=local_file_or_s3_uri] [--subnetId=subnetId] [--log-dest=s3_uri] [--region=region]
  dynamodb-restore --backup-source=s3_uri --table-definition-uri=local_file_or_s3_uri [--tablename=tablename] [--pipeline-definition-uri=local_file_or_s3_uri] [--subnetId=subnetId] [--log-dest=s3_uri] [--region=region]

Options:
  -h --help                         Show this screen.
  --data-only                       Only restore data (table must already exist and must match backup data)
  --pipeline-definition-uri=url     Restore DataPipeline template URL                           [default: s3://is24-cfn-templates/infrastructure/restore-dynamodb-from-s3.datapipeline.json]
  --region=region                   Region to create DynamoDB Table and DataPipeline Job in     [default: eu-west-1]
"""

import sys
from docopt import docopt
from dynamodb_restore import restore
from dynamodb_restore.util import get_first_subnet_id_from_vpc_stack, get_log_dest_from_backup_source
from botocore.exceptions import ClientError

def main():
    try:
        args = docopt(__doc__)
        pipeline_definition_uri = args["--pipeline-definition-uri"]
        table_definition_uri = args["--table-definition-uri"]
        table_name = args["--tablename"]
        backup_source = args["--backup-source"]
        region = args["--region"]
        subnet_id = args["--subnetId"] if args["--subnetId"] else get_first_subnet_id_from_vpc_stack(region)
        log_dest = args["--log-dest"] if args["--log-dest"] else get_log_dest_from_backup_source(backup_source)
        data_only = args["--data-only"]

        print "using subnet id: {0}".format(subnet_id)

        restore(data_only, table_name, table_definition_uri, pipeline_definition_uri, backup_source, subnet_id, log_dest, region)
    except ClientError as e:
        print "Error calling the AWS API. Your credentials may be expired! ({0})".format(e)
        sys.exit(1)
    except Exception as e:
        print e
        sys.exit(1)