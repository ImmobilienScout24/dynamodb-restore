#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
dynamodb-restore.py

Usage:
  dynamodb-restore.py [--pipeline-definition-uri=pipeline_uri] --subnetId=subnetId [--tablename=targetTableName] --backup-source=s3_uri --schema-definition=local_file_or_s3_uri [--region=region]

Options:
  -h --help  Show this screen.
  --pipeline-definition-uri=url  Restore DataPipeline template URL  [default: s3://is24-cfn-templates/infrastructure/restore-dynamodb-from-s3.datapipeline.json]
  --region=region  Region to create DynamoDB Table and DataPipeline Job in  [default: eu-west-1]
"""

from docopt import docopt
from dynamodb_restore import restore_schema, create_datapipeline, load_schema

args = docopt(__doc__)
table_name = args["--tablename"]
region = args["--region"]

table_definition = load_schema(args["--schema-definition"])

print "Restoring schema"
restored_table_name = restore_schema(table_definition, region, table_name)

print "Creating datapipeline"
create_datapipeline(args["--pipeline-definition-uri"], args["--subnetId"], restored_table_name, args["--backup-source"],
                    region)

print "Restore triggered successfully!"