#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
dynamodb-restore.py

Usage:
  dynamodb-restore.py --pipeline-definition-uri=pipeline_uri --subnetId=subnetId --ddbtablename=targetTableName --backup-source=s3_uri --schema-definition=local_file_or_s3_uri

Options:
  -h --help     Show this screen.
"""

from docopt import docopt
from dynamodb_restore import restore_schema, create_datapipeline, load_schema

#args = docopt(__doc__)
#print(args)
#table_name = args["--ddbtablename"]

table_definition = load_schema("/var/folders/lj/6l98gzws3mjcypf6j4p4w0zc0017ft/T/bla-schema.json", "bla")

print table_definition
#restore_schema(table_name)

#create_datapipeline(args["--pipeline-definition-uri"], args["--subnetId"], table_name, args["--backup-source"])