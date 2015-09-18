#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from docopt import docopt
from dynamodb_restore import restore_schema, create_datapipeline

"""
dynamodb-restore.py

Usage:
  dynamodb-restore.py --pipeline-definition-uri=pipeline_uri --subnetId=subnetId --ddbtablename=targetTableName --backup-source=s3_uri

Options:
  -h --help     Show this screen.
"""
if __name__ == "__main__":
    args = docopt(__doc__)
    print(args)
    table_name = args["--ddbtablename"]

    restore_schema(table_name)

    create_datapipeline(args["--pipeline-definition-uri"], args["--subnetId"], table_name, args["--backup-source"])