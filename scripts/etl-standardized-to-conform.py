# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME","S3_STANDARDIZED", "S3_CONFORM"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read files from S3_STANDARDIZED
AmazonS3_node1698416561798 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": [args['S3_STANDARDIZED']], "recurse": True},
    transformation_ctx="AmazonS3_node1698416561798",
)

# Remove records with no passengers
Filter_node1698417242377 = Filter.apply(
    frame=AmazonS3_node1698416561798,
    f=lambda row: (row["passenger_count"] > 0),
    transformation_ctx="Filter_node1698417242377",
)

# Drop unwanted fields
DropFields_node1698417366162 = DropFields.apply(
    frame=Filter_node1698417242377,
    paths=["RatecodeID", "payment_type"],
    transformation_ctx="DropFields_node1698417366162",
)

# Save files to S3_CONFORM bucket
AmazonS3_node1698417454235 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1698417366162,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": args['S3_CONFORM'], "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1698417454235",
)

job.commit()
