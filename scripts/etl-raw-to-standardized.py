# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME","S3_RAW","S3_STANDARDIZED"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read files from S3_RAW bucket
AmazonS3_node1698414162559 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": [args['S3_RAW']], "recurse": True},
    transformation_ctx="AmazonS3_node1698414162559",
)

# Drop duplicates
DropDuplicates_node1698415268970 = DynamicFrame.fromDF(
    AmazonS3_node1698414162559.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1698415268970",
)

# Save the generated files to S3_STANDARDIZED bucket
AmazonS3_node1698415538470 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1698415268970,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": args['S3_STANDARDIZED'],
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1698415538470",
)

job.commit()
