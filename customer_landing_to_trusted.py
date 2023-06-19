import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer landing
customerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mmacedo-data-lakehouse/project3/customer/landing"]
    },
    transformation_ctx="customerlanding_node1",
)

# Script generated for node privacy filter
privacyfilter_node1687142916009 = Filter.apply(
    frame=customerlanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="privacyfilter_node1687142916009",
)

# Script generated for node customer trusted
customertrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=privacyfilter_node1687142916009,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://mmacedo-data-lakehouse/project3/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="customertrusted_node3",
)

job.commit()
