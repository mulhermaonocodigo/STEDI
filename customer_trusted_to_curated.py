import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer landing
accelerometerlanding_node1687143627445 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mmacedo-data-lakehouse/project3/accelerometer/landing"],
        "recurse": True,
    },
    transformation_ctx="accelerometerlanding_node1687143627445",
)

# Script generated for node customer trusted
customertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mmacedo-data-lakehouse/project3/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customertrusted_node1",
)

# Script generated for node Join
Join_node1687143702475 = Join.apply(
    frame1=accelerometerlanding_node1687143627445,
    frame2=customertrusted_node1,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1687143702475",
)

# Script generated for node Drop Fields
DropFields_node1687143708123 = DropFields.apply(
    frame=Join_node1687143702475,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1687143708123",
)

# Script generated for node customer curated
customercurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1687143708123,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://mmacedo-data-lakehouse/project3/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="customercurated_node3",
)

job.commit()
