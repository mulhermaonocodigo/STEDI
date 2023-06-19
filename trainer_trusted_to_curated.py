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

# Script generated for node customer curated
customercurated_node1687143627445 = glueContext.create_dynamic_frame.from_catalog(
    database="mmacedo",
    table_name="customer_curated",
    transformation_ctx="customercurated_node1687143627445",
)

# Script generated for node step trainer landing
steptrainerlanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="mmacedo",
    table_name="step_trainer_landing",
    transformation_ctx="steptrainerlanding_node1",
)

# Script generated for node Join
Join_node1687143702475 = Join.apply(
    frame1=customercurated_node1687143627445,
    frame2=steptrainerlanding_node1,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1687143702475",
)

# Script generated for node Drop Fields
DropFields_node1687143708123 = DropFields.apply(
    frame=Join_node1687143702475,
    paths=[
        "serialnumber",
        "sharewithpublicasofdate",
        "birthday",
        "registrationdate",
        "sharewithresearchasofdate",
        "customername",
        "email",
        "lastupdatedate",
        "phone",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1687143708123",
)

# Script generated for node step trainer trusted
steptrainertrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1687143708123,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://mmacedo-data-lakehouse/project3/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="steptrainertrusted_node3",
)

job.commit()
