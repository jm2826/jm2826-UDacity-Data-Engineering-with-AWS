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

# Script generated for node Customer_Trusted_Source
Customer_Trusted_Source_node1691173174486 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="customer_trusted",
        transformation_ctx="Customer_Trusted_Source_node1691173174486",
    )
)

# Script generated for node Accelerometer_Landing_Bucket
Accelerometer_Landing_Bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="Accelerometer_Landing_Bucket_node1",
)

# Script generated for node Join Customer
JoinCustomer_node1691173051534 = Join.apply(
    frame1=Accelerometer_Landing_Bucket_node1,
    frame2=Customer_Trusted_Source_node1691173174486,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node1691173051534",
)

# Script generated for node Drop Fields
DropFields_node1691173553786 = DropFields.apply(
    frame=JoinCustomer_node1691173051534,
    paths=["z", "y", "x", "timestamp", "user"],
    transformation_ctx="DropFields_node1691173553786",
)

# Script generated for node Customer_curated
Customer_curated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1691173553786,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://joelm-lakehouse/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="Customer_curated_node3",
)

job.commit()
