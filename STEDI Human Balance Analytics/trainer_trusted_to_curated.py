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

# Script generated for node step_trainer_landing
step_trainer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://joelm-lakehouse/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1",
)

# Script generated for node Customer_trusted
Customer_trusted_node1691180922006 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="Customer_trusted_node1691180922006",
)

# Script generated for node Trusted_Customer_SerialNumber
Trusted_Customer_SerialNumber_node1691180951455 = Join.apply(
    frame1=Customer_trusted_node1691180922006,
    frame2=step_trainer_landing_node1,
    keys1=["serialnumber"],
    keys2=["serialNumber"],
    transformation_ctx="Trusted_Customer_SerialNumber_node1691180951455",
)

# Script generated for node Drop Fields
DropFields_node1691207079862 = DropFields.apply(
    frame=Trusted_Customer_SerialNumber_node1691180951455,
    paths=["email", "phone", "birthday", "serialNumber", "serialnumber"],
    transformation_ctx="DropFields_node1691207079862",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1691207079862,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://joelm-lakehouse/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trusted_node3",
)

job.commit()
