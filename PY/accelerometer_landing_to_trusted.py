import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1718541647110 = glueContext.create_dynamic_frame.from_catalog(database="aws-stedi-db", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1718541647110")

# Script generated for node Amazon S3
AmazonS3_node1718541694491 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://aws-stedi-project/starter/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1718541694491")

# Script generated for node Join
Join_node1718541869650 = Join.apply(frame1=AmazonS3_node1718541694491, frame2=AWSGlueDataCatalog_node1718541647110, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1718541869650")

# Script generated for node Amazon S3
AmazonS3_node1718541917017 = glueContext.getSink(path="s3://aws-stedi-project/starter/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1718541917017")
AmazonS3_node1718541917017.setCatalogInfo(catalogDatabase="aws-stedi-db",catalogTableName="accelerometer_trusted")
AmazonS3_node1718541917017.setFormat("json")
AmazonS3_node1718541917017.writeFrame(Join_node1718541869650)
job.commit()