import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1718544276550 = glueContext.create_dynamic_frame.from_catalog(database="aws-stedi-db", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1718544276550")

# Script generated for node Drop Fields
DropFields_node1718544334571 = DropFields.apply(frame=AWSGlueDataCatalog_node1718544276550, paths=["y", "z", "x", "user", "timestamp"], transformation_ctx="DropFields_node1718544334571")

# Script generated for node Drop Duplicates
DropDuplicates_node1718544366521 =  DynamicFrame.fromDF(DropFields_node1718544334571.toDF().dropDuplicates(["email"]), glueContext, "DropDuplicates_node1718544366521")

# Script generated for node customers_curated
customers_curated_node1718544379290 = glueContext.getSink(path="s3://aws-stedi-project/starter/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customers_curated_node1718544379290")
customers_curated_node1718544379290.setCatalogInfo(catalogDatabase="aws-stedi-db",catalogTableName="customer_curated")
customers_curated_node1718544379290.setFormat("glueparquet", compression="snappy")
customers_curated_node1718544379290.writeFrame(DropDuplicates_node1718544366521)
job.commit()