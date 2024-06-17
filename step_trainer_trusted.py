import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1718604566323 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://aws-stedi-project/starter/step_trainer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1718604566323")

# Script generated for node Amazon S3
AmazonS3_node1718603790228 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://aws-stedi-project/starter/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1718603790228")

# Script generated for node Amazon S3
AmazonS3_node1718541694491 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://aws-stedi-project/starter/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1718541694491")

# Script generated for node Rename Field
RenameField_node1718604636486 = RenameField.apply(frame=AmazonS3_node1718604566323, old_name="serialnumber", new_name="right_serialnumber", transformation_ctx="RenameField_node1718604636486")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource
where sharewithpublicasofdate IS NOT NULL;
'''
SQLQuery_node1718603993116 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AmazonS3_node1718603790228}, transformation_ctx = "SQLQuery_node1718603993116")

# Script generated for node Join
Join_node1718541869650 = Join.apply(frame1=AmazonS3_node1718541694491, frame2=SQLQuery_node1718603993116, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1718541869650")

# Script generated for node Drop Fields
DropFields_node1718604305828 = DropFields.apply(frame=Join_node1718541869650, paths=["y", "x", "z", "timestamp", "user"], transformation_ctx="DropFields_node1718604305828")

# Script generated for node Drop Duplicates
DropDuplicates_node1718604351316 =  DynamicFrame.fromDF(DropFields_node1718604305828.toDF().dropDuplicates(["email"]), glueContext, "DropDuplicates_node1718604351316")

# Script generated for node Join
Join_node1718604651440 = Join.apply(frame1=DropDuplicates_node1718604351316, frame2=RenameField_node1718604636486, keys1=["serialNumber"], keys2=["right_serialnumber"], transformation_ctx="Join_node1718604651440")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1718541917017 = glueContext.getSink(path="s3://aws-stedi-project/starter/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1718541917017")
step_trainer_trusted_node1718541917017.setCatalogInfo(catalogDatabase="aws-stedi-db",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1718541917017.setFormat("json")
step_trainer_trusted_node1718541917017.writeFrame(Join_node1718604651440)
job.commit()