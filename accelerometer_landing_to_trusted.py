import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

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
AmazonS3_node1718541694491 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://aws-stedi-project/starter/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1718541694491")

# Script generated for node Amazon S3
AmazonS3_node1718603790228 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://aws-stedi-project/starter/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1718603790228")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource
where sharewithpublicasofdate IS NOT NULL;
'''
SQLQuery_node1718603993116 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AmazonS3_node1718603790228}, transformation_ctx = "SQLQuery_node1718603993116")

# Script generated for node Join
Join_node1718541869650 = Join.apply(frame1=AmazonS3_node1718541694491, frame2=SQLQuery_node1718603993116, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1718541869650")

# Script generated for node Amazon S3
AmazonS3_node1718541917017 = glueContext.getSink(path="s3://aws-stedi-project/starter/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1718541917017")
AmazonS3_node1718541917017.setCatalogInfo(catalogDatabase="aws-stedi-db",catalogTableName="accelerometer_trusted")
AmazonS3_node1718541917017.setFormat("json")
AmazonS3_node1718541917017.writeFrame(Join_node1718541869650)
job.commit()