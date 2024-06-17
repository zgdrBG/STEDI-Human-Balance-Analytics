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
AmazonS3_node1718540621138 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://aws-stedi-project/starter/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1718540621138")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from Customers_landing
where sharewithpublicasofdate IS NOT NULL;
'''
SQLQuery_node1718606318207 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"Customers_landing":AmazonS3_node1718540621138}, transformation_ctx = "SQLQuery_node1718606318207")

# Script generated for node customer_trusted
customer_trusted_node1718540627524 = glueContext.getSink(path="s3://aws-stedi-project/starter/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1718540627524")
customer_trusted_node1718540627524.setCatalogInfo(catalogDatabase="aws-stedi-db",catalogTableName="customer_trusted")
customer_trusted_node1718540627524.setFormat("json")
customer_trusted_node1718540627524.writeFrame(SQLQuery_node1718606318207)
job.commit()