import sys
from awsglue.transforms import Filter
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

# Script generated for node Amazon S3
AmazonS3_node1718540621138 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://aws-stedi-project/starter/customer/landing/"], "recurse": True},
    transformation_ctx="AmazonS3_node1718540621138"
)

# Applying filter transformation
ApplyMapping_node2 = Filter.apply(
    frame=AmazonS3_node1718540621138,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="ApplyMapping_node2"
)

# Script generated for node customer_trusted
customer_trusted_node1718540627524 = glueContext.getSink(
    path="s3://aws-stedi-project/starter/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_node1718540627524"
)
customer_trusted_node1718540627524.setCatalogInfo(
    catalogDatabase="aws-stedi-db",
    catalogTableName="customer_trusted"
)
customer_trusted_node1718540627524.setFormat("json")
customer_trusted_node1718540627524.writeFrame(ApplyMapping_node2)
job.commit()