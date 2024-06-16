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

# Script generated for node step_trainer_landing
step_trainer_landing_node1718546005232 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://aws-stedi-project/starter/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1718546005232")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1718559636153 = glueContext.create_dynamic_frame.from_catalog(database="aws-stedi-db", table_name="customer_curated", transformation_ctx="AWSGlueDataCatalog_node1718559636153")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1718546695036 = ApplyMapping.apply(frame=step_trainer_landing_node1718546005232, mappings=[("sensorreadingtime", "bigint", "sensorreadingtime", "bigint"), ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "int", "right_distancefromobject", "int")], transformation_ctx="RenamedkeysforJoin_node1718546695036")

# Script generated for node Join
Join_node1718546001318 = Join.apply(frame1=RenamedkeysforJoin_node1718546695036, frame2=AWSGlueDataCatalog_node1718559636153, keys1=["right_serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1718546001318")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1718546579104 = glueContext.getSink(path="s3://aws-stedi-project/starter/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1718546579104")
step_trainer_trusted_node1718546579104.setCatalogInfo(catalogDatabase="aws-stedi-db",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1718546579104.setFormat("json")
step_trainer_trusted_node1718546579104.writeFrame(Join_node1718546001318)
job.commit()