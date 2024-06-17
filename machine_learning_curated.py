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

# Script generated for node step_trainer
step_trainer_node1718604566323 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://aws-stedi-project/starter/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_node1718604566323")

# Script generated for node customer_landing
customer_landing_node1718603790228 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://aws-stedi-project/starter/customer/landing/"], "recurse": True}, transformation_ctx="customer_landing_node1718603790228")

# Script generated for node accelerometer_landing
accelerometer_landing_node1718541694491 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://aws-stedi-project/starter/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1718541694491")

# Script generated for node SQL Query
SqlQuery2 = '''
select * from myDataSource
where sharewithpublicasofdate IS NOT NULL;
'''
SQLQuery_node1718603993116 = sparkSqlQuery(glueContext, query = SqlQuery2, mapping = {"myDataSource":customer_landing_node1718603790228}, transformation_ctx = "SQLQuery_node1718603993116")

# Script generated for node SQL Join
SqlQuery0 = '''
SELECT *
FROM customer_landing c
INNER JOIN accelerometer_landing a
    ON c.email = a.user
'''
SQLJoin_node1718611659533 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_landing":SQLQuery_node1718603993116, "accelerometer_landing":accelerometer_landing_node1718541694491}, transformation_ctx = "SQLJoin_node1718611659533")

# Script generated for node customer_accelerometer
customer_accelerometer_node1718604351316 =  DynamicFrame.fromDF(SQLJoin_node1718611659533.toDF().dropDuplicates(["email"]), glueContext, "customer_accelerometer_node1718604351316")

# Script generated for node SQL Query
SqlQuery1 = '''
SELECT *
FROM customer_accelerometer ca
INNER JOIN step_trainer s
    ON ca.serialnumber = s.serialnumber
    AND ca.timestamp = s.sensorreadingtime
'''
SQLQuery_node1718612573258 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"step_trainer":step_trainer_node1718604566323, "customer_accelerometer":customer_accelerometer_node1718604351316}, transformation_ctx = "SQLQuery_node1718612573258")

# Script generated for node machine_learning_curated
machine_learning_curated_node1718541917017 = glueContext.getSink(path="s3://aws-stedi-project/starter/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1718541917017")
machine_learning_curated_node1718541917017.setCatalogInfo(catalogDatabase="aws-stedi-db",catalogTableName="machine_learning_curated")
machine_learning_curated_node1718541917017.setFormat("json")
machine_learning_curated_node1718541917017.writeFrame(SQLQuery_node1718612573258)
job.commit()