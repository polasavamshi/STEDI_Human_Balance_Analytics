import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer_trusted
customer_trusted_node1767269995926 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1767269995926")

# Script generated for node accelerometer_landing
accelerometer_landing_node1767269998476 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelorometer_landing_table", transformation_ctx="accelerometer_landing_node1767269998476")

# Script generated for node Join
Join_node1767270002626 = Join.apply(frame1=accelerometer_landing_node1767269998476, frame2=customer_trusted_node1767269995926, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1767270002626")

# Script generated for node SQL Query
SqlQuery2448 = '''
select user, timestamp, x, y, z from myDataSource

'''
SQLQuery_node1767270011736 = sparkSqlQuery(glueContext, query = SqlQuery2448, mapping = {"myDataSource":Join_node1767270002626}, transformation_ctx = "SQLQuery_node1767270011736")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1767270011736, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1767269208978", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1767270015942 = glueContext.getSink(path="s3://sparkglueproject/Accelorometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1767270015942")
AmazonS3_node1767270015942.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1767270015942.setFormat("json")
AmazonS3_node1767270015942.writeFrame(SQLQuery_node1767270011736)
job.commit()